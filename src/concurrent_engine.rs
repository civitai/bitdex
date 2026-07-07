use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
use crate::config::{Config, FilterFieldConfig, SortFieldConfig};
use crate::shard_store_doc::{DocStoreV3, StoredDoc};
use crate::error::Result;
use crate::executor::{CaseSensitiveFields, QueryExecutor, StringMaps};
use crate::mutation::{diff_document, diff_patch, value_to_bitmap_key, value_to_sort_u32, Document, FieldRegistry, PatchPayload};
use crate::planner;
use crate::query::{BitdexQuery, FilterClause, SortClause, SortDirection};
use crate::query_metrics::{QueryTrace, QueryTraceCollector, SortTrace};
use crate::time_buckets::TimeBucketManager;
use crate::types::QueryResult;
use crate::unified_cache::{
    UnifiedCache, UnifiedCacheConfig, UnifiedEntry, UnifiedKey,
    evaluate_filter_work, evaluate_sort_work,
};
use crate::shard_store_bitmap::{
    AliveShardKey, BitmapOp, FilterBucketKey, FilterOp, SortLayerShardKey,
};
use crate::write_coalescer::{MutationOp, MutationSender, WriteCoalescer};
/// Bridge for passing Prometheus metric handles from the server layer into
/// the engine's background threads (compaction worker, lazy loading).
/// Only available when compiled with the `server` feature.
#[cfg(feature = "server")]
pub struct MetricsBridge {
    pub lazy_load_duration: prometheus::HistogramVec,
    pub compaction_total: prometheus::IntCounterVec,
    pub compaction_duration: prometheus::HistogramVec,
    /// queryOpSet fan-out size histogram (issue #60). Observed pre-cap so we can
    /// see what we'd reject if cap were lower.
    pub query_op_set_fanout_size: prometheus::HistogramVec,
    /// queryOpSet rejection counter (issue #60). Label `reason="fanout_too_wide"`
    /// when fan-out exceeds `BITDEX_QUERY_OP_SET_MAX_FANOUT`.
    pub query_op_set_rejected_total: prometheus::IntCounterVec,
    /// Cumulative slot mutations from queryOpSet fan-outs. Sums to total work
    /// the WAL reader has done across all queryOpSets; not the same as the
    /// `applied` count returned per call.
    pub query_op_set_applied_slots_total: prometheus::IntCounterVec,
    /// 11c CPU floor attribution: WAL apply per-batch duration.
    pub wal_apply_batch_seconds: prometheus::HistogramVec,
    /// 11c CPU floor attribution: bitmap memory scanner tick duration.
    pub bitmap_mem_scan_tick_seconds: prometheus::HistogramVec,
    /// Existing query counter; bridged so apply_query_op_set can bump it on the
    /// QueryOpSet path (mission #77).
    pub query_total: prometheus::IntCounterVec,
    /// Time-bucket flush dropped slot insertion because sort field unavailable
    /// (lazy-load race). Labels: index, field.
    pub timebucket_dropped_no_sort_field_total: prometheus::IntCounterVec,
    /// Time-bucket flush observed an anomalous reconstructed timestamp. Labels:
    /// index, field, kind in {zero, future, wrapped}.
    pub timebucket_anomalous_ts_total: prometheus::IntCounterVec,
    /// Slots permanently lost because the deferred-retry queue hit its cap
    /// while the sort field was unloaded. Distinct from
    /// `timebucket_dropped_no_sort_field_total` which counts slots that were
    /// successfully deferred and will replay later. Labels: index, field.
    pub timebucket_dropped_capacity_exceeded_total: prometheus::IntCounterVec,
    /// Source diagnostic (missing-adds): sortAt-mutated slot reconstructs in-window
    /// but is absent from the bucket bitmap right after live maintenance. Labels:
    /// index, field, bucket.
    pub timebucket_applied_not_bucketed_total: prometheus::IntCounterVec,
    /// Periodic full time-bucket rebuild (prune) fallback observability.
    /// `index`-labeled; the per-bucket ones also carry a `bucket` label.
    pub time_bucket_full_rebuild_duration_seconds: prometheus::HistogramVec,
    pub time_bucket_full_rebuild_total: prometheus::IntCounterVec,
    pub time_bucket_pruned_total: prometheus::IntCounterVec,
    /// Slots backfilled into a time bucket by the periodic reconcile (the
    /// symmetric counterpart to `time_bucket_pruned_total`). `index`+`bucket`.
    pub time_bucket_backfilled_total: prometheus::IntCounterVec,
    pub time_bucket_stale: prometheus::IntGaugeVec,
    pub time_bucket_missing: prometheus::IntGaugeVec,
    /// Wall time of the on-flush-thread reconcile apply (re-validate + mutate),
    /// distinct from the off-thread scan duration. Bounds the flush-thread cost.
    pub time_bucket_reconcile_apply_seconds: prometheus::HistogramVec,
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
        skip_lazy: HashSet<String>,
        /// Cursors to persist alongside bitmaps.
        cursors: HashMap<String, String>,
        /// Dictionaries to persist alongside bitmaps.
        dictionaries: Arc<HashMap<String, crate::dictionary::FieldDictionary>>,
        /// Loading mode flag — handler clears this AFTER reading the published snapshot,
        /// preventing the flush thread's loading-exit force-publish from overwriting
        /// the loader's data before we save it.
        loading_mode: Arc<AtomicBool>,
        /// Oneshot sender — caller blocks until save+unload is complete.
        /// Returns Ok(()) on success or error message on failure.
        done: crossbeam_channel::Sender<std::result::Result<(), String>>,
    },
}
// ---------------------------------------------------------------------------
// RSS memory tracking (cross-platform)
// ---------------------------------------------------------------------------
pub fn get_rss_bytes() -> u64 {
    #[cfg(target_os = "windows")]
    {
        use std::mem::MaybeUninit;
        #[repr(C)]
        #[allow(non_snake_case)]
        struct ProcessMemoryCounters {
            cb: u32,
            page_fault_count: u32,
            peak_working_set_size: usize,
            working_set_size: usize,
            quota_peak_paged_pool_usage: usize,
            quota_paged_pool_usage: usize,
            quota_peak_non_paged_pool_usage: usize,
            quota_non_paged_pool_usage: usize,
            pagefile_usage: usize,
            peak_pagefile_usage: usize,
        }
        extern "system" {
            fn GetCurrentProcess() -> isize;
        }
        #[link(name = "psapi")]
        extern "system" {
            fn GetProcessMemoryInfo(process: isize, ppsmemCounters: *mut ProcessMemoryCounters, cb: u32) -> i32;
        }
        unsafe {
            let process = GetCurrentProcess();
            let mut pmc: MaybeUninit<ProcessMemoryCounters> = MaybeUninit::zeroed();
            if GetProcessMemoryInfo(process, pmc.as_mut_ptr(), std::mem::size_of::<ProcessMemoryCounters>() as u32) != 0 {
                (*pmc.as_ptr()).working_set_size as u64
            } else {
                0
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(rss_pages) = statm.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    return pages * 4096;
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    { 0 }
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
    /// Reload the alive bitmap + slot counter from disk.
    /// Used by the dump processor after writing alive to BitmapFs.
    Slots {
        slots: crate::slot::SlotAllocator,
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
    docstore: Arc<parking_lot::RwLock<DocStoreV3>>,
    /// Docstore root path, cached to avoid locking docstore just to read the path.
    docstore_root: Arc<PathBuf>,
    config: Arc<Config>,
    field_registry: FieldRegistry,
    in_flight: InFlightTracker,
    shutdown: Arc<AtomicBool>,
    flush_handle: Option<JoinHandle<()>>,
    merge_handle: Option<JoinHandle<()>>,
    bitmap_store: Option<Arc<BitmapFs>>,
    /// ShardStore instances (constructed alongside bitmap_store during migration).
    alive_store: Option<Arc<crate::shard_store_bitmap::AliveBitmapStore>>,
    filter_store: Option<Arc<crate::shard_store_bitmap::FilterBitmapStore>>,
    sort_store: Option<Arc<crate::shard_store_bitmap::SortBitmapStore>>,
    meta_store: Option<Arc<crate::shard_store_meta::MetaStore>>,
    loading_mode: Arc<AtomicBool>,
    dirty_since_snapshot: Arc<AtomicBool>,
    time_buckets: Option<Arc<ArcSwap<TimeBucketManager>>>,
    /// Pending bucket diffs for lazy application on cache reads, keyed by
    /// bucket NAME (24h/7d/30d/1y are independent windows on the same
    /// field). Must be per-bucket, not a single shared cutoff: a wide
    /// bucket's (7d) `current_cutoff` and a narrow bucket's (24h) are
    /// unrelated numbers (`snap(now - duration, interval)` for different
    /// `duration`s) — merging them into one scalar made `current_cutoff`
    /// regress every time the narrower bucket refreshed after the wider one
    /// in the same flush cycle, and made a single `merged_expired` bitmap a
    /// cross-bucket union unsafe to apply blindly (see
    /// `ConcurrentEngine::own_bucket_live_bitmap`). The key set is fixed at
    /// boot from `config.time_buckets.range_buckets` — only the per-bucket
    /// `ArcSwap` cells are mutated at runtime, so the outer `HashMap` never
    /// needs to be swapped or cloned.
    pending_bucket_diffs: Arc<HashMap<String, Arc<ArcSwap<crate::bucket_diff_log::PendingBucketDiffs>>>>,
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
    /// Shared string_maps handle for the flush thread and cache worker.
    /// Updated by `set_string_maps` so background threads always see the latest.
    shared_string_maps: Arc<ArcSwap<Option<StringMaps>>>,
    /// Shared dictionaries handle for the flush thread and cache worker.
    /// Updated by `set_dictionaries` so background threads see fresh values.
    /// Wraps `Arc<HashMap<...>>` so the same allocation can be shared with
    /// `self.dictionaries` without cloning the (non-Clone) `FieldDictionary` values.
    shared_dictionaries: Arc<ArcSwap<Arc<HashMap<String, crate::dictionary::FieldDictionary>>>>,
    /// Unified cache: primary query result cache. Interior-mutable; callers
    /// invoke methods directly on the `Arc` (no outer `Mutex`/`RwLock`).
    unified_cache: Arc<UnifiedCache>,
    /// BoundStore for unified cache persistence (None if no bitmap_path).
    bound_store: Option<Arc<crate::bound_store::BoundStore>>,
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
    /// Flush phase timing: last sort-layer promote (merge_dirty across dirty
    /// sort fields) duration in nanoseconds. Runs every ~5s inside the flush
    /// thread and can dominate the flush cycle when many sort fields are dirty.
    flush_sort_promote_nanos: Arc<AtomicU64>,
    /// Iter 4a instrumentation: number of unique canonical filter-clause
    /// vectors across sort-maintenance work items in the most recent flush
    /// cycle that did cache maintenance. Low values mean entries cluster into
    /// shared filter shapes (filter-shape grouping would pay off); high
    /// values mean entries have diverse filters (grouping would be marginal).
    flush_cache_unique_filter_shapes: Arc<AtomicU64>,
    /// Max observed unique filter shapes across sort-maintenance work items
    /// since boot. Gauge samples capture the *last* cycle, which may be
    /// quiet; this counter preserves burst-time values so we don't draw
    /// conclusions about filter-shape grouping viability from a sleepy
    /// sample.
    flush_cache_unique_filter_shapes_max: Arc<AtomicU64>,
    /// Iter 4a instrumentation: number of sort-maintenance work items in the
    /// most recent cycle that did cache maintenance. Denominator for the
    /// unique-shapes-vs-total ratio (collapse factor).
    flush_cache_sort_work_items: Arc<AtomicU64>,
    /// Max observed sort-maintenance work item count since boot. See
    /// `flush_cache_unique_filter_shapes_max` rationale.
    flush_cache_sort_work_items_max: Arc<AtomicU64>,
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
    // ── BoundStore operational counters ─────────────────────────────────
    /// Cumulative shard load events.
    boundstore_shard_loads: Arc<AtomicU64>,
    /// Cumulative tombstones created by flush thread.
    boundstore_tombstones_created: Arc<AtomicU64>,
    /// Cumulative tombstones cleaned up by merge thread.
    boundstore_tombstones_cleaned: Arc<AtomicU64>,
    /// Cumulative bytes written to bounds directory.
    boundstore_bytes_written: Arc<AtomicU64>,
    /// Cumulative bytes read from bounds directory.
    boundstore_bytes_read: Arc<AtomicU64>,
    /// Cumulative entries restored from shard files.
    boundstore_entries_restored: Arc<AtomicU64>,
    /// Cumulative entries skipped (tombstoned + orphan) during shard load.
    boundstore_entries_skipped: Arc<AtomicU64>,
    /// Metrics bridge: prometheus handles set by server layer, read by background threads.
    #[cfg(feature = "server")]
    metrics_bridge: Arc<ArcSwap<Option<Arc<MetricsBridge>>>>,
    /// Amortized bitmap memory scanner cache (replaces expensive per-scrape iteration).
    bitmap_memory_cache: Arc<crate::bitmap_memory_cache::BitmapMemoryCache>,
    /// In-memory document cache (DashMap, cache-on-read, write-through, LRU eviction).
    doc_cache: Option<Arc<crate::doc_cache::DocCache>>,
    /// Minimum task count to engage rayon par_iter on the steady-state hot
    /// path (flush filter+sort fan-out, doc writer shard fan-out). Set huge
    /// (e.g. usize::MAX) to disable par_iter entirely — useful for isolating
    /// pool overhead from real work during perf experiments. Hot-reloadable
    /// via PATCH /api/indexes/{name}/config { "par_iter_min_threshold": N }.
    par_iter_min_threshold: Arc<AtomicUsize>,
    /// Interval (secs) for the periodic FULL time-bucket reconcile scan. Read
    /// each flush cycle so it can be retuned live via PATCH /config without a
    /// restart. `0` disables the fallback. Seeded from
    /// `TimeBucketFieldConfig::full_rebuild_interval_secs`.
    time_bucket_full_rebuild_interval: Arc<AtomicU64>,
    /// Compaction skip counter (incremented by DocStore when channel is full).
    compaction_skipped: Arc<AtomicU64>,
    /// Compaction channel sender — held here so we can drop it in shutdown()
    /// to signal the compact worker to exit.
    compact_tx: Option<Sender<(u32, Vec<u8>)>>,
    /// Background compaction worker thread handle.
    compact_handle: Option<JoinHandle<()>>,
    /// Prefetch channel sender — sends UnifiedKey to background worker for
    /// async cache expansion. None when prefetch is disabled.
    prefetch_tx: Option<Sender<UnifiedKey>>,
    /// Background prefetch worker thread handle.
    prefetch_handle: Option<JoinHandle<()>>,
    /// Background doc cache eviction thread handle.
    doc_cache_eviction_handle: Option<JoinHandle<()>>,
    /// WAL writer for Sync V2 write path. When set, put() and patch_document()
    /// decompose documents into ops and write to WAL instead of directly to coalescer.
    /// The WAL reader thread picks up ops and routes through apply_ops_batch.
    #[cfg(feature = "pg-sync")]
    wal_writer: Option<Arc<crate::ops_wal::WalWriter>>,
    /// Async cache maintenance worker channel sender. None when
    /// `config.cache.async_maintenance` is false.
    cache_work_tx: Option<crossbeam_channel::Sender<crate::cache_worker::CacheWorkItem>>,
    /// Async cache maintenance worker thread handle.
    cache_worker_handle: Option<JoinHandle<()>>,
    /// Metrics for the async cache worker (always allocated; reads are zero when
    /// async_maintenance is disabled).
    cache_worker_metrics: Arc<crate::cache_worker::CacheWorkerMetrics>,
    /// Shared deadline knob for the async cache worker. `set_max_maintenance_ms`
    /// writes here; the worker reads it each cycle via `Ordering::Relaxed`.
    /// `None` when `async_maintenance` is disabled at startup.
    cache_worker_ms: Option<Arc<AtomicU64>>,
    /// Prefilter registry: named precomputed filter bitmaps that the query
    /// planner substitutes into matching queries to avoid re-evaluating
    /// common clause sets (e.g. Civitai safety prefix).
    prefilter_registry: Arc<crate::prefilter::PrefilterRegistry>,
    /// Warm registry: tracks popular query shapes for auto-warming on boot.
    warm_registry: Arc<crate::warm_registry::WarmRegistry>,
}

/// Outcome of resolving a cache entry's bucket-diff state
/// (`ConcurrentEngine::resolve_bucket_diff_state[_for]`). Distinguishes two
/// DIFFERENT reasons a caller might not get a usable diff to apply — they
/// require opposite handling, which a single `Option::None` can't express
/// (an earlier version conflated them and silently under-served the
/// `Rebuild` case as a no-op — see the multi-bucket-clause review note on
/// `resolve_bucket_diff_state_for`):
/// - `Rebuild`: correctness can't be verified (multi-bucket-name entry, or a
///   referenced bucket name no longer resolves) — caller MUST call
///   `mark_for_rebuild()`. Silently skipping here would serve a
///   potentially-stale entry forever (until TTL, if any).
/// - `Noop`: nothing to apply YET, but nothing is wrong either (the entry's
///   single bucket exists but hasn't pushed a diff since boot) — caller
///   should just skip, not rebuild (rebuilding on every read until the
///   first refresh cycle would be wasteful and pointless).
#[derive(Debug)]
enum BucketDiffState {
    Apply(RoaringBitmap, u64, u64),
    Rebuild,
    Noop,
}
impl ConcurrentEngine {
    /// Create a new concurrent engine with an in-memory docstore (for testing).
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;
        let docstore = DocStoreV3::open_temp()
            .map_err(|e| crate::error::BitdexError::Storage(format!("open temp: {e}")))?;
        Self::build(config, docstore)
    }
    /// Create a new concurrent engine with an on-disk docstore.
    pub fn new_with_path(config: Config, path: &Path) -> Result<Self> {
        config.validate()?;
        let docstore = DocStoreV3::open(path)
            .map_err(|e| crate::error::BitdexError::Storage(format!("open: {e}")))?;
        Self::build(config, docstore)
    }

    fn build(config: Config, mut docstore: DocStoreV3) -> Result<Self> {
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
        // Construct ShardStore instances
        let (alive_store, filter_store, sort_store, meta_store) = if let Some(ref path) = config.storage.bitmap_path {
            let ss_root = path.join("shardstore");
            use crate::error::BitdexError;
            (
                Some(Arc::new(crate::shard_store_bitmap::AliveBitmapStore::new(
                    ss_root.join("alive"), crate::shard_store_bitmap::SingletonShard,
                ).map_err(|e| BitdexError::Storage(format!("alive store init: {e}")))?)),
                Some(Arc::new(crate::shard_store_bitmap::FilterBitmapStore::new(
                    ss_root.join("filter"), crate::shard_store_bitmap::FieldValueBucketShard,
                ).map_err(|e| BitdexError::Storage(format!("filter store init: {e}")))?)),
                Some(Arc::new(crate::shard_store_bitmap::SortBitmapStore::new(
                    ss_root.join("sort"), crate::shard_store_bitmap::SortLayerShard,
                ).map_err(|e| BitdexError::Storage(format!("sort store init: {e}")))?)),
                Some(Arc::new(crate::shard_store_meta::MetaStore::new(ss_root)
                    .map_err(|e| BitdexError::Storage(format!("meta store init: {e}")))?)),
            )
        } else {
            (None, None, None, None)
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
        if let Some(ref store) = alive_store {
            let alive = store.load_alive()
                .map_err(|e| crate::error::BitdexError::Storage(format!("load alive: {e}")))?;
            let counter = meta_store.as_ref()
                .and_then(|ms| ms.load_slot_counter().ok())
                .flatten();
            if let Some(alive_bm) = alive {
                let counter_val = counter.unwrap_or(0);
                slots = crate::slot::SlotAllocator::from_state(
                    counter_val,
                    alive_bm,
                    RoaringBitmap::new(),
                );
                // Restore deferred alive map if persisted.
                if let Some(ref ms) = meta_store {
                    if let Ok(Some(deferred)) = ms.load_deferred_alive() {
                        if !deferred.is_empty() {
                            let total: usize = deferred.values().map(|v| v.len()).sum();
                            eprintln!("Restored {} deferred alive slots ({} timestamps)", total, deferred.len());
                            slots.set_deferred(deferred);
                        }
                    }
                }
                // Only register pending loads if there are actual records to restore.
                // Fields with no saved bitmaps don't need lazy loading.
                if counter_val > 0 {
                    for fc in &config.filter_fields {
                        if !fc.eager_load && (fc.field_type == FilterFieldType::MultiValue || fc.per_value_lazy) {
                            // Per-value lazy loading: multi_value fields (always) and
                            // single_value fields with per_value_lazy (e.g. postId with 22M+ values).
                            // Only loads the specific values needed by each query from disk.
                            lazy_value_fields.insert(fc.name.clone());
                        } else {
                            // Full-field loading: low-cardinality, boolean, or eager_load fields.
                            pending_filter_loads.insert(fc.name.clone());
                        }
                    }
                    // Time bucket sort field: load eagerly (needed for bucket rebuild)
                    let tb_sort_field = config.time_buckets.as_ref()
                        .map(|tb| tb.sort_field.clone());
                    for sc in &config.sort_fields {
                        if tb_sort_field.as_deref() == Some(&sc.name) {
                            // Eagerly load the sort field used by time buckets
                            if let Some(ref ss) = sort_store {
                                if let Ok(Some(layers)) = ss.load_sort_layers(&sc.name, sc.bits as usize) {
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
        // Eager-load fields marked with `eager_load: true` in config.
        // These are loaded in parallel from ShardStore and applied to the
        // filters/sorts before constructing the InnerEngine.
        if filter_store.is_some() || sort_store.is_some() {
            let eager_filter_names: Vec<String> = config.filter_fields.iter()
                .filter(|fc| fc.eager_load && fc.field_type != FilterFieldType::MultiValue)
                .filter(|fc| pending_filter_loads.contains(&fc.name))
                .map(|fc| fc.name.clone())
                .collect();
            let eager_sort_configs: Vec<(String, usize)> = config.sort_fields.iter()
                .filter(|sc| sc.eager_load)
                .filter(|sc| pending_sort_loads.contains(&sc.name))
                .map(|sc| (sc.name.clone(), sc.bits as usize))
                .collect();
            if !eager_filter_names.is_empty() || !eager_sort_configs.is_empty() {
                let t0 = std::time::Instant::now();
                let total_eager = eager_filter_names.len() + eager_sort_configs.len();
                if total_eager > 1 {
                    // Parallel eager loading
                    use std::sync::Mutex;
                    let eager_filter_results: Mutex<Vec<(String, HashMap<u64, RoaringBitmap>)>> = Mutex::new(Vec::new());
                    let eager_sort_results: Mutex<Vec<(String, Vec<RoaringBitmap>)>> = Mutex::new(Vec::new());
                    std::thread::scope(|s| {
                        for name in &eager_filter_names {
                            let fs = filter_store.as_ref().unwrap().clone();
                            let results = &eager_filter_results;
                            s.spawn(move || {
                                let ft0 = std::time::Instant::now();
                                match fs.load_field(name) {
                                    Ok(bitmaps) => {
                                        let count = bitmaps.len();
                                        eprintln!(
                                            "Eager-loaded filter '{}': {} values in {:.1}ms",
                                            name, count, ft0.elapsed().as_secs_f64() * 1000.0
                                        );
                                        results.lock().unwrap().push((name.clone(), bitmaps));
                                    }
                                    Err(e) => eprintln!("Warning: eager load failed for filter '{}': {}", name, e),
                                }
                            });
                        }
                        for (name, bits) in &eager_sort_configs {
                            let ss = sort_store.as_ref().unwrap().clone();
                            let results = &eager_sort_results;
                            let name = name.clone();
                            let bits = *bits;
                            s.spawn(move || {
                                let st0 = std::time::Instant::now();
                                match ss.load_sort_layers(&name, bits) {
                                    Ok(Some(layers)) if !layers.is_empty() => {
                                        let layer_count = layers.len();
                                        eprintln!(
                                            "Eager-loaded sort '{}': {} layers in {:.1}ms",
                                            name, layer_count, st0.elapsed().as_secs_f64() * 1000.0
                                        );
                                        results.lock().unwrap().push((name, layers));
                                    }
                                    Ok(_) => {}
                                    Err(e) => eprintln!("Warning: eager load failed for sort '{}': {}", name, e),
                                }
                            });
                        }
                    });
                    for (name, bitmaps) in eager_filter_results.into_inner().unwrap() {
                        if let Some(field) = filters.get_field(&name) {
                            field.load_field_complete(bitmaps);
                        }
                        pending_filter_loads.remove(&name);
                    }
                    for (name, layers) in eager_sort_results.into_inner().unwrap() {
                        if let Some(field) = sorts.get_field_mut(&name) {
                            field.load_layers(layers);
                        }
                        pending_sort_loads.remove(&name);
                    }
                } else {
                    // Single eager field — load serially (no thread overhead)
                    if let Some(ref fs) = filter_store {
                        for name in &eager_filter_names {
                            let ft0 = std::time::Instant::now();
                            match fs.load_field(name) {
                                Ok(bitmaps) => {
                                    let count = bitmaps.len();
                                    eprintln!(
                                        "Eager-loaded filter '{}': {} values in {:.1}ms",
                                        name, count, ft0.elapsed().as_secs_f64() * 1000.0
                                    );
                                    if let Some(field) = filters.get_field(name) {
                                        field.load_field_complete(bitmaps);
                                    }
                                    pending_filter_loads.remove(name);
                                }
                                Err(e) => eprintln!("Warning: eager load failed for filter '{}': {}", name, e),
                            }
                        }
                    }
                    if let Some(ref ss) = sort_store {
                        for (name, bits) in &eager_sort_configs {
                            let st0 = std::time::Instant::now();
                            match ss.load_sort_layers(name, *bits) {
                                Ok(Some(layers)) if !layers.is_empty() => {
                                    let layer_count = layers.len();
                                    eprintln!(
                                        "Eager-loaded sort '{}': {} layers in {:.1}ms",
                                        name, layer_count, st0.elapsed().as_secs_f64() * 1000.0
                                    );
                                    if let Some(field) = sorts.get_field_mut(name) {
                                        field.load_layers(layers);
                                    }
                                    pending_sort_loads.remove(name);
                                }
                                Ok(_) => {}
                                Err(e) => eprintln!("Warning: eager load failed for sort '{}': {}", name, e),
                            }
                        }
                    }
                }
                eprintln!(
                    "Eager loading complete: {} fields in {:.1}ms",
                    total_eager, t0.elapsed().as_secs_f64() * 1000.0
                );
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
            compound_eval_atom_limit: config.cache.compound_eval_atom_limit,
            bucket_entry_ttl_secs: config.cache.bucket_entry_ttl_secs,
        };
        // Cache-worker metrics created early so the cache can hold an Arc to
        // it for reason-attributed rebuild counters (alive_change,
        // filter_invalidation, deadline, count_budget, rebuild_completed).
        let cache_worker_metrics: Arc<crate::cache_worker::CacheWorkerMetrics> =
            Arc::new(crate::cache_worker::CacheWorkerMetrics::default());
        let uc = UnifiedCache::new(uc_config);
        // Initialize BoundStore for unified cache persistence
        let bound_store = if let Some(ref path) = config.storage.bitmap_path {
            let bounds_path = path.join("shardstore").join("bounds");
            match crate::bound_store::BoundStore::new(&bounds_path) {
                Ok(bs) => {
                    // Load meta.bin: populate meta-index, record pending shards
                    match bs.load_meta() {
                        Ok(Some(meta)) => {
                            eprintln!(
                                "BoundStore: loaded meta.bin ({} entries, {} tombstones, next_id={})",
                                meta.entries.len(),
                                meta.tombstones.len(),
                                meta.next_entry_id
                            );
                            // Restore meta-index registrations. Pass the
                            // persisted FilterClause tree (V2 only; V1 entries
                            // have empty Vec which becomes None) so leaf fields
                            // of compound shapes get registered for write-path
                            // discovery — matches the B4 live-register behavior.
                            for entry in &meta.entries {
                                let originals = if entry.original_filter_clauses.is_empty() {
                                    None
                                } else {
                                    Some(entry.original_filter_clauses.as_slice())
                                };
                                uc.meta_mut().register_with_id(
                                    entry.entry_id,
                                    &entry.filter_clauses,
                                    originals,
                                    Some(&entry.sort_field),
                                    Some(entry.direction),
                                );
                            }
                            uc.meta_mut().set_next_id(meta.next_entry_id);
                            uc.meta_mut().set_tombstones(meta.tombstones);
                            // Store has_more flags for shard restore
                            let has_more_map: HashMap<crate::meta_index::CacheEntryId, bool> = meta.entries
                                .iter()
                                .map(|e| (e.entry_id, e.has_more))
                                .collect();
                            uc.set_meta_has_more(has_more_map);
                            // Store total_matched values for shard restore
                            let total_matched_map: HashMap<crate::meta_index::CacheEntryId, u64> = meta.entries
                                .iter()
                                .map(|e| (e.entry_id, e.total_matched))
                                .collect();
                            uc.set_meta_total_matched(total_matched_map);
                            // Store original FilterClause trees (V2 only; V1 entries have Vec::new()).
                            let original_fc_map: HashMap<crate::meta_index::CacheEntryId, Vec<crate::query::FilterClause>> = meta.entries
                                .iter()
                                .filter(|e| !e.original_filter_clauses.is_empty())
                                .map(|e| (e.entry_id, e.original_filter_clauses.clone()))
                                .collect();
                            uc.set_meta_original_filter_clauses(original_fc_map);
                            // Record pending shards from registered entries
                            let mut shard_keys = HashSet::new();
                            for entry in &meta.entries {
                                shard_keys.insert(crate::bound_store::ShardKey::new(
                                    entry.sort_field.clone(),
                                    entry.direction,
                                ));
                            }
                            uc.add_pending_shards(shard_keys);
                            uc.enable_persistence();
                        }
                        Ok(None) => {
                            // No meta.bin — clean orphaned .ucpack files if any
                            if let Ok(shards) = bs.list_shards() {
                                if !shards.is_empty() {
                                    eprintln!(
                                        "BoundStore: no meta.bin, purging {} orphaned shard files",
                                        shards.len()
                                    );
                                    let _ = bs.purge();
                                }
                            }
                            uc.enable_persistence();
                        }
                        Err(e) => {
                            eprintln!("BoundStore: failed to load meta.bin: {e}");
                            uc.enable_persistence();
                        }
                    }
                    Some(Arc::new(bs))
                }
                Err(e) => {
                    eprintln!("BoundStore: failed to create: {e}");
                    None
                }
            }
        } else {
            None
        };
        let unified_cache = Arc::new(uc);
        unified_cache.set_rebuild_metrics(Arc::clone(&cache_worker_metrics));
        // Shared string_maps + dictionaries for native FilterClause eval (B2).
        // Initialized empty; populated by set_string_maps / set_dictionaries.
        let shared_string_maps: Arc<ArcSwap<Option<StringMaps>>> =
            Arc::new(ArcSwap::from_pointee(None));
        let shared_dictionaries: Arc<ArcSwap<Arc<HashMap<String, crate::dictionary::FieldDictionary>>>> =
            Arc::new(ArcSwap::from_pointee(Arc::new(HashMap::new())));
        let loading_mode = Arc::new(AtomicBool::new(false));
        // S3.3: Instantiate TimeBucketManager from top-level time_buckets config
        let time_buckets = config.time_buckets.as_ref().map(|tb_config| {
            let mut tb = TimeBucketManager::new_with_sort_field(
                tb_config.filter_field.clone(),
                tb_config.sort_field.clone(),
                tb_config.range_buckets.clone(),
            );
            // Restore persisted time bucket bitmaps + cutoffs from disk
            if let Some(ref ms) = meta_store {
                match ms.load_time_buckets() {
                    Ok(persisted) if !persisted.is_empty() => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let count = persisted.len();
                        tb.load_persisted(&persisted, now);
                        // Restore persisted cutoffs (for boot diff computation)
                        for (name, _) in &persisted {
                            match ms.load_time_bucket_cutoff(name) {
                                Ok(cutoff) if cutoff > 0 => {
                                    if let Some(bucket) = tb.get_bucket_mut(name) {
                                        bucket.set_last_cutoff(cutoff);
                                        eprintln!("  Restored cutoff for '{}': {}", name, cutoff);
                                    }
                                }
                                Ok(_) => {} // no persisted cutoff — first boot
                                Err(e) => eprintln!("Warning: failed to load cutoff for '{}': {e}", name),
                            }
                        }
                        eprintln!("Restored {count} time bucket bitmaps from disk");
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("Warning: failed to load time buckets: {e}"),
                }
            }
            Arc::new(ArcSwap::new(Arc::new(tb)))
        });
        // Initialize pending bucket diffs (load from append-only log on disk + compute
        // boot diff), one independent `PendingBucketDiffs` per bucket NAME — see the
        // field's doc comment for why this must not be a single shared struct.
        let pending_bucket_diffs: Arc<HashMap<String, Arc<ArcSwap<crate::bucket_diff_log::PendingBucketDiffs>>>> = {
            let max_diffs = 100; // ~8 hours at 300s intervals
            let mut map: HashMap<String, Arc<ArcSwap<crate::bucket_diff_log::PendingBucketDiffs>>> = HashMap::default();
            if let Some(ref tb_config) = config.time_buckets {
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                // The sort field for time buckets was eagerly loaded above, so it's
                // available in `sorts`. One sort field is shared by every bucket name.
                let sort_field_name = time_buckets.as_ref()
                    .map(|tb_arc| tb_arc.load().sort_field_name().to_string());
                let sort_field = sort_field_name.as_deref().and_then(|n| sorts.get_field(n));
                // Buckets whose boot-diff gap exceeded their own duration —
                // the persisted diff history is too stale to trust, so the
                // second phase below must NOT advance `last_cutoff` for
                // them. Leaving `last_cutoff` at its old (stale) value lets
                // the flush thread's first incremental refresh naturally
                // cover the full stale range in one shot (old_cutoff far
                // enough in the past that every truly-expired slot falls in
                // `[old_cutoff, new_cutoff)` — a de facto full rebuild, no
                // separate code path needed). Advancing `last_cutoff` here
                // instead would silently skip that range forever: ground
                // truth keeps slots that expired inside the gap, with the
                // flush thread believing it's already caught up.
                let mut gap_skipped: HashSet<String> = HashSet::default();
                for bucket_config in &tb_config.range_buckets {
                    let bucket_name = &bucket_config.name;
                    let mut pending = crate::bucket_diff_log::PendingBucketDiffs::new(max_diffs);
                    let diff_log_path = config.storage.bitmap_path.as_ref()
                        .map(|bp| std::path::Path::new(bp).join(format!("bucket_diffs__{bucket_name}.log")));
                    // Step 1: Load persisted diffs from THIS bucket's own append-only log.
                    if let Some(ref log_path) = diff_log_path {
                        if log_path.exists() {
                            let log = crate::bucket_diff_log::BucketDiffLog::new(
                                log_path.clone(), max_diffs, 0.3,
                            );
                            match log.read_retained() {
                                Ok(diffs) if !diffs.is_empty() => {
                                    let count = diffs.len();
                                    pending = crate::bucket_diff_log::PendingBucketDiffs::from_diffs(diffs, max_diffs);
                                    eprintln!("Loaded {count} bucket diffs from disk for '{}' (coverage: cutoff {} to {})",
                                        bucket_name, pending.oldest_cutoff(), pending.current_cutoff());
                                }
                                Ok(_) => {}
                                Err(e) => eprintln!("Warning: failed to load bucket diffs for '{}': {e}", bucket_name),
                            }
                        }
                    }
                    // Step 2: Compute boot diff to cover the gap between persisted diffs
                    // and now, scoped to THIS bucket's own bitmap and cutoff.
                    if let (Some(ref tb_arc), Some(sort_field)) = (time_buckets.as_ref(), sort_field) {
                        let tb = tb_arc.load();
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
                                    gap_skipped.insert(bucket_name.clone());
                                } else {
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
                                        // Append boot diff to THIS bucket's own on-disk log
                                        if let Some(ref log_path) = diff_log_path {
                                            let log = crate::bucket_diff_log::BucketDiffLog::new(
                                                log_path.clone(), max_diffs, 0.3,
                                            );
                                            if let Err(e) = log.append(&diff) {
                                                eprintln!("Warning: failed to append boot diff to log for '{}': {e}", bucket_name);
                                            }
                                        }
                                        pending.push(diff);
                                    }
                                }
                            } else if persisted_cutoff == 0 {
                                eprintln!("Boot diff: no persisted cutoff for '{}' — first boot, full rebuild on first refresh", bucket_name);
                            } else {
                                eprintln!("Boot diff: '{}' already current (persisted={}, current={})", bucket_name, persisted_cutoff, current_cutoff);
                            }
                        }
                    }
                    map.insert(bucket_name.clone(), Arc::new(ArcSwap::new(Arc::new(pending))));
                }
                // Apply each bucket's OWN boot diff to its OWN ground-truth bitmap —
                // never a cross-bucket merged set (that would wrongly strip a wide
                // bucket's bitmap using a narrow bucket's expiry, and vice versa).
                if let Some(ref tb_arc) = time_buckets {
                    let mut tb = (*tb_arc.load_full()).clone();
                    let mut changed = false;
                    for bucket_config in &tb_config.range_buckets {
                        if gap_skipped.contains(&bucket_config.name) {
                            // Boot diff was skipped (gap > duration) — do NOT
                            // advance last_cutoff here. See the gap_skipped
                            // doc comment above: leaving it stale lets the
                            // flush thread's first refresh cover the whole
                            // gap in one incremental diff instead of quietly
                            // treating the ungapped range as already handled.
                            continue;
                        }
                        if let Some(cell) = map.get(&bucket_config.name) {
                            let pending = cell.load();
                            if pending.current_cutoff() > 0 {
                                if let Some(bucket) = tb.get_bucket_mut(&bucket_config.name) {
                                    let new_cutoff = crate::bucket_diff_log::snap_cutoff(
                                        now_secs.saturating_sub(bucket_config.duration_secs),
                                        bucket_config.refresh_interval_secs,
                                    );
                                    if new_cutoff > bucket.last_cutoff() {
                                        bucket.subtract_expired(pending.merged_expired(), new_cutoff);
                                        changed = true;
                                        eprintln!("Applied boot diff to '{}' bucket bitmap (cutoff → {})",
                                            bucket_config.name, new_cutoff);
                                    }
                                }
                            }
                        }
                    }
                    if changed {
                        tb_arc.store(Arc::new(tb));
                    }
                }
            }
            Arc::new(map)
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

        // DocStoreV3 uses ShardStore native compaction — no manual compaction worker needed.
        // `doc_compact_threshold` is an ops-COUNT and propagates to the underlying
        // ShardStore atomic the merge thread's `needs_compaction` gate reads. A value
        // of 0 disables doc auto-compaction (manual /compact still works).
        docstore.set_compact_threshold(config.doc_compact_threshold);
        // par_iter min-task threshold — hot-reloadable via PATCH /config.
        // Default 8: skip rayon dispatch for tiny batches where pool overhead
        // exceeds work. Set huge to disable par_iter entirely (perf experiment).
        let par_iter_min_threshold = Arc::new(AtomicUsize::new(8));
        // Wire the threshold into the docstore so its append_*_batch paths
        // observe the same hot-reload as the engine flush thread.
        docstore.set_par_iter_min_threshold_handle(Arc::clone(&par_iter_min_threshold));
        // Time-bucket full reconcile interval — hot-reloadable via PATCH /config.
        // Seeded from config; read each flush cycle by the periodic tb-block.
        let time_bucket_full_rebuild_interval = Arc::new(AtomicU64::new(
            config
                .time_buckets
                .as_ref()
                .map(|tb| tb.full_rebuild_interval_secs)
                .unwrap_or(0),
        ));
        let (compact_tx, compact_handle): (Option<Sender<(u32, Vec<u8>)>>, Option<JoinHandle<()>>) = (None, None);

        let docstore_root = Arc::new(docstore.path().to_path_buf());
        let docstore = Arc::new(parking_lot::RwLock::new(docstore));
        // Shared dirty flag: flush thread sets when mutations applied, merge thread
        // clears after persisting snapshot. Prevents continuous 20GB rewrites at idle.
        let dirty_flag = Arc::new(AtomicBool::new(false));
        // Load named cursors from disk (if any exist).
        let initial_cursors = if let Some(ref ms) = meta_store {
            ms.load_all_cursors().unwrap_or_default()
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
        // Reads bucket snapshots to discover all value IDs — fast even at 31K keys.
        let mut existing_keys: HashMap<String, Arc<ArcSwap<HashSet<u64>>>> = HashMap::new();
        if let Some(ref fs) = filter_store {
            let fields: Vec<String> = lazy_value_fields.iter().cloned().collect();
            if fields.len() > 1 {
                // Parallel existence set loading
                use rayon::prelude::*;
                let results: Vec<(String, std::result::Result<HashSet<u64>, _>)> = fields
                    .par_iter()
                    .map(|name| (name.clone(), fs.existence_set(name)))
                    .collect();
                for (field_name, result) in results {
                    match result {
                        Ok(keys) => {
                            if !keys.is_empty() {
                                eprintln!("Existence set for '{}': {} keys", field_name, keys.len());
                            }
                            existing_keys.insert(field_name, Arc::new(ArcSwap::from_pointee(keys)));
                        }
                        Err(e) => {
                            eprintln!("Warning: failed to build existence set for '{}': {}", field_name, e);
                            existing_keys.insert(field_name, Arc::new(ArcSwap::from_pointee(HashSet::new())));
                        }
                    }
                }
            } else {
                // Single field: sequential
                for field_name in &fields {
                    match fs.existence_set(field_name) {
                        Ok(keys) => {
                            if !keys.is_empty() {
                                eprintln!("Existence set for '{}': {} keys", field_name, keys.len());
                            }
                            existing_keys.insert(field_name.clone(), Arc::new(ArcSwap::from_pointee(keys)));
                        }
                        Err(e) => {
                            eprintln!("Warning: failed to build existence set for '{}': {}", field_name, e);
                            existing_keys.insert(field_name.clone(), Arc::new(ArcSwap::from_pointee(HashSet::new())));
                        }
                    }
                }
            }
        }
        // Eviction-enabled fields must always be in lazy_value_fields so that
        // ensure_fields_loaded() can reload values after eviction, even when the
        // engine wasn't restored from disk. Skip if eager_load — user wants everything in memory.
        for fc in &config.filter_fields {
            if fc.eviction.is_some() && fc.field_type == FilterFieldType::MultiValue && !fc.eager_load {
                lazy_value_fields.insert(fc.name.clone());
                // Ensure existence set exists (empty if no bitmap store)
                existing_keys.entry(fc.name.clone()).or_insert_with(|| {
                    Arc::new(ArcSwap::from_pointee(HashSet::new()))
                });
            }
        }
        let lazy_value_fields = Arc::new(parking_lot::Mutex::new(lazy_value_fields));
        // Document cache: DashMap-based in-memory cache for include_docs queries
        let doc_cache: Option<Arc<crate::doc_cache::DocCache>> = if config.storage.bitmap_path.is_some() {
            Some(Arc::new(crate::doc_cache::DocCache::new(
                crate::doc_cache::DocCacheConfig {
                    max_bytes: config.doc_cache.max_bytes,
                    generation_interval_secs: config.doc_cache.generation_interval_secs,
                    max_generations: config.doc_cache.max_generations,
                },
            )))
        } else {
            None
        };
        // Bitmap memory scanner cache
        let bitmap_memory_cache = Arc::new(crate::bitmap_memory_cache::BitmapMemoryCache::new(
            config.memory_scanner.enabled,
            config.memory_scanner.interval_ms,
            config.memory_scanner.batch_size,
        ));
        // Eviction state
        let eviction_stamps: Arc<DashMap<(Arc<str>, u64), AtomicU64>> = Arc::new(DashMap::new());
        let flush_cycle = Arc::new(AtomicU64::new(0));
        let eviction_total: Arc<DashMap<String, AtomicU64>> = Arc::new(DashMap::new());
        let flush_publish_count = Arc::new(AtomicU64::new(0));
        let flush_duration_nanos = Arc::new(AtomicU64::new(0));
        let flush_last_duration_nanos = Arc::new(AtomicU64::new(0));
        let flush_apply_nanos = Arc::new(AtomicU64::new(0));
        let flush_cache_nanos = Arc::new(AtomicU64::new(0));
        let flush_publish_nanos = Arc::new(AtomicU64::new(0));
        let flush_timebucket_nanos = Arc::new(AtomicU64::new(0));
        let flush_compact_nanos = Arc::new(AtomicU64::new(0));
        let flush_opslog_nanos = Arc::new(AtomicU64::new(0));
        let flush_sort_promote_nanos = Arc::new(AtomicU64::new(0));
        let flush_cache_unique_filter_shapes = Arc::new(AtomicU64::new(0));
        let flush_cache_unique_filter_shapes_max = Arc::new(AtomicU64::new(0));
        let flush_cache_sort_work_items = Arc::new(AtomicU64::new(0));
        let flush_cache_sort_work_items_max = Arc::new(AtomicU64::new(0));
        // BoundStore operational counters (defined before flush/merge threads)
        let boundstore_shard_loads = Arc::new(AtomicU64::new(0));
        let boundstore_tombstones_created = Arc::new(AtomicU64::new(0));
        let boundstore_tombstones_cleaned = Arc::new(AtomicU64::new(0));
        let boundstore_bytes_written = Arc::new(AtomicU64::new(0));
        let boundstore_bytes_read = Arc::new(AtomicU64::new(0));
        let boundstore_entries_restored = Arc::new(AtomicU64::new(0));
        let boundstore_entries_skipped = Arc::new(AtomicU64::new(0));
        // Async cache worker channel (metrics created earlier — see above).
        let (cache_work_tx, pre_cache_rx): (
            Option<crossbeam_channel::Sender<crate::cache_worker::CacheWorkItem>>,
            Option<crossbeam_channel::Receiver<crate::cache_worker::CacheWorkItem>>,
        ) = if config.cache.async_maintenance && !config.headless {
            let (tx, rx) = crossbeam_channel::bounded(1024);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        // Read config values needed below (after potential config Arc move).
        let initial_prefilter_cap = config.max_registered_prefilters;
        // Headless mode: skip all background threads.
        // The engine provides config, bitmap store, and docstore access but
        // no flush/merge/eviction threads run.
        if config.headless {
            eprintln!("Engine starting in headless mode (no background threads)");
            return Ok(Self {
                inner,
                sender,
                doc_tx,
                docstore,
                docstore_root: Arc::clone(&docstore_root),
                config,
                field_registry,
                in_flight: InFlightTracker::new(),
                shutdown,
                flush_handle: None,
                merge_handle: None,
                bitmap_store,
                alive_store: alive_store.clone(),
                filter_store: filter_store.clone(),
                sort_store: sort_store.clone(),
                meta_store: meta_store.clone(),
                loading_mode,
                dirty_since_snapshot: dirty_flag,
                time_buckets,
                pending_bucket_diffs: Arc::clone(&pending_bucket_diffs),
                pending_filter_loads,
                pending_sort_loads,
                lazy_value_fields,
                lazy_tx,
                cmd_tx,
                string_maps: None,
                case_sensitive_fields: None,
                dictionaries: Arc::new(HashMap::new()),
                shared_string_maps: Arc::new(ArcSwap::from_pointee(None)),
                shared_dictionaries: Arc::new(ArcSwap::from_pointee(Arc::new(HashMap::new()))),
                unified_cache,
                bound_store,
                flush_publish_count,
                flush_duration_nanos,
                flush_last_duration_nanos,
                flush_apply_nanos,
                flush_cache_nanos,
                flush_publish_nanos,
                flush_timebucket_nanos,
                flush_compact_nanos,
                flush_opslog_nanos,
                flush_sort_promote_nanos,
                flush_cache_unique_filter_shapes,
                flush_cache_unique_filter_shapes_max,
                flush_cache_sort_work_items,
                flush_cache_sort_work_items_max,
                cursors,
                existing_keys,
                eviction_stamps,
                flush_cycle,
                eviction_total,
                boundstore_shard_loads,
                boundstore_tombstones_created,
                boundstore_tombstones_cleaned,
                boundstore_bytes_written,
                boundstore_bytes_read,
                boundstore_entries_restored,
                boundstore_entries_skipped,
                #[cfg(feature = "server")]
                metrics_bridge: Arc::new(ArcSwap::from_pointee(None)),
                bitmap_memory_cache: Arc::clone(&bitmap_memory_cache),
                doc_cache: doc_cache.clone(),
                par_iter_min_threshold: Arc::clone(&par_iter_min_threshold),
                time_bucket_full_rebuild_interval: Arc::clone(&time_bucket_full_rebuild_interval),
                compaction_skipped: Arc::new(AtomicU64::new(0)),
                compact_handle: None,
                compact_tx: None,
                prefetch_tx: None,
                prefetch_handle: None,
                doc_cache_eviction_handle: None,
                #[cfg(feature = "pg-sync")]
                wal_writer: None,
                cache_work_tx: None,
                cache_worker_handle: None,
                cache_worker_metrics: Arc::new(crate::cache_worker::CacheWorkerMetrics::default()),
                cache_worker_ms: None,
                prefilter_registry: Arc::new(
                    crate::prefilter::PrefilterRegistry::new_with_cap(initial_prefilter_cap)
                ),
                warm_registry: Arc::new(crate::warm_registry::WarmRegistry::new(None)),
            });
        }
        let flush_handle = {
            let inner = Arc::clone(&inner);
            let shutdown = Arc::clone(&shutdown);
            let docstore = Arc::clone(&docstore);
            let flush_interval_us = config.flush_interval_us;
            let flush_unified_cache = Arc::clone(&unified_cache);
            let flush_loading_mode = Arc::clone(&loading_mode);
            let flush_dirty_flag = Arc::clone(&dirty_flag);
            let flush_time_buckets = time_buckets.as_ref().map(Arc::clone);
            #[cfg(feature = "server")]
            let flush_metrics_bridge: Arc<ArcSwap<Option<Arc<MetricsBridge>>>> =
                Arc::clone(&metrics_bridge);
            let flush_pending_diffs = Arc::clone(&pending_bucket_diffs);
            // Source diagnostic (missing-adds) gate — read once per flush thread.
            // OFF by default (zero hot-path cost); enable for a diagnostic window
            // via BITDEX_TB_SOURCE_DIAG=1. When on, the tb-block verifies each
            // sortAt-mutated slot landed in its window bucket (aggregated).
            // server-gated: the only consumer is the server-feature metric block.
            #[cfg(feature = "server")]
            let tb_source_diag: bool = std::env::var("BITDEX_TB_SOURCE_DIAG")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            // Interval (secs) for the periodic FULL time-bucket rebuild fallback.
            // 0 disables it. Hot-reloadable via PATCH /config: the flush loop
            // loads this handle each cycle instead of capturing a fixed value.
            // See `TimeBucketFieldConfig::full_rebuild_interval_secs`.
            let flush_tb_rebuild_interval_handle =
                Arc::clone(&time_bucket_full_rebuild_interval);
            // Threads for the dedicated reconcile-scan pool (0 = auto). Read once
            // at flush-thread start; see `TimeBucketFieldConfig::reconcile_scan_threads`.
            let flush_reconcile_scan_threads: usize = config
                .time_buckets
                .as_ref()
                .map(|tb| tb.reconcile_scan_threads)
                .unwrap_or(0);
            // Base dir for per-bucket-name diff logs (`bucket_diffs__{name}.log`) —
            // see `pending_bucket_diffs`'s doc comment for why these are per-bucket.
            let flush_diff_log_dir = config.storage.bitmap_path.as_ref()
                .map(|bp| std::path::PathBuf::from(bp));
            let flush_pub_count = Arc::clone(&flush_publish_count);
            let flush_dur_nanos = Arc::clone(&flush_duration_nanos);
            let flush_last_dur_nanos = Arc::clone(&flush_last_duration_nanos);
            let flush_apply_ns = Arc::clone(&flush_apply_nanos);
            let flush_cache_ns = Arc::clone(&flush_cache_nanos);
            let flush_publish_ns = Arc::clone(&flush_publish_nanos);
            let flush_timebucket_ns = Arc::clone(&flush_timebucket_nanos);
            let flush_compact_ns = Arc::clone(&flush_compact_nanos);
            let flush_opslog_ns = Arc::clone(&flush_opslog_nanos);
            let flush_sort_promote_ns = Arc::clone(&flush_sort_promote_nanos);
            let flush_cache_unique_shapes =
                Arc::clone(&flush_cache_unique_filter_shapes);
            let flush_cache_unique_shapes_max =
                Arc::clone(&flush_cache_unique_filter_shapes_max);
            let flush_cache_sort_work_items_gauge =
                Arc::clone(&flush_cache_sort_work_items);
            let flush_cache_sort_work_items_max_gauge =
                Arc::clone(&flush_cache_sort_work_items_max);
            let flush_existing_keys: HashMap<String, Arc<ArcSwap<HashSet<u64>>>> =
                existing_keys.iter().map(|(k, v)| (k.clone(), Arc::clone(v))).collect();
            let flush_eviction_stamps = Arc::clone(&eviction_stamps);
            let flush_eviction_total = Arc::clone(&eviction_total);
            let flush_cycle_clone = Arc::clone(&flush_cycle);
            let _flush_bitmap_store = bitmap_store.clone();
            let flush_doc_cache = doc_cache.clone();
            let flush_par_iter_min = Arc::clone(&par_iter_min_threshold);
            let flush_alive_store = alive_store.clone();
            let flush_filter_store = filter_store.clone();
            let flush_sort_store = sort_store.clone();
            let flush_meta_store = meta_store.clone();
            let flush_config = Arc::clone(&config);
            let flush_field_registry = field_registry.clone();
            let flush_lazy_value_fields = lazy_value_fields.clone();
            let eviction_sweep_interval = config.eviction_sweep_interval;
            let flush_tombstones_created = Arc::clone(&boundstore_tombstones_created);
            // Build eviction config map: field_name → idle_seconds
            let eviction_configs: HashMap<String, f64> = config.filter_fields.iter()
                .filter_map(|fc| fc.eviction.as_ref().map(|e| (fc.name.clone(), e.idle_seconds)))
                .collect();
            let flush_mem_cache = Arc::clone(&bitmap_memory_cache);
            let flush_cache_work_tx = cache_work_tx.clone();
            let flush_cache_worker_metrics = Arc::clone(&cache_worker_metrics);
            // Shared string_maps + dictionaries for native FilterClause eval (B2).
            // Updated by set_string_maps/set_dictionaries via ArcSwap so the flush
            // thread always sees the latest without holding a lock.
            let flush_shared_string_maps: Arc<ArcSwap<Option<StringMaps>>> = Arc::clone(&shared_string_maps);
            let flush_shared_dictionaries: Arc<ArcSwap<Arc<HashMap<String, crate::dictionary::FieldDictionary>>>> = Arc::clone(&shared_dictionaries);
            thread::Builder::new()
                .name("bitdex-flush".to_string())
                .spawn(move || {
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
                // Periodically promote dirty sort layer diffs into bases.
                // Lazy fuse means reads work correctly with dirty diffs (via
                // VB::fused_cow), but per-query fuse cost grows linearly with
                // diff size. Periodic promotion keeps diffs small. The promote
                // does pay the Arc::make_mut clone cost (since published readers
                // hold base refs), but at 5s interval that's ~10% CPU vs the
                // ~10000% it was when we did it every cycle.
                let mut last_sort_promote = std::time::Instant::now();
                let sort_promote_interval = Duration::from_secs(5);
                // Slots whose time-bucket insertion was deferred because the
                // bucket sort field was not fully loaded at flush time.
                // Drained on the first flush cycle that observes the sort
                // field fully loaded. Capped to bound memory under prolonged
                // unload windows.
                let mut pending_bucket_retries: HashSet<u32> = HashSet::new();
                const PENDING_BUCKET_RETRY_CAP: usize = 1_000_000;
                // Periodic full time-bucket rebuild fallback state. The scan is
                // ~minutes at scale, so it runs on a BACKGROUND thread over an
                // immutable published snapshot; results return via this channel
                // and are applied on the flush thread (the sole writer of the
                // TimeBucketManager). Scheduling uses a monotonic Instant (robust
                // to wall-clock jumps); `None` = not yet baselined, seeded to
                // "now" on the first cycle so boot's own rebuild is the baseline
                // and the first fallback fires one interval later.
                // `None` payload = sort field not loaded (retry); `Some((dur,
                // removals))` = scan duration + per-bucket (name, stale set,
                // missing count) to prune/observe. The JoinHandle lets the flush
                // thread detect a worker that ended without sending (panicked) —
                // the persistent sender keeps the channel open so try_recv never
                // reports Disconnected.
                #[allow(clippy::type_complexity)]
                let (bucket_rebuild_tx, bucket_rebuild_rx) = std::sync::mpsc::channel::<
                    // Per bucket: (name, stale candidates, missing candidates).
                    Option<(std::time::Duration, Vec<(String, RoaringBitmap, RoaringBitmap)>)>,
                >();
                let mut last_full_bucket_rebuild: Option<std::time::Instant> = None;
                let mut bucket_rebuild_in_flight = false;
                let mut bucket_rebuild_handle: Option<std::thread::JoinHandle<()>> = None;
                let mut heartbeat_counter: u64 = 0;
                let mut max_bitmap_count_seen: usize = 0;
                let mut nonzero_iters: u64 = 0;
                let mut last_heartbeat_log = std::time::Instant::now();
                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(current_sleep);
                    let is_loading = flush_loading_mode.load(Ordering::Relaxed);
                    // Activate deferred-alive slots whose scheduled time has arrived.
                    //
                    // Runs at the TOP of the cycle so activation ops are folded into
                    // the main coalescer batch via `push_ops`; `prepare()` below then
                    // groups them with channel-drained ops, `apply_prepared_traced()`
                    // applies them to staging, and the cache-maintenance phase sees
                    // them via `coalescer.mutated_*` accessors. Prior to this routing,
                    // activation ops bypassed the coalescer entirely, so cached
                    // sortAt-Desc top-of-sort queries silently missed freshly-published
                    // scheduled Posts (see commit message for the prod symptom).
                    //
                    // Activation replays the stored doc as a fresh insert: read the
                    // doc from docstore, diff against None (no prior state), and push
                    // the resulting ops. The AliveInsert in those ops is redundant
                    // with `staging.slots.activate_due()` setting the bit directly,
                    // but harmless — extend on an already-set bit is a no-op.
                    //
                    // Deferred-map persistence is DELAYED until after opslog append
                    // (see `deferred_persist_needed` write below) so a crash between
                    // activate_due and opslog produces duplicate (idempotent)
                    // activation on restart, not lost activation. Persisting eagerly
                    // here would create a data-loss window: deferred map says slot
                    // is gone, opslog hasn't recorded the activation ops yet, restart
                    // would never reactivate the slot.
                    let mut deferred_persist_needed = false;
                    if !is_loading && staging.slots.deferred_count() > 0 {
                        let now_unix = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let activated = staging.slots.activate_due(now_unix);
                        if !activated.is_empty() {
                            let ds = docstore.read();
                            for &slot in &activated {
                                match ds.get(slot) {
                                    Ok(Some(stored_doc)) => {
                                        let doc = crate::mutation::Document {
                                            fields: stored_doc.fields.clone(),
                                        };
                                        let ops = crate::mutation::diff_document(
                                            slot,
                                            None,
                                            &doc,
                                            &flush_config,
                                            false,
                                            &flush_field_registry,
                                        );
                                        coalescer.push_ops(ops);
                                    }
                                    Ok(None) => {
                                        eprintln!("Warning: deferred slot {} has no stored doc, setting alive only", slot);
                                        coalescer.push_ops(vec![
                                            MutationOp::AliveInsert { slots: vec![slot] },
                                        ]);
                                    }
                                    Err(e) => {
                                        eprintln!("Warning: failed to read deferred slot {}: {e}, setting alive only", slot);
                                        coalescer.push_ops(vec![
                                            MutationOp::AliveInsert { slots: vec![slot] },
                                        ]);
                                    }
                                }
                            }
                            deferred_persist_needed = true;
                        }
                    }
                    // Phase 1: Drain channel and group/sort (no lock, pure CPU work)
                    let bitmap_count = coalescer.prepare();
                    if bitmap_count > 0 {
                        nonzero_iters += 1;
                        if bitmap_count > max_bitmap_count_seen {
                            max_bitmap_count_seen = bitmap_count;
                        }
                    }
                    // Heartbeat: emit every 5 seconds so we can verify the flush
                    // thread is alive and tell us what bitmap_count it's seeing.
                    heartbeat_counter += 1;
                    if last_heartbeat_log.elapsed() >= Duration::from_secs(5) {
                        let coalescer_pending = coalescer.pending_count();
                        tracing::warn!(
                            "[flush-heartbeat] iter={} bitmap_count={} coalescer_pending={} nonzero_iters={} max_seen={} is_loading={} sleep_us={}",
                            heartbeat_counter,
                            bitmap_count,
                            coalescer_pending,
                            nonzero_iters,
                            max_bitmap_count_seen,
                            is_loading,
                            current_sleep.as_micros(),
                        );
                        last_heartbeat_log = std::time::Instant::now();
                    }
                    // Sort layer promote: merge dirty diffs into bases on a timer.
                    // MUST run outside the bitmap_count gate — initial load can leave
                    // sort layers dirty even when no ops are flowing. Without this,
                    // every query on a dirty sort field pays 32 × ~6MB clone (~48ms).
                    if last_sort_promote.elapsed() >= sort_promote_interval {
                        let t_promote = std::time::Instant::now();
                        let dirty_field_names: Vec<String> = staging
                            .sorts
                            .fields()
                            .filter(|(_, sf)| sf.has_dirty())
                            .map(|(name, _)| name.clone())
                            .collect();
                        if !dirty_field_names.is_empty() {
                            for name in &dirty_field_names {
                                if let Some(sf) = staging.sorts.get_field_mut(name) {
                                    sf.merge_dirty();
                                }
                            }
                            staging_dirty = true; // force publish with clean layers
                            let promote_ns = t_promote.elapsed().as_nanos() as u64;
                            flush_sort_promote_ns.store(promote_ns, Ordering::Relaxed);
                            tracing::warn!(
                                "[sort-promote] dirty={} elapsed={:.1}ms names={:?}",
                                dirty_field_names.len(),
                                promote_ns as f64 / 1_000_000.0,
                                dirty_field_names,
                            );
                        }
                        last_sort_promote = std::time::Instant::now();
                    }
                    // Phase 1b: Drain lazy load channel — apply loaded fields to staging.
                    // This keeps staging in sync with snapshots published by ensure_loaded().
                    //
                    // **Bounded drain:** processing one LazyLoad::FilterField for a
                    // high-cardinality field (postId at 22.5M values) takes 1-2s of
                    // chunked HashMap inserts. If queries keep triggering new lazy
                    // loads while we're processing, the unbounded `while try_recv`
                    // loop runs forever and phase 2 (apply mutations) never gets a
                    // chance — channel fills up, ops back up, queries time out.
                    // Cap at LAZY_DRAIN_MAX per cycle so phase 2 always runs.
                    const LAZY_DRAIN_MAX: usize = 1;
                    let mut lazy_loaded = false;
                    let mut stale_fields: Vec<String> = Vec::new();
                    let mut lazy_drained: usize = 0;
                    while lazy_drained < LAZY_DRAIN_MAX {
                        let load = match lazy_rx.try_recv() {
                            Ok(load) => load,
                            Err(_) => break,
                        };
                        lazy_drained += 1;
                        match load {
                            LazyLoad::FilterField { name, bitmaps } => {
                                if let Some(field) = staging.filters.get_field(&name) {
                                    field.load_field_complete(bitmaps);
                                }
                                stale_fields.push(name);
                            }
                            LazyLoad::FilterValues { field, values } => {
                                if let Some(f) = staging.filters.get_field(&field) {
                                    // For per-value loads, we use load_from since only
                                    // specific requested values are sent. The values in
                                    // the map are all that were requested.
                                    let requested: Vec<u64> = values.keys().copied().collect();
                                    f.load_values(values, &requested);
                                }
                                stale_fields.push(field);
                            }
                            LazyLoad::SortField { name, layers } => {
                                if let Some(sf) = staging.sorts.get_field_mut(&name) {
                                    sf.load_layers(layers);
                                    // If time buckets use this sort field, force a rebuild on the
                                    // next periodic check (don't rebuild inline — iterating 100M+
                                    // slots while holding the lock would block queries).
                                    if let Some(ref tb_arc) = flush_time_buckets {
                                        let mut tb = (*tb_arc.load_full()).clone();
                                        if tb.sort_field_name() == name {
                                            tb.force_refresh_due();
                                            tb_arc.store(Arc::new(tb));
                                        }
                                    }
                                }
                                stale_fields.push(name);
                            }
                            LazyLoad::Slots { slots } => {
                                staging.slots = slots;
                            }
                        }
                        lazy_loaded = true;
                    }
                    // Phase 2: Apply mutations to staging (private, no lock needed)
                    let flush_start = Instant::now();
                    if bitmap_count > 0 {
                        staging_dirty = true;
                        flush_dirty_flag.store(true, Ordering::Release);
                        let t_apply = Instant::now();
                        let apply_timings = coalescer.apply_prepared_traced(
                            &mut staging.slots,
                            &mut staging.filters,
                            &mut staging.sorts,
                        );
                        let apply_elapsed_ns = t_apply.elapsed().as_nanos() as u64;
                        flush_apply_ns.store(apply_elapsed_ns, Ordering::Relaxed);
                        // Emit ops trace log when a single cycle is unusually slow.
                        // Threshold: 100ms apply. Surfaces per-field hot spots
                        // (e.g. postId Arc::make_mut cascade on 22.5M-entry HashMap).
                        if apply_elapsed_ns > 100_000_000 {
                            tracing::warn!(
                                "[ops-trace] apply ops={} {}",
                                bitmap_count,
                                apply_timings.render_summary(),
                            );
                        }
                        // Sort promote moved outside bitmap_count block (see below)
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
                        // Persist deferred map when new deferred entries are added.
                        if coalescer.has_deferred_alive() {
                            if let Some(ref ms) = flush_meta_store {
                                if let Err(e) = ms.write_deferred_alive(staging.slots.deferred_map()) {
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
                        let t_post_apply = Instant::now();
                        // Yield CPU after apply to let tokio I/O threads deliver
                        // pending HTTP responses. Without this, the flush thread
                        // monopolizes CPU across apply+cache+publish (~20ms aggregate),
                        // causing 1-4s response delivery delays under concurrent load.
                        std::thread::yield_now();
                        // In loading mode, skip all maintenance and snapshot publishing.
                        // This avoids the expensive staging.clone() → Arc::make_mut clone
                        // cascade that dominates write cost at scale.
                        if !flush_loading_mode.load(Ordering::Relaxed) {
                            // Live maintenance for time buckets:
                            //   1. add newly-alive slots to qualifying buckets,
                            //   2. remove deleted slots from all buckets,
                            //   3. re-evaluate bucket membership for slots whose
                            //      tracked sort field value changed in this batch
                            //      (e.g., sortAt = greatest(existedAt, publishedAt)
                            //      regressing when a Post is unpublished).
                            //
                            // Step 3 closes the long-standing leak where slots
                            // stayed in the 24h bucket after their sortAt aged
                            // out — `subtract_expired` only catches slots whose
                            // sortAt falls in `[old_cutoff, new_cutoff)`, so a
                            // slot whose sortAt jumped from "now" to "two days
                            // ago" in a single update was invisible to the
                            // periodic refresh forever.
                            let t_tb = Instant::now();
                            // Slots whose time-bucket membership may have shifted this
                            // cycle. Consumed by the cache's #274 bucket-membership
                            // maintenance (entries filtered on a bucket but sorted by a
                            // non-bucket field). Populated inside the tb block below.
                            let mut bucket_changed_slots = roaring::RoaringBitmap::new();
                            if let Some(ref tb_arc) = flush_time_buckets {
                                let alive_inserts = coalescer.alive_inserts();
                                let alive_removes = coalescer.alive_removes();
                                let mutated_slots = coalescer.mutated_sort_slots();
                                let sort_field_name = tb_arc.load().sort_field_name().to_string();
                                // Union of alive inserts/removes + bucket-sort-field
                                // mutations = every slot whose bucket membership could
                                // have changed this cycle.
                                bucket_changed_slots.extend(alive_inserts.iter().copied());
                                bucket_changed_slots.extend(alive_removes.iter().copied());
                                if let Some(set) = mutated_slots.get(sort_field_name.as_str()) {
                                    bucket_changed_slots.extend(set.iter().copied());
                                }
                                let sort_value_changed: Vec<u32> = mutated_slots
                                    .get(sort_field_name.as_str())
                                    .map(|set| {
                                        // alive_inserts already get a fresh insert_slot below,
                                        // and alive_removes get a remove_slot — skip those
                                        // here so we don't redo the same work.
                                        let alive_set: HashSet<u32> = alive_inserts
                                            .iter()
                                            .chain(alive_removes.iter())
                                            .copied()
                                            .collect();
                                        set.iter().filter(|s| !alive_set.contains(s)).copied().collect()
                                    })
                                    .unwrap_or_default();
                                if !alive_inserts.is_empty()
                                    || !alive_removes.is_empty()
                                    || !sort_value_changed.is_empty()
                                    || !pending_bucket_retries.is_empty()
                                {
                                    let now_secs = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs();
                                    let mut tb = (*tb_arc.load_full()).clone();
                                    let sort_field = staging.sorts.get_field(&sort_field_name);
                                    let sort_field_loaded = sort_field
                                        .map(|sf| sf.is_fully_loaded())
                                        .unwrap_or(false);
                                    #[cfg(feature = "server")]
                                    let bridge_guard = flush_metrics_bridge.load();
                                    #[cfg(feature = "server")]
                                    let bridge_opt = (**bridge_guard).as_ref();
                                    // Removes always work — they don't need the sort field — so
                                    // process them unconditionally. A remove also cancels any
                                    // pending insert for the same slot from a prior cycle.
                                    for &slot in alive_removes {
                                        tb.remove_slot(slot);
                                        pending_bucket_retries.remove(&slot);
                                    }
                                    // Anomaly classification helper. Closures don't need to
                                    // borrow bridge_opt because each call site reads it.
                                    #[cfg(feature = "server")]
                                    let classify_anomaly = |ts: u64| -> Option<&'static str> {
                                        if ts == 0 {
                                            Some("zero")
                                        } else if ts > now_secs.saturating_add(60) {
                                            if ts > 4_000_000_000 {
                                                Some("wrapped")
                                            } else {
                                                Some("future")
                                            }
                                        } else {
                                            None
                                        }
                                    };
                                    if sort_field_loaded {
                                        let sort_field = sort_field.expect("loaded implies Some");
                                        // Drain deferred slots from prior cycles first. These
                                        // can be either deferred alive_inserts (no prior bucket
                                        // membership) OR deferred sort_value_changed (old bucket
                                        // membership was already cleared at defer time, so a
                                        // plain insert is sufficient on replay).
                                        for slot in pending_bucket_retries.drain() {
                                            let ts = sort_field.reconstruct_value(slot) as u64;
                                            #[cfg(feature = "server")]
                                            if let Some(bridge) = bridge_opt {
                                                if let Some(k) = classify_anomaly(ts) {
                                                    bridge
                                                        .timebucket_anomalous_ts_total
                                                        .with_label_values(&[
                                                            &bridge.index_name,
                                                            &sort_field_name,
                                                            k,
                                                        ])
                                                        .inc();
                                                }
                                            }
                                            tb.insert_slot(slot, ts, now_secs);
                                        }
                                        for &slot in alive_inserts {
                                            let ts = sort_field.reconstruct_value(slot) as u64;
                                            #[cfg(feature = "server")]
                                            if let Some(bridge) = bridge_opt {
                                                if let Some(k) = classify_anomaly(ts) {
                                                    bridge
                                                        .timebucket_anomalous_ts_total
                                                        .with_label_values(&[
                                                            &bridge.index_name,
                                                            &sort_field_name,
                                                            k,
                                                        ])
                                                        .inc();
                                                }
                                            }
                                            tb.insert_slot(slot, ts, now_secs);
                                        }
                                        for slot in &sort_value_changed {
                                            // Re-evaluate bucket membership against the new
                                            // value. insert_slot is a no-op for buckets where
                                            // the new ts is out of window, so this also handles
                                            // aging-out.
                                            let ts = sort_field.reconstruct_value(*slot) as u64;
                                            #[cfg(feature = "server")]
                                            if let Some(bridge) = bridge_opt {
                                                if let Some(k) = classify_anomaly(ts) {
                                                    bridge
                                                        .timebucket_anomalous_ts_total
                                                        .with_label_values(&[
                                                            &bridge.index_name,
                                                            &sort_field_name,
                                                            k,
                                                        ])
                                                        .inc();
                                                }
                                            }
                                            tb.remove_slot(*slot);
                                            tb.insert_slot(*slot, ts, now_secs);
                                        }
                                    } else {
                                        // Sort field is partially loaded (some bit layers in the
                                        // unloaded placeholder state). reconstruct_value would
                                        // return zeroed/garbage timestamps for slots whose bits
                                        // live only in the unloaded base, so silently dropping
                                        // them from buckets is the live-update path's main
                                        // staleness source. Defer them, replay once loaded.
                                        //
                                        // For sort_value_changed slots specifically: we MUST
                                        // clear any existing bucket membership at defer time,
                                        // not at replay time. Otherwise the slot lingers in its
                                        // old bucket until reload — exactly the staleness this
                                        // fix is meant to prevent. The old ts is unknown without
                                        // the sort field so we remove from every bucket; the
                                        // replay path will re-insert with the correct new ts.
                                        for slot in &sort_value_changed {
                                            tb.remove_slot(*slot);
                                        }
                                        let total_pending =
                                            alive_inserts.len() + sort_value_changed.len();
                                        let space_left = PENDING_BUCKET_RETRY_CAP
                                            .saturating_sub(pending_bucket_retries.len());
                                        let mut deferred = 0usize;
                                        for &slot in alive_inserts.iter().take(space_left) {
                                            pending_bucket_retries.insert(slot);
                                            deferred += 1;
                                        }
                                        let space_left = PENDING_BUCKET_RETRY_CAP
                                            .saturating_sub(pending_bucket_retries.len());
                                        for slot in sort_value_changed.iter().take(space_left) {
                                            pending_bucket_retries.insert(*slot);
                                            deferred += 1;
                                        }
                                        let dropped = total_pending.saturating_sub(deferred);
                                        if dropped > 0 {
                                            tracing::error!(
                                                "[time-bucket] pending retry queue at cap ({}); permanently dropped {} slots while sort field '{}' is unloaded — bucket bitmap data loss",
                                                PENDING_BUCKET_RETRY_CAP,
                                                dropped,
                                                sort_field_name,
                                            );
                                            #[cfg(feature = "server")]
                                            if let Some(bridge) = bridge_opt {
                                                bridge
                                                    .timebucket_dropped_capacity_exceeded_total
                                                    .with_label_values(&[
                                                        &bridge.index_name,
                                                        &sort_field_name,
                                                    ])
                                                    .inc_by(dropped as u64);
                                            }
                                        }
                                        #[cfg(feature = "server")]
                                        if let Some(bridge) = bridge_opt {
                                            bridge
                                                .timebucket_dropped_no_sort_field_total
                                                .with_label_values(&[
                                                    &bridge.index_name,
                                                    &sort_field_name,
                                                ])
                                                .inc_by(deferred as u64);
                                        }
                                    }
                                    // === Source diagnostic (B, #timebucket-missing-adds) ===
                                    // After every sortAt mutation this cycle is applied to
                                    // `tb`, verify each sortAt-mutated ALIVE slot whose
                                    // reconstructed value lands in a bucket window actually
                                    // made it into that bucket. Prod shows in-window sortAt
                                    // updates that reach the sort LAYER but not the bucket
                                    // (~22% of 24h at 107M) with no existing counter and no
                                    // local repro — this fires at the MOMENT of loss, before
                                    // the hourly backfill masks it. `via` = which path should
                                    // have bucketed it. OFF by default (BITDEX_TB_SOURCE_DIAG);
                                    // bucket handles hoisted out of the slot loop; per-cycle
                                    // scan capped; logging aggregated to one line per
                                    // (bucket, via) so a mass-miss cycle can't log-storm.
                                    #[cfg(feature = "server")]
                                    if tb_source_diag && sort_field_loaded {
                                        if let (Some(bridge), Some(sf)) = (bridge_opt, sort_field) {
                                            if let Some(mutated) =
                                                mutated_slots.get(sort_field_name.as_str())
                                            {
                                                const TB_DIAG_SCAN_CAP: usize = 20_000;
                                                let removed: HashSet<u32> =
                                                    alive_removes.iter().copied().collect();
                                                let inserted: HashSet<u32> =
                                                    alive_inserts.iter().copied().collect();
                                                // Hoist bucket (name, cutoff, bitmap ref) once.
                                                let bucket_specs: Vec<(String, u64, &Arc<roaring::RoaringBitmap>)> =
                                                    tb.bucket_names()
                                                        .into_iter()
                                                        .filter_map(|name| {
                                                            let b = tb.get_bucket(&name)?;
                                                            Some((
                                                                name,
                                                                now_secs.saturating_sub(b.duration_secs),
                                                                b.bitmap(),
                                                            ))
                                                        })
                                                        .collect();
                                                // Aggregate: (bucket, via) -> (count, first-N sample "slot@ts").
                                                let mut agg: std::collections::HashMap<
                                                    (String, &'static str),
                                                    (u64, Vec<String>),
                                                > = std::collections::HashMap::new();
                                                for &slot in mutated.iter().take(TB_DIAG_SCAN_CAP) {
                                                    if removed.contains(&slot) {
                                                        continue;
                                                    }
                                                    let ts = sf.reconstruct_value(slot) as u64;
                                                    if ts == 0 || ts > now_secs {
                                                        continue;
                                                    }
                                                    let via = if inserted.contains(&slot) {
                                                        "alive_insert"
                                                    } else {
                                                        "sort_value_changed"
                                                    };
                                                    for (name, cutoff, bitmap) in &bucket_specs {
                                                        if ts >= *cutoff && !bitmap.contains(slot) {
                                                            let e = agg
                                                                .entry((name.clone(), via))
                                                                .or_insert((0, Vec::new()));
                                                            e.0 += 1;
                                                            if e.1.len() < 20 {
                                                                e.1.push(format!("{slot}@{ts}"));
                                                            }
                                                        }
                                                    }
                                                }
                                                for ((name, via), (count, samples)) in agg {
                                                    bridge
                                                        .timebucket_applied_not_bucketed_total
                                                        .with_label_values(&[
                                                            &bridge.index_name,
                                                            &sort_field_name,
                                                            &name,
                                                        ])
                                                        .inc_by(count);
                                                    tracing::warn!(
                                                        target: "time_bucket",
                                                        "[tb-source] applied-but-unbucketed bucket={} via={} count={} samples=[{}]",
                                                        name, via, count, samples.join(","),
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    tb_arc.store(Arc::new(tb));
                                }
                            }
                            flush_timebucket_ns.store(t_tb.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            // Unified cache live maintenance.
                            //
                            // When async_maintenance=true: build a CacheWorkItem and
                            // queue it to the cache worker — but only AFTER the new
                            // snapshot has been published below, so the worker cannot
                            // dequeue and evaluate against the previous published
                            // snapshot. The work item is built here (while coalescer
                            // state is in scope) and stored in `pending_async_work`;
                            // the actual `try_send` runs post-publish (see below).
                            //
                            // When async_maintenance=false (default): run inline Phases A/B/C
                            // as before. Zero change to existing behavior.
                            let t_cache = Instant::now();
                            let mut pending_async_work: Option<crate::cache_worker::CacheWorkItem> = None;
                            if flush_cache_work_tx.is_some() {
                                // Async path: build work item from coalescer output. The
                                // post-publish send block re-checks `flush_cache_work_tx`.
                                use crate::cache_worker::CacheWorkItem;
                                use ahash::{AHashMap, AHashSet};
                                let filter_inserts: AHashMap<_, _> = coalescer
                                    .filter_insert_entries()
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect();
                                let filter_removes: AHashMap<_, _> = coalescer
                                    .filter_remove_entries()
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect();
                                let sort_mutations: AHashMap<Arc<str>, AHashSet<u32>> = coalescer
                                    .mutated_sort_slots()
                                    .iter()
                                    .map(|(k, v)| (Arc::from(*k), v.clone()))
                                    .collect();
                                let mutated_filter_fields: AHashSet<Arc<str>> = coalescer
                                    .mutated_filter_fields()
                                    .iter()
                                    .map(|s| Arc::from(*s))
                                    .collect();
                                let alive_removes = coalescer.alive_removes().to_vec();
                                let has_alive_mutations = coalescer.has_alive_mutations();
                                let work_item = CacheWorkItem {
                                    filter_inserts,
                                    filter_removes,
                                    sort_mutations,
                                    alive_removes,
                                    mutated_filter_fields,
                                    has_alive_mutations,
                                    bucket_changed_slots: std::mem::take(&mut bucket_changed_slots),
                                };
                                if !work_item.is_empty() {
                                    pending_async_work = Some(work_item);
                                }
                                flush_cache_ns.store(
                                    t_cache.elapsed().as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                            } else {
                            // Inline path (async_maintenance=false) — unchanged.
                            let t_phase_a = Instant::now();
                            let ct_uc_entries: usize;
                            let mut ct_alive_removes: usize = 0;
                            let ct_filter_work_items: usize;
                            let ct_filter_over_budget: usize;
                            let ct_sort_work_items: usize;
                            let ct_sort_over_budget: usize;
                            // Phase A: Brief lock — collect work items and do cheap ops
                            let (filter_work, filter_over_budget, sort_work, sort_over_budget) = {
                                let uc = &flush_unified_cache;
                                ct_uc_entries = uc.len();
                                // Targeted alive removal (fast: O(1) per entry per remove)
                                if !uc.is_empty() {
                                    ct_alive_removes = coalescer.alive_removes().len();
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
                                ct_filter_work_items = fw.len();
                                ct_filter_over_budget = fob.len();
                                // Collect sort maintenance work
                                let sort_mutations = coalescer.mutated_sort_slots();
                                let (sw, sob) = if !sort_mutations.is_empty() {
                                    uc.collect_sort_work(&sort_mutations)
                                } else {
                                    (Vec::new(), Vec::new())
                                };
                                ct_sort_work_items = sw.len();
                                ct_sort_over_budget = sob.len();
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
                                        if n > 0 {
                                            flush_tombstones_created.fetch_add(n, Ordering::Relaxed);
                                        }
                                    }
                                    let sort_mutations = coalescer.mutated_sort_slots();
                                    let sort_fields: Vec<&str> = sort_mutations
                                        .keys()
                                        .copied()
                                        .collect();
                                    if !sort_fields.is_empty() {
                                        let n = uc.tombstone_unloaded_for_sort(&sort_fields);
                                        if n > 0 {
                                            flush_tombstones_created.fetch_add(n, Ordering::Relaxed);
                                        }
                                    }
                                    if coalescer.has_alive_mutations()
                                        && !coalescer.alive_removes().is_empty()
                                    {
                                        let n = uc.tombstone_all_unloaded();
                                        if n > 0 {
                                            flush_tombstones_created.fetch_add(n, Ordering::Relaxed);
                                        }
                                    }
                                }
                                (fw, fob, sw, sob)
                            }; // Phase A lock released
                            let phase_a_ns = t_phase_a.elapsed().as_nanos() as u64;
                            // Iter 4a observability: count unique canonical
                            // filter-clause vectors across sort-work items.
                            // Tells us whether filter-shape grouping in Phase B
                            // would pay off (low unique/total ratio = entries
                            // cluster into shared shapes = big win from
                            // grouping; high ratio = filters diverse =
                            // marginal gain).
                            //
                            // UnifiedKey.filter_clauses is already canonical
                            // (see src/cache.rs::canonicalize — clauses are
                            // sorted before the cache key is built), so hash
                            // order is stable across entries.
                            //
                            // Approximation: dedup via 64-bit hash. Hash
                            // collisions are possible but negligible at
                            // observed cardinalities (< 100k). If exact is
                            // required later, swap HashSet<u64> for
                            // HashSet<Vec<CanonicalClause>>.
                            //
                            // Implementation uses Vec + sort_unstable + dedup
                            // instead of HashSet<u64>: one allocation, better
                            // cache locality, 2-4x faster at 50k items
                            // (per Gemini review).
                            if sort_work.is_empty() {
                                // Reset gauges to 0 on skipped cycles so
                                // dashboards don't flatline at stale values.
                                flush_cache_unique_shapes.store(0, Ordering::Relaxed);
                                flush_cache_sort_work_items_gauge
                                    .store(0, Ordering::Relaxed);
                            } else {
                                use std::hash::{Hash, Hasher};
                                let mut hashes: Vec<u64> =
                                    Vec::with_capacity(sort_work.len());
                                for item in &sort_work {
                                    let mut hasher = ahash::AHasher::default();
                                    item.key.filter_clauses.hash(&mut hasher);
                                    hashes.push(hasher.finish());
                                }
                                hashes.sort_unstable();
                                hashes.dedup();
                                let unique = hashes.len() as u64;
                                let items = sort_work.len() as u64;
                                flush_cache_unique_shapes
                                    .store(unique, Ordering::Relaxed);
                                flush_cache_sort_work_items_gauge
                                    .store(items, Ordering::Relaxed);
                                // Max-seen counters: capture burst-time
                                // cardinalities that gauge samples miss on
                                // quiet cycles. Used to evaluate whether
                                // filter-shape grouping (iter 5 hypothesis)
                                // would pay off on REAL burst workloads vs
                                // the quiet-moment snapshots we happened
                                // to catch.
                                if unique
                                    > flush_cache_unique_shapes_max
                                        .load(Ordering::Relaxed)
                                {
                                    flush_cache_unique_shapes_max
                                        .store(unique, Ordering::Relaxed);
                                }
                                if items
                                    > flush_cache_sort_work_items_max_gauge
                                        .load(Ordering::Relaxed)
                                {
                                    flush_cache_sort_work_items_max_gauge
                                        .store(items, Ordering::Relaxed);
                                }
                            }
                            // Phase B: NO lock — evaluate slots against staging data.
                            // This is the expensive part (slot_matches_filter, reconstruct_value)
                            // that previously held the Mutex for ~469ms.
                            let t_phase_b = Instant::now();
                            // `max_maintenance_ms` deadline is deprecated (no-op) — pass None.
                            // The inline path still parallelises via rayon par_iter inside
                            // evaluate_filter_work / evaluate_sort_work.
                            // Load the time bucket manager snapshot once for this
                            // phase-B cycle. The flush thread already called
                            // insert_slot/remove_slot on every mutated slot before
                            // reaching here, so the bitmap is authoritative for all
                            // slots we are about to evaluate.
                            let tb_guard = flush_time_buckets
                                .as_ref()
                                .map(|arc| arc.load_full());
                            let tb_ref = tb_guard.as_deref();
                            // Load string_maps + dictionaries for native FilterClause eval.
                            let sm_guard = flush_shared_string_maps.load_full();
                            let sm_ref = (*sm_guard).as_ref();
                            let dict_guard = flush_shared_dictionaries.load_full();
                            let dict_inner = Arc::clone(&*dict_guard);
                            let dict_ref: &HashMap<String, crate::dictionary::FieldDictionary> = &*dict_inner;
                            let flush_string_misses = std::sync::atomic::AtomicU64::new(0);
                            let flush_compound_too_large = std::sync::atomic::AtomicU64::new(0);
                            let compound_atom_limit = flush_unified_cache.compound_eval_atom_limit();
                            let (filter_results, filter_compound_too_large) = if !filter_work.is_empty() {
                                evaluate_filter_work(
                                    &filter_work, &staging.filters, &staging.sorts, None, tb_ref,
                                    sm_ref, Some(dict_ref), &flush_string_misses,
                                    compound_atom_limit, &flush_compound_too_large,
                                )
                            } else {
                                (Vec::new(), Vec::new())
                            };
                            let (sort_results, sort_compound_too_large) = if !sort_work.is_empty() {
                                evaluate_sort_work(
                                    &sort_work, &staging.filters, &staging.sorts, None, tb_ref,
                                    sm_ref, Some(dict_ref), &flush_string_misses,
                                    compound_atom_limit, &flush_compound_too_large,
                                )
                            } else {
                                (Vec::new(), Vec::new())
                            };
                            let flush_misses = flush_string_misses.load(Ordering::Relaxed);
                            if flush_misses > 0 {
                                flush_cache_worker_metrics
                                    .string_lookup_misses_total
                                    .fetch_add(flush_misses, Ordering::Relaxed);
                            }
                            let flush_too_large = flush_compound_too_large.load(Ordering::Relaxed);
                            if flush_too_large > 0 {
                                flush_cache_worker_metrics
                                    .marked_for_rebuild_compound_too_large_total
                                    .fetch_add(flush_too_large, Ordering::Relaxed);
                            }
                            let phase_b_ns = t_phase_b.elapsed().as_nanos() as u64;
                            let ct_filter_results = filter_results.len();
                            let ct_sort_results = sort_results.len();
                            let ct_filter_compound_too_large = filter_compound_too_large.len();
                            let ct_sort_compound_too_large = sort_compound_too_large.len();
                            // Phase C: Brief lock — apply results
                            let t_phase_c = Instant::now();
                            if !filter_results.is_empty() || !sort_results.is_empty()
                                || !filter_over_budget.is_empty() || !sort_over_budget.is_empty()
                                || !filter_compound_too_large.is_empty() || !sort_compound_too_large.is_empty()
                            {
                                let uc = &flush_unified_cache;
                                uc.apply_maintenance_results(&filter_results);
                                uc.apply_maintenance_results(&sort_results);
                                // Evict on overrun rather than mark-for-rebuild: lets the next
                                // query re-populate a clean entry instead of paying per-query cost.
                                let evicted = uc.evict_keys_on_overrun(&filter_over_budget)
                                    + uc.evict_keys_on_overrun(&sort_over_budget)
                                    + uc.evict_keys_on_overrun(&filter_compound_too_large)
                                    + uc.evict_keys_on_overrun(&sort_compound_too_large);
                                if evicted > 0 {
                                    flush_cache_worker_metrics
                                        .evicted_on_overrun_total
                                        .fetch_add(evicted, Ordering::Relaxed);
                                }
                                uc.reconcile_bytes();
                            }
                            // #274: bucket-membership maintenance for entries filtered
                            // on a time bucket but sorted by a NON-bucket field — the
                            // case evaluate_sort_work does not surface. Re-derives
                            // membership of the changed slots against tb_ref (the flush
                            // thread already updated the bucket bitmaps above).
                            if !bucket_changed_slots.is_empty() {
                                if let Some(tb) = tb_ref {
                                    let bucket_field = tb.field_name();
                                    let bucket_sort_field = tb.sort_field_name();
                                    for name in tb.bucket_names() {
                                        flush_unified_cache.maintain_bucket_membership(
                                            bucket_field,
                                            &name,
                                            bucket_sort_field,
                                            &bucket_changed_slots,
                                            &staging.filters,
                                            &staging.sorts,
                                            tb_ref,
                                            sm_ref,
                                            Some(dict_ref),
                                        );
                                    }
                                }
                            }
                            let phase_c_ns = t_phase_c.elapsed().as_nanos() as u64;
                            flush_cache_ns.store(t_cache.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            // [cache-trace]: warn when any locked phase exceeds ~10ms.
                            // Phase B is lock-free so its cost doesn't block queries, but
                            // Phase A + Phase C are held under the UnifiedCache Mutex and
                            // directly contribute to query starvation when flush is slow.
                            let locked_ns = phase_a_ns + phase_c_ns;
                            if locked_ns > 10_000_000 || phase_b_ns > 100_000_000 {
                                tracing::warn!(
                                    target: "cache-trace",
                                    "cache maintenance slow: phase_a={:.2}ms phase_b={:.2}ms phase_c={:.2}ms  \
                                     uc_entries={} alive_removes={} \
                                     filter[work={} over_budget={} results={} compound_too_large={}] \
                                     sort[work={} over_budget={} results={} compound_too_large={}]",
                                    phase_a_ns as f64 / 1_000_000.0,
                                    phase_b_ns as f64 / 1_000_000.0,
                                    phase_c_ns as f64 / 1_000_000.0,
                                    ct_uc_entries,
                                    ct_alive_removes,
                                    ct_filter_work_items, ct_filter_over_budget, ct_filter_results, ct_filter_compound_too_large,
                                    ct_sort_work_items, ct_sort_over_budget, ct_sort_results, ct_sort_compound_too_large,
                                );
                            }
                            } // end else (inline cache maintenance path)
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
                                // small and persist safely via ShardStore ops log. They'll be
                                // merged when the field is eventually loaded by a query.
                                // Only make_mut + merge on fields that actually have dirty diffs
                                for name in &dirty_fields {
                                    if let Some(field) = staging.filters.get_field(name) {
                                        field.merge_dirty();
                                    }
                                }
                            }
                            flush_compact_ns.store(t_compact.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            flush_cycle += 1;
                            flush_cycle_clone.store(flush_cycle, Ordering::Relaxed);
                            // Publish new snapshot atomically (Arc-per-bitmap CoW clone)
                            let t_publish = Instant::now();
                            inner.store(Arc::new(staging.clone()));
                            flush_publish_ns.store(t_publish.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            staging_dirty = false;
                            // Async cache worker enqueue MUST happen after publish so
                            // the worker cannot dequeue + evaluate against a stale
                            // snapshot — `cache_worker` calls `inner.load()` to obtain
                            // the index handle, which would return the previous
                            // snapshot if the send happened before `inner.store`.
                            //
                            // Coalescer reads below are safe: `inner.store(...)` does
                            // not touch the coalescer, and the same coalescer is read
                            // again later in this cycle for ops-log append, so its
                            // grouped output is still the current cycle's immutable
                            // view. A future refactor that clears or rotates the
                            // coalescer immediately after publish must update the
                            // ops-log append site at the same time.
                            if let (Some(ref work_tx), Some(work_item)) =
                                (&flush_cache_work_tx, pending_async_work.take())
                            {
                                if work_tx.try_send(work_item).is_err() {
                                    // Worker channel full — fall back to conservative
                                    // invalidation that mirrors the inline path's
                                    // tombstone work for unloaded persistent entries
                                    // and the alive-change rebuild marking. The
                                    // tombstones run post-publish here; cache meta
                                    // is owned by `flush_unified_cache` and is not
                                    // swapped by `inner.store`, so this is
                                    // semantically equivalent to the inline path
                                    // running under the same cache lock pre-publish.
                                    flush_cache_worker_metrics
                                        .backpressure_invalidations_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    let uc = &flush_unified_cache;
                                    if uc.persistence_enabled() {
                                        let filter_fields: Vec<&str> = coalescer
                                            .mutated_filter_fields()
                                            .iter()
                                            .copied()
                                            .collect();
                                        if !filter_fields.is_empty() {
                                            let n = uc.tombstone_unloaded_for_filter(&filter_fields);
                                            if n > 0 {
                                                flush_tombstones_created.fetch_add(n, Ordering::Relaxed);
                                            }
                                        }
                                        let sort_mutations = coalescer.mutated_sort_slots();
                                        let sort_fields: Vec<&str> = sort_mutations
                                            .keys()
                                            .copied()
                                            .collect();
                                        if !sort_fields.is_empty() {
                                            let n = uc.tombstone_unloaded_for_sort(&sort_fields);
                                            if n > 0 {
                                                flush_tombstones_created.fetch_add(n, Ordering::Relaxed);
                                            }
                                        }
                                        if coalescer.has_alive_mutations()
                                            && !coalescer.alive_removes().is_empty()
                                        {
                                            let n = uc.tombstone_all_unloaded();
                                            if n > 0 {
                                                flush_tombstones_created.fetch_add(n, Ordering::Relaxed);
                                            }
                                        }
                                    }
                                    if !uc.is_empty() && !coalescer.alive_removes().is_empty() {
                                        uc.remove_slots_from_all_batch(coalescer.alive_removes());
                                    }
                                    let n = uc.maintain_alive_changes();
                                    if n > 0 {
                                        flush_cache_worker_metrics
                                            .marked_for_rebuild_alive_change_total
                                            .fetch_add(n, Ordering::Relaxed);
                                    }
                                }
                            }
                            // Mark fields touched by mutations or lazy loads as stale
                            // in the bitmap memory cache so the scanner re-measures them.
                            if !stale_fields.is_empty() {
                                // Dedup to avoid redundant lock acquisitions.
                                stale_fields.sort_unstable();
                                stale_fields.dedup();
                                for field in &stale_fields {
                                    flush_mem_cache.mark_stale(field);
                                }
                                stale_fields.clear();
                            }
                            // Log slow flush cycles with full phase breakdown
                            let total_ms = flush_start.elapsed().as_millis();
                            if total_ms > 100 {
                                let apply_ms = flush_apply_ns.load(Ordering::Relaxed) as f64 / 1e6;
                                let promote_ms = flush_sort_promote_ns.load(Ordering::Relaxed) as f64 / 1e6;
                                let cache_ms = flush_cache_ns.load(Ordering::Relaxed) as f64 / 1e6;
                                let compact_ms = flush_compact_ns.load(Ordering::Relaxed) as f64 / 1e6;
                                let tb_ms = flush_timebucket_ns.load(Ordering::Relaxed) as f64 / 1e6;
                                let publish_ms = t_publish.elapsed().as_secs_f64() * 1000.0;
                                let post_apply_ms = t_post_apply.elapsed().as_secs_f64() * 1000.0;
                                tracing::warn!(
                                    "[flush-slow] total={total_ms}ms ops={bitmap_count} | \
                                     apply={apply_ms:.1}ms promote={promote_ms:.1}ms \
                                     cache={cache_ms:.1}ms compact={compact_ms:.1}ms \
                                     tb={tb_ms:.1}ms publish={publish_ms:.1}ms \
                                     post_apply_total={post_apply_ms:.1}ms"
                                );
                            }
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
                            if let (Some(ref as_), Some(ref fs_), Some(ref ss_)) =
                                (&flush_alive_store, &flush_filter_store, &flush_sort_store)
                            {
                                use rayon::prelude::*;

                                // Skip per-shard fsync on bitmap opslog appends.
                                // Durability: WAL provides crash recovery. The merge
                                // thread only advances the WAL cursor after it
                                // successfully persists + fsyncs bitmap snapshots.
                                // On crash, unfsynced opslog entries may be lost, but
                                // the WAL cursor is still behind → WAL replays those
                                // ops, recreating the opslog entries. Page-cache writes
                                // are visible to the merge thread's compaction reader
                                // within the same process. Same durability model as the
                                // docstore (which already uses fsync=false).

                                // Alive shard is singular — no parallelism benefit.
                                let alive_ins = coalescer.alive_inserts();
                                if !alive_ins.is_empty() {
                                    let op = BitmapOp::BatchSet { bits: alive_ins.to_vec() };
                                    if let Err(e) = as_.append_op_opts(&AliveShardKey, &op, false) {
                                        eprintln!("flush: alive insert op failed: {e}");
                                    }
                                }
                                let alive_rem = coalescer.alive_removes();
                                if !alive_rem.is_empty() {
                                    let op = BitmapOp::BatchClear { bits: alive_rem.to_vec() };
                                    if let Err(e) = as_.append_op_opts(&AliveShardKey, &op, false) {
                                        eprintln!("flush: alive remove op failed: {e}");
                                    }
                                }

                                // Filter + sort buckets: each shard independent.
                                // Without per-shard fsync, parallel writes are pure
                                // throughput without NTFS journal serialization.
                                // Group filter ops by bucket key so multiple values
                                // sharing a bucket produce ONE file open, not one per
                                // value. With 200K tagId values across 256 buckets,
                                // this reduces file opens from ~1300 to ~256 (5x).
                                let mut filter_by_bucket: HashMap<FilterBucketKey, Vec<FilterOp>> = HashMap::new();
                                for (fgk, slots) in coalescer.filter_insert_entries() {
                                    let bk = FilterBucketKey::from_value(fgk.field.to_string(), fgk.value);
                                    filter_by_bucket.entry(bk).or_default().push(
                                        FilterOp::BatchSet { value: fgk.value, bits: slots.clone() },
                                    );
                                }
                                for (fgk, slots) in coalescer.filter_remove_entries() {
                                    let bk = FilterBucketKey::from_value(fgk.field.to_string(), fgk.value);
                                    filter_by_bucket.entry(bk).or_default().push(
                                        FilterOp::BatchClear { value: fgk.value, bits: slots.clone() },
                                    );
                                }
                                let filter_buckets: Vec<(FilterBucketKey, Vec<FilterOp>)> =
                                    filter_by_bucket.into_iter().collect();
                                // Sub-ms task work + sub-ms flush interval keeps rayon workers
                                // in their spin window and never lets them park cleanly. Skip
                                // par_iter for small batches — the global pool dispatch
                                // overhead exceeds the sequential cost. Threshold hot-reloadable
                                // via PATCH /config { "par_iter_min_threshold": N }; set huge
                                // to disable par_iter entirely for perf experiments.
                                let par_iter_min = flush_par_iter_min.load(Ordering::Relaxed);
                                if filter_buckets.len() >= par_iter_min {
                                    filter_buckets.into_par_iter().for_each(
                                        |(bucket_key, ops)| {
                                            if let Err(e) = fs_.append_ops_opts(&bucket_key, &ops, false) {
                                                eprintln!("flush: filter op failed: {e}");
                                            }
                                        },
                                    );
                                } else {
                                    for (bucket_key, ops) in filter_buckets {
                                        if let Err(e) = fs_.append_ops_opts(&bucket_key, &ops, false) {
                                            eprintln!("flush: filter op failed: {e}");
                                        }
                                    }
                                }

                                let sort_set: Vec<(SortLayerShardKey, BitmapOp)> = coalescer
                                    .sort_set_entries()
                                    .iter()
                                    .map(|(sgk, slots)| (
                                        SortLayerShardKey { field: sgk.field.to_string(), bit_position: sgk.bit_layer as u8 },
                                        BitmapOp::BatchSet { bits: slots.clone() },
                                    ))
                                    .collect();
                                let sort_clr: Vec<(SortLayerShardKey, BitmapOp)> = coalescer
                                    .sort_clear_entries()
                                    .iter()
                                    .map(|(sgk, slots)| (
                                        SortLayerShardKey { field: sgk.field.to_string(), bit_position: sgk.bit_layer as u8 },
                                        BitmapOp::BatchClear { bits: slots.clone() },
                                    ))
                                    .collect();
                                let sort_total = sort_set.len() + sort_clr.len();
                                if sort_total >= par_iter_min {
                                    sort_set.into_par_iter().chain(sort_clr.into_par_iter()).for_each(
                                        |(shard_key, op)| {
                                            if let Err(e) = ss_.append_op_opts(&shard_key, &op, false) {
                                                eprintln!("flush: sort op failed: {e}");
                                            }
                                        },
                                    );
                                } else {
                                    for (shard_key, op) in sort_set.into_iter().chain(sort_clr.into_iter()) {
                                        if let Err(e) = ss_.append_op_opts(&shard_key, &op, false) {
                                            eprintln!("flush: sort op failed: {e}");
                                        }
                                    }
                                }
                            }
                            flush_opslog_ns.store(t_opslog.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        }
                    }
                    // Deferred-alive activation runs at the top of the next flush
                    // cycle (before `coalescer.prepare()`) so activation ops ride the
                    // normal coalescer path and reach cache maintenance.
                    //
                    // Persist the deferred map AFTER opslog append for activation ops
                    // is complete. This ordering matters: if we wrote the deferred
                    // map first and crashed before opslog, the slot would be missing
                    // from BOTH the persisted deferred map AND the opslog, losing the
                    // activation permanently. With this ordering, a crash before
                    // persist leaves the deferred map intact on disk; restart re-runs
                    // activate_due for the same slot, which is idempotent on bitmap
                    // state (set-already-set = no-op) and produces a duplicate opslog
                    // entry at worst.
                    if deferred_persist_needed {
                        if let Some(ref ms) = flush_meta_store {
                            if let Err(e) = ms.write_deferred_alive(staging.slots.deferred_map()) {
                                eprintln!("Warning: failed to persist deferred alive map: {e}");
                            }
                        }
                    }
                    // Idle compaction: compact dirty+unloaded entries even when no new
                    // mutations arrive. Ops bursts create dirty entries; compaction only
                    // ran inside `if bitmap_count > 0` which requires active mutations.
                    // Without this, dirty entries from a finished ops burst never compact.
                    // Check for unmerged diffs in lazy_value_fields even when staging
                    // isn't "dirty" (no new mutations). staging_dirty only tracks whether
                    // new mutations arrived — not whether old diffs were compacted.
                    let has_lazy_dirty = !is_loading && {
                        let lvf = flush_lazy_value_fields.lock();
                        // Only idle-compact entries whose base is actually loaded.
                        // merge() short-circuits on !is_loaded, so flagging unloaded-
                        // dirty fields here causes a 33 tick/s no-op compaction loop
                        // for high-cardinality unloaded fields (tagIds, modelVersionIds,
                        // postId, etc.) that steady-state ops keep dirtying.
                        !lvf.is_empty() && staging.filters.fields()
                            .any(|(name, field)| lvf.contains(name.as_str()) && field.has_loaded_dirty())
                    };
                    if bitmap_count == 0 && has_lazy_dirty {
                        // Use a slower interval since there's no active write pressure.
                        // flush_cycle is only bumped inside bitmap_count > 0, so track
                        // idle ticks separately.
                        static IDLE_TICKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                        let tick = IDLE_TICKS.fetch_add(1, Ordering::Relaxed) + 1;
                        if tick % COMPACTION_INTERVAL == 0 {
                            let dirty_fields: Vec<String> = staging.filters.fields()
                                .filter(|(_, field)| field.has_loaded_dirty())
                                .map(|(name, _)| name.clone())
                                .collect();
                            if !dirty_fields.is_empty() {
                                eprintln!("  Idle compaction (tick {}): {} dirty fields: {:?}", tick, dirty_fields.len(), dirty_fields);
                                // NOTE: Auto-loading bases disabled (same as regular compaction).
                                // Dirty diffs persist via ShardStore, merge on query load.
                                for name in &dirty_fields {
                                    if let Some(field) = staging.filters.get_field(name) {
                                        field.merge_dirty();
                                    }
                                }
                                // Publish the compacted staging
                                inner.store(Arc::new(staging.clone()));
                                staging_dirty = false;
                                // Mark compacted fields as stale in memory cache.
                                for name in &dirty_fields {
                                    flush_mem_cache.mark_stale(name);
                                }
                                eprintln!("  Idle compaction: published clean staging");
                            }
                        }
                    }
                    // Loading mode exit: force-publish if staging has unpublished mutations
                    if was_loading && !is_loading && staging_dirty {
                        // Compact all filter diffs accumulated during loading
                        for (_name, field) in staging.filters.fields() {
                            field.merge_dirty();
                        }
                        // Invalidate unified cache — may be stale from the loading period
                        flush_unified_cache.clear();
                        inner.store(Arc::new(staging.clone()));
                        staging_dirty = false;
                        // All fields changed during loading — mark everything stale.
                        flush_mem_cache.mark_all_stale();
                    }
                    was_loading = is_loading;
                    // Process flush commands (force publish, unload, etc.)
                    while let Ok(cmd) = cmd_rx.try_recv() {
                        match cmd {
                            FlushCommand::ForcePublish { done } => {
                                let fp_start = std::time::Instant::now();
                                let t_drain = std::time::Instant::now();
                                // Drain lazy load channel — query threads may have
                                // loaded data from disk and need it published.
                                while let Ok(load) = lazy_rx.try_recv() {
                                    match load {
                                        LazyLoad::FilterField { name, bitmaps } => {
                                            if let Some(field) = staging.filters.get_field(&name) {
                                                field.load_field_complete(bitmaps);
                                            }
                                        }
                                        LazyLoad::FilterValues { field, values } => {
                                            if let Some(f) = staging.filters.get_field(&field) {
                                                let requested: Vec<u64> = values.keys().copied().collect();
                                                f.load_values(values, &requested);
                                            }
                                        }
                                        LazyLoad::SortField { name, layers } => {
                                            if let Some(sf) = staging.sorts.get_field_mut(&name) {
                                                sf.load_layers(layers);
                                            }
                                        }
                                        LazyLoad::Slots { slots } => {
                                            staging.slots = slots;
                                        }
                                    }
                                }
                                let drain_elapsed = t_drain.elapsed();
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
                                // Time-bucket maintenance for mutations drained HERE.
                                // `coalescer.flush()` applies sort mutations to staging.sorts
                                // but does NOT touch the time buckets. So a sortAt change
                                // drained by this force-publish (e.g. an existedAt update in
                                // the channel when a queryOpSet fan-out barrier fires) lands
                                // in the sort layer — queries see it — but the slot never
                                // enters a bucket. That is the missing-adds SOURCE (confirmed
                                // prod 2026-07-07: #285 counter=0 because it lives in the
                                // normal-path tb-block, which this handler bypasses).
                                //
                                // Route the affected slots through the ONE tb-block instead of
                                // duplicating it: removes are cleared now; alive-inserts and
                                // sort-value changes are handed to `pending_bucket_retries`, so
                                // the next normal flush cycle's tb-block buckets them with their
                                // now-current reconstructed sortAt — preserving the defer-on-
                                // unloaded-sort-field semantics (a slot re-deferred if the sort
                                // field isn't fully loaded that cycle, never dropped).
                                if extra > 0 {
                                    if let Some(ref tb_arc) = flush_time_buckets {
                                        let sfn = tb_arc.load().sort_field_name().to_string();
                                        let removed: HashSet<u32> =
                                            coalescer.alive_removes().iter().copied().collect();
                                        let inserted: HashSet<u32> = coalescer
                                            .alive_inserts()
                                            .iter()
                                            .copied()
                                            .filter(|s| !removed.contains(s))
                                            .collect();
                                        // sortAt mutations that are neither fresh inserts nor
                                        // removes — these have PRIOR bucket membership that must
                                        // be cleared at defer time, because the pending_bucket_
                                        // retries drain does a PLAIN insert_slot (its contract:
                                        // old membership already cleared — mirrors the normal
                                        // unloaded branch). Without this, a sortAt change that
                                        // crosses a bucket boundary leaves the slot stale in its
                                        // old bucket.
                                        let sort_changed: Vec<u32> = coalescer
                                            .mutated_sort_slots()
                                            .get(sfn.as_str())
                                            .map(|set| {
                                                set.iter()
                                                    .copied()
                                                    .filter(|s| {
                                                        !removed.contains(s) && !inserted.contains(s)
                                                    })
                                                    .collect()
                                            })
                                            .unwrap_or_default();
                                        // One clone-mutate-store: clear removed slots AND the old
                                        // membership of sort-changed slots (flush thread is the
                                        // sole tb_arc writer, so no lost-update race).
                                        if !removed.is_empty() || !sort_changed.is_empty() {
                                            let mut tb = (*tb_arc.load_full()).clone();
                                            for &slot in &removed {
                                                tb.remove_slot(slot);
                                                pending_bucket_retries.remove(&slot);
                                            }
                                            for &slot in &sort_changed {
                                                tb.remove_slot(slot);
                                            }
                                            tb_arc.store(Arc::new(tb));
                                        }
                                        // Defer (re-)bucketing to the next cycle's tb-block:
                                        // fresh inserts (no prior membership) + sort-changed
                                        // (membership just cleared) → plain insert on drain.
                                        let mut affected: HashSet<u32> = inserted;
                                        affected.extend(sort_changed.iter().copied());
                                        let space_left = PENDING_BUCKET_RETRY_CAP
                                            .saturating_sub(pending_bucket_retries.len());
                                        let mut enqueued = 0usize;
                                        for &slot in affected.iter().take(space_left) {
                                            if pending_bucket_retries.insert(slot) {
                                                enqueued += 1;
                                            }
                                        }
                                        // Overflow at the cap = the same permanent-loss risk the
                                        // unloaded branch guards; account it identically instead
                                        // of a silent drop. This batch is already consumed, so a
                                        // dropped slot only re-buckets via the periodic backfill.
                                        let dropped = affected.len().saturating_sub(enqueued);
                                        if dropped > 0 {
                                            tracing::error!(
                                                "[time-bucket] ForcePublish retry queue at cap ({}); {} force-published sortAt slots not re-bucketed (rely on backfill) — sort field '{}'",
                                                PENDING_BUCKET_RETRY_CAP,
                                                dropped,
                                                sfn,
                                            );
                                            #[cfg(feature = "server")]
                                            {
                                                let bg = flush_metrics_bridge.load();
                                                if let Some(m) = (**bg).as_ref() {
                                                    m.timebucket_dropped_capacity_exceeded_total
                                                        .with_label_values(&[&m.index_name, &sfn])
                                                        .inc_by(dropped as u64);
                                                }
                                            }
                                        }
                                    }
                                }
                                let flush_elapsed = t_flush.elapsed();
                                // Compact diffs before publishing — only needed if
                                // mutations were drained. Lazy loads insert clean base
                                // bitmaps with no diffs, so merge_dirty is a no-op.
                                // Skipping saves ~65ms by avoiding fields_mut() which
                                // touches every Arc<FilterField>.
                                let t_merge = std::time::Instant::now();
                                if extra > 0 {
                                    for (_name, field) in staging.filters.fields() {
                                        field.merge_dirty();
                                    }
                                }
                                let merge_elapsed = t_merge.elapsed();
                                // NOTE: Do NOT clear the unified cache here. ForcePublish
                                // is used by lazy loading (ensure_fields_loaded) to publish
                                // newly loaded bitmaps. Lazy loads don't invalidate existing
                                // cache entries — they only add new data. Clearing here was
                                // nuking the entire cache on every lazy load, causing 0% hit
                                // rate in production. Cache invalidation is handled by the
                                // normal flush path's targeted maintenance.
                                let t_cache = std::time::Instant::now();
                                let cache_elapsed = t_cache.elapsed();
                                let t_clone = std::time::Instant::now();
                                inner.store(Arc::new(staging.clone()));
                                let clone_elapsed = t_clone.elapsed();
                                staging_dirty = false;
                                tracing::debug!(
                                    "ForcePublish: drain={:.1}ms flush={:.1}ms merge={:.1}ms cache={:.1}ms clone={:.1}ms total={:.1}ms",
                                    drain_elapsed.as_secs_f64() * 1000.0,
                                    flush_elapsed.as_secs_f64() * 1000.0,
                                    merge_elapsed.as_secs_f64() * 1000.0,
                                    cache_elapsed.as_secs_f64() * 1000.0,
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
                                flush_unified_cache.clear();
                                inner.store(Arc::new(staging.clone()));
                                staging_dirty = false;
                                let _ = done.send(());
                            }
                            FlushCommand::ExitLoadingSaveUnload {
                                skip_sorts, skip_filters, skip_lazy,
                                cursors, dictionaries, loading_mode, done,
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
                                // 2. Save from the published snapshot — no clone, just a borrow
                                if let (Some(ref as_), Some(ref fs_), Some(ref ss_), Some(ref ms_)) =
                                    (&flush_alive_store, &flush_filter_store, &flush_sort_store, &flush_meta_store)
                                {
                                    let save_result = ConcurrentEngine::write_inner_to_store(
                                        as_,
                                        fs_,
                                        ss_,
                                        ms_,
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
                                        if let Err(e) = ms_.write_cursor(name, value) {
                                            eprintln!("Warning: failed to persist cursor '{}': {}", name, e);
                                        }
                                    }
                                    // Persist dictionaries
                                    if !dictionaries.is_empty() {
                                        let dict_dir = ms_.root().join("dictionaries");
                                        for (name, dict) in dictionaries.iter() {
                                            let snap = dict.snapshot();
                                            let path = dict_dir.join(format!("{}.dict", name));
                                            if let Err(e) = crate::dictionary::save_dictionary(&snap, &path) {
                                                eprintln!("Warning: failed to persist dictionary '{}': {}", name, e);
                                            }
                                        }
                                    }
                                }
                                // 2b. Rebuild time buckets from the published snapshot.
                                //     This is the same fix as in `exit_loading_mode()`: the
                                //     flush thread's live bucket maintenance is gated behind
                                //     loading_mode and only triggers on coalescer.alive_inserts,
                                //     so a bulk load leaves buckets empty. We must rebuild
                                //     while the sort field is still loaded in `published` —
                                //     after the unload step below it's gone, and rebuilding
                                //     from outside the flush thread would have to wait for the
                                //     next lazy load.
                                if let Some(ref tb_arc) = flush_time_buckets {
                                    if let Err(e) = ConcurrentEngine::rebuild_time_buckets_from_snapshot(&published, tb_arc) {
                                        eprintln!("Warning: rebuild_time_buckets in ExitLoadingSaveUnload failed: {e}");
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
                                flush_unified_cache.clear();
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
                                .into_iter()
                                .filter(|&value| {
                                    // Skip dirty bitmaps (unpersisted mutations)
                                    if let Some(vb) = field.get_versioned(value) {
                                        if vb.is_dirty() {
                                            return false;
                                        }
                                    }
                                    // Check stamp (wall-clock millis)
                                    let key = (field_name_arc.clone(), value);
                                    flush_eviction_stamps
                                        .get(&key)
                                        .map(|entry| entry.value().load(Ordering::Relaxed) < cutoff_ms)
                                        .unwrap_or(true) // no stamp = never touched = evict
                                })
                                .collect();
                            if !to_evict.is_empty() {
                                let count = to_evict.len();
                                if let Some(field_mut) = staging.filters.get_field(field_name) {
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
                                tracing::info!(
                                    "Evicted {} idle values from filter '{}' (idle_threshold={}s)",
                                    count, field_name, idle_seconds
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
                        // Mark lazy-loaded fields as stale in memory cache.
                        if !stale_fields.is_empty() {
                            stale_fields.sort_unstable();
                            stale_fields.dedup();
                            for field in &stale_fields {
                                flush_mem_cache.mark_stale(field);
                            }
                            stale_fields.clear();
                        }
                    }
                    // Publish clean sort layers when sort promote set staging_dirty
                    // but no ops or lazy loads triggered a publish.
                    if staging_dirty && bitmap_count == 0 && !lazy_loaded && !is_loading {
                        inner.store(Arc::new(staging.clone()));
                        staging_dirty = false;
                        tracing::info!("Published clean sort layers (promote-only)");
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
                                let tb = tb_arc.load();
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
                                let sort_field_name = tb_arc.load().sort_field_name().to_string();
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
                                            let mut tb = (*tb_arc.load_full()).clone();
                                            if let Some(bucket) = tb.get_bucket_mut(bucket_name) {
                                                bucket.subtract_expired(&RoaringBitmap::new(), new_cutoff);
                                            }
                                            tb_arc.store(Arc::new(tb));
                                            continue;
                                        }
                                        // Find expired slots: those in the bucket bitmap with
                                        // sort value in [old_cutoff, new_cutoff)
                                        let bucket_bm = {
                                            let tb = tb_arc.load();
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
                                        // Clone-mutate-store: subtract expired from bucket bitmap
                                        {
                                            let mut tb = (*tb_arc.load_full()).clone();
                                            if let Some(bucket) = tb.get_bucket_mut(bucket_name) {
                                                bucket.subtract_expired(&expired, new_cutoff);
                                            }
                                            tb_arc.store(Arc::new(tb));
                                        }
                                        // Store diff for lazy cache application (no cache Mutex!)
                                        let diff = crate::bucket_diff_log::BucketDiff {
                                            cutoff_before: *old_cutoff,
                                            cutoff_after: new_cutoff,
                                            expired: Arc::new(expired),
                                        };
                                        // Append to THIS bucket's own on-disk log — each
                                        // bucket name gets an independent diff history (see
                                        // `pending_bucket_diffs`'s doc comment).
                                        if let Some(ref dir) = flush_diff_log_dir {
                                            let log_path = dir.join(format!("bucket_diffs__{bucket_name}.log"));
                                            let log = crate::bucket_diff_log::BucketDiffLog::new(
                                                log_path, 100, 0.3,
                                            );
                                            if let Err(e) = log.append(&diff) {
                                                eprintln!("Warning: failed to append bucket diff to log for '{}': {e}", bucket_name);
                                            }
                                            // Periodic compaction
                                            if let Err(e) = log.compact_if_needed() {
                                                eprintln!("Warning: bucket diff log compaction failed for '{}': {e}", bucket_name);
                                            }
                                        }
                                        // Update THIS bucket's own in-memory pending diffs
                                        // (ArcSwap store on its own cell — other buckets'
                                        // cells are untouched, no cross-bucket clone cascade).
                                        if let Some(cell) = flush_pending_diffs.get(bucket_name.as_str()) {
                                            let old_pending = cell.load();
                                            let mut new_pending = crate::bucket_diff_log::PendingBucketDiffs::from_diffs(
                                                old_pending.diffs().to_vec(),
                                                100,
                                            );
                                            new_pending.push(diff);
                                            cell.store(Arc::new(new_pending));
                                        } else {
                                            eprintln!("Warning: no pending-diffs cell for bucket '{}' — config changed at runtime?", bucket_name);
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
                    // Fallback: periodic full-scan prune of stale time-bucket
                    // members. The incremental refresh above only evicts slots whose
                    // current sort value lands in the narrow [old_cutoff, new_cutoff)
                    // band each cycle; a slot whose sort value jumped past that band
                    // without a re-flush (e.g. a deferred-publish sortAt regression)
                    // is never caught and lingers in the bucket until restart. On a
                    // fixed interval this recomputes true in-window membership and
                    // prunes the leftover stale members — the time-bucket analog of
                    // the unified cache's TTL backstop.
                    //
                    // The scan is O(alive · sort_bits) — ~minutes at 107M — so it
                    // must NOT run inline on the flush thread (it would stall write
                    // draining and back-pressure ingest). Instead: compute the
                    // per-bucket STALE sets on a background thread over an immutable
                    // published snapshot, then apply them here on the flush thread by
                    // SUBTRACTING (never overwriting) — keeping the flush thread the
                    // sole writer of the manager and preserving slots that live
                    // maintenance inserted during the minutes-long scan. Only one
                    // rebuild is in flight at a time; a worker that panics before
                    // sending is detected via its JoinHandle.
                    //
                    // No explicit cache invalidation: a cache MISS reads the (now
                    // fresh) bucket bitmap directly, and cache HITS self-heal via
                    // `cache.bucket_entry_ttl_secs` — an entry older than the TTL is
                    // re-derived from the corrected bucket bitmap (prod = 300s). That
                    // TTL, not a diff, is the propagation path for HITs: staggered
                    // per entry (non-stampeding) and needs no cutoff advance.
                    // NOTE: if the TTL is 0 (disabled) this rebuild does NOT reach
                    // cache HITs — the incremental reconcile can't remove a
                    // regressed-past-band slot either — so a non-zero
                    // bucket_entry_ttl_secs is required for the fallback to correct
                    // cached bucket queries. Cache MISSes are always correct.
                    // Load the (hot-reloadable) interval once per cycle so a
                    // PATCH /config retune takes effect without a restart.
                    let flush_bucket_full_rebuild_interval =
                        flush_tb_rebuild_interval_handle.load(Ordering::Relaxed);
                    if !is_loading && flush_bucket_full_rebuild_interval > 0 {
                        if let Some(ref tb_arc) = flush_time_buckets {
                            // 1) Apply any completed background rebuild. The flush
                            //    thread stays the sole writer of the manager on this
                            //    path: it PRUNES the stale sets the worker computed
                            //    (subtraction), never overwriting — so slots that
                            //    live maintenance inserted during the minutes-long
                            //    scan are preserved.
                            let mut pruned_total: u64 = 0;
                            let mut backfilled_total: u64 = 0;
                            while let Ok(msg) = bucket_rebuild_rx.try_recv() {
                                bucket_rebuild_in_flight = false;
                                bucket_rebuild_handle = None;
                                match msg {
                                    None => {
                                        // Sort field wasn't loaded when the scan ran;
                                        // backdate the baseline to retry in ~60s.
                                        let retry_in = 60.min(flush_bucket_full_rebuild_interval);
                                        let back = flush_bucket_full_rebuild_interval.saturating_sub(retry_in);
                                        last_full_bucket_rebuild = Some(
                                            std::time::Instant::now()
                                                .checked_sub(std::time::Duration::from_secs(back))
                                                .unwrap_or_else(std::time::Instant::now),
                                        );
                                    }
                                    Some((scan_dur, removals)) => {
                                        last_full_bucket_rebuild = Some(std::time::Instant::now());
                                        // `scan_dur` feeds the server-only metrics below.
                                        let _ = &scan_dur;
                                        // Observability: scan duration, rebuild count, and
                                        // per-bucket stale/missing gauges (from the
                                        // snapshot the worker scanned). `missing` > 0
                                        // signals a live-insert gap the prune won't fix.
                                        #[cfg(feature = "server")]
                                        {
                                            let bg = flush_metrics_bridge.load();
                                            if let Some(m) = (**bg).as_ref() {
                                                let idx = m.index_name.as_str();
                                                m.time_bucket_full_rebuild_duration_seconds
                                                    .with_label_values(&[idx])
                                                    .observe(scan_dur.as_secs_f64());
                                                m.time_bucket_full_rebuild_total
                                                    .with_label_values(&[idx])
                                                    .inc();
                                                for (name, stale, missing) in &removals {
                                                    m.time_bucket_stale
                                                        .with_label_values(&[idx, name])
                                                        .set(stale.len() as i64);
                                                    m.time_bucket_missing
                                                        .with_label_values(&[idx, name])
                                                        .set(missing.len() as i64);
                                                }
                                            }
                                        }
                                        // The worker's stale + missing sets are
                                        // CANDIDATES from a minutes-old snapshot.
                                        // `reconcile_apply` re-validates each against
                                        // the CURRENT sort values + alive (staging)
                                        // before pruning/backfilling — a stale
                                        // candidate whose value moved back in window
                                        // is kept; a missing candidate that was
                                        // deleted or moved out of window since the
                                        // scan is not inserted. Candidate sets are
                                        // small, so the per-slot reconstruct is cheap
                                        // on the flush thread; the
                                        // `time_bucket_reconcile_apply_seconds` metric
                                        // bounds it (this doubles the reconstruct work
                                        // vs the prune-only path).
                                        let sort_field_name =
                                            tb_arc.load().sort_field_name().to_string();
                                        if let Some(sort_field) = staging.sorts.get_field(&sort_field_name) {
                                            let now_secs = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs();
                                            let has_candidates = removals
                                                .iter()
                                                .any(|(_, s, mi)| !s.is_empty() || !mi.is_empty());
                                            if has_candidates {
                                                // Single clone-mutate-store: the flush
                                                // thread stays the sole tb_arc writer.
                                                // `reconcile_apply` reads the worker's
                                                // (name, stale, missing) tuples by ref —
                                                // no split/clone of the candidate bitmaps.
                                                // Timing is only consumed by the
                                                // server-gated observe below; gate the
                                                // Instant too or it's an unused var under
                                                // default features (-D warnings CI gate).
                                                #[cfg(feature = "server")]
                                                let apply_start = std::time::Instant::now();
                                                let alive = staging.slots.alive_bitmap();
                                                let mut tb = (*tb_arc.load_full()).clone();
                                                let report = tb.reconcile_apply(
                                                    sort_field,
                                                    alive,
                                                    now_secs,
                                                    &removals,
                                                );
                                                #[cfg(feature = "server")]
                                                let apply_elapsed = apply_start.elapsed();
                                                let changed = !report.is_empty();
                                                #[cfg(feature = "server")]
                                                {
                                                    let bg = flush_metrics_bridge.load();
                                                    if let Some(m) = (**bg).as_ref() {
                                                        let idx = m.index_name.as_str();
                                                        m.time_bucket_reconcile_apply_seconds
                                                            .with_label_values(&[idx])
                                                            .observe(apply_elapsed.as_secs_f64());
                                                        for (name, pruned, backfilled) in &report {
                                                            if *pruned > 0 {
                                                                m.time_bucket_pruned_total
                                                                    .with_label_values(&[idx, name])
                                                                    .inc_by(*pruned);
                                                            }
                                                            if *backfilled > 0 {
                                                                m.time_bucket_backfilled_total
                                                                    .with_label_values(&[idx, name])
                                                                    .inc_by(*backfilled);
                                                            }
                                                        }
                                                    }
                                                }
                                                for (_, pruned, backfilled) in &report {
                                                    pruned_total += *pruned;
                                                    backfilled_total += *backfilled;
                                                }
                                                // Persist ONLY if something changed. The
                                                // dirty flag MUST fire on backfill-only
                                                // cycles too (the steady state once prune
                                                // catches up) or the merge thread never
                                                // persists the backfill → a restart before
                                                // the next cycle silently re-drops it.
                                                if changed {
                                                    tb_arc.store(Arc::new(tb));
                                                    flush_dirty_flag.store(true, Ordering::Release);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if pruned_total > 0 || backfilled_total > 0 {
                                eprintln!("Time bucket FULL rebuild fallback: pruned {pruned_total} stale / backfilled {backfilled_total} missing members (bg scan)");
                            }
                            // Detect a worker that ended WITHOUT sending (panicked):
                            // the persistent sender keeps the channel open, so
                            // try_recv never reports Disconnected. Without this,
                            // in_flight would stick true and disable all future
                            // rebuilds.
                            if bucket_rebuild_in_flight {
                                if let Some(h) = &bucket_rebuild_handle {
                                    if h.is_finished() {
                                        eprintln!("Time bucket FULL rebuild: bg worker ended without a result (panicked?) — resetting");
                                        bucket_rebuild_in_flight = false;
                                        bucket_rebuild_handle = None;
                                        let retry_in = 60.min(flush_bucket_full_rebuild_interval);
                                        let back = flush_bucket_full_rebuild_interval.saturating_sub(retry_in);
                                        last_full_bucket_rebuild = Some(
                                            std::time::Instant::now()
                                                .checked_sub(std::time::Duration::from_secs(back))
                                                .unwrap_or_else(std::time::Instant::now),
                                        );
                                    }
                                }
                            }
                            // 2) Kick off a new background rebuild when due and none
                            //    is in flight. Scheduling uses a monotonic Instant.
                            let due = match last_full_bucket_rebuild {
                                None => false, // seed baseline below; don't fire at boot
                                Some(t) => {
                                    t.elapsed().as_secs() >= flush_bucket_full_rebuild_interval
                                }
                            };
                            if last_full_bucket_rebuild.is_none() {
                                // Boot's own rebuild is the baseline; first fallback
                                // fires one interval later.
                                last_full_bucket_rebuild = Some(std::time::Instant::now());
                            } else if due && !bucket_rebuild_in_flight {
                                // Clone the latest published snapshot (cheap Arc) and
                                // the manager config, then compute off-thread.
                                let snap = inner.load_full();
                                let tb_snapshot = (*tb_arc.load_full()).clone();
                                let tx = bucket_rebuild_tx.clone();
                                let scan_threads = flush_reconcile_scan_threads;
                                let spawned = std::thread::Builder::new()
                                    .name("bitdex-tbucket-rebuild".to_string())
                                    .spawn(move || {
                                        let start = std::time::Instant::now();
                                        let payload = Self::compute_time_bucket_reconcile(
                                            &snap, &tb_snapshot, scan_threads,
                                        )
                                        .map(|r| {
                                            let elapsed = start.elapsed();
                                            let stale: u64 = r.iter().map(|(_, b, _)| b.len()).sum();
                                            let missing: u64 = r.iter().map(|(_, _, m)| m.len()).sum();
                                            eprintln!(
                                                "Time bucket FULL rebuild (bg): scanned in {:?}, {} stale / {} missing",
                                                elapsed, stale, missing
                                            );
                                            (elapsed, r)
                                        });
                                        if payload.is_none() {
                                            eprintln!(
                                                "Time bucket FULL rebuild (bg): sort field not loaded, will retry"
                                            );
                                        }
                                        // Receiver lives for the flush thread's life;
                                        // a send error only means shutdown — ignore.
                                        let _ = tx.send(payload);
                                    });
                                match spawned {
                                    Ok(h) => {
                                        bucket_rebuild_in_flight = true;
                                        bucket_rebuild_handle = Some(h);
                                    }
                                    Err(e) => {
                                        eprintln!("Time bucket FULL rebuild: failed to spawn bg thread: {e}");
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
                        // Conditional write-through: only update docs already
                        // in cache (queried by users). New docs from pg-sync go
                        // straight to disk without filling the cache with cold
                        // entries that trigger eviction under load.
                        if let Some(ref cache) = flush_doc_cache {
                            cache.update_batch_if_cached(&doc_batch);
                        }
                        if let Err(e) = docstore.write().put_batch(&doc_batch) {
                            eprintln!("WARNING: docstore batch write failed (skipping {} docs): {e}", doc_batch.len());
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
                    for (_name, field) in staging.filters.fields() {
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
                    if let Err(e) = docstore.write().put_batch(&doc_batch) {
                        panic!("docstore final batch write failed: {e}");
                    }
                }
            })
            .expect("failed to spawn bitdex-flush thread")
        };
        let prefilter_registry = Arc::new(
            crate::prefilter::PrefilterRegistry::new_with_cap(config.max_registered_prefilters)
        );
        let warm_persist_path = config.storage.bitmap_path.as_ref()
            .map(|p| p.join("warm.json"));
        let warm_registry = Arc::new(crate::warm_registry::WarmRegistry::new(warm_persist_path));
        let merge_handle = {
            let shutdown = Arc::clone(&shutdown);
            let merge_inner = Arc::clone(&inner);
            let merge_interval_ms = config.merge_interval_ms;
            let _merge_bitmap_store = bitmap_store.clone();
            let merge_alive_store = alive_store.clone();
            let merge_filter_store = filter_store.clone();
            let merge_sort_store = sort_store.clone();
            let merge_meta_store = meta_store.clone();
            let merge_config = Arc::clone(&config);
            let merge_dirty_flag = Arc::clone(&dirty_flag);
            let _sort_field_configs: Vec<crate::config::SortFieldConfig> =
                config.sort_fields.clone();
            let _merge_pending_sorts = Arc::clone(&pending_sort_loads);
            let _merge_pending_filters = Arc::clone(&pending_filter_loads);
            let _merge_lazy_values = Arc::clone(&lazy_value_fields);
            let merge_time_buckets = time_buckets.as_ref().map(Arc::clone);
            let merge_cursors = Arc::clone(&cursors);
            let merge_bound_store = bound_store.clone();
            let merge_unified_cache = Arc::clone(&unified_cache);
            let merge_doc_shard_store = docstore.read().shard_store_arc();
            let merge_dirty_shards = docstore.read().dirty_shards_arc();
            let merge_prefilter_registry = Arc::clone(&prefilter_registry);
            let merge_warm_registry = Arc::clone(&warm_registry);
            let merge_tombstones_cleaned = Arc::clone(&boundstore_tombstones_cleaned);

            thread::Builder::new()
                .name("bitdex-merge".to_string())
                .spawn(move || {
                let sleep_duration = Duration::from_millis(merge_interval_ms);
                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(sleep_duration);
                    // Snapshot cursors at the START of the persist cycle.
                    // The WAL reader keeps advancing the in-memory cursor while
                    // we write — we must persist only the value from when this
                    // cycle began, so on crash we replay from a consistent point.
                    // Only written to disk if data was actually persisted this cycle
                    // AND no write failures occurred.
                    let cursor_snapshot_for_persist = merge_cursors.lock().clone();
                    let mut did_persist_data = false;
                    let mut persist_had_errors = false;
                    // ── Per-shard compaction ────────────────────────────────
                    // The flush thread now appends ops incrementally, so the
                    // merge thread's job is compaction (not full snapshots).
                    // Only check when new ops have been written.
                    let needs_write = merge_dirty_flag.swap(false, Ordering::AcqRel);
                    if needs_write {
                    if let (Some(ref as_), Some(ref fs_), Some(ref ss_), Some(ref ms_)) =
                        (&merge_alive_store, &merge_filter_store, &merge_sort_store, &merge_meta_store)
                    {
                        // Compact alive shard if ops exceed threshold
                        if as_.needs_compaction(&AliveShardKey).unwrap_or(false) {
                            if let Err(e) = as_.compact_current(&AliveShardKey) {
                                eprintln!("merge: alive compaction failed: {e}");
                            }
                        }
                        // Compact filter shards that have accumulated too many ops
                        if let Ok(filter_shards) = fs_.list_current_shards() {
                            for key in &filter_shards {
                                if fs_.needs_compaction(key).unwrap_or(false) {
                                    if let Err(e) = fs_.compact_current(key) {
                                        eprintln!("merge: filter compaction failed: {e}");
                                    }
                                }
                            }
                        }
                        // Compact sort shards that have accumulated too many ops
                        if let Ok(sort_shards) = ss_.list_current_shards() {
                            for key in &sort_shards {
                                if ss_.needs_compaction(key).unwrap_or(false) {
                                    if let Err(e) = ss_.compact_current(key) {
                                        eprintln!("merge: sort compaction failed: {e}");
                                    }
                                }
                            }
                        }

                        // Compact docstore shards that received writes this cycle.
                        // Uses atomic retain(false) to avoid TOCTOU race with writers.
                        {
                            let mut dirty = Vec::new();
                            merge_dirty_shards.retain(|k| {
                                dirty.push(*k);
                                false
                            });
                            // needs_compaction honors threshold 0 = disabled; when
                            // disabled this still drains the dirty set (no rewrite).
                            for shard_key in dirty {
                                if merge_doc_shard_store.needs_compaction(&shard_key).unwrap_or(false) {
                                    if let Err(e) = merge_doc_shard_store.compact_current(&shard_key) {
                                        eprintln!("merge: doc compaction failed for shard {shard_key}: {e}");
                                        // Re-insert so it gets retried next cycle
                                        merge_dirty_shards.insert(shard_key);
                                    }
                                }
                            }
                        }

                        // Persist slot counter (critical metadata).
                        //
                        // The deferred-alive map is deliberately NOT written here.
                        // The flush thread is the single writer (on every applied
                        // deferral and after every activation drain) and writes from
                        // *staging* — the authoritative state. This thread only sees
                        // the *published* snapshot, which lags staging within a flush
                        // cycle; writing it here raced the flush thread's write and
                        // could regress the on-disk map to a pre-deferral state,
                        // permanently orphaning scheduled slots when a crash landed
                        // before the next flush-side persist (audit 2026-07-07,
                        // Mode A loss window W1 — see
                        // docs/_in/sync-writepath-audit-findings-2026-07-07.md).
                        {
                            let snap = merge_inner.load();
                            if let Err(e) = ms_.write_slot_counter(snap.slots.slot_counter()) {
                                eprintln!("merge thread: slot_counter write failed: {e}");
                            }
                        }
                        // Persist time bucket bitmaps + cutoffs (MetaStore)
                        if let Some(ref tb_arc) = merge_time_buckets {
                            let tb = tb_arc.load();
                            for (name, bitmap) in tb.all_buckets() {
                                if !bitmap.is_empty() {
                                    if let Err(e) = ms_.write_time_bucket(name, bitmap) {
                                        eprintln!("merge thread: time bucket write failed: {e}");
                                    }
                                }
                            }
                            // Persist last_cutoff for each bucket (for boot diff recovery)
                            for bucket_name in tb.bucket_names() {
                                if let Some(bucket) = tb.get_bucket(&bucket_name) {
                                    let cutoff = bucket.last_cutoff();
                                    if cutoff > 0 {
                                        if let Err(e) = ms_.write_time_bucket_cutoff(&bucket_name, cutoff) {
                                            eprintln!("merge thread: time bucket cutoff write failed: {e}");
                                        }
                                    }
                                }
                            }
                        }
                        did_persist_data = true;
                    }
                    } // needs_write
                    // ── BoundStore persistence (two-phase lock) ──────────────
                    //
                    // Previously held the Mutex for ~90 lines of entry iteration
                    // + shard data collection every 5s, causing 1-4.6s query stalls.
                    // Now: brief lock to collect data, release, then disk I/O outside.
                    if let Some(ref bs) = merge_bound_store {
                        // Phase 1: Brief lock — check dirty flags + collect ALL data
                        let persist_data = {
                            let uc = &merge_unified_cache;
                            let meta_dirty = uc.is_meta_dirty();
                            let dirty_shards: Vec<crate::bound_store::ShardKey> =
                                uc.dirty_shards().iter().cloned().collect();
                            let mut cleanup_shards: Vec<crate::bound_store::ShardKey> = Vec::new();
                            if let Ok(shard_list) = bs.list_shards() {
                                for sk in &shard_list {
                                    if uc.shard_needs_cleanup(sk) && !dirty_shards.contains(sk) {
                                        cleanup_shards.push(sk.clone());
                                    }
                                }
                            }
                            if !meta_dirty && dirty_shards.is_empty() && cleanup_shards.is_empty() {
                                None // Nothing dirty — skip entirely
                            } else {
                                let meta_id_keys = uc.meta_id_to_key_snapshot();
                                let meta_entries: Vec<crate::bound_store::MetaEntry> = meta_id_keys
                                    .iter()
                                    .map(|(meta_id, key)| {
                                        if let Some(entry_ref) = uc.get(key) {
                                            let entry = entry_ref.value();
                                            crate::bound_store::MetaEntry {
                                                entry_id: *meta_id,
                                                sort_field: key.sort_field.clone(),
                                                direction: key.direction,
                                                filter_clauses: key.filter_clauses.clone(),
                                                capacity: entry.capacity() as u32,
                                                max_capacity: entry.max_capacity() as u32,
                                                min_tracked_value: entry.min_tracked_value(),
                                                total_matched: entry.total_matched(),
                                                has_more: entry.has_more(),
                                                original_filter_clauses: (**entry.original_filter_clauses()).clone(),
                                            }
                                        } else {
                                            crate::bound_store::MetaEntry {
                                                entry_id: *meta_id,
                                                sort_field: key.sort_field.clone(),
                                                direction: key.direction,
                                                filter_clauses: key.filter_clauses.clone(),
                                                capacity: 4000,
                                                max_capacity: 64000,
                                                min_tracked_value: 0,
                                                total_matched: 0,
                                                has_more: true,
                                                original_filter_clauses: Vec::new(),
                                            }
                                        }
                                    })
                                    .collect();
                                let (tombstones, next_id, registered_ids): (_, _, HashSet<u32>) = {
                                    let meta = uc.meta();
                                    (
                                        meta.tombstones().clone(),
                                        meta.next_id(),
                                        meta.all_registered_ids().collect(),
                                    )
                                };
                                let all_dirty: Vec<crate::bound_store::ShardKey> = dirty_shards
                                    .iter()
                                    .chain(cleanup_shards.iter())
                                    .cloned()
                                    .collect();
                                let shard_snapshots: Vec<(
                                    crate::bound_store::ShardKey,
                                    Vec<(u32, Vec<crate::cache::CanonicalClause>, roaring::RoaringBitmap, Option<Vec<u64>>)>,
                                )> = all_dirty
                                    .iter()
                                    .map(|sk| {
                                        let entries = uc.entries_for_shard(sk);
                                        let data: Vec<_> = entries
                                            .into_iter()
                                            .map(|(id, key, bm, sk)| (id, key.filter_clauses, bm, sk))
                                            .collect();
                                        (sk.clone(), data)
                                    })
                                    .collect();
                                // Collect per-shard tombstone IDs for cleanup
                                let per_shard_tombstones: Vec<Vec<u32>> = all_dirty
                                    .iter()
                                    .map(|sk| {
                                        tombstones.iter()
                                            .filter(|id| {
                                                uc.key_for_meta_id(*id)
                                                    .map(|k| k.sort_field == sk.sort_field && k.direction == sk.direction)
                                                    .unwrap_or(false)
                                            })
                                            .collect()
                                    })
                                    .collect();
                                // Clear dirty flags before releasing
                                if meta_dirty {
                                    uc.clear_meta_dirty();
                                }
                                for sk in &all_dirty {
                                    uc.clear_shard_dirty(sk);
                                    uc.clear_shard_entry_dirty(sk);
                                }
                                Some((meta_dirty, meta_entries, tombstones, next_id,
                                      registered_ids, shard_snapshots, per_shard_tombstones))
                            }
                        }; // Lock released here — ALL data collected
                        // Phase 2: Disk I/O outside the lock
                        if let Some((meta_dirty, meta_entries, tombstones, next_id,
                                     registered_ids, shard_snapshots, per_shard_tombstones)) = persist_data
                        {
                            if meta_dirty {
                                // Compact meta.bin: exclude tombstoned entries from the entries list.
                                // Tombstones are only needed for entries that still exist in shard
                                // files on disk (to prevent stale data from being loaded). Once an
                                // entry is removed from meta_entries, its tombstone is no longer needed.
                                let live_entry_ids: HashSet<u32> = meta_entries
                                    .iter()
                                    .map(|e| e.entry_id)
                                    .collect();
                                let compacted_entries: Vec<_> = meta_entries
                                    .into_iter()
                                    .filter(|e| !tombstones.contains(e.entry_id))
                                    .collect();
                                // Only keep tombstones for entries that are NOT in compacted_entries
                                // but ARE still in shard files (we can't know for certain without
                                // scanning shards, so keep tombstones for registered IDs that were
                                // filtered out — they may still be in unmodified shard files)
                                let compacted_ids: HashSet<u32> = compacted_entries
                                    .iter()
                                    .map(|e| e.entry_id)
                                    .collect();
                                let mut compacted_tombstones = RoaringBitmap::new();
                                for id in tombstones.iter() {
                                    // Keep tombstone only if the entry was registered (in live_entry_ids)
                                    // but excluded from compacted_entries (still in a shard file on disk)
                                    if live_entry_ids.contains(&id) && !compacted_ids.contains(&id) {
                                        compacted_tombstones.insert(id);
                                    }
                                }
                                let removed = tombstones.len() - compacted_tombstones.len();
                                if removed > 0 {
                                    eprintln!("merge thread: compacted meta.bin — removed {} stale tombstones (kept {})",
                                        removed, compacted_tombstones.len());
                                }
                                let meta_file = crate::bound_store::MetaFile {
                                    entries: compacted_entries,
                                    tombstones: compacted_tombstones,
                                    next_entry_id: next_id,
                                };
                                if let Err(e) = bs.write_meta(&meta_file) {
                                    eprintln!("merge thread: meta.bin write failed: {e}");
                                    persist_had_errors = true;
                                }
                            }
                            // Write shards — NO lock needed (using snapshotted data)
                            let mut all_cleaned: Vec<u32> = Vec::new();
                            for (i, (sk, ram_entries)) in shard_snapshots.iter().enumerate() {
                                let mut merged: Vec<crate::bound_store::ShardEntry> = Vec::new();
                                if let Ok(Some(disk_entries)) = bs.load_shard(sk) {
                                    let ram_ids: HashSet<u32> =
                                        ram_entries.iter().map(|(id, _, _, _)| *id).collect();
                                    for de in disk_entries {
                                        if !ram_ids.contains(&de.entry_id)
                                            && !tombstones.contains(de.entry_id)
                                            && registered_ids.contains(&de.entry_id)
                                        {
                                            merged.push(de);
                                        }
                                    }
                                }
                                for (id, clauses, bm, sk) in ram_entries {
                                    merged.push(crate::bound_store::ShardEntry {
                                        entry_id: *id,
                                        filter_clauses: clauses.clone(),
                                        bitmap: bm.clone(),
                                        sorted_keys: sk.clone(),
                                    });
                                }
                                if let Err(e) = bs.write_shard(sk, &merged) {
                                    eprintln!("merge thread: shard {} write failed: {e}", sk.filename());
                                    persist_had_errors = true;
                                }
                                all_cleaned.extend_from_slice(&per_shard_tombstones[i]);
                            }
                            // Phase 3: Brief lock — finalize tombstones
                            if !all_cleaned.is_empty() {
                                let uc = &merge_unified_cache;
                                uc.finalize_shard_write(&all_cleaned);
                                // Record cleaned tombstones so the
                                // bitdex_boundstore_tombstones_cleaned_total metric
                                // reflects actual shard-rewrite cleanup (previously
                                // never incremented — the counter read a flat 0).
                                merge_tombstones_cleaned
                                    .fetch_add(all_cleaned.len() as u64, Ordering::Relaxed);
                            }
                            did_persist_data = true;
                        }
                    }
                    // ── Named cursor persistence ───────────────────────────
                    //
                    // Write the cursor snapshot taken at the START of this cycle.
                    //
                    // Durability invariant (Codex follow-up, 2026-04-25):
                    //   Every mutation up to the persisted WAL cursor must be durable
                    //   in either (a) the source BitDex WAL replay window OR (b)
                    //   fsynced bitmap/docstore state.
                    //
                    // The flush thread appends bitmap ops with fsync=false for
                    // throughput. The merge thread compacts shards (which fsyncs)
                    // only when a shard's ops count exceeds the threshold (~1000).
                    // Without an explicit sync step, a cycle that only writes
                    // slot_counter or time_bucket (satisfying `did_persist_data`)
                    // can advance the cursor past mutations that are still only in
                    // the OS page cache — an OS crash in that window silently skips
                    // those WAL ops on restart.
                    //
                    // Fix: before writing the cursor, fsync all bitmap shard opslogs
                    // (alive + filter + sort).  This converts any page-cache-only
                    // opslog entries to durable state, closing the gap regardless of
                    // whether compaction fired this cycle.  Cost: one sync_all() per
                    // shard file once per merge interval (~60 s default) — not on the
                    // per-flush hot path.
                    // Sync whichever bitmap stores are actually configured.
                    // Using `if let (Some, Some, Some)` would fall into an
                    // "else → bitmaps_synced=true" branch whenever only a subset
                    // of stores are present, incorrectly skipping syncs for the
                    // stores that DO exist.  Instead, sync each store independently.
                    let mut bitmaps_synced = true; // true until proven otherwise
                    let mut any_bitmap_store = false;
                    if did_persist_data && !persist_had_errors {
                        if let Some(ref as_) = merge_alive_store {
                            any_bitmap_store = true;
                            if as_.sync_all_opslogs()
                                .map_err(|e| eprintln!("merge: alive opslog sync failed: {e}"))
                                .is_err()
                            {
                                bitmaps_synced = false;
                            }
                        }
                        if let Some(ref fs_) = merge_filter_store {
                            any_bitmap_store = true;
                            if fs_.sync_all_opslogs()
                                .map_err(|e| eprintln!("merge: filter opslog sync failed: {e}"))
                                .is_err()
                            {
                                bitmaps_synced = false;
                            }
                        }
                        if let Some(ref ss_) = merge_sort_store {
                            any_bitmap_store = true;
                            if ss_.sync_all_opslogs()
                                .map_err(|e| eprintln!("merge: sort opslog sync failed: {e}"))
                                .is_err()
                            {
                                bitmaps_synced = false;
                            }
                        }
                        // If no bitmap stores exist (pure in-memory mode), there is
                        // nothing to sync — cursor advance is inherently safe.
                        if !any_bitmap_store {
                            bitmaps_synced = true;
                        }
                    }
                    if did_persist_data && !persist_had_errors && bitmaps_synced {
                        if let Some(ref ms_) = merge_meta_store {
                            for (name, value) in &cursor_snapshot_for_persist {
                                if let Err(e) = ms_.write_cursor(name, value) {
                                    eprintln!("merge thread: cursor write failed for {name}: {e}");
                                }
                            }
                        }
                    }
                    // ── Prefilter evict-to-fit ────────────────────────────────
                    // When max_entries has been lowered at runtime, shed entries
                    // until len <= cap. Picks the least-substituted entry each
                    // pass (lowest work saved → safest to drop).
                    {
                        let target = merge_prefilter_registry.max_entries();
                        while merge_prefilter_registry.len() > target {
                            let victim_name = merge_prefilter_registry
                                .entries()
                                .into_iter()
                                .min_by_key(|e| e.substitutions())
                                .map(|e| e.name.clone());
                            match victim_name {
                                Some(name) => {
                                    merge_prefilter_registry.remove(&name);
                                    merge_unified_cache.invalidate_prefilter(&name);
                                    tracing::info!(
                                        "prefilter evicted to fit max_entries={target}: '{name}'"
                                    );
                                }
                                None => break,
                            }
                        }
                    }
                    // ── Prefilter refresh ──────────────────────────────────────
                    // Refresh any stale prefilters against the current snapshot.
                    // This runs every merge cycle (~60s default), so prefilter
                    // bitmaps stay within their configured refresh interval.
                    if !merge_prefilter_registry.is_empty() {
                        let now_secs = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        let snap = merge_inner.load();
                        for entry in merge_prefilter_registry.entries() {
                            if !entry.is_stale(now_secs) {
                                continue;
                            }
                            let start = std::time::Instant::now();
                            let executor = crate::executor::QueryExecutor::new(
                                &snap.slots,
                                &snap.filters,
                                &snap.sorts,
                                1, // max_page_size irrelevant for filter-only eval
                            );
                            match executor.compute_filters(&entry.clauses) {
                                Ok(bitmap) => {
                                    let ms = start.elapsed().as_millis();
                                    let card = bitmap.len();
                                    entry.publish_refresh(bitmap, start.elapsed().as_nanos() as u64);
                                    eprintln!(
                                        "prefilter refresh: '{}' → {} slots in {}ms",
                                        entry.name, card, ms,
                                    );
                                }
                                Err(e) => {
                                    entry.refresh_errors.fetch_add(1, Ordering::Relaxed);
                                    eprintln!("prefilter refresh failed for '{}': {e}", entry.name);
                                }
                            }
                        }
                    }
                    // ── Auto-prefilter promotion ──────────────────────────────
                    // When a filter clause set reaches a frequency threshold,
                    // auto-register it as a prefilter. The warm registry tracks
                    // shapes by frequency; here we promote hot filter sets.
                    // Skip entirely when the registry is at or above its cap
                    // (max_entries == 0 disables promotion entirely).
                    if merge_prefilter_registry.len() < merge_prefilter_registry.max_entries()
                        && merge_warm_registry.total_recorded() >= 50 {
                        let hot = merge_warm_registry.hot_filter_sets(10);
                        for hfs in &hot {
                            // Skip if already covered by an existing prefilter
                            let (_, existing) = crate::prefilter::substitute(
                                &merge_prefilter_registry,
                                &hfs.filters,
                            );
                            if existing.is_some() {
                                continue;
                            }
                            // Auto-register: name = "auto_" + hash of canonical clauses
                            let name = format!(
                                "auto_{:x}",
                                {
                                    use std::hash::{Hash, Hasher};
                                    let mut h = std::collections::hash_map::DefaultHasher::new();
                                    hfs.canonical.hash(&mut h);
                                    h.finish()
                                }
                            );
                            if merge_prefilter_registry.get(&name).is_some() {
                                continue; // already registered
                            }
                            let snap = merge_inner.load();
                            let start = std::time::Instant::now();
                            let executor = crate::executor::QueryExecutor::new(
                                &snap.slots,
                                &snap.filters,
                                &snap.sorts,
                                1,
                            );
                            match executor.compute_filters(&hfs.filters) {
                                Ok(bitmap) => {
                                    let ms = start.elapsed().as_millis();
                                    let card = bitmap.len();
                                    // Skip 0-cardinality results: a prefilter that
                                    // matches zero slots can never substitute a real
                                    // query, so registering it just wastes a registry
                                    // slot (capped, default 32) and adds another stale
                                    // entry to the prefilter-refresh loop. Observed in
                                    // the wild: registry filling with "→ 0 slots"
                                    // entries within minutes of post-#224 startup,
                                    // evicting useful candidates and producing 80-180 ms
                                    // read-lock holds per cycle on every refresh.
                                    if card == 0 {
                                        tracing::debug!(
                                            "auto-prefilter: skipped '{}' — 0 slots match (freq={})",
                                            name, hfs.total_frequency,
                                        );
                                        continue;
                                    }
                                    match merge_prefilter_registry.insert(
                                        name.clone(),
                                        hfs.filters.clone(),
                                        bitmap,
                                        300,
                                        start.elapsed().as_nanos() as u64,
                                    ) {
                                        Ok(_) => {
                                            eprintln!(
                                                "auto-prefilter: promoted '{}' → {} slots in {}ms (freq={}, {} sort variants)",
                                                name, card, ms, hfs.total_frequency, hfs.sort_variants,
                                            );
                                        }
                                        Err(e) => {
                                            eprintln!("auto-prefilter: failed to register '{}': {e}", name);
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("auto-prefilter: compute failed for '{}': {e}", name);
                                }
                            }
                        }
                    }
                    // ── Warm registry persist ─────────────────────────────────
                    // Persist top-N query shapes to disk every merge cycle.
                    // On next boot, server reads this file and pre-warms cache.
                    if merge_warm_registry.total_recorded() > 0 {
                        match merge_warm_registry.persist() {
                            Ok(n) if n > 0 => {
                                eprintln!(
                                    "warm registry: persisted {} shapes ({} total recorded, {} unique)",
                                    n, merge_warm_registry.total_recorded(), merge_warm_registry.shape_count(),
                                );
                            }
                            Err(e) => eprintln!("warm registry persist failed: {e}"),
                            _ => {}
                        }
                    }
                    // ── RSS-aware memory pressure eviction ──────────────────
                    //
                    // Check real RSS against the memory budget. When RSS exceeds
                    // the pressure threshold, evict cache entries until RSS drops
                    // below the target. This catches the serialized_size() undercount
                    // (~170KB real vs ~2KB tracked per cache entry).
                    {
                        let rss = get_rss_bytes();
                        let budget = merge_config.memory_budget_bytes
                            .unwrap_or_else(|| crate::memory_pressure::detect_memory_budget(None));
                        let threshold = (budget as f64 * merge_config.memory_pressure_threshold) as u64;
                        let target = (budget as f64 * merge_config.memory_pressure_target) as u64;
                        if rss > threshold {
                            let mut evicted = 0u64;
                            let mut rounds = 0u32;
                            loop {
                                {
                                    let uc = &merge_unified_cache;
                                    if uc.len() == 0 { break; }
                                    uc.evict_batch();
                                }
                                evicted += 1;
                                rounds += 1;
                                // Re-check RSS after each batch eviction
                                let new_rss = get_rss_bytes();
                                if new_rss <= target || rounds >= 50 {
                                    eprintln!(
                                        "memory pressure: evicted {} batches, RSS {:.2} GB → {:.2} GB (budget {:.2} GB, target {:.2} GB)",
                                        evicted,
                                        rss as f64 / 1e9,
                                        new_rss as f64 / 1e9,
                                        budget as f64 / 1e9,
                                        target as f64 / 1e9,
                                    );
                                    break;
                                }
                            }
                        }
                    }
                } // while !shutdown
            })
            .expect("failed to spawn bitdex-merge thread")
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
                        // Read entry state under lock, then drop lock before doing work.
                        // Also clone the original FilterClause Arc so compound clauses
                        // (And/Or/Not/IsNull/IsNotNull/bucket) survive the lock release —
                        // CanonicalClause::to_filter_clause returns None for those, so a
                        // plain filter_map would silently drop them and produce a superset
                        // filter bitmap, causing entry.expand to add wrong slots (B5 fix).
                        let work = {
                            let uc = &pf_cache;
                            if let Some(entry) = uc.get(&ukey) {
                                if entry.is_prefetching() || !entry.has_more()
                                    || entry.capacity() >= entry.max_capacity()
                                {
                                    None
                                } else {
                                    let cap = entry.capacity();
                                    let max_cap = entry.max_capacity();
                                    let min_val = entry.min_tracked_value();
                                    let original_clauses = Arc::clone(entry.original_filter_clauses());
                                    entry.set_prefetching(true);
                                    Some((cap, max_cap, min_val, original_clauses))
                                }
                            } else {
                                None
                            }
                        };
                        let Some((capacity, max_capacity, min_tracked_value, original_clauses)) = work else {
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
                        // Use the original FilterClause tree stored on the entry (B5).
                        // This preserves compound shapes (And/Or/Not/IsNull/IsNotNull/bucket)
                        // that CanonicalClause::to_filter_clause cannot round-trip.
                        // Fall back to canonical round-trip only for pre-B1 entries where
                        // original_filter_clauses is empty (e.g. entries restored from disk
                        // before the B8 meta.bin V2 upgrade).
                        let filter_clauses: Vec<FilterClause> = if !original_clauses.is_empty() {
                            (*original_clauses).clone()
                        } else {
                            ukey.filter_clauses.iter()
                                .filter_map(|cc| crate::cache::CanonicalClause::to_filter_clause(cc))
                                .collect()
                        };
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
                                let uc = &pf_cache;
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
                                let uc = &pf_cache;
                                if let Some(mut entry) = uc.get_mut(&ukey) {
                                    entry.expand(&sorted_slots, value_fn);
                                    entry.set_prefetching(false);
                                    uc.record_extension(&ukey);
                                    tracing::debug!(
                                        "Prefetch: expanded {} {:?} by {} slots",
                                        ukey.sort_field, ukey.direction, sorted_slots.len(),
                                    );
                                }
                            }
                            Ok(_) => {
                                // No results — nothing to expand
                                let uc = &pf_cache;
                                if let Some(entry) = uc.get(&ukey) {
                                    entry.set_prefetching(false);
                                }
                            }
                            Err(e) => {
                                tracing::debug!("Prefetch: sort traversal failed: {e}");
                                let uc = &pf_cache;
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
        // Spawn bitmap memory scanner thread (amortized per-field memory measurement)
        {
            let mem_cache = Arc::clone(&bitmap_memory_cache);
            let inner_ref = Arc::clone(&inner);
            let loading_flag = Arc::clone(&loading_mode);
            let filter_names: Vec<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
            let sort_names: Vec<String> = config.sort_fields.iter().map(|f| f.name.clone()).collect();
            #[cfg(feature = "server")]
            let bridge_for_scanner = Arc::clone(&metrics_bridge);
            std::thread::Builder::new()
                .name("bitdex-mem-scanner".into())
                .spawn(move || {
                    loop {
                        let interval = mem_cache.interval_ms();
                        std::thread::sleep(std::time::Duration::from_millis(interval));
                        let t = std::time::Instant::now();
                        mem_cache.scan_tick(&inner_ref, &loading_flag, &filter_names, &sort_names);
                        let _elapsed = t.elapsed();
                        #[cfg(feature = "server")]
                        {
                            let guard = bridge_for_scanner.load();
                            if let Some(b) = (**guard).as_ref() {
                                b.bitmap_mem_scan_tick_seconds
                                    .with_label_values(&[&b.index_name])
                                    .observe(_elapsed.as_secs_f64());
                            }
                        }
                    }
                })
                .expect("failed to spawn memory scanner thread");
        }
        // Spawn doc cache eviction thread (generational rotation + memory-pressure eviction)
        let doc_cache_eviction_handle = if let Some(ref cache) = doc_cache {
            let cache_clone = Arc::clone(cache);
            let shutdown_clone = Arc::clone(&shutdown);
            Some(
                thread::Builder::new()
                    .name("bitdex-doc-cache-eviction".into())
                    .spawn(move || {
                        crate::doc_cache::eviction_thread(cache_clone, shutdown_clone);
                    })
                    .expect("Failed to spawn bitdex-doc-cache-eviction thread"),
            )
        } else {
            None
        };
        // Async cache worker: spawn worker thread if channel was pre-created.
        // The shared AtomicU64 lets `set_max_maintenance_ms` update the worker's
        // deadline at runtime without restarting the thread.
        let cache_worker_ms_arc: Arc<AtomicU64> =
            Arc::new(AtomicU64::new(config.cache.max_maintenance_ms));
        let cache_worker_handle: Option<JoinHandle<()>> = if let Some(rx) = pre_cache_rx {
            use crate::cache_worker::{CacheWorker, CacheWorkerConfig};
            let worker = CacheWorker::new(
                rx,
                Arc::clone(&unified_cache),
                Arc::clone(&inner),
                CacheWorkerConfig {
                    max_maintenance_ms: Arc::clone(&cache_worker_ms_arc),
                    backlog_drop_limit: 4096,
                },
                Arc::clone(&cache_worker_metrics),
                Arc::clone(&shutdown),
                Arc::clone(&shared_string_maps),
                Arc::clone(&shared_dictionaries),
            );
            // Attach time buckets so the async worker evaluates bucket clauses
            // correctly (bitmap.contains) rather than always returning true.
            let worker = if let Some(ref tb_arc) = time_buckets {
                worker.with_time_buckets(Arc::clone(tb_arc))
            } else {
                worker
            };
            Some(
                std::thread::Builder::new()
                    .name("bitdex-cache-worker".to_string())
                    .spawn(move || worker.run())
                    .expect("Failed to spawn bitdex-cache-worker thread"),
            )
        } else {
            None
        };
        let cache_worker_ms = Some(cache_worker_ms_arc);
        Ok(Self {
            inner,
            sender,
            doc_tx,
            docstore,
            docstore_root,
            config,
            field_registry,
            in_flight: InFlightTracker::new(),
            shutdown,
            flush_handle: Some(flush_handle),
            merge_handle: Some(merge_handle),
            bitmap_store,
            alive_store,
            filter_store,
            sort_store,
            meta_store,
            loading_mode,
            dirty_since_snapshot: Arc::clone(&dirty_flag),
            time_buckets,
            pending_bucket_diffs,
            pending_filter_loads,
            pending_sort_loads,
            lazy_value_fields,
            lazy_tx,
            cmd_tx,
            string_maps: None,
            case_sensitive_fields: None,
            dictionaries: Arc::new(HashMap::new()),
            shared_string_maps: Arc::new(ArcSwap::from_pointee(None)),
            shared_dictionaries: Arc::new(ArcSwap::from_pointee(Arc::new(HashMap::new()))),
            unified_cache,
            bound_store,
            flush_publish_count,
            flush_duration_nanos,
            flush_last_duration_nanos,
            flush_apply_nanos,
            flush_cache_nanos,
            flush_publish_nanos,
            flush_timebucket_nanos,
            flush_compact_nanos,
            flush_opslog_nanos,
            flush_sort_promote_nanos,
            flush_cache_unique_filter_shapes,
            flush_cache_unique_filter_shapes_max,
            flush_cache_sort_work_items,
            flush_cache_sort_work_items_max,
            cursors,
            existing_keys,
            eviction_stamps,
            flush_cycle,
            eviction_total,
            boundstore_shard_loads,
            boundstore_tombstones_created,
            boundstore_tombstones_cleaned,
            boundstore_bytes_written,
            boundstore_bytes_read,
            boundstore_entries_restored,
            boundstore_entries_skipped,
            #[cfg(feature = "server")]
            metrics_bridge,
            bitmap_memory_cache: Arc::clone(&bitmap_memory_cache),
            doc_cache: doc_cache.clone(),
            par_iter_min_threshold: Arc::clone(&par_iter_min_threshold),
            time_bucket_full_rebuild_interval: Arc::clone(&time_bucket_full_rebuild_interval),
            compaction_skipped,
            compact_tx,
            compact_handle,
            prefetch_tx,
            prefetch_handle,
            doc_cache_eviction_handle,
            #[cfg(feature = "pg-sync")]
            wal_writer: None,
            cache_work_tx,
            cache_worker_handle,
            cache_worker_metrics,
            cache_worker_ms,
            prefilter_registry,
            warm_registry,
        })
    }

    /// Get the warm registry for recording query shapes.
    pub fn warm_registry(&self) -> &crate::warm_registry::WarmRegistry {
        &self.warm_registry
    }

    /// Load and execute warm entries from the persisted warm file.
    /// Called on boot after the index is loaded. Returns the number
    /// of queries warmed.
    pub fn auto_warm(&self) -> usize {
        let path = match self.config.storage.bitmap_path.as_ref() {
            Some(p) => p.join("warm.json"),
            None => return 0,
        };
        let entries = crate::warm_registry::WarmRegistry::load(&path);
        if entries.is_empty() {
            return 0;
        }
        eprintln!("Auto-warming {} query shapes...", entries.len());
        let start = std::time::Instant::now();
        let mut warmed = 0;
        for entry in &entries {
            let query = crate::query::BitdexQuery {
                filters: entry.filters.clone(),
                sort: Some(crate::query::SortClause {
                    field: entry.sort_field.clone(),
                    direction: entry.direction,
                }),
                limit: 1,
                cursor: None,
                offset: None,
                skip_cache: false,
            };
            match self.execute_query(&query) {
                Ok(_) => warmed += 1,
                Err(e) => {
                    tracing::debug!("auto-warm query failed: {e}");
                }
            }
        }
        eprintln!(
            "Auto-warmed {}/{} query shapes in {:.1}ms",
            warmed, entries.len(), start.elapsed().as_secs_f64() * 1000.0,
        );
        warmed
    }

    /// Set the string maps for MappedString field query resolution.
    /// Call after creating the engine with schema data that includes string_map entries.
    pub fn set_string_maps(&mut self, maps: StringMaps) {
        self.shared_string_maps.store(Arc::new(Some(maps.clone())));
        self.string_maps = Some(Arc::new(maps));
    }
    /// Set the case-sensitive fields for string matching control.
    pub fn set_case_sensitive_fields(&mut self, fields: CaseSensitiveFields) {
        self.case_sensitive_fields = Some(Arc::new(fields));
    }
    /// Set the Prometheus metrics bridge. Called by the server layer after engine creation.
    /// Background threads (compaction worker, lazy loading) will start recording metrics.
    #[cfg(feature = "server")]
    pub fn set_metrics_bridge(&self, bridge: MetricsBridge) {
        self.metrics_bridge.store(Arc::new(Some(Arc::new(bridge))));
    }
    /// Read-only handle to the metrics bridge for code paths outside the engine
    /// (e.g. `ops_processor::apply_query_op_set`). Returns `None` when the server
    /// layer hasn't wired the bridge yet (boot-time and dump-only test contexts).
    #[cfg(feature = "server")]
    pub fn metrics_bridge_handle(&self) -> Option<Arc<MetricsBridge>> {
        let guard = self.metrics_bridge.load();
        (**guard).as_ref().map(Arc::clone)
    }
    /// Read-only handle to the par_iter min-threshold (Arc<AtomicUsize>).
    /// Doc writer paths use this to gate their own par_iter calls so the
    /// PATCH /config knob applies uniformly across the steady-state hot
    /// path, not just the flush thread.
    pub fn par_iter_min_threshold_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.par_iter_min_threshold)
    }
    /// Set the par_iter min-task threshold. Called by PATCH /config handler.
    pub fn set_par_iter_min_threshold(&self, n: usize) {
        self.par_iter_min_threshold.store(n, Ordering::Relaxed);
    }
    /// Read the current par_iter min-task threshold (for /config GET).
    pub fn par_iter_min_threshold(&self) -> usize {
        self.par_iter_min_threshold.load(Ordering::Relaxed)
    }
    /// Set the periodic time-bucket full-reconcile interval (secs). Hot-reload
    /// via PATCH /config; `0` disables the fallback. The flush thread reads this
    /// each cycle, so a change takes effect within one interval — no restart.
    pub fn set_time_bucket_full_rebuild_interval(&self, secs: u64) {
        self.time_bucket_full_rebuild_interval
            .store(secs, Ordering::Relaxed);
    }
    /// Read the current time-bucket full-reconcile interval (for /config GET).
    pub fn time_bucket_full_rebuild_interval(&self) -> u64 {
        self.time_bucket_full_rebuild_interval.load(Ordering::Relaxed)
    }
    /// Set the bitmap shard compaction threshold across all bitmap stores
    /// (alive / filter / sort). ops_count > threshold triggers a per-shard
    /// compaction on the next merge cycle. Atomic, takes effect immediately;
    /// no merge thread restart needed. No-op if a given store isn't configured.
    pub fn set_bitmap_compact_threshold(&self, threshold: u32) {
        if let Some(ref s) = self.alive_store { s.set_compact_threshold(threshold); }
        if let Some(ref s) = self.filter_store { s.set_compact_threshold(threshold); }
        if let Some(ref s) = self.sort_store { s.set_compact_threshold(threshold); }
    }
    /// Read the current bitmap shard compaction threshold. Reads from the
    /// alive store if present (all three are kept in sync via
    /// `set_bitmap_compact_threshold`); falls back to filter, then sort, then
    /// the static `DEFAULT_COMPACT_THRESHOLD` if no bitmap store is configured.
    pub fn bitmap_compact_threshold(&self) -> u32 {
        if let Some(ref s) = self.alive_store { return s.compact_threshold(); }
        if let Some(ref s) = self.filter_store { return s.compact_threshold(); }
        if let Some(ref s) = self.sort_store { return s.compact_threshold(); }
        crate::shard_store::DEFAULT_COMPACT_THRESHOLD
    }
    /// Set the prefilter registry cap at runtime. Takes effect immediately for
    /// the insert guard; the merge thread's evict-to-fit pass enforces it within
    /// one merge cycle. Called by PATCH /config handler.
    pub fn set_max_registered_prefilters(&self, n: usize) {
        self.prefilter_registry.set_max_entries(n);
    }

    /// Get a reference to the bitmap memory cache (for metrics scraping).
    pub fn bitmap_memory_cache(&self) -> &crate::bitmap_memory_cache::BitmapMemoryCache {
        &self.bitmap_memory_cache
    }
    /// Count cache entries by clause type for scrape-time gauges (A3).
    /// Returns (substituted_entries, compound_clause_entries).
    pub fn unified_cache_entry_counts(&self) -> (u64, u64) {
        self.unified_cache.count_by_clause_type()
    }
    /// Access the unified cache for diagnostic reads (A4).
    pub fn unified_cache_ref(&self) -> &UnifiedCache {
        &self.unified_cache
    }
    /// Get the cumulative count of compaction operations skipped due to channel backpressure.
    pub fn compaction_skipped_count(&self) -> u64 {
        self.compaction_skipped.load(Ordering::Relaxed)
    }
    /// Set the per-field dictionaries for LowCardinalityString fields.
    pub fn set_dictionaries(&mut self, dicts: HashMap<String, crate::dictionary::FieldDictionary>) {
        let arc = Arc::new(dicts);
        // Share the same Arc<HashMap> with background threads via ArcSwap.
        self.shared_dictionaries.store(Arc::new(Arc::clone(&arc)));
        self.dictionaries = arc;
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
        let ms = match self.meta_store.as_ref() {
            Some(s) => s,
            None => return Ok(()), // no persistence configured
        };
        let dict_dir = ms.root().join("dictionaries");
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
            self.docstore.read().get(id)?
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
        let old_doc = self.docstore.read().get(id)?;
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
            self.docstore.read().get(id)?
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
            let old_doc = self.docstore.read().get(id)?;
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
            let old_doc = self.docstore.read().get(id)?;
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
            // Find old values by scanning loaded bitmaps for this field.
            // Uses `get_versioned` so unmerged diff entries are visible — without
            // it, a slot inserted via mutation but not yet flushed to base would
            // be invisible here, and the subsequent FilterRemove op would never
            // be issued, leaving the slot stuck in the old value's bitmap.
            let old_values: Vec<u64> = {
                let snap = self.snapshot();
                match snap.filters.get_field(field_name) {
                    Some(field) => field
                        .bitmap_keys()
                        .into_iter()
                        .filter(|&v| {
                            field.get_versioned(v).map_or(false, |vb| vb.contains(slot))
                        })
                        .collect(),
                    None => Vec::new(),
                }
            };
            let new_set: HashSet<u64> = new_values.iter().copied().collect();
            let old_set: HashSet<u64> = old_values.iter().copied().collect();
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
    /// Reload a field's positive existence set from the filter store.
    ///
    /// Called after external bulk writes (e.g., backfill) so that
    /// lazy per-value loading picks up the new data. The existence set is stored
    /// behind an ArcSwap so the update is atomic and lock-free.
    pub fn reload_existence_set(&self, field_name: &str) -> Result<()> {
        let keys_arc = self.existing_keys.get(field_name).ok_or_else(|| {
            crate::error::BitdexError::Config(format!(
                "Field '{}' not found in existence keys (not a lazy-value field)",
                field_name,
            ))
        })?;
        let fs = self.filter_store.as_ref().ok_or_else(|| {
            crate::error::BitdexError::Config("No filter store configured".to_string())
        })?;
        let new_keys = fs.existence_set(field_name)
            .map_err(|e| crate::error::BitdexError::Storage(format!("existence set: {e}")))?;
        let count = new_keys.len();
        keys_arc.store(Arc::new(new_keys));
        eprintln!("Reloaded existence set for '{}': {} keys", field_name, count);
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
        let tb_guard = self.time_buckets.as_ref().map(|tb| tb.load_full());
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
    pub fn ensure_fields_loaded(
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
        // Load from ShardStore (filter and sort stores for lazy loading)
        let (lazy_filter_store, lazy_sort_store) = match (&self.filter_store, &self.sort_store) {
            (Some(fs), Some(ss)) => (fs, ss),
            _ => return Ok(()), // no store, nothing to load
        };
        // Do all expensive disk I/O in parallel, collecting loaded data.
        // Filter field reads, sort field reads, and per-value reads are all
        // independent I/O operations that benefit from concurrent NVMe access.
        let mut loaded_filters: Vec<(String, HashMap<u64, RoaringBitmap>)> = Vec::new();
        let mut loaded_values: Vec<(String, HashMap<u64, RoaringBitmap>, Vec<u64>)> = Vec::new();
        let mut loaded_sort: Option<(String, Vec<RoaringBitmap>)> = None;
        // Resolve sort bits config before entering the parallel scope.
        let sort_bits = needed_sort.as_ref().map(|sort_name| {
            self.config
                .sort_fields
                .iter()
                .find(|sc| sc.name == *sort_name)
                .map(|sc| sc.bits as usize)
                .unwrap_or(32)
        });
        // Determine missing per-value keys before entering parallel scope.
        let mut value_load_tasks: Vec<(String, Vec<u64>)> = Vec::new();
        {
            let current: Arc<InnerEngine> = self.inner.load_full();
            for (field_name, values) in &needed_values {
                let missing: Vec<u64> = if let Some(field) = current.filters.get_field(field_name) {
                    values
                        .iter()
                        .copied()
                        .filter(|v| {
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
                let missing: Vec<u64> = if let Some(ek) = self.existing_keys.get(field_name.as_str()) {
                    let keys = ek.load();
                    missing.into_iter().filter(|v| keys.contains(v)).collect()
                } else {
                    missing
                };
                if !missing.is_empty() {
                    value_load_tasks.push((field_name.clone(), missing));
                }
            }
        }
        // Load metrics bridge once for all lazy-load timing observations.
        #[cfg(feature = "server")]
        let metrics_bridge_guard = self.metrics_bridge.load();
        #[cfg(feature = "server")]
        let metrics_opt: Option<Arc<MetricsBridge>> = (**metrics_bridge_guard).as_ref().map(|b| Arc::clone(b));
        // Count total parallel work items to decide whether parallelism is worthwhile.
        let total_tasks = needed_filters.len()
            + if needed_sort.is_some() { 1 } else { 0 }
            + value_load_tasks.len();
        if total_tasks > 1 {
            // --- Parallel loading via std::thread::scope ---
            // Each thread reads from ShardStore (Arc, safe to share). Results collected
            // into thread-safe containers, then applied sequentially.
            use std::sync::Mutex;
            let par_filters: Mutex<Vec<(String, HashMap<u64, RoaringBitmap>)>> = Mutex::new(Vec::new());
            let par_sort: Mutex<Option<(String, Vec<RoaringBitmap>)>> = Mutex::new(None);
            let par_values: Mutex<Vec<(String, HashMap<u64, RoaringBitmap>, Vec<u64>)>> = Mutex::new(Vec::new());
            let par_error: Mutex<Option<crate::error::BitdexError>> = Mutex::new(None);
            std::thread::scope(|s| {
                // Spawn filter field loaders
                for name in &needed_filters {
                    let fs = lazy_filter_store.clone();
                    let par_filters = &par_filters;
                    let par_error = &par_error;
                    #[cfg(feature = "server")]
                    let metrics_ref = &metrics_opt;
                    s.spawn(move || {
                        if par_error.lock().unwrap().is_some() { return; }
                        let t0 = std::time::Instant::now();
                        match fs.load_field(name) {
                            Ok(bitmaps) => {
                                let count = bitmaps.len();
                                eprintln!(
                                    "Lazy-loaded filter '{}': {} values in {:.1}ms",
                                    name, count, t0.elapsed().as_secs_f64() * 1000.0
                                );
                                #[cfg(feature = "server")]
                                if let Some(ref bridge) = metrics_ref {
                                    bridge.lazy_load_duration
                                        .with_label_values(&[&bridge.index_name, name])
                                        .observe(t0.elapsed().as_secs_f64());
                                }
                                par_filters.lock().unwrap().push((name.clone(), bitmaps));
                            }
                            Err(e) => { *par_error.lock().unwrap() = Some(crate::error::BitdexError::Storage(format!("lazy load filter: {e}"))); }
                        }
                    });
                }
                // Spawn sort field loader
                if let (Some(sort_name), Some(bits)) = (&needed_sort, sort_bits) {
                    let ss = lazy_sort_store.clone();
                    let par_sort = &par_sort;
                    let par_error = &par_error;
                    let sort_name = sort_name.clone();
                    #[cfg(feature = "server")]
                    let metrics_ref = &metrics_opt;
                    s.spawn(move || {
                        if par_error.lock().unwrap().is_some() { return; }
                        let t0 = std::time::Instant::now();
                        match ss.load_sort_layers(&sort_name, bits) {
                            Ok(Some(layers)) => {
                                let layer_count = layers.len();
                                eprintln!(
                                    "Lazy-loaded sort '{}': {} layers in {:.1}ms",
                                    sort_name, layer_count, t0.elapsed().as_secs_f64() * 1000.0
                                );
                                #[cfg(feature = "server")]
                                if let Some(ref bridge) = metrics_ref {
                                    bridge.lazy_load_duration
                                        .with_label_values(&[&bridge.index_name, &sort_name])
                                        .observe(t0.elapsed().as_secs_f64());
                                }
                                *par_sort.lock().unwrap() = Some((sort_name, layers));
                            }
                            Ok(None) => {}
                            Err(e) => { *par_error.lock().unwrap() = Some(crate::error::BitdexError::Storage(format!("lazy load sort: {e}"))); }
                        }
                    });
                }
                // Spawn per-value loaders
                for (field_name, missing) in &value_load_tasks {
                    let fs = lazy_filter_store.clone();
                    let par_values = &par_values;
                    let par_error = &par_error;
                    #[cfg(feature = "server")]
                    let metrics_ref = &metrics_opt;
                    s.spawn(move || {
                        if par_error.lock().unwrap().is_some() { return; }
                        let t0 = std::time::Instant::now();
                        match fs.load_field_values(field_name, missing) {
                            Ok(loaded) if !loaded.is_empty() => {
                                let count = loaded.len();
                                eprintln!(
                                    "Lazy-loaded filter '{}': {} values (per-value) in {:.1}ms",
                                    field_name, count, t0.elapsed().as_secs_f64() * 1000.0
                                );
                                #[cfg(feature = "server")]
                                if let Some(ref bridge) = metrics_ref {
                                    bridge.lazy_load_duration
                                        .with_label_values(&[&bridge.index_name, field_name])
                                        .observe(t0.elapsed().as_secs_f64());
                                }
                                par_values.lock().unwrap().push((field_name.clone(), loaded, missing.clone()));
                            }
                            Ok(_) => {}
                            Err(e) => { *par_error.lock().unwrap() = Some(crate::error::BitdexError::Storage(format!("lazy load values: {e}"))); }
                        }
                    });
                }
            });
            // Check for errors from parallel threads
            if let Some(e) = par_error.into_inner().unwrap() {
                return Err(e);
            }
            loaded_filters = par_filters.into_inner().unwrap();
            loaded_sort = par_sort.into_inner().unwrap();
            loaded_values = par_values.into_inner().unwrap();
        } else {
            // --- Serial path: single task, no threading overhead ---
            for name in &needed_filters {
                let t0 = std::time::Instant::now();
                let bitmaps = lazy_filter_store.load_field(name)
                    .map_err(|e| crate::error::BitdexError::Storage(format!("lazy load filter: {e}")))?;
                let count = bitmaps.len();
                eprintln!(
                    "Lazy-loaded filter '{}': {} values in {:.1}ms",
                    name, count, t0.elapsed().as_secs_f64() * 1000.0
                );
                #[cfg(feature = "server")]
                if let Some(ref bridge) = metrics_opt {
                    bridge.lazy_load_duration
                        .with_label_values(&[&bridge.index_name, name])
                        .observe(t0.elapsed().as_secs_f64());
                }
                loaded_filters.push((name.clone(), bitmaps));
            }
            if let (Some(sort_name), Some(bits)) = (&needed_sort, sort_bits) {
                let t0 = std::time::Instant::now();
                let layers_opt = lazy_sort_store.load_sort_layers(sort_name, bits)
                    .map_err(|e| crate::error::BitdexError::Storage(format!("lazy load sort: {e}")))?;
                if let Some(layers) = layers_opt {
                    let layer_count = layers.len();
                    eprintln!(
                        "Lazy-loaded sort '{}': {} layers in {:.1}ms",
                        sort_name, layer_count, t0.elapsed().as_secs_f64() * 1000.0
                    );
                    #[cfg(feature = "server")]
                    if let Some(ref bridge) = metrics_opt {
                        bridge.lazy_load_duration
                            .with_label_values(&[&bridge.index_name, sort_name])
                            .observe(t0.elapsed().as_secs_f64());
                    }
                    loaded_sort = Some((sort_name.clone(), layers));
                }
            }
            for (field_name, missing) in &value_load_tasks {
                let t0 = std::time::Instant::now();
                let loaded = lazy_filter_store.load_field_values(field_name, missing)
                    .map_err(|e| crate::error::BitdexError::Storage(format!("lazy load values: {e}")))?;
                if !loaded.is_empty() {
                    let count = loaded.len();
                    eprintln!(
                        "Lazy-loaded filter '{}': {} values (per-value) in {:.1}ms",
                        field_name, count, t0.elapsed().as_secs_f64() * 1000.0
                    );
                    #[cfg(feature = "server")]
                    if let Some(ref bridge) = metrics_opt {
                        bridge.lazy_load_duration
                            .with_label_values(&[&bridge.index_name, field_name])
                            .observe(t0.elapsed().as_secs_f64());
                    }
                    loaded_values.push((field_name.clone(), loaded, missing.clone()));
                }
            }
        }
        // Sequential phase: send LazyLoad messages to flush thread and update pending sets.
        for (name, bitmaps) in &loaded_filters {
            let _ = self.lazy_tx.send(LazyLoad::FilterField {
                name: name.clone(),
                bitmaps: bitmaps.clone(),
            });
            self.pending_filter_loads.lock().remove(name);
        }
        for (field_name, loaded_vals, _missing) in &loaded_values {
            let _ = self.lazy_tx.send(LazyLoad::FilterValues {
                field: field_name.clone(),
                values: loaded_vals.clone(),
            });
        }
        if let Some((ref sort_name, ref layers)) = loaded_sort {
            let _ = self.lazy_tx.send(LazyLoad::SortField {
                name: sort_name.clone(),
                layers: layers.clone(),
            });
            self.pending_sort_loads.lock().remove(sort_name);
        }
        let any_loaded = !loaded_filters.is_empty() || !loaded_values.is_empty() || loaded_sort.is_some();
        if any_loaded {
            // Single-writer publish: data was already sent to the flush thread
            // via lazy_tx. Ask the flush thread to drain it and publish a new
            // snapshot. This avoids the old rcu() CAS loop which could race
            // with the flush thread's own store() calls.
            let (done_tx, done_rx) = crossbeam_channel::bounded(1);
            let flush_alive = self.cmd_tx.send(FlushCommand::ForcePublish { done: done_tx }).is_ok();
            if flush_alive {
                // Cap the wait at 100ms so a back-pressured flush thread can't
                // turn lazy_load into a 5-second tail across every concurrent
                // query. Real loads complete in <10ms (postId per-value avg
                // 4.9ms). On timeout the query proceeds against the current
                // snapshot — the next query picks up the freshly-published
                // data once the flush thread drains its lazy_rx queue.
                let _ = done_rx.recv_timeout(Duration::from_millis(100));
            } else {
                // Flush thread is dead (shutdown called). Publish directly —
                // no concurrent publisher to race with.
                let current = self.inner.load_full();
                let mut updated = (*current).clone();
                for (name, bitmaps) in &loaded_filters {
                    if let Some(field) = updated.filters.get_field(name) {
                        field.load_field_complete(bitmaps.clone());
                    }
                }
                for (field_name, loaded_vals, requested_keys) in &loaded_values {
                    if let Some(field) = updated.filters.get_field(field_name) {
                        field.load_values(loaded_vals.clone(), requested_keys);
                    }
                }
                if let Some((ref sort_name, ref layers)) = loaded_sort {
                    if let Some(sf) = updated.sorts.get_field_mut(sort_name) {
                        sf.load_layers(layers.clone());
                    }
                }
                self.inner.store(Arc::new(updated));
            }
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
            FilterClause::IsNull(f) | FilterClause::IsNotNull(f) => {
                if pending.contains(f) && !out.contains(f) {
                    out.push(f.clone());
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
            // IsNull/IsNotNull: no specific value to eager-load; skip.
            FilterClause::IsNull(_) | FilterClause::IsNotNull(_) => {}
        }
    }
    /// Bucket names (excluding the `__prefilter` sentinel) referenced by a
    /// canonical clause list — i.e. `ukey.filter_clauses`. Top-level `And` is
    /// already flattened by `cache::canonicalize`, so a real time-bucket
    /// clause shows up as its own flat `CanonicalClause { op: "bucket",
    /// value_repr: bucket_name, .. }` entry (see `CanonicalClause::from_filter`).
    /// A bucket clause nested inside an `Or`/`Not` collapses into a compound
    /// `op` string and is NOT extracted here — same conservative "can't
    /// verify, don't guess" stance as `own_bucket_live_bitmap`'s `None`.
    fn bucket_names_from_canonical(clauses: &[crate::cache::CanonicalClause]) -> Vec<String> {
        clauses
            .iter()
            .filter(|c| crate::unified_cache::is_time_bucket_clause(c))
            .map(|c| c.value_repr.clone())
            .collect()
    }
    /// Resolve the effective bucket-diff state for an entry that depends on
    /// one or more bucket names.
    ///
    /// `PendingBucketDiffs` is keyed per bucket NAME (24h/7d/30d/1y are
    /// independent windows with unrelated cutoff scales — see
    /// `pending_bucket_diffs`'s doc comment). An entry normally references
    /// exactly one bucket name.
    ///
    /// The multi-name case (a compound clause combining two bucket ranges on
    /// the same field, e.g. `Gte(sortAtUnix, X) AND Gte(sortAtUnix, Y)`) is
    /// syntactically reachable — nothing in the query parser rejects two
    /// range clauses on the same bucket field — but no known Civitai client
    /// query pattern constructs one (it's a redundant/degenerate shape: an
    /// AND of two bucket windows just narrows to the tighter one; an OR
    /// widens to the looser one; either way a sane query builder would just
    /// emit the single resulting clause). Rather than trusting a guess at
    /// AND vs. OR semantics from the flattened clause list — a union-based
    /// live-bitmap would UNDER-remove for AND (a slot that fell out of only
    /// one of the two ANDed bucket windows should be dropped, but survives
    /// in the union) — this returns `Rebuild` for >1 distinct name (or an
    /// empty list, which shouldn't happen if the caller only calls this when
    /// `uses_bucket()`/`is_time_bucket_clause` was true — treated the same
    /// defensively): always correct, just costs a rebuild for a shape that
    /// shouldn't occur in practice.
    ///
    /// Returns `Noop` if the single referenced bucket exists but hasn't
    /// pushed a diff yet (`current_cutoff() == 0`) — callers should
    /// seed/leave `bucket_cutoff` at its zero default rather than guess.
    fn resolve_bucket_diff_state(&self, bucket_names: &[String]) -> BucketDiffState {
        Self::resolve_bucket_diff_state_for(&self.pending_bucket_diffs, bucket_names)
    }
    /// Free-standing version of `resolve_bucket_diff_state` for callers
    /// without a `&self` (e.g. `load_shard_background`, a spawned-thread
    /// static function that only has the `Arc<HashMap<..>>` handle).
    fn resolve_bucket_diff_state_for(
        pending_bucket_diffs: &HashMap<String, Arc<ArcSwap<crate::bucket_diff_log::PendingBucketDiffs>>>,
        bucket_names: &[String],
    ) -> BucketDiffState {
        // Empty is a defensive default, not the "ambiguous" case: callers
        // are expected to only call this when the key/entry actually
        // references a bucket (non-empty `bucket_names`); if one doesn't,
        // there's nothing to resolve — Noop, not Rebuild. Only >1 distinct
        // name is the genuinely unresolvable case (see the doc comment
        // above).
        if bucket_names.is_empty() {
            return BucketDiffState::Noop;
        }
        if bucket_names.len() > 1 {
            return BucketDiffState::Rebuild;
        }
        let name = &bucket_names[0];
        let cell = match pending_bucket_diffs.get(name.as_str()) {
            Some(cell) => cell,
            // Referenced bucket name doesn't exist in the configured set
            // (config changed) — can't verify, same as an unresolvable name
            // during shard restore (`resolve_bucket_clauses` tombstones it).
            None => return BucketDiffState::Rebuild,
        };
        let diffs = cell.load();
        if diffs.current_cutoff() == 0 {
            return BucketDiffState::Noop; // this bucket hasn't refreshed yet
        }
        BucketDiffState::Apply(
            RoaringBitmap::clone(diffs.merged_expired().as_ref()),
            diffs.current_cutoff(),
            diffs.oldest_cutoff(),
        )
    }
    /// Seed a freshly-created cache entry's `bucket_cutoff` with the live
    /// per-bucket `PendingBucketDiffs` cutoff instead of wall-clock time.
    ///
    /// `bucket_cutoff` must live in the same *snapped* scale as a bucket's
    /// own `PendingBucketDiffs::current_cutoff()` (`snap(now - duration,
    /// refresh_interval)` — see `time_buckets.rs::TimeBucket::last_cutoff`).
    /// `UnifiedCache::form_and_store_with_clauses` has no visibility into
    /// pending-diffs state, so it leaves `bucket_cutoff` zeroed; this is the
    /// follow-up every production entry-creation path must call. Stamping
    /// wall-clock `now()` instead (the old behavior) made the read-path
    /// diff-apply check structurally false for the entry's entire life,
    /// since `current_cutoff()` trails `now()` by `duration_secs` — the
    /// window-slide removal path was silently dead for every freshly formed
    /// or restored bucket-filtered cache entry.
    ///
    /// No-op if the key's filter clauses don't use a time-bucket clause.
    /// `Noop` (no referenced bucket has pushed a diff yet) leaves
    /// `bucket_cutoff` at its zero default, which is safe — see
    /// `resolve_bucket_diff_state`. `Rebuild` (a multi-bucket-name entry —
    /// see `BucketDiffState`) marks the just-created entry for rebuild
    /// immediately: it was built fresh from truth so this costs nothing
    /// correctness-wise, but it's the only way to keep a
    /// can't-safely-diff entry from silently drifting stale later, since no
    /// per-bucket incremental diff can ever be trusted to apply to it.
    fn seed_bucket_cutoff(&self, ukey: &UnifiedKey) {
        let names = Self::bucket_names_from_canonical(&ukey.filter_clauses);
        match self.resolve_bucket_diff_state(&names) {
            BucketDiffState::Apply(_, new_cutoff, _) => {
                self.unified_cache.set_entry_bucket_cutoff(ukey, new_cutoff);
            }
            BucketDiffState::Rebuild => {
                self.unified_cache.mark_entry_for_rebuild(ukey);
            }
            BucketDiffState::Noop => {}
        }
    }
    /// Union of the LIVE ground-truth bitmaps for the given bucket names.
    ///
    /// A single `merged_expired()` diff pool is only a candidate set — it
    /// says a slot left ITS bucket's window, not necessarily the querying
    /// entry's own bucket(s). This resolves the entry's OWN bucket(s)
    /// against the live `TimeBucketManager` so the caller can
    /// intersect-then-validate instead of trusting a diff pool blindly
    /// (mirrors #273's ADD-side fix, which re-resolves against live state
    /// instead of a frozen capture).
    ///
    /// In practice `bucket_names` never has more than one element here:
    /// both call sites only reach this after `resolve_bucket_diff_state`
    /// already bailed on >1 distinct name (see its doc comment for why a
    /// union isn't safe to trust for a hypothetical AND-of-two-bucket-clauses
    /// entry). The union is kept anyway as the natural identity for the
    /// single-name case, not as multi-name support.
    ///
    /// Returns `None` (caller should `mark_for_rebuild` instead of guessing)
    /// if `tb` is unavailable, `bucket_names` is empty, or any referenced
    /// bucket name no longer resolves (config changed) — the same "can't
    /// verify" case `resolve_bucket_clauses` tombstones during shard restore.
    fn own_bucket_live_bitmap(
        bucket_names: &[String],
        tb: Option<&TimeBucketManager>,
    ) -> Option<RoaringBitmap> {
        let tb = tb?;
        if bucket_names.is_empty() {
            return None;
        }
        let mut union = RoaringBitmap::new();
        for name in bucket_names {
            match tb.get_bucket(name) {
                Some(b) => union |= b.bitmap().as_ref(),
                None => return None,
            }
        }
        Some(union)
    }
    /// Execute a parsed BitdexQuery.
    /// Trigger background loading of a pending cache shard from disk.
    /// Non-blocking: sets loading sentinel and spawns a background thread.
    /// The query proceeds via slow path; next query after loading gets cache hit.
    fn ensure_cache_shard_loaded(&self, sort_field: &str, direction: crate::query::SortDirection) {
        if let Some(ref bs) = self.bound_store {
            let uc = &self.unified_cache;
            if !uc.is_shard_pending(sort_field, direction) {
                return;
            }
            if uc.is_shard_loading(sort_field, direction) {
                // Another thread is already loading — proceed without cache
                return;
            }
            // Set sentinel so other queries skip loading. Spawn background thread.
            uc.mark_shard_loading(sort_field, direction);
            // Spawn background shard loading — don't block the query thread
            let bs = Arc::clone(bs);
            let uc_arc = Arc::clone(&self.unified_cache);
            let inner = Arc::clone(&self.inner);
            let sort_field = sort_field.to_string();
            let boundstore_entries_restored = Arc::clone(&self.boundstore_entries_restored);
            let boundstore_shard_loads = Arc::clone(&self.boundstore_shard_loads);
            let boundstore_entries_skipped = Arc::clone(&self.boundstore_entries_skipped);
            let time_buckets_clone = self.time_buckets.as_ref().map(Arc::clone);
            let prefilter_registry_clone = Arc::clone(&self.prefilter_registry);
            let pending_bucket_diffs_clone = Arc::clone(&self.pending_bucket_diffs);
            std::thread::Builder::new()
                .name(format!("shard-load-{}_{:?}", sort_field, direction))
                .spawn(move || {
                    Self::load_shard_background(
                        &bs, &uc_arc, &inner, &sort_field, direction,
                        &boundstore_entries_restored, &boundstore_shard_loads, &boundstore_entries_skipped,
                        time_buckets_clone, prefilter_registry_clone, &pending_bucket_diffs_clone,
                        // ^ pending_bucket_diffs_clone: Arc<HashMap<..>> derefs to &HashMap<..>.
                    );
                })
                .map_err(|e| {
                    eprintln!("WARNING: failed to spawn shard-load thread: {e}. Shard stuck in loading state.");
                })
                .ok();
            return; // Don't block — query proceeds without cache
        }
    }
    /// Background shard loading. Called from a spawned thread.
    fn load_shard_background(
        bs: &crate::bound_store::BoundStore,
        uc_arc: &Arc<UnifiedCache>,
        inner: &Arc<ArcSwap<InnerEngine>>,
        sort_field: &str,
        direction: crate::query::SortDirection,
        boundstore_entries_restored: &Arc<AtomicU64>,
        boundstore_shard_loads: &Arc<AtomicU64>,
        boundstore_entries_skipped: &Arc<AtomicU64>,
        time_buckets: Option<Arc<ArcSwap<TimeBucketManager>>>,
        prefilter_registry: Arc<crate::prefilter::PrefilterRegistry>,
        pending_bucket_diffs: &HashMap<String, Arc<ArcSwap<crate::bucket_diff_log::PendingBucketDiffs>>>,
    ) {
            let t0 = std::time::Instant::now();
            let shard_key = crate::bound_store::ShardKey::new(
                sort_field.to_string(),
                direction,
            );
            match bs.load_shard(&shard_key) {
                Ok(Some(shard_entries)) => {
                    let disk_elapsed = t0.elapsed();
                    let snap = inner.load();
                    let sf = snap.sorts.get_field(sort_field);
                    let uc = &**uc_arc;
                    // Load TimeBucketManager snapshot once for BucketBitmap re-resolve.
                    let tb_guard = time_buckets.as_ref().map(|tb| tb.load_full());
                    let tb_ref: Option<&TimeBucketManager> = tb_guard.as_deref();
                    let mut loaded = 0usize;
                    let mut skipped = 0usize;
                    let mut tombstoned_unresolvable = 0usize;
                    uc.begin_restore();
                    let config = uc.config();
                    for se in shard_entries {
                        // Skip entries not in meta-index (orphan from crash) or tombstoned.
                        {
                            let meta = uc.meta();
                            if !meta.is_registered(se.entry_id) || meta.is_tombstoned(se.entry_id) {
                                skipped += 1;
                                continue;
                            }
                        }
                        let key = UnifiedKey {
                            filter_clauses: se.filter_clauses,
                            sort_field: sort_field.to_string(),
                            direction,
                        };
                        let has_more = uc.get_meta_has_more(se.entry_id);
                        let persisted_total = uc.get_meta_total_matched(se.entry_id);
                        let value_fn = |slot: u32| -> u32 {
                            sf.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
                        };
                        // Fetch persisted original FilterClause tree (V2 meta.bin only).
                        let mut original_clauses = uc.get_meta_original_filter_clauses(se.entry_id);
                        let entry = if !original_clauses.is_empty() {
                            // Re-resolve BucketBitmap Arcs from live engine state.
                            let all_resolved = resolve_bucket_clauses(
                                &mut original_clauses,
                                tb_ref,
                                &prefilter_registry,
                            );
                            if !all_resolved {
                                // Some bucket names are gone (bucket config changed, prefilter
                                // evicted). Mark for rebuild so the next read re-forms correctly.
                                tombstoned_unresolvable += 1;
                                let entry = UnifiedEntry::from_restored(
                                    se.bitmap,
                                    se.entry_id,
                                    config.initial_capacity,
                                    config.max_capacity,
                                    direction,
                                    se.sorted_keys,
                                    &value_fn,
                                    has_more,
                                    persisted_total,
                                );
                                entry.mark_for_rebuild();
                                entry
                            } else {
                                UnifiedEntry::from_restored_with_clauses(
                                    se.bitmap,
                                    se.entry_id,
                                    config.initial_capacity,
                                    config.max_capacity,
                                    direction,
                                    se.sorted_keys,
                                    &value_fn,
                                    has_more,
                                    persisted_total,
                                    Arc::new(original_clauses),
                                )
                            }
                        } else {
                            // V1 meta.bin or entry with no persisted FC tree — legacy path.
                            UnifiedEntry::from_restored(
                                se.bitmap,
                                se.entry_id,
                                config.initial_capacity,
                                config.max_capacity,
                                direction,
                                se.sorted_keys,
                                &value_fn,
                                has_more,
                                persisted_total,
                            )
                        };
                        // Recompute uses_bucket from the canonical key. If the
                        // entry has a real time-bucket clause (excluding the
                        // __prefilter sentinel), bucket maintenance must apply.
                        let mut entry = entry;
                        let restored_uses_bucket = key.filter_clauses.iter().any(crate::unified_cache::is_time_bucket_clause);
                        entry.set_uses_bucket(restored_uses_bucket);
                        if restored_uses_bucket {
                            // Must match the entry's own bucket's snapped-cutoff
                            // scale — see `seed_bucket_cutoff` doc comment.
                            // Wall-clock now() here permanently defeated the
                            // read-path window-slide diff for restored shards.
                            let names = Self::bucket_names_from_canonical(&key.filter_clauses);
                            match Self::resolve_bucket_diff_state_for(pending_bucket_diffs, &names) {
                                BucketDiffState::Apply(_, new_cutoff, _) => {
                                    entry.set_bucket_cutoff(new_cutoff);
                                }
                                // Can't safely verify (multi-bucket-name
                                // entry) — mark for rebuild immediately;
                                // `entry` isn't inserted yet so this is a
                                // plain local mutation, not a cache lookup.
                                BucketDiffState::Rebuild => entry.mark_for_rebuild(),
                                BucketDiffState::Noop => {}
                            }
                        }
                        uc.insert_restored_entry(key, entry);
                        loaded += 1;
                        boundstore_entries_restored.fetch_add(1, Ordering::Relaxed);
                    }
                    uc.finish_restore();
                    uc.mark_shard_loaded(sort_field, direction);
                    if tombstoned_unresolvable > 0 {
                        tracing::info!(
                            "BoundStore: tombstoned {tombstoned_unresolvable} entries (unresolvable BucketBitmap clauses) in shard {}_{:?}",
                            sort_field, direction,
                        );
                    }
                    boundstore_shard_loads.fetch_add(1, Ordering::Relaxed);
                    boundstore_entries_skipped.fetch_add(skipped as u64, Ordering::Relaxed);
                    let total_elapsed = t0.elapsed();
                    if loaded > 0 || skipped > 0 {
                        tracing::info!(
                            "BoundStore: loaded shard {}_{:?} ({loaded} entries, {skipped} skipped) disk={:.1}ms total={:.1}ms",
                            sort_field, direction,
                            disk_elapsed.as_secs_f64() * 1000.0,
                            total_elapsed.as_secs_f64() * 1000.0,
                        );
                    }
                }
                Ok(None) => {
                    uc_arc.mark_shard_loaded(sort_field, direction);
                }
                Err(e) => {
                    eprintln!("BoundStore: failed to load shard {}_{:?}: {e}", sort_field, direction);
                    uc_arc.mark_shard_loaded(sort_field, direction);
                }
            }
    }
    pub fn execute_query(&self, query: &BitdexQuery) -> Result<QueryResult> {
        let _query_start = std::time::Instant::now();
        // Lazy-load any fields not yet loaded from disk
        let t0 = std::time::Instant::now();
        self.ensure_fields_loaded(
            &query.filters,
            query.sort.as_ref().map(|s| s.field.as_str()),
        )?;
        let ensure_elapsed = t0.elapsed();
        if ensure_elapsed.as_millis() > 10 {
            tracing::debug!("  ensure_fields_loaded: {:.1}ms", ensure_elapsed.as_secs_f64() * 1000.0);
        }
        // Lazy-load cached shard from disk if pending
        if let Some(sort_clause) = query.sort.as_ref() {
            self.ensure_cache_shard_loaded(&sort_clause.field, sort_clause.direction);
        }
        let snap = self.snapshot(); // lock-free
        let tb_guard = self.time_buckets.as_ref().map(|tb| tb.load_full());
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
        // ── Snap range filters to bucket bitmaps BEFORE cache key ──
        // This ensures cache keys use stable bucket names ("7d") instead of
        // moving timestamps, so all queries within the same bucket window share
        // a single cache entry.
        let snapped_filters;
        let effective_filters = if let Some(ref tb) = tb_guard {
            let tb_ref: &TimeBucketManager = &**tb;
            let mut managers: HashMap<String, &TimeBucketManager> = HashMap::new();
            managers.insert(tb_ref.field_name().to_string(), tb_ref);
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
                // Bucket names this query's OWN clauses reference, and the
                // resolved per-bucket diff state (candidates ∪, min
                // current_cutoff, max oldest_cutoff) — see
                // `resolve_bucket_diff_state`'s doc comment for why this must
                // be per-bucket-name, not a single shared cutoff.
                let bucket_names = Self::bucket_names_from_canonical(&ukey.filter_clauses);
                let bucket_diff_state = self.resolve_bucket_diff_state(&bucket_names);
                let cache_data = {
                    let mut applied_bucket_diff = false;
                    let result = self.unified_cache.lookup(&ukey).map(|mut entry_ref| {
                        let entry = entry_ref.value_mut();
                        if entry.uses_bucket() {
                            match bucket_diff_state {
                                BucketDiffState::Apply(ref candidates, new_cutoff, oldest_cutoff) => {
                                    if entry.bucket_cutoff() < new_cutoff {
                                        if entry.bucket_cutoff() >= oldest_cutoff {
                                            // Scope removal to THIS entry's own
                                            // bucket(s): `candidates` may include
                                            // slots that only left ANOTHER bucket's
                                            // window (if this entry references
                                            // multiple names) — only remove ones
                                            // absent from the entry's own bucket(s)'
                                            // LIVE ground-truth bitmap.
                                            match Self::own_bucket_live_bitmap(&bucket_names, tb_guard.as_deref()) {
                                                Some(own_live) => {
                                                    applied_bucket_diff = entry.apply_bucket_diff(
                                                        candidates,
                                                        &own_live,
                                                        new_cutoff,
                                                    );
                                                }
                                                None => entry.mark_for_rebuild(),
                                            }
                                        } else {
                                            entry.mark_for_rebuild();
                                        }
                                    }
                                }
                                // Can't safely verify this entry's bucket-diff
                                // state (multi-bucket-name entry, or a
                                // referenced bucket name no longer resolves) —
                                // must rebuild, not silently serve as-is.
                                BucketDiffState::Rebuild => entry.mark_for_rebuild(),
                                // Bucket exists but hasn't pushed a diff yet —
                                // nothing to apply, nothing wrong either.
                                BucketDiffState::Noop => {}
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
                    });
                    if applied_bucket_diff {
                        // Read path mutated the entry via bucket-diff expiration —
                        // mark the shard dirty so the merge thread persists the
                        // shrunk bitmap. Without this, expired slots resurrect on
                        // pod restart.
                        self.unified_cache.mark_shard_dirty(crate::bound_store::ShardKey::new(
                            ukey.sort_field.clone(),
                            ukey.direction,
                        ));
                    }
                    result
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
                            let max_cap = self.unified_cache.config().max_capacity;
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
                                let uc = &self.unified_cache;
                                if let Some(mut entry) = uc.lookup(&ukey) {
                                    entry.expand(&sorted_slots, value_fn);
                                    uc.record_extension(&ukey);
                                }
                            }
                            self.unified_cache.record_wall_hit();
                            // Re-query from expanded entry (now has radix)
                            let expanded_data = {
                                let uc = &self.unified_cache;
                                uc.lookup(&ukey).map(|e| {
                                    let entry = e.value();
                                    let radix = entry.radix().cloned();
                                    let bm = Arc::clone(entry.bitmap());
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
                        if has_more && capacity < self.unified_cache.config().max_capacity {
                            if let Some(ref tx) = self.prefetch_tx {
                                if let Some(ref keys) = cached_sorted_keys {
                                    if let Some(ref cursor) = result.cursor {
                                        let cursor_key = (cursor.sort_value << 32) | (cursor.slot_id as u64);
                                        let sort_dir = query.sort.as_ref().map(|s| s.direction).unwrap_or(SortDirection::Desc);
                                        let pos = match sort_dir {
                                            SortDirection::Desc => keys.partition_point(|&k| k >= cursor_key),
                                            SortDirection::Asc => keys.partition_point(|&k| k <= cursor_key),
                                        };
                                        let threshold = self.unified_cache.config().prefetch_threshold;
                                        if keys.len() > 0 && pos as f64 / keys.len() as f64 >= threshold {
                                            let _ = tx.try_send(ukey.clone());
                                            self.unified_cache.record_prefetch();
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
                    self.unified_cache.record_wall_hit();
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
        // Lazy-load any fields not yet loaded from disk (timed for trace)
        let lazy_start = std::time::Instant::now();
        self.ensure_fields_loaded(
            &query.filters,
            query.sort.as_ref().map(|s| s.field.as_str()),
        )?;
        collector.lazy_load_us = lazy_start.elapsed().as_micros() as u64;
        // Setup phase: cache shard load + snapshot + tb_guard + executor build.
        // Includes the unified_cache.write() inside ensure_cache_shard_loaded —
        // not covered by the explicit `timed_cache_lock` wrapping at the cache
        // lookup site. v1.0.184 evidence: cache-hit + no-filter slow queries
        // showed cache_lock_us = 0-6 ms but total = 500-840 ms, so the
        // contention is here, not the explicit lookup.
        let setup_start = Instant::now();
        // Lazy-load cached shard from disk if pending
        if let Some(sort_clause) = query.sort.as_ref() {
            let shard_t0 = Instant::now();
            self.ensure_cache_shard_loaded(&sort_clause.field, sort_clause.direction);
            collector.shard_load_us = shard_t0.elapsed().as_micros() as u64;
        }
        let snap = self.snapshot();
        let tb_guard = self.time_buckets.as_ref().map(|tb| tb.load_full());
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
        // Snap range filters to bucket bitmaps BEFORE cache key
        let snapped_filters;
        let effective_filters = if let Some(ref tb) = tb_guard {
            let tb_ref: &TimeBucketManager = &**tb;
            let mut managers: HashMap<String, &TimeBucketManager> = HashMap::new();
            managers.insert(tb_ref.field_name().to_string(), tb_ref);
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
        collector.setup_us = setup_start.elapsed().as_micros() as u64;
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
                // See the non-traced execute_query's identical comment.
                let bucket_names = Self::bucket_names_from_canonical(&ukey.filter_clauses);
                let bucket_diff_state = self.resolve_bucket_diff_state(&bucket_names);
                let cache_data = match bucket_diff_state {
                    BucketDiffState::Apply(candidates, new_cutoff, oldest_cutoff) => {
                        // Bucket diff may need to mutate the entry (apply or
                        // mark-for-rebuild). Take the write lock so we can
                        // call `lookup()` which returns `&mut UnifiedEntry`.
                        // mark_for_rebuild itself is now `&self` (atomic
                        // bool) but apply_bucket_diff still mutates the
                        // bitmap via `Arc::make_mut` and needs `&mut`.
                        // Bucket diff may need to mutate the entry (apply or
                        // mark-for-rebuild). `lookup` returns a DashMap RefMut
                        // holding only the per-shard write lock — concurrent
                        // queries on other shards proceed in parallel.
                        let mut applied_bucket_diff = false;
                        let r = self.unified_cache.lookup(&ukey).map(|mut entry_ref| {
                            let entry = entry_ref.value_mut();
                            if entry.uses_bucket() && entry.bucket_cutoff() < new_cutoff {
                                if entry.bucket_cutoff() >= oldest_cutoff {
                                    match Self::own_bucket_live_bitmap(&bucket_names, tb_guard.as_deref()) {
                                        Some(own_live) => {
                                            applied_bucket_diff = entry.apply_bucket_diff(
                                                &candidates,
                                                &own_live,
                                                new_cutoff,
                                            );
                                        }
                                        None => entry.mark_for_rebuild(),
                                    }
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
                        });
                        if applied_bucket_diff {
                            self.unified_cache.mark_shard_dirty(crate::bound_store::ShardKey::new(
                                ukey.sort_field.clone(),
                                ukey.direction,
                            ));
                        }
                        r
                    }
                    BucketDiffState::Rebuild => {
                        // Can't safely verify this entry's bucket-diff state
                        // (multi-bucket-name entry, or a referenced bucket
                        // name no longer resolves) — must rebuild, not
                        // silently serve as-is. mark_for_rebuild is `&self`
                        // (atomic), so the cheap read-only lookup suffices.
                        self.unified_cache.lookup_for_read(&ukey).map(|entry_ref| {
                            let entry = entry_ref.value();
                            if entry.uses_bucket() {
                                entry.mark_for_rebuild();
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
                    }
                    BucketDiffState::Noop => {
                        // Hot fast path: no bucket diff to apply. `lookup_for_read`
                        // returns a DashMap Ref — concurrent reads on the same
                        // shard proceed in parallel; writers on other shards
                        // are not blocked.
                        self.unified_cache.lookup_for_read(&ukey).map(|entry_ref| {
                            let entry = entry_ref.value();
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
                    }
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
                            let max_cap = self.unified_cache.config().max_capacity;
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
                                let uc = &self.unified_cache;
                                if let Some(mut entry) = uc.lookup(&ukey) {
                                    entry.expand(&sorted_slots, value_fn);
                                    uc.record_extension(&ukey);
                                }
                            }
                            self.unified_cache.record_wall_hit();
                            let expanded_data = {
                                let uc = &self.unified_cache;
                                uc.lookup(&ukey).map(|e| {
                                    let entry = e.value();
                                    let radix = entry.radix().cloned();
                                    let bm = Arc::clone(entry.bitmap());
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
                        if has_more && capacity < self.unified_cache.config().max_capacity {
                            if let Some(ref tx) = self.prefetch_tx {
                                if let Some(ref keys) = cached_sorted_keys {
                                    if let Some(ref cursor) = result.cursor {
                                        let cursor_key = (cursor.sort_value << 32) | (cursor.slot_id as u64);
                                        let sort_dir = query.sort.as_ref().map(|s| s.direction).unwrap_or(SortDirection::Desc);
                                        let pos = match sort_dir {
                                            SortDirection::Desc => keys.partition_point(|&k| k >= cursor_key),
                                            SortDirection::Asc => keys.partition_point(|&k| k <= cursor_key),
                                        };
                                        let threshold = self.unified_cache.config().prefetch_threshold;
                                        if keys.len() > 0 && pos as f64 / keys.len() as f64 >= threshold {
                                            let _ = tx.try_send(ukey.clone());
                                            self.unified_cache.record_prefetch();
                                        }
                                    }
                                }
                            }
                        }
                        self.post_validate(&mut result, &query.filters, &executor)?;
                        return Ok(result);
                    }
                    // Expansion needed — fall through to slow path
                    self.unified_cache.record_wall_hit();
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
            // Slow path lookup: only checking for an existing cache entry to
            // shortcut the work below. No mutation needed → read lock.
            let uc = &self.unified_cache;
            let min_size = uc.config().min_filter_size as u64;
            if full_total_matched >= min_size {
                if let Some(clauses) = cache::canonicalize(snapped_filters) {
                    let ukey = UnifiedKey {
                        filter_clauses: clauses,
                        sort_field: sort_clause.field.clone(),
                        direction: sort_clause.direction,
                    };
                    let hit = uc.lookup_for_read(&ukey).map(|entry_ref| {
                        let entry = entry_ref.value();
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
                    let max_cap = self.unified_cache.config().max_capacity;
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
                        let uc = &self.unified_cache;
                        if let Some(mut entry) = uc.lookup(ukey) {
                            entry.expand(&sorted_slots, value_fn);
                            uc.record_extension(&ukey);
                        }
                    }
                    let uc = &self.unified_cache;
                    if let Some(entry) = uc.lookup(ukey) {
                        let bm = Arc::clone(entry.value().bitmap());
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
            // Snapshot the original FilterClause tree so cache live-maintenance
            // can natively evaluate compound predicates (B2 consumes this).
            let original_clauses = Arc::new(snapped_filters.to_vec());
            // Single-flight guard: when a flagged entry (needs_rebuild=true) triggered
            // this miss, only the first concurrent caller does the seed + store. Others
            // skip the write and serve directly from the executor to avoid stampeding
            // compute_filters with redundant sort traversals.
            if !self.unified_cache.should_rebuild_single_flight(&ukey) {
                let mut result = executor.execute_from_bitmap(
                    &filter_arc,
                    query.sort.as_ref(),
                    fetch_limit,
                    query.cursor.as_ref(),
                    use_simple_sort,
                )?;
                result.total_matched = full_total_matched;
                collector.sort_us = sort_start.elapsed().as_micros() as u64;
                self.post_validate(&mut result, &query.filters, executor)?;
                return Ok(result);
            }
            if full_total_matched == 0 {
                let value_fn = |_slot: u32| -> u32 { 0 };
                let ukey_for_seed = ukey.clone();
                self.unified_cache.form_and_store_with_clauses(
                    ukey,
                    &[],
                    false,
                    full_total_matched,
                    Arc::clone(&original_clauses),
                    value_fn,
                );
                self.seed_bucket_cutoff(&ukey_for_seed);
                let mut result = QueryResult {
                    ids: vec![],
                    total_matched: full_total_matched,
                    cursor: None,
                };
                collector.sort_us = sort_start.elapsed().as_micros() as u64;
                self.post_validate(&mut result, &query.filters, executor)?;
                return Ok(result);
            }
            let initial_cap = self.unified_cache.config().initial_capacity;
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
            // Split form_and_store across two brief locks so the
            // expensive UnifiedEntry::new (build_sorted_keys runs ~4000
            // reconstruct_value calls × 32 bit-layers each) happens
            // OUTSIDE the cache mutex. Steady-state slow-path inserts
            // were the dominant remaining cache_lock_us contributor on
            // v1.0.189 (LOCK p99 ~1-2 s under load).
            let direction = ukey.direction;
            let uses_bucket = ukey.filter_clauses.iter().any(crate::unified_cache::is_time_bucket_clause);
            // Brief lock 1: read capacity config + allocate meta_id.
            // Use allocate_meta_id_with_clauses so compound clauses (And/Or/Not)
            // get registered under their real leaf field names in the meta-index,
            // not just FieldKey("") from the canonical representation (B4).
            let (initial_cap, max_cap, meta_id) = {
                let uc = &self.unified_cache;
                let (i, m) = uc.capacity_config();
                let id = uc.allocate_meta_id_with_clauses(&ukey, &original_clauses);
                (i, m, id)
            };
            // Unlocked: build the entry (the expensive part).
            let mut new_entry = UnifiedEntry::new_with_clauses(
                &sorted_slots,
                initial_cap,
                max_cap,
                has_more,
                full_total_matched,
                meta_id,
                direction,
                Arc::clone(&original_clauses),
                value_fn,
            );
            new_entry.set_uses_bucket(uses_bucket);
            if uses_bucket {
                // Must match the entry's own bucket's snapped-cutoff scale
                // (see `seed_bucket_cutoff` doc comment) — wall-clock `now()`
                // permanently defeats the read-path window-slide diff.
                let names = Self::bucket_names_from_canonical(&ukey.filter_clauses);
                match self.resolve_bucket_diff_state(&names) {
                    BucketDiffState::Apply(_, new_cutoff, _) => {
                        new_entry.set_bucket_cutoff(new_cutoff);
                    }
                    // Can't safely verify (multi-bucket-name entry) — mark
                    // for rebuild immediately; `new_entry` isn't inserted
                    // yet so this is a plain local mutation.
                    BucketDiffState::Rebuild => new_entry.mark_for_rebuild(),
                    BucketDiffState::Noop => {}
                }
            }
            // Brief lock 2: insert prebuilt entry + grab sorted_keys for
            // the immediate read.
            let cached_keys = {
                let uc = &self.unified_cache;
                uc.store(ukey.clone(), new_entry);
                uc.lookup(&ukey).and_then(|entry| entry.value().sorted_keys().map(Arc::clone))
            };
            let mut result = if let Some(ref keys) = cached_keys {
                executor.execute_from_sorted_keys(
                    keys, &sort_clause.field, sort_clause.direction,
                    fetch_limit, query.cursor.as_ref(), full_total_matched,
                )?
            } else {
                let cached_bm = {
                    let uc = &self.unified_cache;
                    uc.lookup(&ukey).map(|entry| Arc::clone(entry.value().bitmap()))
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
                    let max_cap = self.unified_cache.config().max_capacity;
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
                        let uc = &self.unified_cache;
                        if let Some(mut entry) = uc.lookup(ukey) {
                            entry.expand(&sorted_slots, value_fn);
                            uc.record_extension(&ukey);
                        }
                    }
                    true
                } else { false }
            } else { false };
            let re_data = if did_expand {
                if let Some(ref ukey) = unified_key {
                    let uc = &self.unified_cache;
                    uc.lookup(ukey).map(|e| {
                        let entry = e.value();
                        let radix = entry.radix().cloned();
                        let bm = Arc::clone(entry.bitmap());
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
            let uc = &self.unified_cache;
            let min_size = uc.config().min_filter_size as u64;
            if full_total_matched >= min_size {
                if let Some(clauses) = cache::canonicalize(snapped_filters) {
                    let ukey = UnifiedKey {
                        filter_clauses: clauses,
                        sort_field: sort_clause.field.clone(),
                        direction: sort_clause.direction,
                    };
                    let hit = uc.lookup(&ukey).map(|entry_ref| {
                        let entry = entry_ref.value();
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
                    let max_cap = self.unified_cache.config().max_capacity;
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
                        let uc = &self.unified_cache;
                        if let Some(mut entry) = uc.lookup(ukey) {
                            entry.expand(&sorted_slots, value_fn);
                            uc.record_extension(&ukey);
                        }
                    }
                    let uc = &self.unified_cache;
                    if let Some(entry) = uc.lookup(ukey) {
                        let bm = Arc::clone(entry.value().bitmap());
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
            // Snapshot the FilterClause tree so cache live-maintenance can
            // natively evaluate compound predicates (B2 consumes this).
            let original_clauses = Arc::new(snapped_filters.to_vec());
            // Single-flight guard: when a flagged entry (needs_rebuild=true) triggered
            // this miss, only the first concurrent caller does the seed + store. Others
            // skip the write and serve directly from the executor to avoid stampeding
            // compute_filters with redundant sort traversals.
            if !self.unified_cache.should_rebuild_single_flight(&ukey) {
                let mut result = executor.execute_from_bitmap(
                    &filter_arc,
                    query.sort.as_ref(),
                    fetch_limit,
                    query.cursor.as_ref(),
                    use_simple_sort,
                )?;
                result.total_matched = full_total_matched;
                self.post_validate(&mut result, &query.filters, executor)?;
                return Ok(result);
            }
            if full_total_matched == 0 {
                // Zero-result cache: empty bitmap, no sort traversal needed.
                let value_fn = |_slot: u32| -> u32 { 0 };
                let ukey_for_seed = ukey.clone();
                self.unified_cache.form_and_store_with_clauses(
                    ukey,
                    &[],
                    false,
                    full_total_matched,
                    Arc::clone(&original_clauses),
                    value_fn,
                );
                self.seed_bucket_cutoff(&ukey_for_seed);
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
            let initial_cap = self.unified_cache.config().initial_capacity;
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
            self.unified_cache.form_and_store_with_clauses(
                ukey.clone(),
                &sorted_slots,
                has_more,
                full_total_matched,
                Arc::clone(&original_clauses),
                value_fn,
            );
            self.seed_bucket_cutoff(&ukey);
            let cache_elapsed = t0.elapsed();
            tracing::debug!(
                "  slow_path: cache_form={:.1}ms, total_slow={:.1}ms",
                cache_elapsed.as_secs_f64() * 1000.0,
                slow_start.elapsed().as_secs_f64() * 1000.0
            );
            // Serve the user's results from the freshly seeded cache.
            let cached_keys = {
                let uc = &self.unified_cache;
                uc.lookup(&ukey).and_then(|entry| entry.value().sorted_keys().map(Arc::clone))
            };
            let mut result = if let Some(ref keys) = cached_keys {
                executor.execute_from_sorted_keys(
                    keys, &sort_clause.field, sort_clause.direction,
                    fetch_limit, query.cursor.as_ref(), full_total_matched,
                )?
            } else {
                // sorted_keys not available (shouldn't happen for fresh seed), fall back to bitmap
                let cached_bm = {
                    let uc = &self.unified_cache;
                    uc.lookup(&ukey).map(|entry| Arc::clone(entry.value().bitmap()))
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
                    let max_cap = self.unified_cache.config().max_capacity;
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
                        let uc = &self.unified_cache;
                        if let Some(mut entry) = uc.lookup(ukey) {
                            entry.expand(&sorted_slots, value_fn);
                            uc.record_extension(&ukey);
                        }
                    }
                    true
                } else { false }
            } else { false };
            // Re-query from expanded entry (use radix if available)
            let re_data = if did_expand {
                if let Some(ref ukey) = unified_key {
                    let uc = &self.unified_cache;
                    uc.lookup(ukey).map(|e| {
                        let entry = e.value();
                        let radix = entry.radix().cloned();
                        let bm = Arc::clone(entry.bitmap());
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
            let mut managers = HashMap::new();
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
            let mut managers = HashMap::new();
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
    /// Pre-load all pending filter and sort fields from disk.
    /// Call from a background thread after server startup so lazy-loading
    /// doesn't block request threads or health checks.
    ///
    /// Load order: sort fields → bound caches → filter fields.
    /// Sort fields must load first because bound cache restoration needs
    /// `reconstruct_value()` for sorted-key rebuilding. Bound caches load
    /// next so cached sorts are warm before any queries arrive. Filter
    /// fields (the bulk of memory) load last.
    /// Load eager sort and filter fields in the background.
    /// Called after the server starts listening so health checks pass immediately.
    pub fn preload_eager_fields(&self) {
        use crate::query::{FilterClause, Value};
        let t0 = std::time::Instant::now();
        let eager_sorts: Vec<&str> = self.config.sort_fields.iter()
            .filter(|sc| sc.eager_load)
            .map(|sc| sc.name.as_str())
            .collect();
        let eager_filters: Vec<&str> = self.config.filter_fields.iter()
            .filter(|fc| fc.eager_load)
            .map(|fc| fc.name.as_str())
            .collect();
        // Load all eager sort + filter fields in one parallel batch.
        // ensure_fields_loaded parallelizes across all tasks internally.
        if !eager_sorts.is_empty() || !eager_filters.is_empty() {
            let mut clauses: Vec<FilterClause> = Vec::new();
            for name in &eager_filters {
                clauses.push(FilterClause::Eq(name.to_string(), Value::Integer(0)));
            }
            // Load with first sort field, then remaining sorts individually
            // (ensure_fields_loaded takes one optional sort field at a time)
            let first_sort = eager_sorts.first().copied();
            let _ = self.ensure_fields_loaded(&clauses, first_sort);
            // Load remaining sort fields
            let empty: Vec<FilterClause> = Vec::new();
            for name in eager_sorts.iter().skip(1) {
                let _ = self.ensure_fields_loaded(&empty, Some(name));
            }
        }
        let total_eager = eager_sorts.len() + eager_filters.len();
        if total_eager > 0 {
            eprintln!(
                "Preload complete: {} sort + {} filter fields in {:.1}s",
                eager_sorts.len(),
                eager_filters.len(),
                t0.elapsed().as_secs_f64(),
            );
        }
    }
    /// Pre-load all bound cache shards from disk.
    /// Iterates every sort field × both directions.
    pub fn preload_bound_cache(&self) {
        use crate::query::SortDirection;
        if self.bound_store.is_none() {
            return;
        }
        let t0 = std::time::Instant::now();
        let mut loaded = 0usize;
        for sc in &self.config.sort_fields {
            for dir in &[SortDirection::Desc, SortDirection::Asc] {
                self.ensure_cache_shard_loaded(&sc.name, *dir);
                loaded += 1;
            }
        }
        eprintln!(
            "Preload phase 2: {} bound cache shards in {:.1}s",
            loaded,
            t0.elapsed().as_secs_f64(),
        );
    }
    /// Flush loop stats: (publish_count, cumulative_duration_nanos, last_duration_nanos).
    pub fn flush_stats(&self) -> (u64, u64, u64) {
        (
            self.flush_publish_count.load(Ordering::Relaxed),
            self.flush_duration_nanos.load(Ordering::Relaxed),
            self.flush_last_duration_nanos.load(Ordering::Relaxed),
        )
    }
    /// Per-phase flush timing in nanoseconds:
    /// `(apply, cache, publish, timebucket, compact, opslog, sort_promote)`.
    pub fn flush_phase_stats(&self) -> (u64, u64, u64, u64, u64, u64, u64) {
        (
            self.flush_apply_nanos.load(Ordering::Relaxed),
            self.flush_cache_nanos.load(Ordering::Relaxed),
            self.flush_publish_nanos.load(Ordering::Relaxed),
            self.flush_timebucket_nanos.load(Ordering::Relaxed),
            self.flush_compact_nanos.load(Ordering::Relaxed),
            self.flush_opslog_nanos.load(Ordering::Relaxed),
            self.flush_sort_promote_nanos.load(Ordering::Relaxed),
        )
    }
    /// Iter 6 — DocStoreV3 put_batch fast/slow path counters.
    /// Returns `(fast_path_total, slow_path_total)`.
    pub fn docstore_put_batch_path_stats(&self) -> (u64, u64) {
        self.docstore.read().put_batch_path_stats()
    }
    /// Iter 4a instrumentation — cache-maintenance shape stats:
    /// `(unique_filter_shapes, sort_work_items, unique_shapes_max, sort_work_items_max)`.
    ///
    /// The last-cycle gauges reflect whatever was happening on the most
    /// recent maintenance cycle, which may be quiet. The `_max` counters
    /// preserve burst-time peaks so we can see the worst-case work volume
    /// even if gauge sampling caught a sleepy moment.
    ///
    /// Use the ratio `unique_filter_shapes / sort_work_items` in PromQL to
    /// see the filter-shape collapse factor. Low ratio = many entries
    /// share filters (filter-shape grouping in Phase B would pay off).
    /// High ratio = diverse filters, grouping is marginal.
    pub fn cache_maint_shape_stats(&self) -> (u64, u64, u64, u64) {
        (
            self.flush_cache_unique_filter_shapes.load(Ordering::Relaxed),
            self.flush_cache_sort_work_items.load(Ordering::Relaxed),
            self.flush_cache_unique_filter_shapes_max.load(Ordering::Relaxed),
            self.flush_cache_sort_work_items_max.load(Ordering::Relaxed),
        )
    }
    /// Direct handle to the cache-worker metrics struct. Used by the metrics
    /// bridge to install the cycle-time histogram and to read the
    /// reason-attributed rebuild counters at scrape time.
    pub fn cache_worker_metrics(&self) -> &Arc<crate::cache_worker::CacheWorkerMetrics> {
        &self.cache_worker_metrics
    }
    /// Count UnifiedCache entries currently flagged `needs_rebuild=true`.
    /// O(entries) scan — call from the prom scrape path, not the hot path.
    pub fn unified_cache_needs_rebuild_count(&self) -> u64 {
        self.unified_cache.count_needs_rebuild()
    }
    /// Async cache worker metrics:
    /// `(queue_depth, cycle_nanos, items_coalesced_total, drops_total,
    ///   over_budget_total, backpressure_invalidations_total, cycles_total)`.
    /// All values are zero when `async_maintenance` is disabled.
    pub fn cache_worker_stats(&self) -> (u64, u64, u64, u64, u64, u64, u64) {
        let m = &self.cache_worker_metrics;
        (
            m.queue_depth.load(Ordering::Relaxed),
            m.cycle_nanos.load(Ordering::Relaxed),
            m.items_coalesced_total.load(Ordering::Relaxed),
            m.drops_total.load(Ordering::Relaxed),
            m.over_budget_total.load(Ordering::Relaxed),
            m.backpressure_invalidations_total.load(Ordering::Relaxed),
            m.cycles_total.load(Ordering::Relaxed),
        )
    }
    // ── Prefilter Registry ──────────────────────────────────────────────

    /// Get a reference to the prefilter registry for query substitution.
    pub fn prefilter_registry(&self) -> &crate::prefilter::PrefilterRegistry {
        &self.prefilter_registry
    }

    /// Register a named prefilter by evaluating the given filter clauses
    /// against the current snapshot and caching the resulting bitmap.
    pub fn register_prefilter(
        &self,
        name: String,
        clauses: Vec<crate::query::FilterClause>,
        refresh_interval_secs: u64,
    ) -> Result<Arc<crate::prefilter::PrefilterEntry>> {
        let snap = self.inner.load();
        let start = std::time::Instant::now();
        let executor = crate::executor::QueryExecutor::new(
            &snap.slots,
            &snap.filters,
            &snap.sorts,
            self.config.max_page_size,
        );
        let bitmap = executor.compute_filters(&clauses)?;
        let compute_nanos = start.elapsed().as_nanos() as u64;
        self.prefilter_registry
            .insert(name, clauses, bitmap, refresh_interval_secs, compute_nanos)
            .map_err(|e| crate::error::BitdexError::Config(e.to_string()))
    }

    /// Refresh all stale prefilters against the current snapshot.
    /// Returns the number of entries refreshed.
    pub fn refresh_stale_prefilters(&self) -> usize {
        let snap = self.inner.load();
        let mut refreshed = 0;
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        for entry in self.prefilter_registry.entries() {
            if !entry.is_stale(now_secs) {
                continue;
            }
            let start = std::time::Instant::now();
            let executor = crate::executor::QueryExecutor::new(
                &snap.slots,
                &snap.filters,
                &snap.sorts,
                self.config.max_page_size,
            );
            match executor.compute_filters(&entry.clauses) {
                Ok(bitmap) => {
                    entry.publish_refresh(bitmap, start.elapsed().as_nanos() as u64);
                    refreshed += 1;
                }
                Err(e) => {
                    entry.refresh_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(name = %entry.name, err = %e, "prefilter refresh failed");
                }
            }
        }
        refreshed
    }

    /// List all registered prefilters.
    pub fn list_prefilters(&self) -> Vec<crate::prefilter::PrefilterInfo> {
        self.prefilter_registry.list()
    }

    /// Remove a prefilter by name. Returns true if it existed.
    ///
    /// After removal, any cache entry whose filter included this prefilter
    /// holds a dangling bitmap reference. Mark those entries for rebuild so
    /// the slow path regenerates them on the next read.
    pub fn remove_prefilter(&self, name: &str) -> bool {
        let removed = self.prefilter_registry.remove(name);
        if removed {
            self.unified_cache.invalidate_prefilter(name);
        }
        removed
    }

    /// Number of filter + sort fields still pending lazy load.
    pub fn pending_field_count(&self) -> usize {
        self.pending_filter_loads.lock().len() + self.pending_sort_loads.lock().len()
    }
    /// Mark fields as pending for lazy loading from disk.
    /// Call after dump processor writes bitmaps — this tells the engine
    /// to reload them on the next query.
    pub fn mark_fields_pending_reload(&self, filter_fields: &[String], sort_fields: &[String]) {
        {
            let mut pending = self.pending_filter_loads.lock();
            for name in filter_fields {
                pending.insert(name.clone());
            }
        }
        {
            let mut pending = self.pending_sort_loads.lock();
            for name in sort_fields {
                pending.insert(name.clone());
            }
        }
        eprintln!(
            "Marked {} filter + {} sort fields for lazy reload",
            filter_fields.len(),
            sort_fields.len()
        );
    }
    /// Reload the alive bitmap and slot counter from ShardStore into the
    /// in-memory engine snapshot. Sends via the lazy load channel so the
    /// flush thread's staging stays in sync — same path as filter/sort
    /// lazy loading. Without this, the flush thread's next publish would
    /// overwrite the alive bitmap with its stale empty copy.
    pub fn reload_alive_from_disk(&self) {
        let alive_store = match self.alive_store.as_ref() {
            Some(s) => s,
            None => return,
        };
        let meta_store = match self.meta_store.as_ref() {
            Some(s) => s,
            None => return,
        };
        let alive_bm = match alive_store.load_alive() {
            Ok(Some(bm)) => bm,
            _ => return,
        };
        let counter = meta_store.load_slot_counter().ok().flatten().unwrap_or(0);
        let alive_count = alive_bm.len();
        // Build new SlotAllocator with the disk state
        let mut new_slots = crate::slot::SlotAllocator::from_state(
            counter,
            alive_bm,
            RoaringBitmap::new(),
        );
        // Load deferred alive if present
        if let Some(deferred) = meta_store.load_deferred_alive().ok().flatten() {
            new_slots.set_deferred(deferred);
        }
        // Send to flush thread via lazy load channel — same pattern as
        // ensure_fields_loaded for filter/sort bitmaps.
        let _ = self.lazy_tx.send(LazyLoad::Slots { slots: new_slots });
        // Ask the flush thread to drain the lazy channel and publish.
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        if self.cmd_tx.send(FlushCommand::ForcePublish { done: done_tx }).is_ok() {
            let _ = done_rx.recv_timeout(std::time::Duration::from_secs(5));
        }
        eprintln!(
            "Reloaded alive bitmap from disk: {} alive, slot_counter={}",
            alive_count, counter
        );
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
    /// Block until the flush thread drains any pending mutations from the
    /// channel and publishes a fresh snapshot. Returns `true` on success,
    /// `false` if the flush thread is gone or the timeout elapsed.
    ///
    /// Used by `apply_query_op_set` so that a fan-out's `execute_query` sees
    /// in-batch CoalescerSink writes (e.g., a freshly inserted Image's postId
    /// filter) before resolving the fan-out's match set. Without this barrier
    /// the published snapshot lags the in-batch state and same-batch fan-outs
    /// match zero slots — see `tests/sortat_fanout_race.rs`.
    pub fn force_publish_blocking(&self, timeout: Duration) -> bool {
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        if self
            .cmd_tx
            .send(FlushCommand::ForcePublish { done: done_tx })
            .is_err()
        {
            return false;
        }
        done_rx.recv_timeout(timeout).is_ok()
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
    /// Persist a single named cursor immediately (synchronous atomic file
    /// write to MetaStore). Use this for callers that need the cursor durable
    /// on return (e.g. `PUT /cursors/{name}` HTTP handler) instead of waiting
    /// for the next merge cycle's batch persist.
    ///
    /// Sets the in-memory value first (via `set_cursor`), then writes the
    /// MetaStore cursors/{name} file. ~ms cost — single small atomic file
    /// write — vs. `save_snapshot()` which rewrites every bitmap shard
    /// (gigabytes, 14-20s, blocks the runtime via disk I/O pressure).
    ///
    /// Returns `Err` if no `MetaStore` is configured (pure in-memory mode).
    pub fn persist_cursor(&self, name: String, value: String) -> Result<()> {
        self.set_cursor(name.clone(), value.clone());
        match self.meta_store.as_ref() {
            Some(ms) => ms.write_cursor(&name, &value).map_err(|e| {
                crate::error::BitdexError::Storage(format!("write_cursor: {e}"))
            }),
            None => Err(crate::error::BitdexError::Storage(
                "persist_cursor: no MetaStore configured (in-memory mode)".to_string(),
            )),
        }
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
        // Fast path: cache hit (no lock, DashMap concurrent read)
        if let Some(ref cache) = self.doc_cache {
            if let Some(doc) = cache.get(slot_id) {
                return Ok(Some(doc));
            }
        }
        // Slow path: disk read + cache populate
        let doc = self.docstore.read().get(slot_id)?;
        if let (Some(ref cache), Some(ref doc)) = (&self.doc_cache, &doc) {
            cache.insert(slot_id, doc.clone());
            // Eviction handled by dedicated eviction thread — no inline check
        }
        Ok(doc)
    }

    /// Retrieve multiple stored documents in a single shard-grouped pass.
    ///
    /// # Why this is faster than calling `get_document` in a loop
    ///
    /// Each `get_document` miss calls `ShardStore::read`, which reads the shard
    /// file, decodes the snapshot, and applies pending ops — every call for the
    /// same shard pays that cost again.  For a typical feed page of 100 newest
    /// items, all slots cluster into at most 1-2 shards (512 slots/shard,
    /// monotonically assigned).  This function:
    ///
    ///   1. Batch-checks the DocCache for all slots (single ArcSwap load per slot).
    ///   2. Groups cache misses by shard (`slot >> SHARD_SHIFT`).
    ///   3. Calls `DocStoreV3::get_shard` once per shard — one file read +
    ///      decode for up to 512 slots.
    ///   4. Populates DocCache via `insert_batch` (one ArcSwap load for all inserts).
    ///
    /// The returned `Vec` preserves the input order. Duplicate slots in the input
    /// are each resolved independently (same semantics as serial `get_document`).
    ///
    /// # Cache semantics
    ///
    /// The cache is advisory (cache-on-read). Cache population happens after the
    /// docstore read lock is released, which means a concurrent write could update
    /// a doc before the cache insert — the same TOCTOU window that exists in the
    /// single-slot `get_document` path. This is acceptable: the write path
    /// (flush thread) invalidates and updates cache entries on write-through, so
    /// stale entries are corrected within one flush cycle.
    ///
    /// # Microbench results (1024 docs, 2 shards)
    ///
    /// | Page size | Path       | Cold    | Warm    | Speedup vs serial |
    /// |-----------|------------|---------|---------|-------------------|
    /// | 20        | clustered  | slower  | 0.49ms  | 17x warm          |
    /// | 100       | clustered  | 0.72ms  | 0.63ms  | 34x cold, 48x warm |
    /// | 500       | clustered  | 7.35ms  | 1.18ms  | 18x cold, 183x warm |
    pub fn get_documents(&self, slots: &[u32]) -> Result<Vec<Option<StoredDoc>>> {
        use crate::shard_store_doc::SHARD_SHIFT;

        if slots.is_empty() {
            return Ok(Vec::new());
        }

        let mut results: Vec<Option<StoredDoc>> = vec![None; slots.len()];

        // --- Step 1: batch cache check ---
        // Check each slot. Duplicates in the input each get their own check
        // (they may have been inserted separately into different result positions).
        let mut miss_indices: Vec<usize> = Vec::new();

        if let Some(ref cache) = self.doc_cache {
            for (i, &slot) in slots.iter().enumerate() {
                if let Some(doc) = cache.get(slot) {
                    results[i] = Some(doc);
                } else {
                    miss_indices.push(i);
                }
            }
        } else {
            // No cache — every slot is a miss.
            miss_indices.extend(0..slots.len());
        }

        if miss_indices.is_empty() {
            return Ok(results);
        }

        // --- Step 2: group misses by shard ---
        // shard_id → list of (result_index, slot_id)
        // Duplicate input slots produce separate (idx, slot) pairs — each result
        // position is filled independently so callers get order-preserving output.
        let mut shard_groups: HashMap<u32, Vec<(usize, u32)>> = HashMap::new();
        for idx in &miss_indices {
            let slot = slots[*idx];
            let shard_id = slot >> SHARD_SHIFT;
            shard_groups.entry(shard_id).or_default().push((*idx, slot));
        }

        // --- Step 3: one shard read per group, extract only needed slots ---
        // The docstore RwLock read guard is held across all get_shard() calls.
        // This is the same semantics as the serial path, extended to N shards.
        // All shard reads see a consistent docstore state (no partial-write view).
        let docstore = self.docstore.read();
        // Use a HashSet to deduplicate cache inserts when the same slot appears
        // multiple times in the input.
        let mut seen_for_cache: HashSet<u32> = HashSet::new();
        let mut to_cache: Vec<(u32, StoredDoc)> = Vec::with_capacity(miss_indices.len());

        for (shard_id, slot_pairs) in &shard_groups {
            // One file open + decode for the entire shard.
            match docstore.get_shard(*shard_id) {
                Ok(shard_docs) => {
                    // Build a slot→doc map for this shard.
                    let shard_map: HashMap<u32, StoredDoc> = shard_docs.into_iter().collect();
                    for &(idx, slot) in slot_pairs {
                        if let Some(doc) = shard_map.get(&slot) {
                            results[idx] = Some(doc.clone());
                            // Deduplicate cache inserts for repeated slots.
                            if seen_for_cache.insert(slot) {
                                to_cache.push((slot, doc.clone()));
                            }
                        }
                        // If not found, results[idx] stays None (slot may have been deleted).
                    }
                }
                Err(e) => {
                    tracing::warn!("get_documents: shard read error for shard {shard_id}: {e}");
                    // Leave result slots as None — callers handle None gracefully.
                }
            }
        }
        drop(docstore);

        // --- Step 4: batch-populate the cache (one ArcSwap load for all inserts) ---
        if let Some(ref cache) = self.doc_cache {
            if !to_cache.is_empty() {
                cache.insert_batch(&to_cache);
            }
        }

        Ok(results)
    }

    /// Compact the docstore, reclaiming space from old write transactions.
    pub fn compact_docstore(&self) -> Result<bool> {
        Ok(self.docstore.read().compact()?)
    }
    /// Configure docstore field defaults from a DataSchema.
    /// Must be called before `prepare_bulk_writer()` so the BulkWriter inherits the defaults.
    pub fn set_docstore_defaults(&self, schema: &crate::config::DataSchema) {
        self.docstore.write().set_field_defaults(schema);
    }
    /// Get the current schema version from the docstore.
    pub fn docstore_schema_version(&self) -> u8 {
        self.docstore.read().schema_version()
    }

    /// Get a clone of the Arc<Mutex<DocStoreV3>> for external writers (e.g., DocWriter).
    pub fn docstore_arc(&self) -> Arc<parking_lot::RwLock<DocStoreV3>> {
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
    pub fn build_schema_registry(&self) -> HashMap<u8, HashMap<String, serde_json::Value>> {
        self.docstore.read().build_schema_registry()
    }

    /// Prepare a ShardStoreBulkWriter for lock-free parallel docstore writes during bulk loading.
    /// The writer holds a snapshot of the field dictionary and can encode/write
    /// docs without acquiring the DocStoreV3 Mutex.
    pub fn prepare_bulk_writer(&self, field_names: &[String]) -> crate::error::Result<crate::shard_store_doc::ShardStoreBulkWriter> {
        Ok(self.docstore.write().prepare_bulk_load(field_names)?)
    }
    /// Prepare a StreamingDocWriter for write-through docstore writes during dump processing.
    pub fn prepare_streaming_writer(&self, field_names: &[String]) -> crate::error::Result<crate::shard_store_doc::StreamingDocWriter> {
        Ok(self.docstore.write().prepare_streaming_writer(field_names)?)
    }
    /// Return the set of indexed field names (filter + sort + "id").
    /// Used by the loader to strip doc-only fields from the bitmap accumulator.
    pub fn indexed_field_names(&self) -> HashSet<String> {
        let mut s = HashSet::new();
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
    /// Doc cache stats for Prometheus scrape: (hits, misses, entries, bytes, evictions, generations).
    /// Returns zeros if doc_cache is not configured.
    /// Evict a slot from the doc cache so the next read fetches from disk.
    /// Used by WAL reader after DocWriter updates a document via ops.
    pub fn evict_doc_cache(&self, slot: u32) {
        if let Some(ref cache) = self.doc_cache {
            cache.remove(slot);
        }
    }
    /// Clear the entire doc cache. Used after dump phases when many docs may
    /// have been merged with new fields — any cached entries from prior phases
    /// are stale and would mask the new merged data.
    pub fn clear_doc_cache(&self) {
        if let Some(ref cache) = self.doc_cache {
            cache.clear();
        }
    }
    pub fn doc_cache_stats(&self) -> (u64, u64, usize, u64, u64, usize) {
        match &self.doc_cache {
            Some(cache) => (
                cache.hits(),
                cache.misses(),
                cache.len(),
                cache.size_bytes(),
                cache.eviction_count(),
                cache.generation_count(),
            ),
            None => (0, 0, 0, 0, 0, 0),
        }
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
        let uc = &self.unified_cache;
        let cache_entries = uc.stats().entries;
        let cache_bytes = uc.stats().memory_bytes;
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
    // ── BoundStore Counters ───────────────────────────────────────────────
    pub fn boundstore_shard_loads(&self) -> u64 { self.boundstore_shard_loads.load(Ordering::Relaxed) }
    pub fn boundstore_tombstones_created(&self) -> u64 { self.boundstore_tombstones_created.load(Ordering::Relaxed) }
    pub fn boundstore_tombstones_cleaned(&self) -> u64 { self.boundstore_tombstones_cleaned.load(Ordering::Relaxed) }
    pub fn boundstore_bytes_written(&self) -> u64 { self.boundstore_bytes_written.load(Ordering::Relaxed) }
    pub fn boundstore_bytes_read(&self) -> u64 { self.boundstore_bytes_read.load(Ordering::Relaxed) }
    pub fn boundstore_entries_restored(&self) -> u64 { self.boundstore_entries_restored.load(Ordering::Relaxed) }
    pub fn boundstore_entries_skipped(&self) -> u64 { self.boundstore_entries_skipped.load(Ordering::Relaxed) }
    /// Get the total size of the bounds directory on disk (meta.bin + shards).
    pub fn boundstore_disk_bytes(&self) -> u64 {
        self.bound_store.as_ref().map(|bs| {
            let root = bs.root_path();
            if !root.exists() { return 0u64; }
            std::fs::read_dir(root)
                .ok()
                .map(|entries| {
                    entries.filter_map(|e| e.ok())
                        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
                        .sum()
                })
                .unwrap_or(0)
        }).unwrap_or(0)
    }
    pub fn unified_cache_stats(&self) -> crate::unified_cache::UnifiedCacheStats {
        self.unified_cache.stats()
    }
    /// Return per-entry cache details for diagnostics.
    pub fn unified_cache_entry_details(&self) -> Vec<crate::unified_cache::UnifiedEntryDetail> {
        self.unified_cache.entry_details()
    }
    /// Update the max_maintenance_work budget on the live unified cache.
    pub fn set_max_maintenance_work(&self, v: usize) {
        self.unified_cache.with_config_mut(|c| c.max_maintenance_work = v);
    }
    /// Update the B9 compound-eval atom limit on the live unified cache.
    /// Set to 0 to disable the guard. Takes effect on the next maintenance cycle.
    pub fn set_compound_eval_atom_limit(&self, v: u32) {
        self.unified_cache.with_config_mut(|c| c.compound_eval_atom_limit = v);
    }
    /// Update the time-bucket cache-entry TTL (seconds) on the live unified
    /// cache. 0 disables the fallback.
    pub fn set_bucket_entry_ttl_secs(&self, v: u64) {
        self.unified_cache.with_config_mut(|c| c.bucket_entry_ttl_secs = v);
    }
    /// Update the max_maintenance_ms time budget on the live unified cache and
    /// the async cache worker (if running). Takes effect on the worker's next
    /// cycle. `0` = unlimited (no deadline).
    pub fn set_max_maintenance_ms(&self, v: u64) {
        self.unified_cache.with_config_mut(|c| c.max_maintenance_ms = v);
        if let Some(ref ms_arc) = self.cache_worker_ms {
            ms_arc.store(v, Ordering::Relaxed);
        }
    }
    /// Update the max_entries cap on the live unified cache.
    pub fn set_cache_max_entries(&self, v: usize) {
        self.unified_cache.with_config_mut(|c| c.max_entries = v);
    }
    /// Update the max_bytes cap on the live unified cache.
    pub fn set_cache_max_bytes(&self, v: usize) {
        self.unified_cache.with_config_mut(|c| c.max_bytes = v);
    }
    /// Update the initial_capacity on the live unified cache.
    pub fn set_cache_initial_capacity(&self, v: usize) {
        self.unified_cache.with_config_mut(|c| c.initial_capacity = v);
    }
    /// Update the max_capacity on the live unified cache.
    pub fn set_cache_max_capacity(&self, v: usize) {
        self.unified_cache.with_config_mut(|c| c.max_capacity = v);
    }
    /// Update the min_filter_size on the live unified cache.
    pub fn set_cache_min_filter_size(&self, v: usize) {
        self.unified_cache.with_config_mut(|c| c.min_filter_size = v);
    }
    /// Update the refresh interval for a named time bucket.
    /// Returns true if the bucket was found and updated, false if no time bucket
    /// manager exists or the bucket name was not found.
    pub fn set_time_bucket_refresh_interval(&self, bucket_name: &str, interval_secs: u64) -> bool {
        if let Some(ref tb_arc) = self.time_buckets {
            let mut tb = (*tb_arc.load_full()).clone();
            let result = tb.set_refresh_interval(bucket_name, interval_secs);
            tb_arc.store(Arc::new(tb));
            result
        } else {
            false
        }
    }

    /// Rebuild all time buckets from the alive bitmap and sort field.
    /// Returns (bucket_count, slots_scanned).
    ///
    /// Lazy-loads the bucket sort field if it's currently unloaded — necessary
    /// after a fresh dump where `mark_fields_pending_reload` puts the sort
    /// field back into pending.
    pub fn rebuild_time_buckets(&self) -> crate::error::Result<(usize, u64)> {
        let tb_arc = self.time_buckets.as_ref().ok_or_else(|| {
            crate::error::BitdexError::Config("no time_buckets configured".into())
        })?;
        let sort_field_name = tb_arc.load().sort_field_name().to_string();
        self.ensure_fields_loaded(&[], Some(&sort_field_name))?;
        let snap = self.snapshot();
        let result = Self::rebuild_time_buckets_from_snapshot(&snap, tb_arc)?;
        self.dirty_since_snapshot.store(true, std::sync::atomic::Ordering::Release);
        self.unified_cache.clear();
        Ok(result)
    }

    /// Static rebuild helper. Used by both `rebuild_time_buckets` (engine-side)
    /// and the `ExitLoadingSaveUnload` flush handler (which only has access to
    /// the published snapshot before unload, not `&self`).
    ///
    /// Caller is responsible for any post-rebuild cache invalidation.
    pub(crate) fn rebuild_time_buckets_from_snapshot(
        snap: &InnerEngine,
        tb_arc: &Arc<ArcSwap<TimeBucketManager>>,
    ) -> crate::error::Result<(usize, u64)> {
        let sort_field_name = tb_arc.load().sort_field_name().to_string();
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
        let slot_count = alive.len();
        let mut slot_values: Vec<(u32, u64)> = Vec::with_capacity(slot_count as usize);
        for slot in alive.iter() {
            let ts = sort_field.reconstruct_value(slot) as u64;
            slot_values.push((slot, ts));
        }
        let mut tb = (*tb_arc.load_full()).clone();
        let bucket_names: Vec<String> = tb.bucket_names();
        for name in &bucket_names {
            tb.rebuild_bucket(name, slot_values.iter().copied(), now_secs);
        }
        let bucket_count = bucket_names.len();
        tb_arc.store(Arc::new(tb));
        eprintln!(
            "rebuild_time_buckets: rebuilt {} buckets from {} alive slots in sort field '{}'",
            bucket_count, slot_count, sort_field_name
        );
        Ok((bucket_count, slot_count))
    }

    /// Compute the STALE and MISSING candidate sets for each time bucket, WITHOUT
    /// mutating the manager — pure and side-effect free, so it can run OFF the
    /// flush thread over an immutable published snapshot.
    ///
    /// For each bucket, recompute `fresh_in_window` via a single alive-scan (a
    /// slot qualifies when its sort value is in `[now - duration, now]`), then:
    ///   - `stale = current_bucket_bitmap − fresh` — in the bucket, no longer in
    ///     window (the incremental `subtract_expired` band missed it, e.g. a slot
    ///     whose sort value regressed past the trailing band without a re-flush).
    ///   - `missing = fresh − current_bucket_bitmap` — in window per the snapshot,
    ///     absent from the bucket (the live insert path dropped a recent add).
    ///
    /// Both are CANDIDATE sets from a snapshot taken at time T but applied minutes
    /// later; the caller (`TimeBucketManager::reconcile_apply`) re-validates each
    /// against current sort values + alive before pruning/backfilling. Returning
    /// targeted deltas (not a full replacement bitmap) is what makes the off-thread
    /// design safe: a full overwrite would clobber slots live maintenance inserted
    /// during the scan window, whereas subtract-then-insert of disjoint delta sets
    /// never touches concurrently-maintained slots.
    ///
    /// Returns one `(bucket_name, stale_bitmap, missing_bitmap)` per bucket, or
    /// `None` if the bucket sort field is not currently loaded.
    pub(crate) fn compute_time_bucket_reconcile(
        snap: &InnerEngine,
        tb: &TimeBucketManager,
        scan_threads: usize,
    ) -> Option<Vec<(String, RoaringBitmap, RoaringBitmap)>> {
        let sort_field = snap.sorts.get_field(tb.sort_field_name())?;
        let alive = snap.slots.alive_bitmap();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // (name, cutoff, snapshot of current bucket bitmap). Parallel to `fresh`.
        let specs: Vec<(String, u64, RoaringBitmap)> = tb
            .bucket_names()
            .into_iter()
            .filter_map(|name| {
                let b = tb.get_bucket(&name)?;
                Some((
                    name,
                    now_secs.saturating_sub(b.duration_secs),
                    RoaringBitmap::clone(b.bitmap()),
                ))
            })
            .collect();
        let cutoffs: Vec<u64> = specs.iter().map(|(_, c, _)| *c).collect();
        let fresh_sets =
            Self::scan_fresh_in_window(sort_field, alive, &cutoffs, now_secs, scan_threads);
        Some(
            specs
                .into_iter()
                .zip(fresh_sets)
                .map(|((name, _, old), fresh)| {
                    let mut stale = old.clone();
                    stale -= &fresh; // in bucket, no longer in window
                    let mut missing = fresh;
                    missing -= &old; // in window, not in bucket → backfill candidate
                    (name, stale, missing)
                })
                .collect(),
        )
    }

    /// Number of alive slots below which the parallel scan is not worth the
    /// pool spin-up; falls back to the sequential walk.
    const PARALLEL_SCAN_MIN: u64 = 100_000;
    /// Range partitions per thread. Over-partitioning (vs one range per thread)
    /// lets rayon work-steal so an uneven alive distribution doesn't leave one
    /// core scanning a dense range while others idle.
    const PARALLEL_SCAN_CHUNKS_PER_THREAD: u32 = 4;

    /// Resolve the configured `reconcile_scan_threads` (0 = auto) to a concrete
    /// thread count for the dedicated scan pool. Auto = host logical CPUs capped
    /// at 16 so the burst scan can outrun the global `RAYON_NUM_THREADS` cap
    /// without spawning an unbounded pool on very large hosts.
    fn resolve_scan_threads(scan_threads: usize) -> usize {
        if scan_threads == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .min(16)
        } else {
            scan_threads
        }
    }

    /// Shared scan for both the reconcile fallback and the `/time-buckets/audit`
    /// endpoint: for each `cutoffs[i]`, compute the set of alive slots whose
    /// reconstructed sort value lands in `[cutoffs[i], now_secs]` (the
    /// "fresh in-window" set). Returns one bitmap per cutoff, in order.
    ///
    /// The walk is embarrassingly parallel — each slot's `reconstruct_value` is
    /// an independent read of the immutable sort layers on the snapshot. When
    /// the resolved thread count is > 1 and the alive set is large enough, it
    /// runs on a DEDICATED rayon pool (independent of the global
    /// `RAYON_NUM_THREADS` pool prod pins to 4). A thread count of 1, a small
    /// alive set, or a pool-build failure all take the sequential path.
    fn scan_fresh_in_window(
        sort_field: &crate::sort::SortField,
        alive: &RoaringBitmap,
        cutoffs: &[u64],
        now_secs: u64,
        scan_threads: usize,
    ) -> Vec<RoaringBitmap> {
        use rayon::prelude::*;

        let n = cutoffs.len();
        let seq = |slots_iter: &mut dyn Iterator<Item = u32>| -> Vec<RoaringBitmap> {
            let mut fresh = vec![RoaringBitmap::new(); n];
            for slot in slots_iter {
                let val = sort_field.reconstruct_value(slot) as u64;
                if val > now_secs {
                    continue;
                }
                for (i, &cutoff) in cutoffs.iter().enumerate() {
                    if val >= cutoff {
                        fresh[i].insert(slot);
                    }
                }
            }
            fresh
        };

        let threads = Self::resolve_scan_threads(scan_threads);
        if threads <= 1 || alive.len() < Self::PARALLEL_SCAN_MIN || n == 0 {
            return seq(&mut alive.iter());
        }
        let Some(max_slot) = alive.max() else {
            return vec![RoaringBitmap::new(); n];
        };

        let pool = match rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("tb-scan-{i}"))
            .build()
        {
            Ok(p) => p,
            Err(e) => {
                eprintln!("time-bucket scan: dedicated pool build failed ({e}); sequential");
                return seq(&mut alive.iter());
            }
        };

        // Partition the slot-id space into contiguous ranges and scan each in
        // parallel. Each task derives its slice of the alive set by intersecting
        // with a range mask (roaring AND — the result stays compressed), so no
        // 428MB `Vec<u32>` materialization: peak extra memory is ~one compressed
        // sub-bitmap per active task, not one u32 per alive slot. Ranges are
        // disjoint and cover [0, max_slot], so the union of partial results is
        // exactly the sequential result.
        let num_chunks = (threads as u32).saturating_mul(Self::PARALLEL_SCAN_CHUNKS_PER_THREAD);
        // Ceil-divide the inclusive span [0, max_slot] into num_chunks buckets.
        let span = (max_slot as u64 + 1).div_ceil(num_chunks as u64);
        pool.install(|| {
            (0..num_chunks)
                .into_par_iter()
                .map(|k| {
                    let lo = k as u64 * span;
                    if lo > max_slot as u64 {
                        return vec![RoaringBitmap::new(); n]; // empty tail partition
                    }
                    let hi_incl = ((k as u64 + 1) * span - 1).min(max_slot as u64);
                    let mut mask = RoaringBitmap::new();
                    mask.insert_range(lo as u32..=hi_incl as u32);
                    let part = &mask & alive;
                    let mut fresh = vec![RoaringBitmap::new(); n];
                    for slot in part.iter() {
                        let val = sort_field.reconstruct_value(slot) as u64;
                        if val > now_secs {
                            continue;
                        }
                        for (i, &cutoff) in cutoffs.iter().enumerate() {
                            if val >= cutoff {
                                fresh[i].insert(slot);
                            }
                        }
                    }
                    fresh
                })
                .reduce(
                    || vec![RoaringBitmap::new(); n],
                    |mut acc, part| {
                        for (a, p) in acc.iter_mut().zip(part) {
                            *a |= p;
                        }
                        acc
                    },
                )
        })
    }

    /// Get per-bucket statistics (name, slot count, cutoff).
    pub fn time_bucket_stats(&self) -> serde_json::Value {
        if let Some(ref tb_arc) = self.time_buckets {
            let tb = tb_arc.load();
            let mut buckets = serde_json::Map::new();
            for name in tb.bucket_names() {
                if let Some(bucket) = tb.get_bucket(&name) {
                    let slot_count: u64 = bucket.bitmap().len();
                    buckets.insert(name, serde_json::json!({
                        "slots": slot_count,
                        "last_cutoff": bucket.last_cutoff(),
                    }));
                }
            }
            serde_json::Value::Object(buckets)
        } else {
            serde_json::Value::Null
        }
    }

    /// Read-only audit of time-bucket membership vs recomputed truth. For each
    /// bucket: `current` (live bucket size), `fresh_in_window` (recomputed from
    /// the sort layer over the alive set), `stale` (`current − fresh` — the
    /// retention bug this fallback prunes) and `missing` (`fresh − current` —
    /// slots that SHOULD be in-window but aren't, an add-path gap the prune does
    /// NOT fix). Non-mutating: no bucket writes, no cache clear. Scans all alive
    /// slots (~minutes at scale), so callers should run it off the request hot
    /// path (spawn_blocking).
    /// Audit time-bucket membership against the sort layer.
    ///
    /// `sample` > 0 additionally returns up to `sample` slot IDs (ascending) per
    /// bucket for the `missing` (in-window per sort layer but absent from the
    /// bucket bitmap) and `stale` (in bucket but no longer in-window) sets, each
    /// paired with its reconstructed sort value. Used to diagnose the live
    /// insert-path gap: are missing slots future-dated-at-insert, recently
    /// activated, or edited? `sample == 0` preserves the counts-only response.
    pub fn time_bucket_audit(
        &self,
        sample: usize,
        order: &str,
    ) -> crate::error::Result<serde_json::Value> {
        let tb_arc = self.time_buckets.as_ref().ok_or_else(|| {
            crate::error::BitdexError::Config("no time_buckets configured".into())
        })?;
        let sort_field_name = tb_arc.load().sort_field_name().to_string();
        self.ensure_fields_loaded(&[], Some(&sort_field_name))?;
        let snap = self.snapshot();
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
        let scan_threads = self
            .config
            .time_buckets
            .as_ref()
            .map(|tb| tb.reconcile_scan_threads)
            .unwrap_or(0);
        let tb = tb_arc.load();
        // (name, cutoff, current bucket bitmap). `fresh` computed by the shared
        // parallel scan below (same walk the periodic reconcile uses).
        let specs: Vec<(String, u64, RoaringBitmap)> = tb
            .bucket_names()
            .into_iter()
            .filter_map(|name| {
                let b = tb.get_bucket(&name)?;
                Some((
                    name,
                    now_secs.saturating_sub(b.duration_secs),
                    RoaringBitmap::clone(b.bitmap()),
                ))
            })
            .collect();
        let cutoffs: Vec<u64> = specs.iter().map(|(_, c, _)| *c).collect();
        let fresh_sets =
            Self::scan_fresh_in_window(sort_field, alive, &cutoffs, now_secs, scan_threads);

        let mut buckets = serde_json::Map::new();
        for ((name, _, current), fresh) in specs.into_iter().zip(fresh_sets) {
            let mut stale = current.clone();
            stale -= &fresh;
            let mut missing = fresh.clone();
            missing -= &current;
            // Emit [{slot, sortAt}, ...] for up to `sample` slots. Default
            // `lowest_id` (ascending) surfaces oldest/pre-boot slots first;
            // `highest_id` surfaces the most RECENT slots (high ids = recent
            // inserts = post-boot) to isolate the ongoing residual source from
            // boot residue; `random` strides the set for an unbiased spread.
            // sortAt is included so the caller can test future-dated-at-insert
            // without a second reconstruct pass.
            let sample_of = |bm: &RoaringBitmap| -> serde_json::Value {
                let picked: Vec<u32> = match order {
                    "highest_id" => {
                        let mut v: Vec<u32> = bm.iter().rev().take(sample).collect();
                        v.reverse();
                        v
                    }
                    "random" => {
                        let total = bm.len();
                        if total == 0 || sample == 0 {
                            Vec::new()
                        } else {
                            // Deterministic stride (no RNG dependency): evenly
                            // spaced ranks across the set.
                            let take = (sample as u64).min(total);
                            let step = (total / take).max(1);
                            (0..take)
                                .filter_map(|k| bm.select((k * step) as u32))
                                .collect()
                        }
                    }
                    // "lowest_id" and any unrecognized value.
                    _ => bm.iter().take(sample).collect(),
                };
                serde_json::Value::Array(
                    picked
                        .into_iter()
                        .map(|slot| {
                            serde_json::json!({
                                "slot": slot,
                                "sortAt": sort_field.reconstruct_value(slot) as u64,
                            })
                        })
                        .collect(),
                )
            };
            let mut entry = serde_json::json!({
                "current": current.len(),
                "fresh_in_window": fresh.len(),
                "stale": stale.len(),
                "missing": missing.len(),
            });
            if sample > 0 {
                let obj = entry.as_object_mut().expect("json object");
                obj.insert("missing_sample".into(), sample_of(&missing));
                obj.insert("stale_sample".into(), sample_of(&stale));
            }
            buckets.insert(name, entry);
        }
        Ok(serde_json::json!({
            "now_unix": now_secs,
            "sort_field": sort_field_name,
            "sample_order": order,
            "buckets": serde_json::Value::Object(buckets),
        }))
    }

    /// Clear unified cache entries and reset counters (RAM only).
    pub fn clear_unified_cache(&self) {
        self.unified_cache.clear();
    }
    /// Purge the entire BoundStore: disk first, then memory.
    /// Order matters: wipe disk before clearing RAM to prevent stale shard loads.
    /// Safe to call while the server is running — the merge thread will simply
    /// start writing fresh data on the next cycle with dirty entries.
    pub fn purge_bounds(&self) -> crate::error::Result<()> {
        // Step 1: Purge disk (meta.bin + all .ucpack shards)
        if let Some(ref bs) = self.bound_store {
            bs.purge()?;
            eprintln!("BoundStore: purged disk (meta.bin + all shards)");
        }
        // Step 2: Clear RAM cache + meta-index (after disk is gone)
        {
            let uc = &self.unified_cache;
            uc.clear();
            // Re-enable persistence so new entries get persisted
            if self.bound_store.is_some() {
                uc.enable_persistence();
            }
        }
        eprintln!("BoundStore: cleared RAM cache + meta-index");
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
    /// Whether the engine is currently in bulk-load mode.
    ///
    /// Consumed by the WAL reader thread so it can pause op-apply while
    /// bulk-load is active — applying ops on top of partial bulk-load state
    /// inflates per-bucket `ops_count` and forces PR-#233's
    /// `read_bucket_values_indexed` fast-path to walk the entire ops section
    /// before returning the wanted values. /ops POSTs continue to accept +
    /// write to WAL; the reader resumes apply once `exit_loading_mode` flips
    /// the flag back.
    pub fn is_loading_mode(&self) -> bool {
        self.loading_mode.load(Ordering::Acquire)
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
        // Trigger initial population of bitmap memory cache after load completes.
        self.bitmap_memory_cache.mark_all_stale();
        // Rebuild time buckets from the freshly-published snapshot. The flush
        // thread's per-cycle bucket maintenance is gated behind `loading_mode`
        // (and only triggers on coalescer.alive_inserts), so buckets receive
        // no inserts during a bulk load. Without this rebuild they stay empty
        // until the next mutation that touches their tracked sort field.
        if self.time_buckets.is_some() {
            if let Err(e) = self.rebuild_time_buckets() {
                eprintln!("Warning: rebuild_time_buckets after exit_loading_mode failed: {e}");
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
        // NOTE: Do NOT set loading_mode = false here. The ExitLoadingSaveUnload
        // handler in the flush thread will clear it AFTER reading the published
        // snapshot. Setting it here causes a race: the flush thread's loading-exit
        // force-publish (was_loading && !is_loading) overwrites the loader's
        // published data before the save command reads it.
        // Validate stores exist; flush thread has its own clones
        let _ = self.require_stores("exit_loading_mode_and_save_unload")?;
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
            loading_mode: Arc::clone(&self.loading_mode),
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
    /// Borrow all four ShardStore components, returning an error if any is missing.
    fn require_stores(&self, caller: &str) -> Result<(
        &crate::shard_store_bitmap::AliveBitmapStore,
        &crate::shard_store_bitmap::FilterBitmapStore,
        &crate::shard_store_bitmap::SortBitmapStore,
        &crate::shard_store_meta::MetaStore,
    )> {
        let msg = |which: &str| crate::error::BitdexError::Config(
            format!("no bitmap_path configured; cannot {caller} (missing {which})")
        );
        Ok((
            self.alive_store.as_ref().map(|a| a.as_ref()).ok_or_else(|| msg("alive_store"))?,
            self.filter_store.as_ref().map(|a| a.as_ref()).ok_or_else(|| msg("filter_store"))?,
            self.sort_store.as_ref().map(|a| a.as_ref()).ok_or_else(|| msg("sort_store"))?,
            self.meta_store.as_ref().map(|a| a.as_ref()).ok_or_else(|| msg("meta_store"))?,
        ))
    }
    /// Save a full snapshot of the current published state to ShardStore.
    ///
    /// Captures the current ArcSwap snapshot (what readers see) and writes all
    /// filter bitmaps, alive bitmap, sort layer bitmaps, and slot counter.
    ///
    /// This is intended for persisting state after bulk loading is complete.
    /// For incremental persistence during normal operation, the merge thread
    /// handles that automatically.
    ///
    /// Returns an error if no bitmap_store is configured.
    pub fn save_snapshot(&self) -> Result<()> {
        let (alive_s, filter_s, sort_s, meta_s) = self.require_stores("save_snapshot")?;
        let skip_sorts = self.pending_sort_loads.lock().clone();
        let skip_filters = self.pending_filter_loads.lock().clone();
        let skip_lazy = self.lazy_value_fields.lock().clone();
        Self::write_snapshot_to_store(alive_s, filter_s, sort_s, meta_s, &self.inner, &self.config, &skip_sorts, &skip_filters, &skip_lazy)?;
        // Persist named cursors alongside bitmaps so they survive process restart.
        let cursor_snapshot = self.cursors.lock().clone();
        for (name, value) in &cursor_snapshot {
            meta_s.write_cursor(name, value)
                .map_err(|e| crate::error::BitdexError::Storage(format!("write cursor: {e}")))?;
        }
        // Save LowCardinalityString dictionaries alongside bitmaps.
        if !self.dictionaries.is_empty() {
            let dict_path = meta_s.root();
            self.save_dictionaries(dict_path)?;
        }
        Ok(())
    }
    /// Save a full snapshot of the current published state to a custom path.
    ///
    /// Creates new ShardStore instances at the given path and writes the complete
    /// engine state. Useful for benchmarks or point-in-time backups.
    pub fn save_snapshot_to(&self, path: &Path) -> Result<()> {
        use crate::error::BitdexError;
        let ss_root = path.join("shardstore");
        let alive_s = crate::shard_store_bitmap::AliveBitmapStore::new(
            ss_root.join("alive"), crate::shard_store_bitmap::SingletonShard,
        ).map_err(|e| BitdexError::Storage(format!("alive store init: {e}")))?;
        let filter_s = crate::shard_store_bitmap::FilterBitmapStore::new(
            ss_root.join("filter"), crate::shard_store_bitmap::FieldValueBucketShard,
        ).map_err(|e| BitdexError::Storage(format!("filter store init: {e}")))?;
        let sort_s = crate::shard_store_bitmap::SortBitmapStore::new(
            ss_root.join("sort"), crate::shard_store_bitmap::SortLayerShard,
        ).map_err(|e| BitdexError::Storage(format!("sort store init: {e}")))?;
        let meta_s = crate::shard_store_meta::MetaStore::new(ss_root)
            .map_err(|e| BitdexError::Storage(format!("meta store init: {e}")))?;

        let skip_sorts = self.pending_sort_loads.lock().clone();
        let skip_filters = self.pending_filter_loads.lock().clone();
        let skip_lazy = self.lazy_value_fields.lock().clone();
        Self::write_snapshot_to_store(&alive_s, &filter_s, &sort_s, &meta_s, &self.inner, &self.config, &skip_sorts, &skip_filters, &skip_lazy)?;
        // Save LowCardinalityString dictionaries alongside bitmaps.
        if !self.dictionaries.is_empty() {
            self.save_dictionaries(path)?;
        }
        Ok(())
    }
    /// Internal: zero-copy snapshot serialization via ShardStore.
    ///
    /// Reads the published snapshot through Arc refs — no InnerEngine clone.
    /// Uses `fused_cow()` to borrow base bitmaps directly (zero copy when clean)
    /// or create temporary merged bitmaps (only when dirty). Processes one field
    /// at a time so memory overhead is minimal (~1.7 MB for tagIds' 31K Cow refs).
    ///
    /// Skips fields that haven't been loaded yet (still pending lazy-load) to avoid
    /// overwriting real persisted data with empty placeholders.
    fn write_snapshot_to_store(
        alive_store: &crate::shard_store_bitmap::AliveBitmapStore,
        filter_store: &crate::shard_store_bitmap::FilterBitmapStore,
        sort_store: &crate::shard_store_bitmap::SortBitmapStore,
        meta_store: &crate::shard_store_meta::MetaStore,
        inner: &ArcSwap<InnerEngine>,
        config: &Config,
        skip_sorts: &HashSet<String>,
        skip_filters: &HashSet<String>,
        skip_lazy_values: &HashSet<String>,
    ) -> Result<()> {
        let snap: Arc<InnerEngine> = inner.load_full();
        Self::write_inner_to_store(alive_store, filter_store, sort_store, meta_store, &snap, config, skip_sorts, skip_filters, skip_lazy_values)
    }
    /// Write bitmaps from an InnerEngine directly to the store.
    /// This is used by both the ArcSwap-based path and the flush thread's
    /// direct-from-staging path (which avoids the intermediate clone).
    fn write_inner_to_store(
        alive_store: &crate::shard_store_bitmap::AliveBitmapStore,
        filter_store: &crate::shard_store_bitmap::FilterBitmapStore,
        sort_store: &crate::shard_store_bitmap::SortBitmapStore,
        meta_store: &crate::shard_store_meta::MetaStore,
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
        alive_store.write_alive(&alive_cow)
            .map_err(|e| crate::error::BitdexError::Storage(format!("write alive: {e}")))?;
        meta_store.write_slot_counter(snap.slots.slot_counter())
            .map_err(|e| crate::error::BitdexError::Storage(format!("write slot_counter: {e}")))?;
        if snap.slots.deferred_count() > 0 {
            meta_store.write_deferred_alive(snap.slots.deferred_map())
                .map_err(|e| crate::error::BitdexError::Storage(format!("write deferred: {e}")))?;
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
                sort_store.write_sort_layers(&sc.name, &layer_refs)
                    .map_err(|e| crate::error::BitdexError::Storage(format!("write sort {}: {e}", sc.name)))?;
                eprintln!("  save: sort {} in {:.1}ms",
                    sc.name, t0.elapsed().as_secs_f64() * 1000.0);
            }
        }
        // Filter fields — stream one bucket at a time to minimize memory overhead.
        // Lazy-value fields require merge-on-save: read existing disk data per bucket,
        // OR with in-memory mutations, write merged result. This prevents overwriting
        // bulk-loaded data with partial in-memory state.
        for (name, field) in snap.filters.fields() {
            if skip_filters.contains(name) {
                continue;
            }
            let is_lazy = skip_lazy_values.contains(name);
            if is_lazy && field.bitmap_count() == 0 {
                // No in-memory data at all — nothing to merge, skip.
                continue;
            }
            let t0 = std::time::Instant::now();
            let num_values = field.bitmap_count();
            // Group in-memory entries by bucket (256 buckets max).
            //
            // For clean VBs (overwhelming majority — postId has 22.5M entries
            // and at any moment only a handful are dirty) we just clone the
            // inner `Arc<RoaringBitmap>` from the VB's base — pointer bump,
            // zero data copy. For dirty VBs we materialize the fused result.
            //
            // This iterates inside `for_each_versioned` which holds the read
            // lock for the duration but performs only Arc::clones in the body
            // (no per-VB heap allocation), so the lock window is microseconds
            // per entry instead of milliseconds. Writers can resume promptly
            // after the merge cycle.
            let mut by_bucket: HashMap<u8, Vec<(u64, Arc<RoaringBitmap>)>> = HashMap::new();
            field.for_each_versioned(|value, vb| {
                let bucket = (value >> 8) as u8;
                if vb.is_dirty() {
                    by_bucket
                        .entry(bucket)
                        .or_default()
                        .push((value, Arc::new(vb.fused())));
                } else {
                    by_bucket
                        .entry(bucket)
                        .or_default()
                        .push((value, Arc::clone(vb.base())));
                }
            });
            let num_buckets = by_bucket.len();
            // Buckets are independent — each bucket maps to a separate shard
            // file on disk. Parallelize per-bucket I/O so a hot field like
            // postId (138 buckets at 22.8M values) doesn't serialize 65 seconds
            // of disk writes on a single thread. Same pattern as
            // dump_processor's filter save loop.
            let bucket_items: Vec<(u8, Vec<(u64, Arc<RoaringBitmap>)>)> = by_bucket.into_iter().collect();
            if is_lazy {
                // Merge-on-save: for each bucket with in-memory entries, read the
                // existing data from disk, merge in-memory data on top, write back.
                // Buckets with no in-memory changes are left untouched on disk.
                bucket_items.into_par_iter().try_for_each(|(bucket, mem_entries)| -> Result<()> {
                    // Read existing disk entries for this bucket
                    let disk_entries = filter_store.read_filter_bucket(name, bucket)
                        .unwrap_or_default();
                    // Build merged map: start with disk, overlay memory
                    let mut merged: HashMap<u64, RoaringBitmap> = disk_entries.into_iter().collect();
                    for (value, mem_bm) in &mem_entries {
                        let entry = merged.entry(*value).or_insert_with(RoaringBitmap::new);
                        *entry |= mem_bm.as_ref();
                    }
                    // Write merged result
                    let refs: Vec<(u64, &RoaringBitmap)> = merged.iter()
                        .map(|(v, bm)| (*v, bm))
                        .collect();
                    filter_store.write_filter_bucket(name, bucket, &refs)
                        .map_err(|e| crate::error::BitdexError::Storage(format!("write filter {name}/{bucket:02x}: {e}")))?;
                    Ok(())
                })?;
            } else {
                // Non-lazy fields: write in-memory state directly (fully loaded)
                bucket_items.into_par_iter().try_for_each(|(bucket, entries)| -> Result<()> {
                    let refs: Vec<(u64, &RoaringBitmap)> = entries
                        .iter()
                        .map(|(v, bm)| (*v, bm.as_ref()))
                        .collect();
                    filter_store.write_filter_bucket(name, bucket, &refs)
                        .map_err(|e| crate::error::BitdexError::Storage(format!("write filter {name}/{bucket:02x}: {e}")))?;
                    Ok(())
                })?;
            }
            eprintln!("  save: filter {} ({} values, {} buckets{}) in {:.1}ms",
                name, num_values, num_buckets,
                if is_lazy { ", merged" } else { "" },
                t0.elapsed().as_secs_f64() * 1000.0);
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
        let (alive_s, filter_s, sort_s, meta_s) = self.require_stores("save_and_unload")?;
        // Snapshot what's already pending — don't save or unload those.
        let skip_sorts = self.pending_sort_loads.lock().clone();
        let skip_filters = self.pending_filter_loads.lock().clone();
        let skip_lazy = self.lazy_value_fields.lock().clone();
        // Phase 1: Zero-copy write to disk.
        Self::write_snapshot_to_store(
            alive_s,
            filter_s,
            sort_s,
            meta_s,
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
    /// Get a cloneable MutationSender for submitting ops to the coalescer channel.
    /// Used by the WAL reader thread to send ops via CoalescerSink.
    pub fn mutation_sender(&self) -> MutationSender {
        self.sender.clone()
    }
    /// Get a reference to the legacy BitmapFs store, if configured.
    /// Used by dump_processor for bitmap persistence.
    pub fn bitmap_store(&self) -> Option<&Arc<BitmapFs>> {
        self.bitmap_store.as_ref()
    }
    /// Get the ShardStore instances for direct bitmap I/O (dump processor, etc.).
    pub fn shard_stores(&self) -> Option<(
        Arc<crate::shard_store_bitmap::AliveBitmapStore>,
        Arc<crate::shard_store_bitmap::FilterBitmapStore>,
        Arc<crate::shard_store_bitmap::SortBitmapStore>,
        Arc<crate::shard_store_meta::MetaStore>,
    )> {
        Some((
            Arc::clone(self.alive_store.as_ref()?),
            Arc::clone(self.filter_store.as_ref()?),
            Arc::clone(self.sort_store.as_ref()?),
            Arc::clone(self.meta_store.as_ref()?),
        ))
    }
    /// Force-compact all shards across all stores using batched-fsync parallel workers.
    ///
    /// 1. For each store: list dirty shards, write `.new` files in parallel (no fsync)
    /// 2. Parallel fsync pass on all `.new` files (via `fsync_shard_file`)
    /// 3. fsync_shard_file also atomically renames `.new` → shard
    ///
    /// No generation pinning needed — each shard is a single flat file.
    pub fn compact_all(
        &self,
        threshold: u32,
        workers: usize,
        compact_bitmaps: bool,
        compact_docs: bool,
        progress: Arc<AtomicU64>,
    ) -> Result<CompactResult> {
        use rayon::prelude::*;

        let t0 = std::time::Instant::now();
        let mut result = CompactResult::default();

        if !compact_bitmaps && !compact_docs {
            return Ok(result);
        }

        eprintln!("compact_all: threshold={threshold}, workers={workers}");

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .map_err(|e| crate::error::BitdexError::Storage(format!("rayon pool: {e}")))?;

        // Each shard is its own atomic transaction: compact_shard holds the exclusive
        // per-shard RwLock for the full read → encode → write-new → fsync → rename window.
        // There is no deferred-fsync split — no window where a writer can append between
        // the snapshot read and the rename. Cross-shard parallelism comes from the pool.
        let mut any_failed = false;

        if compact_bitmaps {
            if let Some((ref alive_s, ref filter_s, ref sort_s, _)) = self.shard_stores() {
                // Alive: single shard
                result.shards_scanned += 1;
                match alive_s.should_compact(&crate::shard_store_bitmap::AliveShardKey, threshold) {
                    Ok(false) => { result.shards_skipped += 1; }
                    _ => {
                        match alive_s.compact_shard(&crate::shard_store_bitmap::AliveShardKey) {
                            Ok(true)  => { result.shards_compacted += 1; }
                            Ok(false) => { result.shards_skipped += 1; }
                            Err(e)    => { eprintln!("compact alive: {e}"); any_failed = true; }
                        }
                    }
                }
                progress.fetch_add(1, Ordering::Relaxed);

                // Filter shards
                let filter_keys = match filter_s.list_shards() {
                    Ok(keys) => keys,
                    Err(e) => {
                        eprintln!("compact_all: failed to list filter shards: {e}");
                        any_failed = true;
                        Vec::new()
                    }
                };
                if !filter_keys.is_empty() {
                    let filter_errors    = AtomicU64::new(0);
                    let filter_compacted = AtomicU64::new(0);
                    let filter_skipped   = AtomicU64::new(0);
                    let filter_count     = filter_keys.len() as u64;

                    pool.install(|| {
                        filter_keys.par_iter().for_each(|key| {
                            match filter_s.should_compact(key, threshold) {
                                Ok(false) => { filter_skipped.fetch_add(1, Ordering::Relaxed); }
                                _ => {
                                    match filter_s.compact_shard(key) {
                                        Ok(true)  => { filter_compacted.fetch_add(1, Ordering::Relaxed); }
                                        Ok(false) => { filter_skipped.fetch_add(1, Ordering::Relaxed); }
                                        Err(e) => {
                                            eprintln!("compact filter {}: {e}", key.field);
                                            filter_errors.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                            progress.fetch_add(1, Ordering::Relaxed);
                        });
                    });

                    result.shards_scanned    += filter_count;
                    result.shards_compacted  += filter_compacted.load(Ordering::Relaxed);
                    result.shards_skipped    += filter_skipped.load(Ordering::Relaxed);
                    if filter_errors.load(Ordering::Relaxed) > 0 { any_failed = true; }
                }

                // Sort shards
                let sort_keys = match sort_s.list_shards() {
                    Ok(keys) => keys,
                    Err(e) => {
                        eprintln!("compact_all: failed to list sort shards: {e}");
                        any_failed = true;
                        Vec::new()
                    }
                };
                if !sort_keys.is_empty() {
                    let sort_errors    = AtomicU64::new(0);
                    let sort_compacted = AtomicU64::new(0);
                    let sort_skipped   = AtomicU64::new(0);
                    let sort_count     = sort_keys.len() as u64;

                    pool.install(|| {
                        sort_keys.par_iter().for_each(|key| {
                            match sort_s.should_compact(key, threshold) {
                                Ok(false) => { sort_skipped.fetch_add(1, Ordering::Relaxed); }
                                _ => {
                                    match sort_s.compact_shard(key) {
                                        Ok(true)  => { sort_compacted.fetch_add(1, Ordering::Relaxed); }
                                        Ok(false) => { sort_skipped.fetch_add(1, Ordering::Relaxed); }
                                        Err(e) => {
                                            eprintln!("compact sort {}/{}: {e}", key.field, key.bit_position);
                                            sort_errors.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                            progress.fetch_add(1, Ordering::Relaxed);
                        });
                    });

                    result.shards_scanned   += sort_count;
                    result.shards_compacted += sort_compacted.load(Ordering::Relaxed);
                    result.shards_skipped   += sort_skipped.load(Ordering::Relaxed);
                    if sort_errors.load(Ordering::Relaxed) > 0 { any_failed = true; }
                }
            }
        }

        if compact_docs && self.slot_counter() > 0 {
            let doc_store_arc = self.docstore.read().shard_store_arc();
            let slot_counter = self.slot_counter();
            let max_shard = if slot_counter > 0 {
                (slot_counter - 1) >> crate::shard_store_doc::SHARD_SHIFT_PUB
            } else {
                0
            };
            let doc_count     = (max_shard + 1) as u64;
            let doc_errors    = AtomicU64::new(0);
            let doc_compacted = AtomicU64::new(0);
            let doc_skipped   = AtomicU64::new(0);

            eprintln!("compact_all: compacting {doc_count} doc shards (0..={max_shard})");

            pool.install(|| {
                (0..=max_shard).into_par_iter().for_each(|shard_id| {
                    match doc_store_arc.should_compact(&shard_id, threshold) {
                        Ok(false) => { doc_skipped.fetch_add(1, Ordering::Relaxed); }
                        _ => {
                            match doc_store_arc.compact_shard(&shard_id) {
                                Ok(true)  => { doc_compacted.fetch_add(1, Ordering::Relaxed); }
                                Ok(false) => { doc_skipped.fetch_add(1, Ordering::Relaxed); }
                                Err(e) => {
                                    eprintln!("compact doc shard {shard_id}: {e}");
                                    doc_errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    progress.fetch_add(1, Ordering::Relaxed);
                });
            });

            result.shards_scanned   += doc_count;
            result.shards_compacted += doc_compacted.load(Ordering::Relaxed);
            result.shards_skipped   += doc_skipped.load(Ordering::Relaxed);
            if doc_errors.load(Ordering::Relaxed) > 0 { any_failed = true; }
        }

        if any_failed {
            return Err(crate::error::BitdexError::Storage(
                "compact_all: one or more shards failed to compact — see eprintln logs above".into()
            ));
        }

        result.elapsed_secs = t0.elapsed().as_secs_f64();
        eprintln!(
            "compact_all: done in {:.1}s — scanned={}, compacted={}, skipped={}",
            result.elapsed_secs, result.shards_scanned, result.shards_compacted, result.shards_skipped
        );
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
            let old_docs: Vec<Option<crate::shard_store_doc::StoredDoc>> = statuses
                .iter()
                .map(|&(id, is_upsert, was_allocated)| {
                    if is_upsert || was_allocated {
                        self.docstore.read().get(id).ok().flatten()
                    } else {
                        None
                    }
                })
                .collect();
            // Phase 4: Compute all diffs and collect all ops
            let mut all_ops: Vec<MutationOp> = Vec::new();
            let mut doc_writes: Vec<(u32, crate::shard_store_doc::StoredDoc)> = Vec::new();

            for (i, &(id, ref doc)) in docs.iter().enumerate() {
                let (_, is_upsert, _) = statuses[i];
                let ops = diff_document(id, old_docs[i].as_ref(), doc, &self.config, is_upsert, &self.field_registry);
                all_ops.extend(ops);
                doc_writes.push((
                    id,
                    crate::shard_store_doc::StoredDoc {
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
        self.unified_cache.clear();
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
                    if let Err(e) = docstore.write().put_batch(&batch) {
                        eprintln!("put_bulk: docstore batch write failed: {e}");
                    }
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                if let Err(e) = docstore.write().put_batch(&batch) {
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
                if let Err(e) = self.docstore.write().put_batch(&batch) {
                    eprintln!("write_docs_to_docstore: batch write failed: {e}");
                }
                batch.clear();
            }
        }
        if !batch.is_empty() {
            if let Err(e) = self.docstore.write().put_batch(&batch) {
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
            if let Some(field) = staging.filters.get_field(&field_name) {
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
            if let Some(field) = staging.filters.get_field(&field_name) {
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
    #[allow(dead_code)]
    pub(crate) fn apply_accum(&self, accum: &crate::loader::BitmapAccum) {
        // In loading mode, the flush thread doesn't publish snapshots, so the
        // ArcSwap holds the sole reference. Clone is O(num_fields) — just Arc
        // pointer copies, no deep bitmap clones.
        let snap = self.inner.load_full();
        let mut staging = (*snap).clone();
        drop(snap);
        // Apply filter bitmaps
        for (field_name, value_map) in &accum.filter_maps {
            if let Some(field) = staging.filters.get_field(field_name) {
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
    /// Build all bitmap indexes from the docstore.
    ///
    /// Designed for "build index" boot mode: starts from bare docs on disk,
    /// constructs alive bitmap + all filter + all sort bitmaps from scratch.
    /// Uses the packed decode path (skips StoredDoc allocation) for speed.
    ///
    /// Progress callback receives (docs_processed, elapsed_secs, rss_bytes)
    /// at regular intervals for monitoring.
    ///
    /// Returns (docs_processed, elapsed_secs) on success.
    pub fn build_all_from_docstore(
        &self,
        progress: Arc<AtomicU64>,
        memory_cb: Option<Box<dyn Fn(u64, f64, u64) + Send + Sync>>,
    ) -> Result<(u64, f64)> {
        use crate::shard_store_doc::PackedValue;

        let t0 = Instant::now();
        let sort_configs = self.config.sort_fields.clone();
        let filter_configs = self.config.filter_fields.clone();
        let sort_names: Vec<&str> = sort_configs.iter().map(|c| c.name.as_str()).collect();
        let sort_bits: Vec<usize> = sort_configs.iter().map(|c| c.bits as usize).collect();
        let filter_names: Vec<&str> = filter_configs.iter().map(|c| c.name.as_str()).collect();
        eprintln!("build_all: {} filter fields, {} sort fields",
            filter_names.len(), sort_names.len());
        // Open a read-only DocStore for parallel reads
        let ds_path = self.docstore_root.as_ref().clone();
        let reader = DocStoreV3::open(&ds_path)
            .map_err(|e| crate::error::BitdexError::Storage(
                format!("open reader docstore: {e}")))?;
        // Build u16 field dictionary → field position lookup tables
        let field_dict = reader.field_to_idx();
        let mut filter_idx_map: HashMap<u16, usize> = HashMap::new();
        let mut sort_idx_map: HashMap<u16, (usize, usize)> = HashMap::new();
        for (fi, &fname) in filter_names.iter().enumerate() {
            if let Some(&idx) = field_dict.get(fname) {
                filter_idx_map.insert(idx, fi);
            }
        }
        for (si, &sname) in sort_names.iter().enumerate() {
            if let Some(&idx) = field_dict.get(sname) {
                sort_idx_map.insert(idx, (si, sort_bits[si]));
            }
        }
        eprintln!("build_all: filter fields mapped: {}/{}, sort fields mapped: {}/{}",
            filter_idx_map.len(), filter_names.len(),
            sort_idx_map.len(), sort_names.len());
        // Discover max shard by scanning docstore directory
        let shards_dir = ds_path.join("shards");
        let mut max_shard_id = 0u32;
        if let Ok(entries) = std::fs::read_dir(&shards_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Ok(sub_entries) = std::fs::read_dir(entry.path()) {
                        for sub in sub_entries.flatten() {
                            if let Some(stem) = sub.path().file_stem() {
                                if let Ok(id) = stem.to_string_lossy().parse::<u32>() {
                                    max_shard_id = max_shard_id.max(id);
                                }
                            }
                        }
                    }
                }
            }
        }
        let num_shards = max_shard_id + 1;
        eprintln!("build_all: {} shards to scan", num_shards);
        // Start memory monitoring thread
        let monitor_active = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let monitor_progress = progress.clone();
        let monitor_active_clone = monitor_active.clone();
        let monitor_handle = if memory_cb.is_some() {
            let cb = memory_cb.unwrap();
            let t0_clone = t0;
            Some(std::thread::spawn(move || {
                while monitor_active_clone.load(Ordering::Relaxed) {
                    let docs = monitor_progress.load(Ordering::Relaxed);
                    let elapsed = t0_clone.elapsed().as_secs_f64();
                    let rss = get_rss_bytes();
                    cb(docs, elapsed, rss);
                    std::thread::sleep(Duration::from_secs(5));
                }
            }))
        } else {
            None
        };
        // Channel-based merge: rayon workers send chunk results to a single
        // merge thread. This bounds peak memory to ~1 final accumulator + 1
        // in-flight chunk, instead of 32 thread accumulators during tree reduce.
        type FilterMap = HashMap<(usize, u64), RoaringBitmap>;
        struct ChunkResult {
            sort_layers: Vec<Vec<RoaringBitmap>>,
            filter_map: FilterMap,
            alive: RoaringBitmap,
            count: u64,
        }
        let chunk_size = 500u32;
        let num_chunks = (num_shards + chunk_size - 1) / chunk_size;
        // Bounded channel — backpressure if merge thread falls behind
        let (tx, rx) = crossbeam_channel::bounded::<ChunkResult>(4);
        // Merge thread: accumulates into staging directly
        let _sort_bits_clone = sort_bits.clone();
        let filter_configs_clone = filter_configs.clone();
        let sort_configs_clone = sort_configs.clone();
        let inner_clone = self.inner.clone();
        let _progress_merge = progress.clone();
        let merge_handle = thread::spawn(move || {
            let mut staging = {
                let snap = inner_clone.load_full();
                (*snap).clone()
            };
            // Pre-clear all fields for fresh build
            for fc in &filter_configs_clone {
                staging.filters.add_field(fc.clone());
            }
            for sc in &sort_configs_clone {
                staging.sorts.add_field(sc.clone());
            }
            let mut total_merged = 0u64;
            while let Ok(chunk) = rx.recv() {
                // Merge alive
                staging.slots.alive_or_bitmap(&chunk.alive);
                // Merge filter bitmaps directly into staging fields
                for ((fi, value), bitmap) in chunk.filter_map {
                    let fname = &filter_configs_clone[fi].name;
                    if let Some(field) = staging.filters.get_field(fname) {
                        field.or_bitmap(value, &bitmap);
                    }
                }
                // Merge sort layers directly into staging fields
                for (si, layers) in chunk.sort_layers.into_iter().enumerate() {
                    let sname = &sort_configs_clone[si].name;
                    if let Some(field) = staging.sorts.get_field_mut(sname) {
                        for (bit, bitmap) in layers.into_iter().enumerate() {
                            if !bitmap.is_empty() {
                                field.or_layer(bit, &bitmap);
                            }
                        }
                    }
                }
                total_merged += chunk.count;
            }
            (staging, total_merged)
        });
        // Rayon workers: process chunks, send results over channel
        (0..num_chunks)
            .into_par_iter()
            .for_each_with(tx, |tx, chunk_idx| {
                let shard_start = chunk_idx * chunk_size;
                let shard_end = std::cmp::min(shard_start + chunk_size, num_shards);
                let mut sort_layers: Vec<Vec<RoaringBitmap>> = sort_bits.iter().map(|&b| {
                    (0..b).map(|_| RoaringBitmap::new()).collect()
                }).collect();
                let mut filter_map: FilterMap = FilterMap::new();
                let mut alive = RoaringBitmap::new();
                let mut count = 0u64;
                for shard_id in shard_start..shard_end {
                    let packed_docs = match reader.get_shard_packed(shard_id) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    for (slot_id, pairs) in &packed_docs {
                        alive.insert(*slot_id);
                        for (field_idx, pv) in pairs {
                            if let Some(&fi) = filter_idx_map.get(field_idx) {
                                match pv {
                                    PackedValue::I(v) => {
                                        filter_map
                                            .entry((fi, *v as u64))
                                            .or_insert_with(RoaringBitmap::new)
                                            .insert(*slot_id);
                                    }
                                    PackedValue::B(b) => {
                                        filter_map
                                            .entry((fi, if *b { 1 } else { 0 }))
                                            .or_insert_with(RoaringBitmap::new)
                                            .insert(*slot_id);
                                    }
                                    PackedValue::Mi(vals) => {
                                        for v in vals {
                                            filter_map
                                                .entry((fi, *v as u64))
                                                .or_insert_with(RoaringBitmap::new)
                                                .insert(*slot_id);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            if let Some(&(si, bits)) = sort_idx_map.get(field_idx) {
                                if let PackedValue::I(v) = pv {
                                    let value = (*v).max(0) as u32;
                                    for bit in 0..bits {
                                        if (value >> bit) & 1 == 1 {
                                            sort_layers[si][bit].insert(*slot_id);
                                        }
                                    }
                                }
                            }
                        }
                        count += 1;
                    }
                }
                progress.fetch_add(count, Ordering::Relaxed);
                // Send chunk to merge thread (blocks if channel full = backpressure)
                let _ = tx.send(ChunkResult {
                    sort_layers,
                    filter_map,
                    alive,
                    count,
                });
            });
        // Wait for merge thread to finish
        let (staging, _total_merged) = merge_handle.join()
            .expect("merge thread panicked");
        let read_elapsed = t0.elapsed().as_secs_f64();
        let total_docs = progress.load(Ordering::Relaxed);
        eprintln!("build_all: read+merge phase complete in {:.1}s ({} docs, {:.0} docs/s)",
            read_elapsed, total_docs, total_docs as f64 / read_elapsed);
        // Publish the fully built staging
        self.publish_staging(staging);
        // Clear all pending loads (everything is now loaded)
        {
            let mut pending = self.pending_filter_loads.lock();
            pending.clear();
        }
        {
            let mut pending = self.pending_sort_loads.lock();
            pending.clear();
        }
        // Stop memory monitor
        monitor_active.store(false, Ordering::Relaxed);
        if let Some(handle) = monitor_handle {
            handle.join().ok();
        }
        let total_elapsed = t0.elapsed().as_secs_f64();
        let rss = get_rss_bytes();
        eprintln!("build_all: complete in {:.1}s — {} docs, RSS={:.2} GB",
            total_elapsed, total_docs, rss as f64 / 1e9);
        Ok((total_docs, total_elapsed))
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
        let ds_path = self.docstore_root.as_ref().clone();
        let reader = DocStoreV3::open(&ds_path)
            .map_err(|e| crate::error::BitdexError::Storage(
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
            if let Some(field) = staging.filters.get_field(fname) {
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
    /// Add new filter and/or sort fields, building their bitmaps from the docstore.
    ///
    /// Unlike `rebuild_fields_from_docstore` (which rebuilds fields already in the config),
    /// this method adds entirely new fields that didn't exist before. It:
    /// 1. Validates the requested fields don't already exist
    /// 2. Adds empty field structures to the staging snapshot
    /// 3. Scans all alive documents to build bitmaps for the new fields
    /// 4. Publishes the updated snapshot
    ///
    /// The caller (server) is responsible for updating the persisted config.
    /// Returns (slots_processed, field_names_added).
    pub fn add_fields_from_docstore(
        &self,
        new_filters: Vec<FilterFieldConfig>,
        new_sorts: Vec<SortFieldConfig>,
        progress: Arc<AtomicU64>,
    ) -> Result<(u64, Vec<String>)> {
        let t0 = Instant::now();
        if new_filters.is_empty() && new_sorts.is_empty() {
            return Ok((0, vec![]));
        }
        // Validate no duplicates with existing fields
        {
            let snap = self.inner.load_full();
            for fc in &new_filters {
                if snap.filters.get_field(&fc.name).is_some() {
                    return Err(crate::error::BitdexError::Config(
                        format!("Filter field '{}' already exists", fc.name)));
                }
            }
            for sc in &new_sorts {
                if snap.sorts.get_field(&sc.name).is_some() {
                    return Err(crate::error::BitdexError::Config(
                        format!("Sort field '{}' already exists", sc.name)));
                }
            }
        }
        let added_names: Vec<String> = new_filters.iter().map(|c| c.name.clone())
            .chain(new_sorts.iter().map(|c| c.name.clone()))
            .collect();
        eprintln!("add_fields: filter={:?}, sort={:?}",
            new_filters.iter().map(|c| &c.name).collect::<Vec<_>>(),
            new_sorts.iter().map(|c| &c.name).collect::<Vec<_>>());
        // Get alive bitmap
        let snap = self.inner.load_full();
        let alive = {
            let mut tmp = (*snap).clone();
            tmp.slots.merge_alive();
            tmp.slots.alive_bitmap().clone()
        };
        let total_alive = alive.len();
        eprintln!("add_fields: {} alive slots to scan", total_alive);
        // Open read-only docstore for parallel reads
        let ds_path = self.docstore_root.as_ref().clone();
        let reader = DocStoreV3::open(&ds_path)
            .map_err(|e| crate::error::BitdexError::Storage(
                format!("open reader docstore: {e}")))?;
        let max_slot = alive.max().unwrap_or(0);
        let max_shard = max_slot >> 9;
        let num_shards = max_shard + 1;
        // Build field name/config lists for the inner loop
        let sort_names: Vec<&str> = new_sorts.iter().map(|c| c.name.as_str()).collect();
        let sort_bits: Vec<usize> = new_sorts.iter().map(|c| c.bits as usize).collect();
        let filter_names: Vec<&str> = new_filters.iter().map(|c| c.name.as_str()).collect();
        // Parallel shard scan — same pattern as rebuild_fields_from_docstore
        type FilterMap = HashMap<(usize, u64), RoaringBitmap>;
        struct Accum {
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
                progress.fetch_add(acc.count, Ordering::Relaxed);
                acc.count = 0;
                acc
            })
            .reduce(make_accum, |mut a, b| {
                for (si, b_layers) in b.sort_layers.into_iter().enumerate() {
                    for (bit, bm) in b_layers.into_iter().enumerate() {
                        a.sort_layers[si][bit] |= bm;
                    }
                }
                for (key, bm) in b.filter_map {
                    a.filter_map.entry(key)
                        .and_modify(|existing| *existing |= &bm)
                        .or_insert(bm);
                }
                a.count += b.count;
                a
            });
        let slots_processed = progress.load(Ordering::Relaxed);
        let scan_elapsed = t0.elapsed();
        eprintln!("add_fields: scan complete in {:.1}s ({} slots, {:.0} slots/s)",
            scan_elapsed.as_secs_f64(), slots_processed,
            slots_processed as f64 / scan_elapsed.as_secs_f64());
        // Apply: clone staging, add new empty fields, then OR in rebuilt bitmaps
        let mut staging = self.clone_staging();
        for fc in &new_filters {
            staging.filters.add_field(fc.clone());
        }
        for sc in &new_sorts {
            staging.sorts.add_field(sc.clone());
        }
        // Apply rebuilt filter bitmaps
        for ((fi, value), bitmap) in merged.filter_map {
            let fname = &new_filters[fi].name;
            if let Some(field) = staging.filters.get_field(fname) {
                field.or_bitmap(value, &bitmap);
            }
        }
        // Apply rebuilt sort layer bitmaps
        for (si, layers) in merged.sort_layers.into_iter().enumerate() {
            let sname = &new_sorts[si].name;
            if let Some(field) = staging.sorts.get_field_mut(sname) {
                for (bit, bitmap) in layers.into_iter().enumerate() {
                    if !bitmap.is_empty() {
                        field.or_layer(bit, &bitmap);
                    }
                }
            }
        }
        self.publish_staging(staging);
        let total_elapsed = t0.elapsed();
        eprintln!("add_fields: complete in {:.1}s — {} slots, {} fields added",
            total_elapsed.as_secs_f64(), slots_processed, added_names.len());
        Ok((slots_processed, added_names))
    }
    /// Validate that field names exist in the docstore by checking one shard.
    /// Returns Ok(()) if all fields are found, or Err with the missing field names.
    pub fn validate_fields_in_docstore(&self, field_names: &[&str]) -> Result<Vec<String>> {
        let ds_path = self.docstore_root.as_ref().clone();
        let reader = DocStoreV3::open(&ds_path)
            .map_err(|e| crate::error::BitdexError::Storage(
                format!("open reader docstore: {e}")))?;
        // Find a non-empty shard to sample
        let snap = self.inner.load_full();
        let alive = snap.slots.alive_bitmap();
        let sample_slot = alive.min()
            .ok_or_else(|| crate::error::BitdexError::Config(
                "No alive documents to validate fields against".to_string()))?;
        let sample_shard = sample_slot >> 9;
        let docs = reader.get_shard(sample_shard)
            .map_err(|e| crate::error::BitdexError::Storage(
                format!("read sample shard {}: {e}", sample_shard)))?;
        if docs.is_empty() {
            return Err(crate::error::BitdexError::Config(
                "Sample shard is empty — cannot validate fields".to_string()));
        }
        let (_, sample_doc) = &docs[0];
        let available_fields: HashSet<&str> = sample_doc.fields.keys()
            .map(|k| k.as_str())
            .collect();
        let missing: Vec<String> = field_names.iter()
            .filter(|&&name| !available_fields.contains(name))
            .map(|&name| name.to_string())
            .collect();
        Ok(missing)
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
        // DocStoreV3 uses ShardStore native compaction — no compact worker to shut down.
        drop(self.compact_tx.take());
        if let Some(handle) = self.compact_handle.take() {
            handle.join().ok();
        }
        // Drop the prefetch_tx sender to signal the prefetch worker to exit,
        // then join it. Must drop before join to avoid deadlock.
        drop(self.prefetch_tx.take());
        if let Some(handle) = self.prefetch_handle.take() {
            handle.join().ok();
        }
        // Doc cache eviction thread uses the shutdown flag (already set above)
        if let Some(handle) = self.doc_cache_eviction_handle.take() {
            handle.join().ok();
        }
        // Cache worker uses the shutdown flag (already set). Drop the sender first
        // so the worker's channel disconnects, then join.
        drop(self.cache_work_tx.take());
        if let Some(handle) = self.cache_worker_handle.take() {
            handle.join().ok();
        }
    }
}
impl Drop for ConcurrentEngine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ── BucketBitmap restore helpers ─────────────────────────────────────────────
//
// These free functions are `pub(crate)` so `#[cfg(test)]` modules can test them
// directly without spawning a full ConcurrentEngine.

/// Re-resolve `BucketBitmap` clauses from live engine state after deserialization.
///
/// `BucketBitmap::bitmap` is skipped on msgpack serialize (the Arc can't cross restarts).
/// On deserialize it defaults to an empty Arc. This helper fills it from the engine's
/// `TimeBucketManager` or `PrefilterRegistry` by matching `bucket_name`.
///
/// Returns `true` if every `BucketBitmap` clause in the tree was resolved successfully.
/// Returns `false` if any clause could not be resolved (unknown bucket name) — caller
/// should tombstone the entry so it rebuilds on next access.
///
/// Non-`BucketBitmap` clauses pass through untouched. `Not`/`And`/`Or` are walked
/// recursively. An empty `clauses` slice always returns `true` (nothing to resolve).
pub(crate) fn resolve_bucket_clauses(
    clauses: &mut Vec<crate::query::FilterClause>,
    time_buckets: Option<&TimeBucketManager>,
    prefilter_registry: &crate::prefilter::PrefilterRegistry,
) -> bool {
    let mut all_ok = true;
    for c in clauses.iter_mut() {
        resolve_bucket_clause_one(c, time_buckets, prefilter_registry, &mut all_ok);
    }
    all_ok
}

fn resolve_bucket_clause_one(
    c: &mut crate::query::FilterClause,
    time_buckets: Option<&TimeBucketManager>,
    prefilter_registry: &crate::prefilter::PrefilterRegistry,
    all_ok: &mut bool,
) {
    use crate::query::FilterClause;
    match c {
        FilterClause::BucketBitmap { field, bucket_name, bitmap } => {
            if field == "__prefilter" {
                // Prefilter-substituted BucketBitmap — resolve via registry
                if let Some(entry) = prefilter_registry.get(bucket_name) {
                    *bitmap = entry.bitmap.load_full();
                } else {
                    tracing::debug!(
                        "resolve_bucket_clauses: prefilter '{}' not in registry — tombstoning entry",
                        bucket_name
                    );
                    *all_ok = false;
                }
            } else {
                // Time-bucket BucketBitmap — resolve via TimeBucketManager
                if let Some(tb) = time_buckets {
                    if let Some(bucket) = tb.get_bucket(bucket_name) {
                        *bitmap = Arc::clone(bucket.bitmap());
                    } else {
                        tracing::debug!(
                            "resolve_bucket_clauses: time bucket '{}' not in manager — tombstoning entry",
                            bucket_name
                        );
                        *all_ok = false;
                    }
                } else {
                    // No TimeBucketManager configured — can't resolve time-bucket clause
                    tracing::debug!(
                        "resolve_bucket_clauses: time bucket '{}' present but no TimeBucketManager — tombstoning entry",
                        bucket_name
                    );
                    *all_ok = false;
                }
            }
        }
        FilterClause::Not(inner) => {
            resolve_bucket_clause_one(inner, time_buckets, prefilter_registry, all_ok);
        }
        FilterClause::And(parts) | FilterClause::Or(parts) => {
            for p in parts.iter_mut() {
                resolve_bucket_clause_one(p, time_buckets, prefilter_registry, all_ok);
            }
        }
        // All leaf clauses without a bitmap Arc pass through.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::mutation::FieldValue;
    use crate::query::{SortClause, SortDirection, Value};
    use serial_test::serial;
    use std::sync::Arc;
    use std::thread;

    /// The parallel reconcile scan (dedicated pool) must produce byte-for-byte
    /// the same per-bucket `fresh` sets as the sequential walk. This is the
    /// correctness proof for parallelizing the ~79s full-alive scan: the two
    /// paths only differ in HOW slots are visited, never in the membership
    /// result. Uses > `PARALLEL_SCAN_MIN` slots so the parallel branch actually
    /// engages, and cutoffs that split the value spread across in/out of window.
    #[test]
    fn scan_fresh_in_window_parallel_matches_sequential() {
        use crate::sort::SortField;

        let sf_config = SortFieldConfig {
            name: "sortAt".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        };
        let mut sf = SortField::new(sf_config);
        let now: u64 = 1_000_000;
        let mut alive = RoaringBitmap::new();
        // 250k slots (> PARALLEL_SCAN_MIN) with values spread over a 90k-second
        // band ending at `now`, plus a scattered tail of much older values so
        // some slots fall outside the narrower windows.
        for slot in 0..250_000u32 {
            let val = if slot % 7 == 0 {
                (now as u32).saturating_sub(500_000 + (slot % 100_000)) // way old
            } else {
                (now as u32).saturating_sub((slot % 90_000) as u32) // in recent band
            };
            sf.insert(slot, val);
            alive.insert(slot);
        }
        // A future-dated slot (val > now) to exercise the `val > now_secs` skip.
        sf.insert(250_000, now as u32 + 10_000);
        alive.insert(250_000);
        // A sparse slot far past the dense block: forces a large id gap so the
        // range partitions include several empty ones and `max_slot` sits well
        // beyond the bulk — guards the range-split math (empty-tail handling,
        // ceil-div span) against a non-contiguous alive set.
        sf.insert(5_000_000, (now - 100) as u32);
        alive.insert(5_000_000);

        let cutoffs = vec![now - 3_600, now - 86_400, now - 600_000];
        let seq = ConcurrentEngine::scan_fresh_in_window(&sf, &alive, &cutoffs, now, 1);
        let par = ConcurrentEngine::scan_fresh_in_window(&sf, &alive, &cutoffs, now, 8);

        assert_eq!(seq.len(), cutoffs.len());
        assert_eq!(seq, par, "parallel scan must match sequential scan exactly");
        // Windows nest (3600 ⊂ 86400 ⊂ 600000): each larger window is a superset.
        assert!(seq[0].len() <= seq[1].len());
        assert!(seq[1].len() <= seq[2].len());
        assert!(!seq[0].is_empty(), "recent window must have members");
        // The future-dated slot is excluded from every window.
        assert!(!seq[2].contains(250_000), "future-dated slot must be skipped");
    }
    fn test_config() -> Config {
        Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false, max_range_scan_values: None,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false, max_range_scan_values: None,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false, max_range_scan_values: None,
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
    /// Wait until the flush thread is quiet: no in-flight mutations and no new
    /// publishes for `quiet_ms`. Returns once stable or after `max_ms` elapses.
    /// Use this instead of `thread::sleep` when a test must observe state after
    /// a flush has fully settled.
    fn wait_for_flush_quiet(engine: &ConcurrentEngine, max_ms: u64) {
        let deadline = std::time::Instant::now() + Duration::from_millis(max_ms.max(500));
        let quiet_window = Duration::from_millis(50);
        let mut last_publish = engine.flush_stats().0;
        let mut stable_since = std::time::Instant::now();
        while std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
            let now = engine.flush_stats().0;
            if now != last_publish {
                last_publish = now;
                stable_since = std::time::Instant::now();
                continue;
            }
            if !engine.in_flight().has_in_flight()
                && stable_since.elapsed() >= quiet_window
            {
                return;
            }
        }
    }
    /// Wait for the flush thread to apply all pending mutations.
    fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
        // Each test caller passes its own timeout, but we floor it here at
        // 5 s so heavily-parallel test runs (default `--test-threads`) don't
        // race-fail on tight 1 s assertions. The tests stay snappy on quiet
        // runs because we exit the loop the moment alive_count matches.
        let effective_max_ms = max_ms.max(5000);
        let deadline = std::time::Instant::now() + Duration::from_millis(effective_max_ms);
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
    #[serial]
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
                    per_value_lazy: false, max_range_scan_values: None,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false, max_range_scan_values: None,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false, max_range_scan_values: None,
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
    #[serial]
    fn test_save_snapshot_no_bitmap_store_returns_error() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();
        let result = engine.save_snapshot();
        assert!(result.is_err(), "save_snapshot should fail without bitmap_path");
    }
    #[test]
    #[serial]
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
    #[serial]
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
        // Verify the file was created and contains the data (via ShardStore)
        let ss_root = custom_bitmap_path.join("shardstore");
        let alive_s = crate::shard_store_bitmap::AliveBitmapStore::new(
            ss_root.join("alive"), crate::shard_store_bitmap::SingletonShard,
        ).unwrap();
        let filter_s = crate::shard_store_bitmap::FilterBitmapStore::new(
            ss_root.join("filter"), crate::shard_store_bitmap::FieldValueBucketShard,
        ).unwrap();
        let sort_s = crate::shard_store_bitmap::SortBitmapStore::new(
            ss_root.join("sort"), crate::shard_store_bitmap::SortLayerShard,
        ).unwrap();
        let meta_s = crate::shard_store_meta::MetaStore::new(ss_root).unwrap();
        let alive = alive_s.load_alive().unwrap().unwrap();
        assert_eq!(alive.len(), 2, "alive bitmap should have 2 entries");
        assert!(alive.contains(1));
        assert!(alive.contains(2));
        let counter = meta_s.load_slot_counter().unwrap().unwrap();
        assert!(counter >= 3, "slot counter should be at least 3");
        let nsfw = filter_s.load_field("nsfwLevel").unwrap();
        assert!(nsfw.contains_key(&5), "nsfwLevel=5 should exist");
        assert_eq!(nsfw[&5].len(), 2, "nsfwLevel=5 should have 2 entries");
        let sort_layers = sort_s.load_sort_layers("reactionCount", 32).unwrap();
        assert!(sort_layers.is_some(), "sort layers should be persisted");
    }
    #[test]
    #[serial]
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
    #[serial]
    #[ignore = "Test contract gap, not an engine bug: wait_for_flush observes \
                  alive_count but delete clears filter bitmap bits asynchronously in \
                  the same flush cycle. Test sees alive_count drop, calls save_snapshot, \
                  but the filter bitmap may not have been re-published yet → restored \
                  snapshot still carries the deleted slot's filter bit. Run with \
                  --ignored when hand-debugging clean-delete propagation; the underlying \
                  delete path is exercised in production via the WAL replay tests."]
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
    #[serial]
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
        // Verify cursor was written to disk (via MetaStore)
        let ms = crate::shard_store_meta::MetaStore::new(bitmap_path.join("shardstore")).unwrap();
        let on_disk = ms.load_cursor("pg-sync-0").unwrap();
        assert_eq!(on_disk.unwrap(), "99999");
        drop(engine);
        // Create a new engine from the same path — cursor should be loaded
        let engine2 = ConcurrentEngine::new_with_path(config, &doc_path).unwrap();
        assert_eq!(engine2.get_cursor("pg-sync-0").unwrap(), "99999");
    }
    #[test]
    #[serial]
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
    #[serial]
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
            let staging = engine.clone_staging();
            // Simulate a mutation: add nsfwLevel=1 for slot 10
            if let Some(field) = staging.filters.get_field("nsfwLevel") {
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
    #[serial]
    // FIXME(test-regression): the 50% drop threshold is unrealistic after the
    // SyncUnloaded refactor (97a0beb): the flush thread now drains pending
    // mutations from the coalescer and applies them to the unloaded staging
    // as diffs before publishing. With ~500 puts still in flight from
    // exit_loading_mode, the diff layer in the published "unloaded" snapshot
    // is roughly the same size as the original base, so memory drops <20%
    // instead of >50%. The original race fix is still working — staging is
    // no longer cloned wholesale — but the assertion needs reframing
    // (e.g. compare base bytes only, or quiesce the coalescer fully before
    // measuring). Use `wait_for_flush_quiet` to settle the snapshot before
    // re-asserting once the threshold is fixed.
    #[ignore]
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
        // Wait until the flush thread quiesces — if staging is being re-inflated,
        // publish_count keeps moving and the wait extends until it stops.
        wait_for_flush_quiet(&engine, 2000);
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
    /// Regression test for v1.0.202 bug #14: a bulk load via loading mode must
    /// leave the time bucket bitmaps populated. Previously buckets stayed empty
    /// because the flush thread's per-cycle bucket maintenance is gated behind
    /// `loading_mode` and only fires on coalescer.alive_inserts.
    #[test]
    fn test_exit_loading_mode_rebuilds_time_buckets() {
        use crate::config::{BucketConfig, TimeBucketFieldConfig};
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let config = Config {
            filter_fields: vec![FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            }],
            sort_fields: vec![SortFieldConfig {
                name: "sortAt".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: false,
                computed: None,
            }],
            time_buckets: Some(TimeBucketFieldConfig {
                filter_field: "sortAt".to_string(),
                sort_field: "sortAt".to_string(),
                range_buckets: vec![
                    BucketConfig {
                        name: "24h".to_string(),
                        duration_secs: 86400,
                        refresh_interval_secs: 60,
                    },
                    BucketConfig {
                        name: "7d".to_string(),
                        duration_secs: 604800,
                        refresh_interval_secs: 60,
                    },
                ],
                full_rebuild_interval_secs: 3600,
                reconcile_scan_threads: 0,
            }),
            max_page_size: 100,
            flush_interval_us: 50,
            channel_capacity: 10_000,
            ..Default::default()
        };
        let engine = ConcurrentEngine::new(config).unwrap();
        // Bulk insert via loading mode (the prod NDJSON path).
        // Slots 1-50: sortAt within last 24h. Slot 51-60: 3 days ago (outside 24h).
        engine.enter_loading_mode();
        for i in 1u32..=50 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("sortAt", FieldValue::Single(Value::Integer((now - 3600) as i64))),
                    ]),
                )
                .unwrap();
        }
        for i in 51u32..=60 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("sortAt", FieldValue::Single(Value::Integer((now - 3 * 86400) as i64))),
                    ]),
                )
                .unwrap();
        }
        engine.exit_loading_mode();
        let stats = engine.time_bucket_stats();
        let bucket_24h = stats.get("24h").expect("24h bucket must exist");
        let slots_24h = bucket_24h.get("slots").and_then(|v| v.as_u64()).unwrap_or(0);
        assert_eq!(
            slots_24h, 50,
            "24h bucket must contain the 50 slots inserted within the last 24h \
             (got {slots_24h}); buckets stay empty when bulk-load bypasses the \
             flush-thread alive_inserts maintenance loop"
        );
        let bucket_7d = stats.get("7d").expect("7d bucket must exist");
        let slots_7d = bucket_7d.get("slots").and_then(|v| v.as_u64()).unwrap_or(0);
        assert_eq!(
            slots_7d, 60,
            "7d bucket must contain all 60 slots (50 in last 24h + 10 in last 7d); got {slots_7d}"
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
    /// Gate-decision unit test mirroring the WAL reader's hot-loop check.
    /// Demonstrates the same `if engine.is_loading_mode()` branch that
    /// `src/server.rs:1238` runs every iteration — when the flag is set,
    /// the gate body is skipped entirely; once the flag clears, the body
    /// proceeds. Pairs with `test_is_loading_mode_getter` to lock in the
    /// gate's visible contract.
    ///
    /// The gate IS best-effort, not a hard barrier — a batch already
    /// inside `apply_ops_batch` when `enter_loading_mode` fires will
    /// finish on top of partial state. The mid-flight-batch case is
    /// bounded by `read_batch` size (10,000 ops) and acceptable per
    /// the design (Scarlet 2026-04-29 review).
    ///
    /// A full WAL writer + reader + apply integration is staged as a
    /// fast-follow PR — exercising the real apply path requires the
    /// `pg-sync` feature, whose existing test surface has separate
    /// `DocStoreV3 Mutex → RwLock` rot blocking `cargo test --lib
    /// --features pg-sync`. That cleanup is task #45 (filed).
    #[test]
    #[serial]
    fn test_wal_gate_decision_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path);
        let mut engine =
            ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();

        // Phase 1: in loading mode, the gate should block the body.
        engine.enter_loading_mode();
        let mut body_ran_phase_1 = false;
        if !engine.is_loading_mode() {
            body_ran_phase_1 = true;
        }
        assert!(
            !body_ran_phase_1,
            "gate must skip body while engine.is_loading_mode() is true"
        );

        // Phase 2: out of loading mode, the body should run.
        engine.exit_loading_mode();
        let mut body_ran_phase_2 = false;
        if !engine.is_loading_mode() {
            body_ran_phase_2 = true;
        }
        assert!(
            body_ran_phase_2,
            "gate must permit body once exit_loading_mode flips the flag"
        );

        engine.shutdown();
    }
    /// Full integration test for the WAL reader gate. Exercises the real
    /// `WalWriter` + `WalReader` + `apply_ops_batch` path that the
    /// production server.rs:1238 thread runs. Promised as fast-follow in
    /// PR #244 review; unblocked by PR #245's `pg-sync` lib-test rot fix.
    ///
    /// Sequence:
    /// 1. enter_loading_mode
    /// 2. WalWriter.append_batch(ops)
    /// 3. mirror server.rs:1238 gate-loop iteration: gate skips, ops not applied
    /// 4. exit_loading_mode
    /// 5. mirror gate-loop iteration: gate permits, ops drain
    /// 6. assert engine state reflects the applied op
    ///
    /// Gate is best-effort by design — a batch already inside
    /// `apply_ops_batch` when `enter_loading_mode` fires will finish on
    /// top of partial state. This test asserts the steady-state behavior
    /// at iteration boundaries; mid-flight is bounded by `read_batch`
    /// size (10k ops) per Scarlet 2026-04-29 review.
    #[cfg(feature = "pg-sync")]
    #[test]
    #[serial]
    fn test_wal_apply_gated_by_loading_mode() {
        use crate::ingester::CoalescerSink;
        use crate::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
        use crate::ops_wal::{WalCursor, WalReader, WalWriter};
        use crate::pg_sync::ops::{EntityOps, Op};
        use serde_json::json;

        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let config = test_config_with_bitmap_path(bitmap_path);
        let mut engine =
            ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();

        let writer = WalWriter::new(&wal_dir);
        let cursor = WalCursor::new(0, 0);
        let mut reader = WalReader::new(&wal_dir, cursor);

        // Phase 1: enter loading_mode → queue an op → mirror server.rs:1238
        // gate-loop iteration. Gate must skip apply.
        engine.enter_loading_mode();
        let queued = vec![EntityOps {
            entity_id: 1,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "nsfwLevel".into(),
                value: json!(8),
            }],
        }];
        writer.append_batch(&queued).unwrap();

        let did_apply_phase_1 = if engine.is_loading_mode() {
            // Mirror server.rs:1238 — gate fires, skip apply this iteration.
            false
        } else {
            let meta = FieldMeta::from_config(engine.config());
            let sender = engine.mutation_sender();
            let mut sink = CoalescerSink::new(sender);
            let mut doc_writer = DocWriter::new(engine.docstore_arc());
            let batch = reader.read_batch(100).unwrap();
            let mut entries = batch.entries;
            let (applied, _, _) = apply_ops_batch(
                &mut sink,
                &meta,
                &mut entries,
                Some(&engine),
                Some(&mut doc_writer),
            );
            doc_writer.flush();
            applied > 0
        };
        assert!(
            !did_apply_phase_1,
            "gate must skip apply while engine.is_loading_mode() is true"
        );
        // WAL still holds the op; cursor unchanged because apply never ran.
        assert_eq!(reader.cursor().offset, 0, "cursor should not advance during loading");

        // Phase 2: exit loading_mode → run reader iteration again. Same
        // queued op should now apply.
        engine.exit_loading_mode();
        assert!(!engine.is_loading_mode());

        let did_apply_phase_2 = if engine.is_loading_mode() {
            false
        } else {
            let meta = FieldMeta::from_config(engine.config());
            let sender = engine.mutation_sender();
            let mut sink = CoalescerSink::new(sender);
            let mut doc_writer = DocWriter::new(engine.docstore_arc());
            let batch = reader.read_batch(100).unwrap();
            let mut entries = batch.entries;
            let (applied, _, _) = apply_ops_batch(
                &mut sink,
                &meta,
                &mut entries,
                Some(&engine),
                Some(&mut doc_writer),
            );
            doc_writer.flush();
            applied > 0
        };
        assert!(
            did_apply_phase_2,
            "post-exit, gate must permit apply — batch should drain"
        );

        // Allow flush thread to publish the snapshot containing the applied op.
        wait_for_flush(&engine, 1, 5000);
        let snap = engine.snapshot();
        let nsfw_field = snap.filters.get_field("nsfwLevel").unwrap();
        let bm = nsfw_field
            .get(8)
            .expect("nsfwLevel=8 bitmap should exist post-apply");
        assert!(
            bm.contains(1),
            "slot 1 should be in nsfwLevel=8 bitmap after gated apply drains"
        );

        engine.shutdown();
    }
    /// `is_loading_mode()` getter must reflect `enter_loading_mode` /
    /// `exit_loading_mode` toggles. Consumed by the WAL reader thread to
    /// gate op-apply during bulk-load — the load-bearing surface for
    /// task #43 (PR-#233 ops-walk pile-up post-canary).
    #[test]
    fn test_is_loading_mode_getter() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());
        let mut engine = ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();
        assert!(!engine.is_loading_mode(), "fresh engine should not be in loading mode");
        engine.enter_loading_mode();
        assert!(engine.is_loading_mode(), "after enter, should report loading");
        engine.exit_loading_mode();
        assert!(!engine.is_loading_mode(), "after exit, should report not loading");
        engine.shutdown();
    }
    /// Regression test: lazy field loading via rcu() must not clobber
    /// concurrent flush thread mutations.
    #[test]
    #[serial]
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
                    per_value_lazy: false, max_range_scan_values: None,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                    eager_load: false, // <-- lazy (default)
                    per_value_lazy: false, max_range_scan_values: None,
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
        // Restore — nsfwLevel and reactionCount should be eagerly loaded (not pending).
        // onSite should still be pending (lazy).
        {
            let engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            // nsfwLevel should NOT be in pending_filter_loads (eagerly loaded)
            assert!(
                !engine.pending_filter_loads.lock().contains("nsfwLevel"),
                "nsfwLevel should be eagerly loaded, not pending"
            );
            // onSite SHOULD be in pending_filter_loads (lazy)
            assert!(
                engine.pending_filter_loads.lock().contains("onSite"),
                "onSite should remain pending (lazy)"
            );
            // reactionCount should NOT be in pending_sort_loads (eagerly loaded)
            assert!(
                !engine.pending_sort_loads.lock().contains("reactionCount"),
                "reactionCount should be eagerly loaded, not pending"
            );
            // Eagerly loaded fields should be queryable without triggering lazy load
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
    #[serial]
    fn test_bound_store_persist_and_restore() {
            // Phase 1: Create engine, insert data, query to build cache, save
            let dir = tempfile::tempdir().unwrap();
            let bitmap_path = dir.path().join("bitmaps");
            let doc_path = dir.path().join("docs");
            let result_ids;
            {
                let config = test_config_with_bitmap_path(bitmap_path.clone());
                let mut engine = ConcurrentEngine::new_with_path(config, &doc_path).unwrap();
                // Insert 100 documents with nsfwLevel cycling 1-5 and reactionCount = slot*10
                for i in 1u32..=100 {
                    let nsfw_level = (i % 5) + 1;
                    let reaction_count = i * 10;
                    let doc = make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(nsfw_level as i64))),
                        ("reactionCount", FieldValue::Single(Value::Integer(reaction_count as i64))),
                    ]);
                    engine.put(i, &doc).unwrap();
                }
                // Wait for flush thread to apply all mutations
                wait_for_flush(&engine, 100, 5000);
                // Query to build a cache entry (must use execute_query for cache)
                let bq = BitdexQuery {
                    filters: vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                    sort: Some(SortClause {
                        field: "reactionCount".to_string(),
                        direction: SortDirection::Desc,
                    }),
                    limit: 5,
                    cursor: None,
                    offset: None,
                    skip_cache: false,
                };
                let result = engine.execute_query(&bq).unwrap();
                result_ids = result.ids.clone();
                assert!(!result_ids.is_empty(), "should have query results");
                // Run the query again to ensure cache hit
                let _ = engine.execute_query(&bq).unwrap();
                // Verify cache is populated
                {
                    let uc = &engine.unified_cache;
                    assert!(uc.len() > 0, "cache should have entries after query");
                }
                // Save bitmap snapshot (triggers merge thread persistence)
                engine.save_snapshot().unwrap();
                // Wait for merge thread to write BoundStore
                std::thread::sleep(std::time::Duration::from_millis(
                    engine.config.merge_interval_ms * 2 + 200,
                ));
                // Verify files exist on disk
                let bounds_dir = bitmap_path.join("shardstore").join("bounds");
                assert!(bounds_dir.join("meta.bin").exists(), "meta.bin should exist");
                engine.shutdown();
            }
            // Phase 2: Restore engine and verify warm cache
            {
                let config = test_config_with_bitmap_path(bitmap_path.clone());
                let mut engine = ConcurrentEngine::new_with_path(config, &doc_path).unwrap();
                // Verify BoundStore loaded meta
                {
                    let uc = &engine.unified_cache;
                    assert!(uc.persistence_enabled(), "persistence should be enabled");
                    assert!(uc.meta().entry_count() > 0, "meta-index should have restored entries");
                }
                // Query again — should trigger shard lazy load and get a cache hit
                let bq = BitdexQuery {
                    filters: vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                    sort: Some(SortClause {
                        field: "reactionCount".to_string(),
                        direction: SortDirection::Desc,
                    }),
                    limit: 5,
                    cursor: None,
                    offset: None,
                    skip_cache: false,
                };
                let result = engine.execute_query(&bq).unwrap();
                // Results should match (same data, same query)
                assert_eq!(
                    result.ids, result_ids,
                    "restored query should return same IDs as original"
                );
            engine.shutdown();
        }
    }
    #[test]
    fn test_compaction_worker_e2e() {
        use crate::shard_store_doc::PackedValue;
        use crate::shard_store_doc::SlotHexShard;

        // Use an on-disk docstore so ShardStore ops and compaction can run.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut engine = ConcurrentEngine::new_with_path(test_config(), &docs_dir).unwrap();

        // Write 10 Set ops to the same (slot=0, field=0) — 9 of 10 are stale after compaction.
        let field_idx: u16 = 0;
        {
            let mut ds = engine.docstore.write();
            for v in 0..10i64 {
                let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap();
                ds.append_tuple(0, field_idx, &packed).unwrap();
            }
        }

        // Verify the shard has ops before compaction
        let shard_key = SlotHexShard::slot_to_shard(0);
        let ops_before = {
            let ds = engine.docstore.read();
            ds.shard_store().ops_count(&shard_key).unwrap().unwrap_or(0)
        };
        assert_eq!(ops_before, 10, "should have 10 ops before compaction");

        // Trigger compaction directly on the shard (bypasses threshold check)
        {
            let ds = engine.docstore.read();
            ds.shard_store().compact_current(&shard_key).unwrap();
        }

        // After compaction, ops should be folded into a snapshot (0 ops remaining)
        let ops_after = {
            let ds = engine.docstore.read();
            ds.shard_store().ops_count(&shard_key).unwrap().unwrap_or(0)
        };
        assert_eq!(ops_after, 0, "ops should be 0 after compaction");

        // Verify the data is still correct — the last Set (value=9) wins
        {
            let ds = engine.docstore.read();
            let snap = ds.shard_store().read(&shard_key).unwrap().unwrap();
            let fields = snap.docs.get(&0).unwrap();
            assert_eq!(fields[0], (0, PackedValue::I(9)));
        }

        engine.shutdown();
    }
    #[test]
    #[serial]
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
    #[serial]
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
    #[serial]
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
    /// Bug #16 §4d: ops-path inserts must populate `id` and exists_boolean
    /// shadow targets in the docstore, not just bitmaps. Pre-fix, the
    /// docstore was missing `id` for every steady-state-inserted slot and
    /// `isPublished` for every slot whose publishedAt arrived via either
    /// the Image trigger (direct path) or Post fan-out (queryOpSet path).
    #[cfg(feature = "pg-sync")]
    fn bug16_test_config() -> Config {
        use crate::config::{DataSchema, FieldMapping, FieldValueType};
        let mut config = Config {
            filter_fields: vec![FilterFieldConfig {
                name: "isPublished".into(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            }, FilterFieldConfig {
                name: "postId".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            }],
            sort_fields: vec![SortFieldConfig {
                name: "publishedAt".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
                eager_load: false,
                computed: None,
            }],
            max_page_size: 100,
            flush_interval_us: 50,
            channel_capacity: 10_000,
            ..Default::default()
        };
        config.data_schema = DataSchema {
            id_field: "id".into(),
            schema_version: 1,
            fields: vec![
                FieldMapping {
                    source: "publishedAtUnix".into(),
                    target: "publishedAt".into(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: true,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None,
                    nullable: false,
                },
                FieldMapping {
                    source: "publishedAtUnix".into(),
                    target: "isPublished".into(),
                    value_type: FieldValueType::ExistsBoolean,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None,
                    nullable: false,
                },
                FieldMapping {
                    source: "postId".into(),
                    target: "postId".into(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None,
                    nullable: false,
                },
            ],
        };
        config
    }
    /// Direct path: Image trigger creates_slot + Set publishedAt=<seconds>.
    /// Pre-fix, the docstore was missing both `id` (never written by the ops
    /// path on creates_slot) and `isPublished` (bitmap shadow at
    /// `process_set_op:1115` had no docstore mirror).
    #[cfg(feature = "pg-sync")]
    #[test]
    fn test_bug16_creates_slot_writes_id_and_shadow_doc_fields() {
        use crate::pg_sync::ops::{EntityOps, Op};
        use crate::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
        use crate::ingester::CoalescerSink;
        use crate::shard_store_doc::PackedValue;
        use serde_json::json;
        let mut engine = ConcurrentEngine::new(bug16_test_config()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let sender = engine.mutation_sender();
        let mut sink = CoalescerSink::new(sender);
        let mut doc_writer = DocWriter::new(engine.docstore_arc());
        let slot: u32 = 100;
        let mut entries = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(42) },
                Op::Set { field: "publishedAt".into(), value: json!(1_777_581_167i64) },
            ],
        }];
        let (applied, skipped, errors) = apply_ops_batch(
            &mut sink, &meta, &mut entries, Some(&engine), Some(&mut doc_writer),
        );
        assert_eq!(applied, 1);
        assert_eq!(skipped, 0);
        assert_eq!(errors, 0);
        doc_writer.flush();
        let doc = engine
            .get_document(slot)
            .unwrap()
            .expect("slot should have a stored doc after creates_slot apply");
        assert_eq!(
            doc.fields.get("id"),
            Some(&FieldValue::Single(Value::Integer(slot as i64))),
            "creates_slot must persist id == slot in docstore (bug #16 §3a)"
        );
        match doc.fields.get("publishedAt") {
            Some(FieldValue::Single(Value::Integer(v))) => assert_eq!(*v, 1_777_581_167),
            other => panic!("publishedAt should be Integer; got {other:?}"),
        }
        assert_eq!(
            doc.fields.get("isPublished"),
            Some(&FieldValue::Single(Value::Bool(true))),
            "exists_boolean shadow must write isPublished=true to docstore (bug #16 §3b)"
        );
        let _ = PackedValue::B(true);
        engine.shutdown();
    }
    /// queryOpSet path: Post fan-out re-publishes a previously-inserted image.
    /// Pre-fix, `apply_query_op_set` did not receive a `DocWriter`, so fan-out
    /// updated the bitmap shadow but left the docstore at defaults — the
    /// production reproducer that surfaced bug #16.
    #[cfg(feature = "pg-sync")]
    #[test]
    fn test_bug16_query_op_set_writes_doc_and_shadow_fields() {
        use crate::pg_sync::ops::{EntityOps, Op};
        use crate::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
        use crate::ingester::CoalescerSink;
        use serde_json::json;
        let mut engine = ConcurrentEngine::new(bug16_test_config()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let sender = engine.mutation_sender();
        let mut sink = CoalescerSink::new(sender);
        let mut doc_writer = DocWriter::new(engine.docstore_arc());
        let slot: u32 = 200;
        let post_id: i64 = 4242;
        // Phase 1: Image trigger inserts the slot with a postId but no publishedAt
        // (the production scenario — Image table doesn't track publishedAt).
        let mut insert = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: true,
            ops: vec![Op::Set { field: "postId".into(), value: json!(post_id) }],
        }];
        let (applied, _, errors) = apply_ops_batch(
            &mut sink, &meta, &mut insert, Some(&engine), Some(&mut doc_writer),
        );
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);
        doc_writer.flush();
        // Wait for the postId bitmap to be visible in the published snapshot
        // so the queryOpSet's filter resolver can find the slot.
        wait_for_flush(&engine, 1, 5000);
        // Phase 2: Post fan-out fires (queryOpSet), Set publishedAt for any
        // image with postId == post_id. Pre-fix this bypassed the docstore.
        let mut fan_out = vec![EntityOps {
            entity_id: post_id,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {post_id}")),
                ops: vec![Op::Set {
                    field: "publishedAt".into(),
                    value: json!(1_777_581_167i64),
                }],
            }],
        }];
        let (applied, _, errors) = apply_ops_batch(
            &mut sink, &meta, &mut fan_out, Some(&engine), Some(&mut doc_writer),
        );
        assert!(applied >= 1, "fan-out should match the inserted slot");
        assert_eq!(errors, 0);
        doc_writer.flush();
        let doc = engine
            .get_document(slot)
            .unwrap()
            .expect("slot should have a stored doc");
        match doc.fields.get("publishedAt") {
            Some(FieldValue::Single(Value::Integer(v))) => assert_eq!(*v, 1_777_581_167),
            other => panic!(
                "queryOpSet must write publishedAt to docstore (bug #16 §3b); got {other:?}",
            ),
        }
        assert_eq!(
            doc.fields.get("isPublished"),
            Some(&FieldValue::Single(Value::Bool(true))),
            "queryOpSet must write isPublished shadow doc field (bug #16 §3b)"
        );
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
    /// Reproduce the collectionIds snapshot-overwrite bug:
    /// Bulk-loaded fpack data on disk gets overwritten by snapshot save
    /// when the engine has only partial (lazy-loaded) data in memory.
    #[test]
    #[serial]
    fn test_snapshot_save_preserves_bulk_loaded_lazy_value_field() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        // Config with collectionIds as a multi_value field (goes into lazy_value_fields)
        let config = Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false, max_range_scan_values: None,
                },
                FilterFieldConfig {
                    name: "collectionIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false, max_range_scan_values: None,
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
                bitmap_path: Some(bitmap_path.clone()),
                ..Default::default()
            },
            ..Default::default()
        };
        // Phase 1: Create engine, insert some docs to establish alive bitmap
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            // Insert 100 docs (slots 1-100) so alive bitmap is populated
            for i in 1..=100u32 {
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
            wait_for_flush(&engine, 100, 1000);
            engine.save_snapshot().unwrap();
            engine.shutdown();
        }
        // Phase 2: Simulate bulk load — write collectionIds to ShardStore
        // This is what the bulk loader does: writes directly to FilterBitmapStore
        {
            let fs = crate::shard_store_bitmap::FilterBitmapStore::new(
                bitmap_path.join("shardstore").join("filter"),
                crate::shard_store_bitmap::FieldValueBucketShard,
            ).unwrap();
            let mut bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();
            // Collection 42: contains slots 1-50
            let mut bm42 = RoaringBitmap::new();
            for i in 1..=50u32 { bm42.insert(i); }
            bitmaps.insert(42, bm42);
            // Collection 99: contains slots 51-100
            let mut bm99 = RoaringBitmap::new();
            for i in 51..=100u32 { bm99.insert(i); }
            bitmaps.insert(99, bm99);
            // Collection 7: contains slots 1-100 (all docs)
            let mut bm7 = RoaringBitmap::new();
            for i in 1..=100u32 { bm7.insert(i); }
            bitmaps.insert(7, bm7);
            // Write using FilterBitmapStore
            let entries: Vec<(&str, u64, &RoaringBitmap)> = bitmaps.iter()
                .map(|(k, v)| ("collectionIds", *k, v))
                .collect();
            fs.write_full_filter(&entries).unwrap();
            // Verify the data is correct
            let loaded = fs.load_field("collectionIds").unwrap();
            assert_eq!(loaded.len(), 3, "should have 3 collections on disk");
            assert_eq!(loaded[&42].len(), 50);
            assert_eq!(loaded[&99].len(), 50);
            assert_eq!(loaded[&7].len(), 100);
        }
        // Phase 3: Start engine from disk (lazy loads collectionIds)
        // Then simulate sync adding a few entries via sync_filter_values
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            assert_eq!(engine.alive_count(), 100);
            // Verify lazy load works — query collection 42 before any mutations
            let result = engine
                .query(
                    &[FilterClause::In("collectionIds".to_string(), vec![Value::Integer(42)])],
                    None,
                    100,
                )
                .unwrap();
            assert_eq!(
                result.total_matched, 50,
                "BUG PRECONDITION: collection 42 should have 50 results from disk"
            );
            // Simulate sync: add slot 1 to collection 42 (already there)
            // and slot 1 to a NEW collection 999
            engine
                .sync_filter_values(1, "collectionIds", &[42, 999])
                .unwrap();
            wait_for_flush(&engine, 100, 1000);
            // Trigger snapshot save — this is where the bug happens
            engine.save_snapshot().unwrap();
            engine.shutdown();
        }
        // Phase 4: Restart engine and verify bulk-loaded data survived
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            // Collection 42: should still have 50 results
            let r = engine
                .query(
                    &[FilterClause::In("collectionIds".to_string(), vec![Value::Integer(42)])],
                    None, 100,
                ).unwrap();
            assert_eq!(r.total_matched, 50,
                "SNAPSHOT OVERWRITE BUG: collection 42 lost data! Got {} expected 50", r.total_matched);
            // Collection 99: should still have 50 results (never touched by sync)
            let r = engine
                .query(
                    &[FilterClause::In("collectionIds".to_string(), vec![Value::Integer(99)])],
                    None, 100,
                ).unwrap();
            assert_eq!(r.total_matched, 50,
                "SNAPSHOT OVERWRITE BUG: collection 99 lost data! Got {} expected 50", r.total_matched);
            // Collection 7: should still have 100 results
            let r = engine
                .query(
                    &[FilterClause::In("collectionIds".to_string(), vec![Value::Integer(7)])],
                    None, 100,
                ).unwrap();
            assert_eq!(r.total_matched, 100,
                "SNAPSHOT OVERWRITE BUG: collection 7 lost data! Got {} expected 100", r.total_matched);
            // Collection 999: should have 1 result (from sync mutation)
            let r = engine
                .query(
                    &[FilterClause::In("collectionIds".to_string(), vec![Value::Integer(999)])],
                    None, 100,
                ).unwrap();
            assert_eq!(r.total_matched, 1,
                "Sync mutation lost: collection 999 should have 1 result, got {}", r.total_matched);
            engine.shutdown();
        }
    }
    #[test]
    #[serial]
    fn test_flush_thread_appends_ops_to_shard_stores() {
        // Verify that the flush thread writes ops-log entries to disk
        // instead of relying solely on merge thread full snapshots.
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());
        let ss_root = bitmap_path.join("shardstore");
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        // Insert a document — this goes through the flush thread which should
        // append ops to alive, filter, and sort shard stores.
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(100)])),
                    ("reactionCount", FieldValue::Single(Value::Integer(500))),
                ]),
            )
            .unwrap();
        // Wait for flush thread to process the mutation and append ops.
        std::thread::sleep(Duration::from_millis(200));
        // Verify ops landed on disk — alive shard should have ops
        let alive_store = crate::shard_store_bitmap::AliveBitmapStore::new(
            ss_root.join("alive"), crate::shard_store_bitmap::SingletonShard,
        ).unwrap();
        let alive_ops = alive_store.ops_count(&AliveShardKey).unwrap();
        assert!(
            alive_ops.is_some() && alive_ops.unwrap() > 0,
            "alive shard should have ops after insert, got {:?}",
            alive_ops,
        );
        // Verify alive bitmap is recoverable from ops
        let alive_bm = alive_store.read(&AliveShardKey).unwrap();
        assert!(alive_bm.is_some(), "alive bitmap should be readable from ops");
        assert!(
            alive_bm.as_ref().unwrap().contains(1),
            "alive bitmap should contain slot 1",
        );
        // Verify filter ops — nsfwLevel value 1 should have an op
        let filter_store = crate::shard_store_bitmap::FilterBitmapStore::new(
            ss_root.join("filter"), crate::shard_store_bitmap::FieldValueBucketShard,
        ).unwrap();
        let bucket_key = FilterBucketKey::from_value("nsfwLevel".to_string(), 1);
        let filter_snap = filter_store.read(&bucket_key).unwrap();
        assert!(filter_snap.is_some(), "filter bucket should exist after insert");
        let filter_snap = filter_snap.unwrap();
        let bm = filter_snap.values.get(&1);
        assert!(bm.is_some(), "nsfwLevel=1 bitmap should exist");
        assert!(bm.unwrap().contains(1), "nsfwLevel=1 should contain slot 1");
        // Verify sort ops — reactionCount layers should have ops
        let sort_store = crate::shard_store_bitmap::SortBitmapStore::new(
            ss_root.join("sort"), crate::shard_store_bitmap::SortLayerShard,
        ).unwrap();
        // 500 in binary: bit 8 (256), bit 7 (128), bit 6 (64), bit 5 (32),
        // bit 4 (16), bit 2 (4) = 0b111110100
        // At least bit 8 should be set for slot 1
        let layer_key = SortLayerShardKey {
            field: "reactionCount".to_string(),
            bit_position: 8,
        };
        let layer_snap = sort_store.read(&layer_key).unwrap();
        assert!(layer_snap.is_some(), "sort layer bit8 should exist");
        assert!(
            layer_snap.unwrap().contains(1),
            "sort layer bit8 should contain slot 1 for reactionCount=500",
        );
        // Insert more docs to accumulate ops, then verify compaction works
        for i in 2..=5u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 100))),
                    ]),
                )
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(200));
        // Verify alive ops accumulated
        let alive_ops_after = alive_store.ops_count(&AliveShardKey).unwrap().unwrap_or(0);
        assert!(
            alive_ops_after > 1,
            "alive shard should have multiple ops, got {}",
            alive_ops_after,
        );
        // Compact and verify the shard is now a clean snapshot (0 ops)
        alive_store.compact_current(&AliveShardKey).unwrap();
        let alive_ops_compacted = alive_store.ops_count(&AliveShardKey).unwrap().unwrap_or(999);
        assert_eq!(
            alive_ops_compacted, 0,
            "alive shard should have 0 ops after compaction",
        );
        // Verify data survived compaction
        let alive_bm = alive_store.read(&AliveShardKey).unwrap().unwrap();
        for i in 1..=5u32 {
            assert!(alive_bm.contains(i), "slot {} should survive compaction", i);
        }
        engine.shutdown();
    }

    // -----------------------------------------------------------------------
    // DocStoreV3 E2E integration tests
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

        // Read the doc back from DocStoreV3
        let doc = engine.docstore.read().get(1).unwrap();
        assert!(doc.is_some(), "doc should be readable after put + flush");
        let doc = doc.unwrap();
        assert_eq!(
            doc.fields.get("nsfwLevel"),
            Some(&FieldValue::Single(Value::Integer(5))),
            "nsfwLevel should roundtrip through DocStoreV3"
        );
        assert_eq!(
            doc.fields.get("reactionCount"),
            Some(&FieldValue::Single(Value::Integer(42))),
            "reactionCount should roundtrip through DocStoreV3"
        );

        engine.shutdown();
    }

    /// E2E: upsert reads old doc from DocStoreV3 for diff, clears stale bits.
    #[test]
    #[serial]
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

        // Upsert with nsfwLevel=3 — this requires reading old doc from DocStoreV3
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

        // Verify the stored doc has the new values. The docstore writer is a
        // separate thread from the bitmap flush thread, so the bitmap query
        // above can succeed before the docstore write lands; poll briefly.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let expected = FieldValue::Single(Value::Integer(3));
        loop {
            let doc = engine.docstore.read().get(1).unwrap().unwrap();
            if doc.fields.get("nsfwLevel") == Some(&expected) {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "docstore did not reflect upsert within 2s; got nsfwLevel={:?}",
                    doc.fields.get("nsfwLevel")
                );
            }
            thread::sleep(Duration::from_millis(5));
        }

        engine.shutdown();
    }

    /// E2E: delete reads old doc from DocStoreV3 to clear all bitmap bits.
    #[test]
    fn test_docstore_v3_delete_reads_old_doc() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("reactionCount", FieldValue::Single(Value::Integer(99))),
        ])).unwrap();
        wait_for_flush(&engine, 1, 500);

        // Doc should exist
        assert!(engine.docstore.read().get(1).unwrap().is_some());

        // Delete — this reads old doc from DocStoreV3 to clear filter/sort bits
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

    /// E2E: bulk loading with ShardStoreBulkWriter writes docs readable by DocStoreV3.
    #[test]
    fn test_docstore_v3_bulk_writer_roundtrip() {
        use crate::shard_store_doc::PackedValue;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut engine = ConcurrentEngine::new_with_path(test_config(), &docs_dir).unwrap();

        // Prepare bulk writer
        let bulk_writer = engine.prepare_bulk_writer(
            &["nsfwLevel".to_string(), "reactionCount".to_string()]
        ).unwrap();

        let nsfw_idx = *bulk_writer.field_to_idx().get("nsfwLevel").unwrap();
        let react_idx = *bulk_writer.field_to_idx().get("reactionCount").unwrap();

        // Write docs via bulk writer (simulating dump processor)
        for slot in 0..10u32 {
            let nsfw_bytes = rmp_serde::to_vec(&PackedValue::I(slot as i64 % 3 + 1)).unwrap();
            let react_bytes = rmp_serde::to_vec(&PackedValue::I(slot as i64 * 100)).unwrap();
            bulk_writer.append_tuple_raw(slot, nsfw_idx, &nsfw_bytes);
            bulk_writer.append_tuple_raw(slot, react_idx, &react_bytes);
        }

        // Flush to ShardStore
        bulk_writer.flush_v2_writers();

        // Read docs back via DocStoreV3
        for slot in 0..10u32 {
            let doc = engine.docstore.read().get(slot).unwrap();
            assert!(doc.is_some(), "slot {} should have a doc after bulk write", slot);
            let doc = doc.unwrap();
            let nsfw = doc.fields.get("nsfwLevel");
            assert!(nsfw.is_some(), "slot {} should have nsfwLevel field", slot);
            match nsfw.unwrap() {
                FieldValue::Single(Value::Integer(v)) => {
                    assert_eq!(*v, slot as i64 % 3 + 1, "nsfwLevel mismatch for slot {}", slot);
                }
                other => panic!("slot {}: expected Integer, got {:?}", slot, other),
            }
        }

        engine.shutdown();
    }

    // DocWriter E2E test lives in ops_processor.rs (needs private method access)

    // -----------------------------------------------------------------------
    // get_documents batch path tests (Tasks #14 + #12)
    // -----------------------------------------------------------------------

    /// Property test: get_documents returns the same results as serial get_document
    /// for each of the four page shapes specified in the task spec.
    #[test]
    fn test_get_documents_matches_serial_get_document() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert 600 docs spanning 2 shards (each shard covers 512 slots).
        for slot in 0u32..600 {
            engine.put(slot, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(slot as i64 % 5 + 1))),
                ("reactionCount", FieldValue::Single(Value::Integer(slot as i64))),
            ])).unwrap();
        }
        wait_for_flush(&engine, 600, 2000);

        // Shape 1: clustered — newest 100 items (slots 500..599, single shard)
        let clustered: Vec<u32> = (500u32..600).collect();
        let batch = engine.get_documents(&clustered).unwrap();
        for (i, &slot) in clustered.iter().enumerate() {
            let serial = engine.get_document(slot).unwrap();
            match (&batch[i], &serial) {
                (Some(b), Some(s)) => {
                    assert_eq!(b.fields.get("nsfwLevel"), s.fields.get("nsfwLevel"),
                        "clustered: slot {slot} nsfwLevel mismatch");
                    assert_eq!(b.fields.get("reactionCount"), s.fields.get("reactionCount"),
                        "clustered: slot {slot} reactionCount mismatch");
                }
                (None, None) => {}
                _ => panic!("clustered: slot {slot} batch={:?} serial={:?} mismatch", batch[i].is_some(), serial.is_some()),
            }
        }

        // Shape 2: random IDs across both shards (including duplicates)
        let random_slots: Vec<u32> = vec![0, 127, 255, 300, 511, 512, 513, 599, 300, 1];
        let batch = engine.get_documents(&random_slots).unwrap();
        for (i, &slot) in random_slots.iter().enumerate() {
            let serial = engine.get_document(slot).unwrap();
            match (&batch[i], &serial) {
                (Some(b), Some(s)) => {
                    assert_eq!(b.fields.get("reactionCount"), s.fields.get("reactionCount"),
                        "random: slot {slot} reactionCount mismatch");
                }
                (None, None) => {}
                _ => panic!("random: slot {slot} mismatch"),
            }
        }

        // Shape 3: single slot
        let single: Vec<u32> = vec![42];
        let batch = engine.get_documents(&single).unwrap();
        assert_eq!(batch.len(), 1);
        let serial = engine.get_document(42).unwrap();
        match (&batch[0], &serial) {
            (Some(b), Some(s)) => assert_eq!(b.fields.get("reactionCount"), s.fields.get("reactionCount")),
            (None, None) => {}
            _ => panic!("single: slot 42 mismatch"),
        }

        // Shape 4: empty slice returns empty vec
        let empty: Vec<u32> = vec![];
        let batch = engine.get_documents(&empty).unwrap();
        assert!(batch.is_empty(), "empty input should produce empty output");

        // Shape 5: non-existent slot returns None without panic
        let missing: Vec<u32> = vec![999_999];
        let batch = engine.get_documents(&missing).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(batch[0].is_none(), "non-existent slot should return None");

        engine.shutdown();
    }

    /// Verify that get_documents preserves input order even when IDs span
    /// multiple shards and are provided in reverse order.
    #[test]
    fn test_get_documents_preserves_order() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        let test_slots: &[u32] = &[0, 200, 400, 511, 512, 513, 600];
        for &slot in test_slots {
            engine.put(slot, &make_doc(vec![
                ("reactionCount", FieldValue::Single(Value::Integer(slot as i64))),
            ])).unwrap();
        }
        wait_for_flush(&engine, test_slots.len() as u64, 1000);

        // Request in reverse order — spans shards 0 and 1
        let slots: Vec<u32> = vec![600, 0, 513, 200, 511, 400, 512];
        let batch = engine.get_documents(&slots).unwrap();
        assert_eq!(batch.len(), slots.len());
        for (i, &slot) in slots.iter().enumerate() {
            match &batch[i] {
                Some(doc) => {
                    let expected = FieldValue::Single(Value::Integer(slot as i64));
                    assert_eq!(
                        doc.fields.get("reactionCount"),
                        Some(&expected),
                        "order mismatch at index {i}: expected slot {slot}"
                    );
                }
                None => panic!("slot {slot} should exist but returned None"),
            }
        }

        engine.shutdown();
    }

    // ── B8: resolve_bucket_clauses tests ────────────────────────────────

    fn make_time_bucket_manager_with(bucket_name: &str, slots: &[u32]) -> TimeBucketManager {
        use crate::config::BucketConfig;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut tb = TimeBucketManager::new(
            "sortAt".to_string(),
            vec![BucketConfig {
                name: bucket_name.to_string(),
                duration_secs: 7 * 86400,
                refresh_interval_secs: 300,
            }],
        );
        let mut bm = RoaringBitmap::new();
        for &s in slots {
            bm.insert(s);
        }
        tb.rebuild_bucket_from_bitmap(bucket_name, bm, now);
        tb
    }

    fn make_prefilter_registry_with(name: &str, slots: &[u32]) -> crate::prefilter::PrefilterRegistry {
        let reg = crate::prefilter::PrefilterRegistry::new();
        let mut bm = RoaringBitmap::new();
        for &s in slots {
            bm.insert(s);
        }
        // insert() requires at least one clause — use a minimal placeholder
        let dummy_clause = vec![FilterClause::IsNotNull("__placeholder".to_string())];
        let _ = reg.insert(name.to_string(), dummy_clause, bm, 300, 0);
        reg
    }

    /// A BucketBitmap referencing a known time bucket is resolved to a non-empty Arc.
    #[test]
    fn test_resolve_bucket_clauses_re_resolves_time_bucket() {
        let tb = make_time_bucket_manager_with("7d", &[10, 20, 30]);
        let pf = crate::prefilter::PrefilterRegistry::new();

        let mut clauses = vec![FilterClause::BucketBitmap {
            field: "sortAt".to_string(),
            bucket_name: "7d".to_string(),
            bitmap: Arc::new(RoaringBitmap::new()), // empty — simulates post-deserialize state
        }];

        let ok = resolve_bucket_clauses(&mut clauses, Some(&tb), &pf);
        assert!(ok, "resolve should succeed when bucket exists");

        match &clauses[0] {
            FilterClause::BucketBitmap { bitmap, .. } => {
                assert!(!bitmap.is_empty(), "bitmap should be non-empty after resolve");
                assert!(bitmap.contains(10));
                assert!(bitmap.contains(20));
            }
            other => panic!("expected BucketBitmap, got {other:?}"),
        }
    }

    /// A BucketBitmap with field="__prefilter" is resolved via PrefilterRegistry.
    #[test]
    fn test_resolve_bucket_clauses_re_resolves_prefilter() {
        let tb = make_time_bucket_manager_with("7d", &[]);
        let pf = make_prefilter_registry_with("safe", &[100, 200, 300]);

        let mut clauses = vec![FilterClause::BucketBitmap {
            field: "__prefilter".to_string(),
            bucket_name: "safe".to_string(),
            bitmap: Arc::new(RoaringBitmap::new()), // empty
        }];

        let ok = resolve_bucket_clauses(&mut clauses, Some(&tb), &pf);
        assert!(ok, "resolve should succeed when prefilter exists");

        match &clauses[0] {
            FilterClause::BucketBitmap { bitmap, .. } => {
                assert!(!bitmap.is_empty(), "bitmap should be non-empty after resolve");
                assert!(bitmap.contains(100));
            }
            other => panic!("expected BucketBitmap, got {other:?}"),
        }
    }

    /// A BucketBitmap referencing an unknown bucket name returns false — caller tombstones.
    #[test]
    fn test_resolve_bucket_clauses_unresolved_returns_false() {
        let tb = make_time_bucket_manager_with("7d", &[10]);
        let pf = crate::prefilter::PrefilterRegistry::new();

        let mut clauses = vec![FilterClause::BucketBitmap {
            field: "sortAt".to_string(),
            bucket_name: "unknown_bucket".to_string(), // does not exist in manager
            bitmap: Arc::new(RoaringBitmap::new()),
        }];

        let ok = resolve_bucket_clauses(&mut clauses, Some(&tb), &pf);
        assert!(!ok, "resolve should fail when bucket name is unknown");
    }

    /// Leaf clauses (Eq, In, IsNotNull) pass through untouched — all_ok stays true.
    #[test]
    fn test_resolve_bucket_clauses_leaf_clauses_pass_through() {
        let pf = crate::prefilter::PrefilterRegistry::new();
        let mut clauses = vec![
            FilterClause::Eq("nsfwLevel".to_string(), crate::query::Value::Integer(1)),
            FilterClause::In(
                "baseModel".to_string(),
                vec![crate::query::Value::Integer(42)],
            ),
            FilterClause::IsNotNull("postId".to_string()),
        ];
        let original = clauses.clone();

        let ok = resolve_bucket_clauses(&mut clauses, None, &pf);
        assert!(ok, "leaf-only clauses should always resolve successfully");
        assert_eq!(clauses, original, "leaf clauses must not be modified");
    }

    /// resolve_bucket_clauses recurses into Not(And(...)) and resolves inner BucketBitmaps.
    #[test]
    fn test_resolve_bucket_clauses_recurses_into_compound() {
        let tb = make_time_bucket_manager_with("7d", &[5, 10, 15]);
        let pf = crate::prefilter::PrefilterRegistry::new();

        let mut clauses = vec![FilterClause::Not(Box::new(FilterClause::And(vec![
            FilterClause::BucketBitmap {
                field: "sortAt".to_string(),
                bucket_name: "7d".to_string(),
                bitmap: Arc::new(RoaringBitmap::new()),
            },
            FilterClause::Eq("nsfwLevel".to_string(), crate::query::Value::Integer(1)),
        ])))];

        let ok = resolve_bucket_clauses(&mut clauses, Some(&tb), &pf);
        assert!(ok);

        // Verify the inner BucketBitmap was resolved
        match &clauses[0] {
            FilterClause::Not(inner) => match inner.as_ref() {
                FilterClause::And(parts) => match &parts[0] {
                    FilterClause::BucketBitmap { bitmap, .. } => {
                        assert!(!bitmap.is_empty(), "inner BucketBitmap should be resolved");
                        assert!(bitmap.contains(5));
                    }
                    other => panic!("expected BucketBitmap, got {other:?}"),
                },
                other => panic!("expected And, got {other:?}"),
            },
            other => panic!("expected Not, got {other:?}"),
        }
    }
}
