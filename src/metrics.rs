//! Prometheus metrics for BitDex.
//!
//! All metrics are registered in a custom `Registry` and exposed via
//! `gather_metrics()` which returns the Prometheus text exposition format.
//! Gauges for bitmap memory, cache stats, and document counts are refreshed
//! on each scrape (collect-on-scrape pattern).

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};

/// All BitDex Prometheus metrics.
pub struct Metrics {
    pub registry: Registry,

    // -- Document lifecycle --
    pub alive_documents: IntGaugeVec,
    pub slot_high_water: IntGaugeVec,
    pub upsert_total: IntCounterVec,
    pub delete_total: IntCounterVec,

    // -- Query performance --
    pub query_total: IntCounterVec,
    pub query_duration_seconds: HistogramVec,
    pub query_filter_seconds: HistogramVec,
    pub query_sort_seconds: HistogramVec,
    pub query_docs_seconds: HistogramVec,
    pub query_filter_clause_count: Histogram,

    // -- Cache --
    pub cache_hits_total: IntGaugeVec,
    pub cache_misses_total: IntGaugeVec,
    pub cache_inserts_total: IntGaugeVec,
    pub cache_updates_total: IntGaugeVec,
    pub cache_evictions_total: IntGaugeVec,
    pub cache_invalidations_total: IntGaugeVec,
    pub cache_entries: IntGaugeVec,
    pub cache_bytes: IntGaugeVec,
    pub cache_entries_initial: IntGaugeVec,
    pub cache_entries_expanded: IntGaugeVec,
    pub cache_extensions_total: IntGaugeVec,
    pub cache_wall_hits_total: IntGaugeVec,
    pub cache_prefetch_total: IntGaugeVec,
    // -- Bitmap memory --
    pub filter_bitmap_bytes: IntGaugeVec,
    pub filter_bitmap_count: IntGaugeVec,
    pub sort_bitmap_bytes: IntGaugeVec,
    pub slot_bitmap_bytes: IntGaugeVec,

    // -- Process memory --
    pub process_rss_bytes: IntGauge,
    pub process_rss_peak_bytes: IntGauge,

    // -- Jemalloc memory (populated when heap-prof feature is active) --
    pub jemalloc_allocated_bytes: IntGauge,
    pub jemalloc_resident_bytes: IntGauge,

    // -- Startup --
    pub startup_duration_seconds: IntGauge,

    // -- Write pipeline --
    pub flush_last_duration_seconds: IntGaugeVec,
    pub snapshot_publish_total: IntGaugeVec,
    // -- Flush phase timing --
    pub flush_apply_nanos: IntGaugeVec,
    pub flush_cache_nanos: IntGaugeVec,
    /// Phase A (collect work, LOCK HELD) duration of the most recent
    /// cache maintenance cycle. Combined with `flush_phase_c_nanos` it
    /// gives the actual mutex hold time queries pay against.
    pub flush_phase_a_nanos: IntGaugeVec,
    /// Phase C (apply results, LOCK HELD) duration of the most recent
    /// cache maintenance cycle.
    pub flush_phase_c_nanos: IntGaugeVec,
    /// Cumulative count of cache-maintenance cycles run by the flush
    /// thread. Paired with the phase nanos gauges via PromQL `rate()`
    /// to compute lock_held_per_second:
    ///   sum(flush_phase_a_nanos + flush_phase_c_nanos) * rate(flush_cache_cycles_total[1m]) / 1e9
    pub flush_cache_cycles_total: IntGaugeVec,
    pub flush_publish_nanos: IntGaugeVec,
    pub flush_timebucket_nanos: IntGaugeVec,
    pub flush_compact_nanos: IntGaugeVec,
    pub flush_opslog_nanos: IntGaugeVec,
    pub flush_sort_promote_nanos: IntGaugeVec,
    pub cache_maint_unique_filter_shapes: IntGaugeVec,
    pub cache_maint_sort_work_items: IntGaugeVec,
    pub cache_maint_unique_filter_shapes_max: IntGaugeVec,
    pub cache_maint_sort_work_items_max: IntGaugeVec,
    pub docstore_put_batch_fast_path_total: IntGaugeVec,
    pub docstore_put_batch_slow_path_total: IntGaugeVec,
    pub docstore_append_tuples_fast_path_total: IntGaugeVec,
    pub docstore_append_tuples_slow_path_total: IntGaugeVec,
    pub docstore_append_multi_ops_fast_path_total: IntGaugeVec,
    pub docstore_append_multi_ops_slow_path_total: IntGaugeVec,

    // -- Tier 2: Lazy loading --
    pub lazy_load_duration_seconds: HistogramVec,
    pub pending_fields: IntGaugeVec,

    // -- Eviction --
    pub eviction_total: IntGaugeVec,
    pub eviction_resident_values: IntGaugeVec,

    // -- Shard compaction (merge thread) --
    pub compaction_total: IntCounterVec,
    pub compaction_duration_seconds: HistogramVec,
    pub compaction_skipped_total: IntGaugeVec,

    // -- Compact endpoint --
    pub compact_running: IntGauge,
    pub compact_shards_scanned: IntGauge,
    pub compact_shards_compacted: IntGauge,
    pub compact_shards_skipped: IntGauge,
    pub compact_runs_total: IntCounter,
    pub compact_duration_seconds: Histogram,

    // -- Query concurrency --
    pub queries_in_flight: IntGauge,
    pub queries_in_flight_peak: IntGauge,
    pub queries_rejected_total: IntCounter,

    // -- BoundStore (cache persistence) --
    pub boundstore_meta_entries: IntGaugeVec,
    pub boundstore_tombstones: IntGaugeVec,
    pub boundstore_pending_shards: IntGaugeVec,
    pub boundstore_disk_bytes: IntGaugeVec,
    pub boundstore_shard_loads_total: IntGaugeVec,
    pub boundstore_tombstones_created: IntGaugeVec,
    pub boundstore_tombstones_cleaned: IntGaugeVec,
    pub boundstore_entries_restored: IntGaugeVec,
    pub boundstore_bytes_written: IntGaugeVec,
    pub boundstore_bytes_read: IntGaugeVec,

    // -- HTTP round-trip (wall-clock from request arrival to response sent) --
    pub http_response_seconds: HistogramVec,

    // -- Phase 2.5: DocStore I/O observability --
    pub docstore_read_seconds: HistogramVec,
    pub docstore_concurrent_reads: IntGauge,
    // -- Phase 1 (2026-04-09 batch doc fetch): shard read cost split --
    pub docstore_shard_file_read_seconds: HistogramVec,
    pub docstore_shard_decode_seconds: HistogramVec,
    pub docstore_batch_unique_shards: HistogramVec,
    // -- Apr 10 2026 doc fetch path per-stage split (Ivy+Aidan catalog) --
    // Isolates C1 spawn_blocking dispatch, C4 StoredDoc::clone on cache
    // hits, C2 format_document loop, and C10 json!(docs) response wrap.
    // Together with existing filter/sort/docs histograms, covers the
    // entire path from request parse to response build.
    pub query_spawn_blocking_dispatch_seconds: HistogramVec,
    pub query_doc_cache_probe_seconds: HistogramVec,
    pub query_doc_disk_fetch_seconds: HistogramVec,
    pub query_doc_format_seconds: HistogramVec,
    pub query_response_build_seconds: HistogramVec,
    // -- Apr 11 2026 tokio .await return delay + inline/spawn_blocking split --
    // Directly measures the time between the blocking closure completing
    // and the .await resuming on the async side. Under reactor saturation
    // this delay dominates query_docs P95 (was ~571ms avg in prior analysis).
    // Labeled counter to see inline-vs-spawn_blocking hit rate.
    pub query_docs_path_total: IntCounterVec,
    pub query_tokio_return_delay_seconds: HistogramVec,
    // -- Apr 11 2026 get_many serial vs parallel path split --
    // At unique_shards > 4, get_many dispatches via rayon. Below that it
    // goes serial. This counter shows how often each path fires, so we
    // can tell if the parallel optimization is actually kicking in on
    // cache-miss batches or whether most batches are too small to qualify.
    pub docstore_read_path_total: IntCounterVec,
    // -- Apr 10 2026 lock-wait instrumentation (Justin's flush-blocks-reads hypothesis) --
    // Measures how long query threads BLOCK waiting for locks held by the
    // flush thread. Zero under no contention; spikes when flush thread
    // holds locks during Phase A/C (unified_cache Mutex) or put_batch
    // (docstore RwLock write).
    pub query_cache_lock_wait_seconds: HistogramVec,
    pub query_cache_hold_seconds: HistogramVec,
    pub query_lazy_load_seconds: HistogramVec,
    pub query_overhead_seconds: HistogramVec,
    pub query_docstore_lock_wait_seconds: HistogramVec,
    pub save_snapshot_seconds: HistogramVec,
    pub flush_queue_depth: IntGauge,

    // -- Phase 2.5: Doc cache --
    pub doc_cache_hit_total: IntGaugeVec,
    pub doc_cache_miss_total: IntGaugeVec,
    pub doc_cache_entries: IntGaugeVec,
    pub doc_cache_bytes: IntGaugeVec,
    pub doc_cache_evictions_total: IntGaugeVec,
    pub doc_cache_generations: IntGaugeVec,
    pub doc_cache_backlog: IntGaugeVec,

    // -- Phase 2.5: ShardStore ops (stub — wired when Phase 1 lands) --
    pub shardstore_ops_count: IntGaugeVec,

    // -- Phase 2.5: PG-Sync observability --
    pub pgsync_cycle_seconds: HistogramVec,
    pub pgsync_rows_fetched_total: IntCounterVec,
    pub pgsync_cursor_position: IntGaugeVec,

    pub pgsync_errors_total: IntCounterVec,

    // V2 sync metrics (unified namespace with source label)
    pub sync_cursor_position: IntGaugeVec,
    pub sync_max_id: IntGaugeVec,
    pub sync_lag_rows: IntGaugeVec,
    pub sync_ops_total: IntCounterVec,
    pub sync_wal_bytes: IntGaugeVec,
    pub sync_cycle_duration_seconds: HistogramVec,
    pub sync_wal_pending_bytes: IntGaugeVec,
    pub sync_batch_size: IntGaugeVec,

    // -- WAL read-side observability --
    pub wal_ops_processed_total: IntCounter,
    pub wal_read_cursor_bytes: IntGauge,

    // -- WAL write-side observability --
    pub wal_ops_written_total: IntCounter,
    pub wal_last_applied_timestamp_seconds: IntGauge,

    // -- Boot phase breakdown --
    pub boot_phase_seconds: IntGaugeVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let alive_documents = IntGaugeVec::new(
            Opts::new("bitdex_alive_documents", "Number of live documents"),
            &["index"],
        )
        .unwrap();

        let slot_high_water = IntGaugeVec::new(
            Opts::new("bitdex_slot_high_water", "High-water slot counter"),
            &["index"],
        )
        .unwrap();

        let upsert_total = IntCounterVec::new(
            Opts::new("bitdex_upsert_total", "Total upsert operations"),
            &["index"],
        )
        .unwrap();

        let delete_total = IntCounterVec::new(
            Opts::new("bitdex_delete_total", "Total delete operations"),
            &["index"],
        )
        .unwrap();

        let query_total = IntCounterVec::new(
            Opts::new("bitdex_query_total", "Total queries served"),
            &["index"],
        )
        .unwrap();

        // Tuned for production: 100% of queries land under 50ms with doc cache.
        // Dense sub-ms resolution where most queries live, sparse upper for outliers.
        let query_buckets = vec![
            0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0,
        ];
        let query_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_duration_seconds",
                "Query latency distribution",
            )
            .buckets(query_buckets),
            &["index"],
        )
        .unwrap();

        // HTTP round-trip: wall-clock from request arrival to response sent.
        // Wide buckets to catch the gap between fast queries and slow responses.
        let http_buckets = vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        let http_response_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_http_response_seconds",
                "Full HTTP round-trip time from request arrival to response sent",
            )
            .buckets(http_buckets),
            &["method", "path"],
        )
        .unwrap();

        // Phase histograms (`query_filter_seconds`, `query_sort_seconds`,
        // `query_docs_seconds`). The top bucket used to be 5.0 seconds,
        // which saturated the query_docs_seconds P95/P99 for the entire
        // Apr 9 2026 "OOM + P95 regression" crisis — we spent hours
        // chasing a "pinned 5 s tail" that turned out to be histogram
        // ceiling saturation, not a real regression. The real tail was
        // under 500 ms the whole time.
        //
        // Top bucket is now 60 s so even a pathological backlog event
        // (disk wedge, GC pause, runaway query) is still measurable.
        // Added 2.5 between 1 and 5 for resolution in the "tail but
        // still reasonable" range where most production alerting lives.
        //
        // Note: changing histogram buckets mid-flight is Prometheus-safe.
        // Existing dashboards that compute `histogram_quantile(0.95, ...)`
        // over the bucket series continue to work (cumulative bucket
        // counts are unchanged for existing boundaries). The new buckets
        // appear as additional time series with zero backfill.
        let phase_buckets = vec![
            0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5,
            1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
        ];
        let query_filter_seconds = HistogramVec::new(
            HistogramOpts::new("bitdex_query_filter_seconds", "Bitmap filter evaluation time")
                .buckets(phase_buckets.clone()),
            &["index"],
        ).unwrap();
        let query_sort_seconds = HistogramVec::new(
            HistogramOpts::new("bitdex_query_sort_seconds", "Bitmap sort traversal time")
                .buckets(phase_buckets.clone()),
            &["index"],
        ).unwrap();
        let query_docs_seconds = HistogramVec::new(
            HistogramOpts::new("bitdex_query_docs_seconds", "Document fetch from disk time")
                .buckets(phase_buckets),
            &["index"],
        ).unwrap();

        let query_filter_clause_count = Histogram::with_opts(
            HistogramOpts::new(
                "bitdex_query_filter_clause_count",
                "Number of filter clauses (ANDs) per query",
            )
            .buckets(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0]),
        ).unwrap();

        let cache_hits_total = IntGaugeVec::new(
            Opts::new("bitdex_cache_hits_total", "Unified cache cumulative hits"),
            &["index"],
        )
        .unwrap();

        let cache_misses_total = IntGaugeVec::new(
            Opts::new("bitdex_cache_misses_total", "Unified cache cumulative misses"),
            &["index"],
        )
        .unwrap();

        let cache_inserts_total = IntGaugeVec::new(
            Opts::new("bitdex_cache_inserts_total", "Cache entries created"),
            &["index"],
        )
        .unwrap();

        let cache_updates_total = IntGaugeVec::new(
            Opts::new("bitdex_cache_updates_total", "Cache entries updated by maintenance"),
            &["index"],
        )
        .unwrap();

        let cache_evictions_total = IntGaugeVec::new(
            Opts::new("bitdex_cache_evictions_total", "Cache entries evicted by LRU"),
            &["index"],
        )
        .unwrap();

        let cache_invalidations_total = IntGaugeVec::new(
            Opts::new("bitdex_cache_invalidations_total", "Cache entries invalidated by field changes"),
            &["index"],
        )
        .unwrap();

        let cache_entries = IntGaugeVec::new(
            Opts::new("bitdex_cache_entries", "Unified cache entry count"),
            &["index"],
        )
        .unwrap();

        let cache_bytes = IntGaugeVec::new(
            Opts::new("bitdex_cache_bytes", "Unified cache memory bytes"),
            &["index"],
        )
        .unwrap();

        let cache_entries_initial = IntGaugeVec::new(
            Opts::new("bitdex_cache_entries_initial", "Cache entries at initial capacity (sorted vec)"),
            &["index"],
        )
        .unwrap();

        let cache_entries_expanded = IntGaugeVec::new(
            Opts::new("bitdex_cache_entries_expanded", "Cache entries at expanded capacity (radix)"),
            &["index"],
        )
        .unwrap();

        let cache_extensions_total = IntGaugeVec::new(
            Opts::new("bitdex_cache_extensions_total", "Cumulative cache entry expansions from initial to expanded"),
            &["index"],
        )
        .unwrap();

        let cache_wall_hits_total = IntGaugeVec::new(
            Opts::new("bitdex_cache_wall_hits_total", "Cumulative cache wall hits (cursor past cached entries)"),
            &["index"],
        )
        .unwrap();

        let cache_prefetch_total = IntGaugeVec::new(
            Opts::new("bitdex_cache_prefetch_total", "Cumulative prefetch triggers for background expansion"),
            &["index"],
        )
        .unwrap();

        let filter_bitmap_bytes = IntGaugeVec::new(
            Opts::new(
                "bitdex_filter_bitmap_bytes",
                "Filter bitmap memory per field",
            ),
            &["index", "field"],
        )
        .unwrap();

        let filter_bitmap_count = IntGaugeVec::new(
            Opts::new(
                "bitdex_filter_bitmap_count",
                "Number of distinct bitmaps per filter field",
            ),
            &["index", "field"],
        )
        .unwrap();

        let sort_bitmap_bytes = IntGaugeVec::new(
            Opts::new("bitdex_sort_bitmap_bytes", "Sort bitmap memory per field"),
            &["index", "field"],
        )
        .unwrap();

        let slot_bitmap_bytes = IntGaugeVec::new(
            Opts::new("bitdex_slot_bitmap_bytes", "Alive/slot bitmap memory"),
            &["index"],
        )
        .unwrap();

        let flush_last_duration_seconds = IntGaugeVec::new(
            Opts::new(
                "bitdex_flush_last_duration_nanos",
                "Most recent flush loop duration in nanoseconds",
            ),
            &["index"],
        )
        .unwrap();

        let snapshot_publish_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_snapshot_publish_total",
                "Total ArcSwap snapshot publishes",
            ),
            &["index"],
        )
        .unwrap();

        // Process memory
        let process_rss_bytes = IntGauge::new(
            "bitdex_process_rss_bytes", "Process resident set size in bytes",
        ).unwrap();
        let process_rss_peak_bytes = IntGauge::new(
            "bitdex_process_rss_peak_bytes", "Peak process RSS in bytes since startup",
        ).unwrap();

        // Jemalloc memory (refreshed on scrape when heap-prof feature is active)
        let jemalloc_allocated_bytes = IntGauge::new(
            "bitdex_jemalloc_allocated_bytes", "Jemalloc stats.allocated — total bytes allocated by the application",
        ).unwrap();
        let jemalloc_resident_bytes = IntGauge::new(
            "bitdex_jemalloc_resident_bytes", "Jemalloc stats.resident — RSS bytes accounted for by jemalloc",
        ).unwrap();

        // Startup duration (set once after index restore completes)
        let startup_duration_seconds = IntGauge::new(
            "bitdex_startup_duration_seconds", "Time spent loading bitmap indexes at startup",
        ).unwrap();

        let flush_apply_nanos = IntGaugeVec::new(
            Opts::new("bitdex_flush_apply_nanos", "Last flush apply_prepared duration in nanoseconds"),
            &["index"],
        )
        .unwrap();
        let flush_cache_nanos = IntGaugeVec::new(
            Opts::new("bitdex_flush_cache_nanos", "Last flush cache maintenance duration in nanoseconds (Phase A + Phase B + Phase C)"),
            &["index"],
        )
        .unwrap();
        let flush_phase_a_nanos = IntGaugeVec::new(
            Opts::new(
                "bitdex_flush_phase_a_nanos",
                "Last cache-maintenance Phase A duration (collect work, LOCK HELD on unified_cache mutex)",
            ),
            &["index"],
        )
        .unwrap();
        let flush_phase_c_nanos = IntGaugeVec::new(
            Opts::new(
                "bitdex_flush_phase_c_nanos",
                "Last cache-maintenance Phase C duration (apply results, LOCK HELD on unified_cache mutex)",
            ),
            &["index"],
        )
        .unwrap();
        let flush_cache_cycles_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_flush_cache_cycles_total",
                "Cumulative count of cache-maintenance cycles run by the flush thread (use rate() to derive cycles/sec)",
            ),
            &["index"],
        )
        .unwrap();
        let flush_publish_nanos = IntGaugeVec::new(
            Opts::new("bitdex_flush_publish_nanos", "Last flush staging clone + ArcSwap publish duration in nanoseconds"),
            &["index"],
        )
        .unwrap();
        let flush_timebucket_nanos = IntGaugeVec::new(
            Opts::new("bitdex_flush_timebucket_nanos", "Last flush time bucket maintenance duration in nanoseconds"),
            &["index"],
        )
        .unwrap();
        let flush_compact_nanos = IntGaugeVec::new(
            Opts::new("bitdex_flush_compact_nanos", "Last flush diff compaction duration in nanoseconds"),
            &["index"],
        )
        .unwrap();
        let flush_opslog_nanos = IntGaugeVec::new(
            Opts::new(
                "bitdex_flush_opslog_nanos",
                "Last flush ops-log append duration in nanoseconds (runs after publish, not included in flush_last_duration_nanos)",
            ),
            &["index"],
        )
        .unwrap();
        let flush_sort_promote_nanos = IntGaugeVec::new(
            Opts::new(
                "bitdex_flush_sort_promote_nanos",
                "Duration of the most recent sort-layer promote pass (merge_dirty across dirty sort fields) in nanoseconds. Sort-promote runs inside the flush thread but only fires every ~5s, so this gauge updates on promote cycles, NOT on every flush. A value of 0 means no promote has run since boot. Contributes to flush_last_duration_nanos on the cycle it fires.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_maint_unique_filter_shapes = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_maint_unique_filter_shapes",
                "Number of unique canonical filter-clause vectors across sort-maintenance work items in the most recent cache-maintenance cycle. Compare to cache_maint_sort_work_items to get the collapse factor (shapes/items). Low collapse = many entries share filters, filter-shape grouping in Phase B would pay off. High collapse = filters are diverse.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_maint_sort_work_items = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_maint_sort_work_items",
                "Number of sort-maintenance work items collected in the most recent cache-maintenance cycle. Denominator for the filter-shape collapse ratio (see cache_maint_unique_filter_shapes).",
            ),
            &["index"],
        )
        .unwrap();
        let cache_maint_unique_filter_shapes_max = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_maint_unique_filter_shapes_max",
                "Maximum observed unique canonical filter-clause vectors in a single cache-maintenance cycle since boot. Captures burst-time peaks that the last-cycle gauge can miss on quiet samples.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_maint_sort_work_items_max = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_maint_sort_work_items_max",
                "Maximum observed sort-maintenance work item count in a single cache-maintenance cycle since boot. Denominator for the burst-time filter-shape collapse ratio.",
            ),
            &["index"],
        )
        .unwrap();
        let docstore_put_batch_fast_path_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_docstore_put_batch_fast_path_total",
                "Cumulative count of DocStoreV3::put_batch_known_fields invocations that took the concurrent-read fast path (field dict already contained all batch fields). Ratio with slow_path tells us how often we avoid the outer RwLock write guard.",
            ),
            &["index"],
        )
        .unwrap();
        let docstore_put_batch_slow_path_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_docstore_put_batch_slow_path_total",
                "Cumulative count of DocStoreV3::put_batch calls (write-lock path), either from dict-update fallback or direct calls that bypass the fast path.",
            ),
            &["index"],
        )
        .unwrap();
        let docstore_append_tuples_fast_path_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_docstore_append_tuples_fast_path_total",
                "Cumulative count of DocStoreV3::append_tuples_batch_concurrent invocations — the steady-state hot path for Set ops from the metrics poller and PG ops sync. Expected to dominate put_batch counters in prod.",
            ),
            &["index"],
        )
        .unwrap();
        let docstore_append_tuples_slow_path_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_docstore_append_tuples_slow_path_total",
                "Cumulative count of the legacy `append_tuples_batch(&mut self)` write-lock path. Expected ~zero in steady state once all callers migrate to the concurrent variant.",
            ),
            &["index"],
        )
        .unwrap();
        let docstore_append_multi_ops_fast_path_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_docstore_append_multi_ops_fast_path_total",
                "Cumulative count of DocStoreV3::append_multi_ops_batch_concurrent invocations — used for Append/Remove ops on multi-value fields (tag adds/removes etc).",
            ),
            &["index"],
        )
        .unwrap();
        let docstore_append_multi_ops_slow_path_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_docstore_append_multi_ops_slow_path_total",
                "Cumulative count of the legacy `append_multi_ops_batch(&mut self)` write-lock path.",
            ),
            &["index"],
        )
        .unwrap();

        let lazy_load_buckets = vec![0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0];
        let lazy_load_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_lazy_load_duration_seconds",
                "Time to lazy-load field bitmaps on first query",
            )
            .buckets(lazy_load_buckets),
            &["index", "field"],
        )
        .unwrap();

        let pending_fields = IntGaugeVec::new(
            Opts::new(
                "bitdex_pending_fields",
                "Filter+sort fields not yet loaded into memory",
            ),
            &["index"],
        )
        .unwrap();

        let eviction_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_eviction_total",
                "Total values evicted from filter fields since startup",
            ),
            &["index", "field"],
        )
        .unwrap();

        let eviction_resident_values = IntGaugeVec::new(
            Opts::new(
                "bitdex_eviction_resident_values",
                "Currently resident value count for eviction-enabled fields",
            ),
            &["index", "field"],
        )
        .unwrap();

        // Shard compaction metrics
        let compaction_total = IntCounterVec::new(
            Opts::new("bitdex_compaction_total", "Total shard compactions performed"),
            &["index"],
        )
        .unwrap();
        let compaction_buckets = vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1];
        let compaction_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_compaction_duration_seconds",
                "Shard compaction latency distribution",
            )
            .buckets(compaction_buckets),
            &["index"],
        )
        .unwrap();
        let compaction_skipped_total = IntGaugeVec::new(
            Opts::new("bitdex_compaction_skipped_total", "Compactions skipped (channel full)"),
            &["index"],
        )
        .unwrap();

        // Compact endpoint metrics
        let compact_running = IntGauge::new(
            "bitdex_compact_running", "Whether a compact endpoint task is currently running (0 or 1)",
        ).unwrap();
        let compact_shards_scanned = IntGauge::new(
            "bitdex_compact_shards_scanned", "Shards scanned in current/last compact run",
        ).unwrap();
        let compact_shards_compacted = IntGauge::new(
            "bitdex_compact_shards_compacted", "Shards actually compacted in current/last compact run",
        ).unwrap();
        let compact_shards_skipped = IntGauge::new(
            "bitdex_compact_shards_skipped", "Shards skipped (already clean) in current/last compact run",
        ).unwrap();
        let compact_runs_total = IntCounter::new(
            "bitdex_compact_runs_total", "Total compact endpoint invocations",
        ).unwrap();
        let compact_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "bitdex_compact_duration_seconds", "Compact endpoint total duration",
            ).buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]),
        ).unwrap();

        // Query concurrency metrics
        let queries_in_flight = IntGauge::new(
            "bitdex_queries_in_flight", "Queries currently executing",
        ).unwrap();
        let queries_in_flight_peak = IntGauge::new(
            "bitdex_queries_in_flight_peak", "Peak concurrent queries since startup",
        ).unwrap();
        let queries_rejected_total = IntCounter::new(
            "bitdex_queries_rejected_total", "Queries rejected by backpressure",
        ).unwrap();

        // BoundStore metrics
        let boundstore_meta_entries = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_meta_entries", "Cache entries registered in meta-index"),
            &["index"],
        ).unwrap();
        let boundstore_tombstones = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_tombstones", "Current tombstone count"),
            &["index"],
        ).unwrap();
        let boundstore_pending_shards = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_pending_shards", "Shards awaiting lazy load"),
            &["index"],
        ).unwrap();
        let boundstore_disk_bytes = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_disk_bytes", "Total bounds directory size on disk"),
            &["index"],
        ).unwrap();
        let boundstore_shard_loads_total = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_shard_loads_total", "Cumulative shard load events"),
            &["index"],
        ).unwrap();
        let boundstore_tombstones_created = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_tombstones_created_total", "Cumulative tombstones created"),
            &["index"],
        ).unwrap();
        let boundstore_tombstones_cleaned = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_tombstones_cleaned_total", "Cumulative tombstones cleaned"),
            &["index"],
        ).unwrap();
        let boundstore_entries_restored = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_entries_restored_total", "Cumulative entries loaded from shard"),
            &["index"],
        ).unwrap();
        let boundstore_bytes_written = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_bytes_written_total", "Cumulative bytes written to bounds"),
            &["index"],
        ).unwrap();
        let boundstore_bytes_read = IntGaugeVec::new(
            Opts::new("bitdex_boundstore_bytes_read_total", "Cumulative bytes read from bounds"),
            &["index"],
        ).unwrap();

        // Phase 2.5: DocStore I/O observability
        let docstore_read_buckets = vec![0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0];
        let docstore_read_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_docstore_read_seconds",
                "Individual document read latency from disk",
            )
            .buckets(docstore_read_buckets),
            &["index"],
        )
        .unwrap();
        let docstore_concurrent_reads = IntGauge::new(
            "bitdex_docstore_concurrent_reads",
            "Number of concurrent docstore reads in progress",
        )
        .unwrap();

        // Phase 1 (2026-04-09): split the per-shard read cost into its
        // two dominant phases so we can tell disk-bound from decode-bound
        // work without guessing. Same bucket shape as docstore_read_seconds.
        let shard_read_buckets = vec![
            0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
        ];
        let docstore_shard_file_read_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_docstore_shard_file_read_seconds",
                "Raw shard file read time (disk + kernel, excludes decode)",
            )
            .buckets(shard_read_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let docstore_shard_decode_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_docstore_shard_decode_seconds",
                "Shard snapshot decode + op-apply time (CPU, excludes disk)",
            )
            .buckets(shard_read_buckets),
            &["index"],
        )
        .unwrap();
        // How many unique shards a single batch doc fetch has to read.
        // Low values mean shards are clustering (good for batch effect),
        // high values mean random access (batch effect is weaker).
        let docstore_batch_unique_shards = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_docstore_batch_unique_shards",
                "Unique shards touched per batch doc fetch call",
            )
            .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]),
            &["index"],
        )
        .unwrap();

        // Apr 10 2026: doc fetch path per-stage split. Uses phase_buckets
        // (same as query_filter/sort/docs) so dashboards comparing phases
        // share axes. See comment on the struct fields for the rationale.
        let doc_phase_buckets = vec![
            0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5,
            1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
        ];
        let query_spawn_blocking_dispatch_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_spawn_blocking_dispatch_seconds",
                "tokio::spawn_blocking dispatch gap: time between spawn call and closure first line",
            )
            .buckets(doc_phase_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let query_doc_cache_probe_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_doc_cache_probe_seconds",
                "Phase 1 of get_documents_batch: DocCache probe loop (includes StoredDoc::clone on hits)",
            )
            .buckets(doc_phase_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let query_doc_disk_fetch_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_doc_disk_fetch_seconds",
                "Phase 2 of get_documents_batch: wrapping get_many call (shard read + by_shard build + scatter)",
            )
            .buckets(doc_phase_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let query_doc_format_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_doc_format_seconds",
                "format_document loop inside spawn_blocking: StoredDoc → serde_json::Value per doc",
            )
            .buckets(doc_phase_buckets.clone()),
            &["index"],
        )
        .unwrap();
        // Apr 11 2026: tokio .await return delay instrumentation
        // Doc phase buckets extend to 60s since reactor-saturation delays
        // can be multi-second.
        let query_docs_path_total = IntCounterVec::new(
            Opts::new(
                "bitdex_query_docs_path_total",
                "Cumulative count of doc fetch path taken: inline (full cache hit) vs spawn_blocking (any miss)",
            ),
            &["index", "path"],
        )
        .unwrap();
        let docstore_read_path_total = IntCounterVec::new(
            Opts::new(
                "bitdex_docstore_read_path_total",
                "Cumulative count of DocStoreV3::get_many path taken: serial (≤4 shards) vs parallel (>4 shards via rayon)",
            ),
            &["index", "path"],
        )
        .unwrap();
        let query_tokio_return_delay_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_tokio_return_delay_seconds",
                "Time from blocking closure completion to .await resuming on async side (reactor wake delay)",
            )
            .buckets(doc_phase_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let query_response_build_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_response_build_seconds",
                "Response build window: json!(docs) wrap + final Json(response) serialize (currently unattributed 9ms gap)",
            )
            .buckets(doc_phase_buckets),
            &["index"],
        )
        .unwrap();

        // Lock-wait histograms: measure time query threads spend BLOCKED
        // waiting for locks held by the flush thread. Fine-grained buckets
        // in the 10µs–500ms range where contention shows up.
        let lock_wait_buckets = vec![
            0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5,
        ];
        let query_cache_lock_wait_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_cache_lock_wait_seconds",
                "Time query thread blocks waiting for unified_cache Mutex (contended by flush Phase A/C)",
            )
            .buckets(lock_wait_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let query_cache_hold_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_cache_hold_seconds",
                "Time spent HOLDING unified_cache Mutex during lookup (work done under lock, not wait time)",
            )
            .buckets(lock_wait_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let query_lazy_load_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_lazy_load_seconds",
                "Time ensure_fields_loaded takes per query (lazy bitmap loading from disk)",
            )
            .buckets(lock_wait_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let query_overhead_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_overhead_seconds",
                "Unexplained residual: query_duration - filter - sort - lazy_load - cache_lock_wait (cuts through histogram P50 artifacts)",
            )
            .buckets(lock_wait_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let query_docstore_lock_wait_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_docstore_lock_wait_seconds",
                "Time query thread blocks waiting for docstore RwLock read (contended by flush put_batch write)",
            )
            .buckets(lock_wait_buckets),
            &["index"],
        )
        .unwrap();

        let save_snapshot_buckets = vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0];
        let save_snapshot_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_save_snapshot_seconds",
                "Bitmap snapshot save duration",
            )
            .buckets(save_snapshot_buckets),
            &["index"],
        )
        .unwrap();
        let flush_queue_depth = IntGauge::new(
            "bitdex_flush_queue_depth",
            "Pending MutationOps in the write coalescer channel",
        )
        .unwrap();

        // Phase 2.5: Doc cache — synced from DocCache atomics on each scrape
        let doc_cache_hit_total = IntGaugeVec::new(
            Opts::new("bitdex_doc_cache_hit_total", "Document cache cumulative hits"),
            &["index"],
        )
        .unwrap();
        let doc_cache_miss_total = IntGaugeVec::new(
            Opts::new("bitdex_doc_cache_miss_total", "Document cache cumulative misses"),
            &["index"],
        )
        .unwrap();
        let doc_cache_entries = IntGaugeVec::new(
            Opts::new("bitdex_doc_cache_entries", "Document cache entry count"),
            &["index"],
        )
        .unwrap();
        let doc_cache_bytes = IntGaugeVec::new(
            Opts::new("bitdex_doc_cache_bytes", "Document cache memory bytes"),
            &["index"],
        )
        .unwrap();
        let doc_cache_evictions_total = IntGaugeVec::new(
            Opts::new("bitdex_doc_cache_evictions_total", "Document cache cumulative evictions"),
            &["index"],
        )
        .unwrap();
        let doc_cache_generations = IntGaugeVec::new(
            Opts::new("bitdex_doc_cache_generations", "Document cache active generation count"),
            &["index"],
        )
        .unwrap();
        let doc_cache_backlog = IntGaugeVec::new(
            Opts::new("bitdex_doc_cache_backlog", "Document cache write-through channel backlog"),
            &["index"],
        )
        .unwrap();

        // Phase 2.5: ShardStore ops stub (wired when Phase 1 lands)
        let shardstore_ops_count = IntGaugeVec::new(
            Opts::new("bitdex_shardstore_ops_count", "Pending ops per shard store"),
            &["index", "store"],
        )
        .unwrap();

        // Phase 2.5: PG-Sync observability
        let pgsync_cycle_buckets = vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0];
        let pgsync_cycle_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_pgsync_cycle_seconds",
                "Outbox poller cycle duration",
            )
            .buckets(pgsync_cycle_buckets),
            &["replica"],
        )
        .unwrap();
        let pgsync_rows_fetched_total = IntCounterVec::new(
            Opts::new("bitdex_pgsync_rows_fetched_total", "Total outbox rows fetched from Postgres"),
            &["replica"],
        )
        .unwrap();
        let pgsync_cursor_position = IntGaugeVec::new(
            Opts::new("bitdex_pgsync_cursor_position", "Current outbox cursor position"),
            &["replica"],
        )
        .unwrap();
        let pgsync_errors_total = IntCounterVec::new(
            Opts::new("bitdex_pgsync_errors_total", "Total sync errors (poll failures, WAL read errors)"),
            &["source"],
        )
        .unwrap();

        // V2 sync metrics (unified namespace)
        let sync_cursor_position = IntGaugeVec::new(
            Opts::new("bitdex_sync_cursor_position", "Current sync cursor position"),
            &["source"],
        ).unwrap();
        let sync_max_id = IntGaugeVec::new(
            Opts::new("bitdex_sync_max_id", "Max ops table ID (for lag calculation)"),
            &["source"],
        ).unwrap();
        let sync_lag_rows = IntGaugeVec::new(
            Opts::new("bitdex_sync_lag_rows", "Number of ops rows behind"),
            &["source"],
        ).unwrap();
        let sync_ops_total = IntCounterVec::new(
            Opts::new("bitdex_sync_ops_total", "Total ops received from sync sources"),
            &["source"],
        ).unwrap();
        let sync_wal_bytes = IntGaugeVec::new(
            Opts::new("bitdex_sync_wal_bytes", "Current WAL file size in bytes"),
            &["source"],
        ).unwrap();
        let sync_cycle_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_sync_cycle_duration_seconds",
                "WAL reader cycle processing duration",
            ).buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["source"],
        ).unwrap();
        let sync_wal_pending_bytes = IntGaugeVec::new(
            Opts::new("bitdex_sync_wal_pending_bytes", "Unprocessed WAL bytes (file size - cursor)"),
            &["source"],
        ).unwrap();
        let sync_batch_size = IntGaugeVec::new(
            Opts::new("bitdex_sync_batch_size", "Number of ops in most recent sync batch"),
            &["source"],
        ).unwrap();

        // WAL read-side observability
        let wal_ops_processed_total = IntCounter::new(
            "bitdex_wal_ops_processed_total", "Total ops successfully applied from WAL reader",
        ).unwrap();
        let wal_read_cursor_bytes = IntGauge::new(
            "bitdex_wal_read_cursor_bytes", "WAL reader cursor position in bytes",
        ).unwrap();

        // WAL write-side observability
        let wal_ops_written_total = IntCounter::new(
            "bitdex_wal_ops_written_total", "Total ops written to WAL via POST /ops",
        ).unwrap();
        let wal_last_applied_timestamp_seconds = IntGauge::new(
            "bitdex_wal_last_applied_timestamp_seconds", "Unix epoch of last successful WAL op application",
        ).unwrap();

        // Boot phase breakdown
        let boot_phase_seconds = IntGaugeVec::new(
            Opts::new("bitdex_boot_phase_seconds", "Duration of each boot phase in seconds"),
            &["phase"],
        ).unwrap();

        // Register all metrics
        registry.register(Box::new(alive_documents.clone())).unwrap();
        registry.register(Box::new(slot_high_water.clone())).unwrap();
        registry.register(Box::new(upsert_total.clone())).unwrap();
        registry.register(Box::new(delete_total.clone())).unwrap();
        registry.register(Box::new(query_total.clone())).unwrap();
        registry
            .register(Box::new(query_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(http_response_seconds.clone()))
            .unwrap();
        registry.register(Box::new(query_filter_seconds.clone())).unwrap();
        registry.register(Box::new(query_sort_seconds.clone())).unwrap();
        registry.register(Box::new(query_docs_seconds.clone())).unwrap();
        registry.register(Box::new(query_filter_clause_count.clone())).unwrap();
        registry
            .register(Box::new(cache_hits_total.clone()))
            .unwrap();
        registry
            .register(Box::new(cache_misses_total.clone()))
            .unwrap();
        registry.register(Box::new(cache_inserts_total.clone())).unwrap();
        registry.register(Box::new(cache_updates_total.clone())).unwrap();
        registry.register(Box::new(cache_evictions_total.clone())).unwrap();
        registry.register(Box::new(cache_invalidations_total.clone())).unwrap();
        registry.register(Box::new(cache_entries.clone())).unwrap();
        registry.register(Box::new(cache_bytes.clone())).unwrap();
        registry.register(Box::new(cache_entries_initial.clone())).unwrap();
        registry.register(Box::new(cache_entries_expanded.clone())).unwrap();
        registry.register(Box::new(cache_extensions_total.clone())).unwrap();
        registry.register(Box::new(cache_wall_hits_total.clone())).unwrap();
        registry.register(Box::new(cache_prefetch_total.clone())).unwrap();
        registry
            .register(Box::new(filter_bitmap_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(filter_bitmap_count.clone()))
            .unwrap();
        registry
            .register(Box::new(sort_bitmap_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(slot_bitmap_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(flush_last_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(snapshot_publish_total.clone()))
            .unwrap();
        registry.register(Box::new(process_rss_bytes.clone())).unwrap();
        registry.register(Box::new(process_rss_peak_bytes.clone())).unwrap();
        registry.register(Box::new(jemalloc_allocated_bytes.clone())).unwrap();
        registry.register(Box::new(jemalloc_resident_bytes.clone())).unwrap();
        registry.register(Box::new(startup_duration_seconds.clone())).unwrap();
        registry.register(Box::new(flush_apply_nanos.clone())).unwrap();
        registry.register(Box::new(flush_cache_nanos.clone())).unwrap();
        registry.register(Box::new(flush_phase_a_nanos.clone())).unwrap();
        registry.register(Box::new(flush_phase_c_nanos.clone())).unwrap();
        registry.register(Box::new(flush_cache_cycles_total.clone())).unwrap();
        registry.register(Box::new(flush_publish_nanos.clone())).unwrap();
        registry.register(Box::new(flush_timebucket_nanos.clone())).unwrap();
        registry.register(Box::new(flush_compact_nanos.clone())).unwrap();
        registry.register(Box::new(flush_opslog_nanos.clone())).unwrap();
        registry.register(Box::new(flush_sort_promote_nanos.clone())).unwrap();
        registry.register(Box::new(cache_maint_unique_filter_shapes.clone())).unwrap();
        registry.register(Box::new(cache_maint_sort_work_items.clone())).unwrap();
        registry.register(Box::new(cache_maint_unique_filter_shapes_max.clone())).unwrap();
        registry.register(Box::new(cache_maint_sort_work_items_max.clone())).unwrap();
        registry.register(Box::new(docstore_put_batch_fast_path_total.clone())).unwrap();
        registry.register(Box::new(docstore_put_batch_slow_path_total.clone())).unwrap();
        registry.register(Box::new(docstore_append_tuples_fast_path_total.clone())).unwrap();
        registry.register(Box::new(docstore_append_tuples_slow_path_total.clone())).unwrap();
        registry.register(Box::new(docstore_append_multi_ops_fast_path_total.clone())).unwrap();
        registry.register(Box::new(docstore_append_multi_ops_slow_path_total.clone())).unwrap();
        registry
            .register(Box::new(lazy_load_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(pending_fields.clone()))
            .unwrap();
        registry
            .register(Box::new(eviction_total.clone()))
            .unwrap();
        registry
            .register(Box::new(eviction_resident_values.clone()))
            .unwrap();
        registry.register(Box::new(compaction_total.clone())).unwrap();
        registry.register(Box::new(compaction_duration_seconds.clone())).unwrap();
        registry.register(Box::new(compaction_skipped_total.clone())).unwrap();
        registry.register(Box::new(compact_running.clone())).unwrap();
        registry.register(Box::new(compact_shards_scanned.clone())).unwrap();
        registry.register(Box::new(compact_shards_compacted.clone())).unwrap();
        registry.register(Box::new(compact_shards_skipped.clone())).unwrap();
        registry.register(Box::new(compact_runs_total.clone())).unwrap();
        registry.register(Box::new(compact_duration_seconds.clone())).unwrap();
        registry.register(Box::new(queries_in_flight.clone())).unwrap();
        registry.register(Box::new(queries_in_flight_peak.clone())).unwrap();
        registry.register(Box::new(queries_rejected_total.clone())).unwrap();
        registry.register(Box::new(boundstore_meta_entries.clone())).unwrap();
        registry.register(Box::new(boundstore_tombstones.clone())).unwrap();
        registry.register(Box::new(boundstore_pending_shards.clone())).unwrap();
        registry.register(Box::new(boundstore_disk_bytes.clone())).unwrap();
        registry.register(Box::new(boundstore_shard_loads_total.clone())).unwrap();
        registry.register(Box::new(boundstore_tombstones_created.clone())).unwrap();
        registry.register(Box::new(boundstore_tombstones_cleaned.clone())).unwrap();
        registry.register(Box::new(boundstore_entries_restored.clone())).unwrap();
        registry.register(Box::new(boundstore_bytes_written.clone())).unwrap();
        registry.register(Box::new(boundstore_bytes_read.clone())).unwrap();
        // Phase 2.5
        registry.register(Box::new(docstore_read_seconds.clone())).unwrap();
        registry.register(Box::new(docstore_concurrent_reads.clone())).unwrap();
        registry.register(Box::new(docstore_shard_file_read_seconds.clone())).unwrap();
        registry.register(Box::new(docstore_shard_decode_seconds.clone())).unwrap();
        registry.register(Box::new(docstore_batch_unique_shards.clone())).unwrap();
        registry.register(Box::new(query_spawn_blocking_dispatch_seconds.clone())).unwrap();
        registry.register(Box::new(query_doc_cache_probe_seconds.clone())).unwrap();
        registry.register(Box::new(query_doc_disk_fetch_seconds.clone())).unwrap();
        registry.register(Box::new(query_doc_format_seconds.clone())).unwrap();
        registry.register(Box::new(query_docs_path_total.clone())).unwrap();
        registry.register(Box::new(docstore_read_path_total.clone())).unwrap();
        registry.register(Box::new(query_tokio_return_delay_seconds.clone())).unwrap();
        registry.register(Box::new(query_response_build_seconds.clone())).unwrap();
        registry.register(Box::new(query_cache_lock_wait_seconds.clone())).unwrap();
        registry.register(Box::new(query_cache_hold_seconds.clone())).unwrap();
        registry.register(Box::new(query_lazy_load_seconds.clone())).unwrap();
        registry.register(Box::new(query_overhead_seconds.clone())).unwrap();
        registry.register(Box::new(query_docstore_lock_wait_seconds.clone())).unwrap();
        registry.register(Box::new(save_snapshot_seconds.clone())).unwrap();
        registry.register(Box::new(flush_queue_depth.clone())).unwrap();
        registry.register(Box::new(doc_cache_hit_total.clone())).unwrap();
        registry.register(Box::new(doc_cache_miss_total.clone())).unwrap();
        registry.register(Box::new(doc_cache_entries.clone())).unwrap();
        registry.register(Box::new(doc_cache_bytes.clone())).unwrap();
        registry.register(Box::new(doc_cache_evictions_total.clone())).unwrap();
        registry.register(Box::new(doc_cache_generations.clone())).unwrap();
        registry.register(Box::new(doc_cache_backlog.clone())).unwrap();
        registry.register(Box::new(shardstore_ops_count.clone())).unwrap();
        registry.register(Box::new(pgsync_cycle_seconds.clone())).unwrap();
        registry.register(Box::new(pgsync_rows_fetched_total.clone())).unwrap();
        registry.register(Box::new(pgsync_cursor_position.clone())).unwrap();
        registry.register(Box::new(pgsync_errors_total.clone())).unwrap();
        registry.register(Box::new(sync_cursor_position.clone())).unwrap();
        registry.register(Box::new(sync_max_id.clone())).unwrap();
        registry.register(Box::new(sync_lag_rows.clone())).unwrap();
        registry.register(Box::new(sync_ops_total.clone())).unwrap();
        registry.register(Box::new(sync_wal_bytes.clone())).unwrap();
        registry.register(Box::new(sync_cycle_duration_seconds.clone())).unwrap();
        registry.register(Box::new(sync_wal_pending_bytes.clone())).unwrap();
        registry.register(Box::new(sync_batch_size.clone())).unwrap();
        registry.register(Box::new(wal_ops_processed_total.clone())).unwrap();
        registry.register(Box::new(wal_read_cursor_bytes.clone())).unwrap();
        registry.register(Box::new(wal_ops_written_total.clone())).unwrap();
        registry.register(Box::new(wal_last_applied_timestamp_seconds.clone())).unwrap();
        registry.register(Box::new(boot_phase_seconds.clone())).unwrap();

        Self {
            registry,
            alive_documents,
            slot_high_water,
            upsert_total,
            delete_total,
            query_total,
            query_duration_seconds,
            http_response_seconds,
            query_filter_seconds,
            query_sort_seconds,
            query_docs_seconds,
            query_filter_clause_count,
            cache_hits_total,
            cache_misses_total,
            cache_inserts_total,
            cache_updates_total,
            cache_evictions_total,
            cache_invalidations_total,
            cache_entries,
            cache_bytes,
            cache_entries_initial,
            cache_entries_expanded,
            cache_extensions_total,
            cache_wall_hits_total,
            cache_prefetch_total,
            filter_bitmap_bytes,
            filter_bitmap_count,
            sort_bitmap_bytes,
            slot_bitmap_bytes,
            flush_last_duration_seconds,
            snapshot_publish_total,
            process_rss_bytes,
            process_rss_peak_bytes,
            jemalloc_allocated_bytes,
            jemalloc_resident_bytes,
            startup_duration_seconds,
            flush_apply_nanos,
            flush_cache_nanos,
            flush_phase_a_nanos,
            flush_phase_c_nanos,
            flush_cache_cycles_total,
            flush_publish_nanos,
            flush_timebucket_nanos,
            flush_compact_nanos,
            flush_opslog_nanos,
            flush_sort_promote_nanos,
            cache_maint_unique_filter_shapes,
            cache_maint_sort_work_items,
            cache_maint_unique_filter_shapes_max,
            cache_maint_sort_work_items_max,
            docstore_put_batch_fast_path_total,
            docstore_put_batch_slow_path_total,
            docstore_append_tuples_fast_path_total,
            docstore_append_tuples_slow_path_total,
            docstore_append_multi_ops_fast_path_total,
            docstore_append_multi_ops_slow_path_total,
            lazy_load_duration_seconds,
            pending_fields,
            eviction_total,
            eviction_resident_values,
            compaction_total,
            compaction_duration_seconds,
            compaction_skipped_total,
            compact_running,
            compact_shards_scanned,
            compact_shards_compacted,
            compact_shards_skipped,
            compact_runs_total,
            compact_duration_seconds,
            queries_in_flight,
            queries_in_flight_peak,
            queries_rejected_total,
            boundstore_meta_entries,
            boundstore_tombstones,
            boundstore_pending_shards,
            boundstore_disk_bytes,
            boundstore_shard_loads_total,
            boundstore_tombstones_created,
            boundstore_tombstones_cleaned,
            boundstore_entries_restored,
            boundstore_bytes_written,
            boundstore_bytes_read,
            // Phase 2.5
            docstore_read_seconds,
            docstore_concurrent_reads,
            docstore_shard_file_read_seconds,
            docstore_shard_decode_seconds,
            docstore_batch_unique_shards,
            query_spawn_blocking_dispatch_seconds,
            query_doc_cache_probe_seconds,
            query_doc_disk_fetch_seconds,
            query_doc_format_seconds,
            query_docs_path_total,
            docstore_read_path_total,
            query_tokio_return_delay_seconds,
            query_response_build_seconds,
            query_cache_lock_wait_seconds,
            query_cache_hold_seconds,
            query_lazy_load_seconds,
            query_overhead_seconds,
            query_docstore_lock_wait_seconds,
            save_snapshot_seconds,
            flush_queue_depth,
            doc_cache_hit_total,
            doc_cache_miss_total,
            doc_cache_entries,
            doc_cache_bytes,
            doc_cache_evictions_total,
            doc_cache_generations,
            doc_cache_backlog,
            shardstore_ops_count,
            pgsync_cycle_seconds,
            pgsync_rows_fetched_total,
            pgsync_cursor_position,
            pgsync_errors_total,
            sync_cursor_position,
            sync_max_id,
            sync_lag_rows,
            sync_ops_total,
            sync_wal_bytes,
            sync_cycle_duration_seconds,
            sync_wal_pending_bytes,
            sync_batch_size,
            wal_ops_processed_total,
            wal_read_cursor_bytes,
            wal_ops_written_total,
            wal_last_applied_timestamp_seconds,
            boot_phase_seconds,
        }
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn gather(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}
