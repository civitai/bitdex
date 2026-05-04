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
    pub flush_publish_nanos: IntGaugeVec,
    pub flush_timebucket_nanos: IntGaugeVec,
    pub flush_compact_nanos: IntGaugeVec,
    pub flush_opslog_nanos: IntGaugeVec,
    pub flush_sort_promote_nanos: IntGaugeVec,
    pub cache_maint_unique_filter_shapes: IntGaugeVec,
    pub cache_maint_sort_work_items: IntGaugeVec,
    pub cache_maint_unique_filter_shapes_max: IntGaugeVec,
    pub cache_maint_sort_work_items_max: IntGaugeVec,

    // -- Async cache worker (Phase 1a) --
    pub cache_worker_queue_depth: IntGaugeVec,
    pub cache_worker_cycle_nanos: IntGaugeVec,
    pub cache_worker_cycle_seconds: HistogramVec,
    pub cache_worker_items_coalesced_total: IntGaugeVec,
    pub cache_worker_drops_total: IntGaugeVec,
    pub cache_worker_over_budget_total: IntGaugeVec,
    pub cache_backpressure_invalidations_total: IntGaugeVec,
    pub cache_worker_cycles_total: IntGaugeVec,
    /// Number of cache entries currently flagged `needs_rebuild=true`. Sampled
    /// at scrape time. Gives Justin the live "shed backlog" depth the cache
    /// is carrying — matching `marked_for_rebuild_total` minus
    /// `rebuild_completed_total` (approximately, modulo evictions).
    pub cache_entries_needs_rebuild: IntGaugeVec,
    /// Cumulative count of cache entries marked for rebuild, attributed by
    /// reason. Replaces the single-bucket semantics of `over_budget_total`
    /// with reason labels so we can see WHY entries are being shed.
    pub cache_marked_for_rebuild_total: IntGaugeVec,
    /// Cumulative count of cache rebuilds that completed (entry replaced via
    /// `store()` while the prior entry had `needs_rebuild=true`).
    pub cache_rebuild_completed_total: IntGaugeVec,

    pub docstore_put_batch_fast_path_total: IntGaugeVec,
    pub docstore_put_batch_slow_path_total: IntGaugeVec,

    // -- Compound-clause cache maintenance observability (Commit 1, A3) --
    /// Per-entry compound-clause evaluation latency in microseconds.
    /// Buckets span 1μs (trivial) → 10ms (budget limit). Populated from
    /// Commit 3 (B2) once native FilterClause eval is wired.
    // TODO(plan A5): add Prom alert in monitoring repo: cache_entries_needs_rebuild > 0 for > 5m
    pub cache_maint_compound_eval_us: HistogramVec,
    /// Current count of cache entries containing a `__prefilter` clause.
    /// Sampled at scrape time alongside other cache_* gauges.
    pub cache_substituted_entries: IntGaugeVec,
    /// Conservative skip counter: incremented when slot_matches_clause hits a
    /// fall-through arm rather than explicitly evaluating the clause. Buckets by
    /// reason so we can distinguish compound_and, compound_or, compound_not,
    /// isnull, isnotnull, unknown_op. Must read zero in prod after B2 ships.
    pub cache_maint_conservative_total: IntCounterVec,
    /// Incremented when an `In`-arm string value can't be resolved to a u64 key
    /// (requires StringMaps/FieldDictionary not yet threaded). Goes to zero after B2.
    pub cache_maint_string_lookup_miss_total: IntCounter,
    /// Current count of entries with at least one canonical clause whose op is
    /// one of: and, or, not, isnull, isnotnull. Sampled at scrape time.
    pub cache_entries_compound_clause_count: IntGaugeVec,

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
    /// Counts range scans rejected by max_range_scan_values cap, labeled by field name.
    pub range_scan_rejected_total: IntCounterVec,

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
    /// Per-phase timing inside the query handler. Phases:
    ///   to_handler  — middleware enter → handler entered (tokio task scheduling)
    ///   to_engine   — handler entered → engine block_in_place enter
    ///   engine      — engine block_in_place duration
    ///   doc_fetch   — engine done → spawn_blocking doc fetch return
    ///   to_response — doc fetch done → middleware exit
    pub http_handler_phase_seconds: HistogramVec,

    // -- Phase 2.5: DocStore I/O observability --
    pub docstore_read_seconds: HistogramVec,
    pub docstore_concurrent_reads: IntGauge,
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

    // -- Shard rewrite attribution (sourced from shard_store atomics at scrape time) --
    pub shard_rewrites_total: IntGaugeVec,

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
    /// End-to-end latency of POST /ops WAL append (lock acquisition + write + fsync).
    pub wal_append_duration_seconds: Histogram,

    // -- queryOpSet fan-out cost (issue #60) --
    /// Histogram of result.ids size per apply_query_op_set call. Buckets cover the
    /// full observed range from postId-eq narrow matches (1) to wide nsfwLevel-style
    /// matches (10M+). Used to size the BITDEX_QUERY_OP_SET_MAX_FANOUT cap from real
    /// prod data instead of guessing.
    pub query_op_set_fanout_size: HistogramVec,
    /// Counter incremented when a queryOpSet is rejected for exceeding the fan-out
    /// cap. Label `reason="fanout_too_wide"`. Alert when rate > 0 — every rejection
    /// is a missed mutation (data drift) until #62 (paginate-instead-of-skip) lands.
    pub query_op_set_rejected_total: IntCounterVec,
    /// Counter of total slot mutations applied via queryOpSet fan-out. Lets us
    /// distinguish narrow (postId-eq, ~1 slot) from wide (nsfwLevel-eq, millions)
    /// at the work-unit level rather than the API-call level.
    pub query_op_set_applied_slots_total: IntCounterVec,

    // -- 11c CPU floor attribution (2026-04-30) --
    /// Wall-clock duration of `apply_ops_batch` per WAL-reader batch. Sum × rate
    /// = CPU spent in op apply. Mission status v1.0.178 cites 11c steady-state
    /// floor in server mode; this metric attributes the WAL reader's contribution.
    pub wal_apply_batch_seconds: HistogramVec,
    /// Wall-clock duration of one `BitmapMemoryCache::scan_tick`. Background
    /// scanner thread runs every interval_ms; this histogram shows how much CPU
    /// each tick consumes (postId full-walk at 23M distinct values is the
    /// suspected ~1c contributor).
    pub bitmap_mem_scan_tick_seconds: HistogramVec,

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
            .buckets(http_buckets.clone()),
            &["method", "path"],
        )
        .unwrap();

        let http_handler_phase_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_http_handler_phase_seconds",
                "Per-phase wall-clock inside the query handler. Phase label: to_handler|to_engine|engine|doc_fetch|to_response",
            )
            .buckets(vec![
                0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005,
                0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0,
            ]),
            &["phase"],
        )
        .unwrap();

        let phase_buckets = vec![0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];
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
            Opts::new("bitdex_flush_cache_nanos", "Last flush cache maintenance duration in nanoseconds"),
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
        let cache_worker_queue_depth = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_worker_queue_depth",
                "Number of pending CacheWorkItems in the async cache worker channel.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_worker_cycle_nanos = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_worker_cycle_nanos",
                "Duration of the most recent async cache worker cycle in nanoseconds.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_worker_cycle_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_cache_worker_cycle_seconds",
                "Wall-clock duration of each async cache worker cycle, in seconds.",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
            &["index"],
        )
        .unwrap();
        let cache_worker_items_coalesced_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_worker_items_coalesced_total",
                "Cumulative count of CacheWorkItems merged by the coalescing step. Coalescing ratio = items_coalesced / cycles.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_worker_drops_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_worker_drops_total",
                "Number of times the cache worker backlog exceeded the drop limit and fell back to invalidation.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_worker_over_budget_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_worker_over_budget_total",
                "Number of cache entries that timed out during worker evaluation and were marked for rebuild.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_backpressure_invalidations_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_backpressure_invalidations_total",
                "Number of times the flush thread's try_send to the cache worker failed (channel full) and fell back to invalidation.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_worker_cycles_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_worker_cycles_total",
                "Total number of completed async cache worker cycles.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_entries_needs_rebuild = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_entries_needs_rebuild",
                "Number of UnifiedCache entries currently flagged needs_rebuild=true. Live backlog of stale entries that next access will treat as a miss.",
            ),
            &["index"],
        )
        .unwrap();
        let cache_marked_for_rebuild_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_marked_for_rebuild_total",
                "Cumulative count of cache entries marked for rebuild, labeled by the proximate reason.",
            ),
            &["index", "reason"],
        )
        .unwrap();
        let cache_rebuild_completed_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_rebuild_completed_total",
                "Cumulative count of cache rebuilds that completed (stale entry replaced via store()).",
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
        let range_scan_rejected_total = IntCounterVec::new(
            Opts::new(
                "bitdex_range_scan_rejected_total",
                "Range scan queries rejected by max_range_scan_values cap",
            ),
            &["field"],
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

        // Shard rewrite attribution — sourced from shard_store atomics at scrape time.
        // Labels: source = "compact" | "cold_create" | "snapshot"
        let shard_rewrites_total = IntGaugeVec::new(
            Opts::new(
                "bitdex_shard_rewrites_total",
                "Total atomic shard file rewrites by source (compact/cold_create/snapshot)",
            ),
            &["source"],
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
        let wal_append_buckets = vec![0.00001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0];
        let wal_append_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "bitdex_wal_append_duration_seconds",
                "End-to-end duration of POST /ops WAL append (lock + write + fsync)",
            )
            .buckets(wal_append_buckets),
        )
        .unwrap();

        // queryOpSet fan-out cost (issue #60).
        // Histogram buckets span the full observed range:
        // narrow (postId-eq, ~1 slot) → moderate (modelVersionIds, 1K-100K) → wide
        // (nsfwLevel-eq, 10M+). Powers of 10 with a 22.9M anchor for the local
        // nsfwLevel=1 size we measured during the OOM repro work.
        let query_op_set_fanout_buckets = vec![
            1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0, 10_000_000.0, 100_000_000.0,
        ];
        let query_op_set_fanout_size = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_query_op_set_fanout_size",
                "Number of slots matched by a queryOpSet's filter, observed before per-slot apply",
            )
            .buckets(query_op_set_fanout_buckets),
            &["index"],
        )
        .unwrap();
        let query_op_set_rejected_total = IntCounterVec::new(
            Opts::new(
                "bitdex_query_op_set_rejected_total",
                "queryOpSet ops rejected before per-slot apply (e.g. fan-out exceeded cap)",
            ),
            &["index", "reason"],
        )
        .unwrap();
        let query_op_set_applied_slots_total = IntCounterVec::new(
            Opts::new(
                "bitdex_query_op_set_applied_slots_total",
                "Total slot mutations applied via queryOpSet fan-out (sum of result.ids sizes)",
            ),
            &["index"],
        )
        .unwrap();

        // 11c CPU floor attribution (2026-04-30): WAL apply per-batch + mem-scanner tick.
        // Buckets cover sub-ms (WAL batch fast path) through multi-second (postId
        // full-walk on the 23M-distinct-value bitmap).
        let apply_seconds_buckets = vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ];
        let wal_apply_batch_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_wal_apply_batch_seconds",
                "Wall-clock duration of apply_ops_batch per WAL-reader batch",
            )
            .buckets(apply_seconds_buckets.clone()),
            &["index"],
        )
        .unwrap();
        let bitmap_mem_scan_tick_seconds = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_bitmap_mem_scan_tick_seconds",
                "Wall-clock duration of one BitmapMemoryCache::scan_tick (background scanner thread)",
            )
            .buckets(apply_seconds_buckets),
            &["index"],
        )
        .unwrap();

        // Boot phase breakdown
        let boot_phase_seconds = IntGaugeVec::new(
            Opts::new("bitdex_boot_phase_seconds", "Duration of each boot phase in seconds"),
            &["phase"],
        ).unwrap();

        // Compound-clause cache maintenance observability (Commit 1, A3)
        let cache_maint_compound_eval_us = HistogramVec::new(
            HistogramOpts::new(
                "bitdex_cache_maint_compound_eval_us",
                "Per-entry compound-clause evaluation latency (microseconds). Populated after B2.",
            )
            .buckets(vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10_000.0]),
            &["index"],
        ).unwrap();

        let cache_substituted_entries = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_substituted_entries",
                "Cache entries containing a __prefilter clause (sampled at scrape)",
            ),
            &["index"],
        ).unwrap();

        let cache_maint_conservative_total = IntCounterVec::new(
            Opts::new(
                "bitdex_cache_maint_conservative_total",
                "Conservative skip count by reason: compound_and|compound_or|compound_not|isnull|isnotnull|unknown_op. Must be 0 in prod after B2.",
            ),
            &["reason"],
        ).unwrap();

        let cache_maint_string_lookup_miss_total = IntCounter::new(
            "bitdex_cache_maint_string_lookup_miss_total",
            "In-arm string value unresolvable to u64 key. Goes to zero after B2 threads StringMaps.",
        ).unwrap();

        let cache_entries_compound_clause_count = IntGaugeVec::new(
            Opts::new(
                "bitdex_cache_entries_compound_clause_count",
                "Cache entries with at least one compound clause (and/or/not/isnull/isnotnull). Sampled at scrape.",
            ),
            &["index"],
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
        registry
            .register(Box::new(http_handler_phase_seconds.clone()))
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
        registry.register(Box::new(flush_publish_nanos.clone())).unwrap();
        registry.register(Box::new(flush_timebucket_nanos.clone())).unwrap();
        registry.register(Box::new(flush_compact_nanos.clone())).unwrap();
        registry.register(Box::new(flush_opslog_nanos.clone())).unwrap();
        registry.register(Box::new(flush_sort_promote_nanos.clone())).unwrap();
        registry.register(Box::new(cache_maint_unique_filter_shapes.clone())).unwrap();
        registry.register(Box::new(cache_maint_sort_work_items.clone())).unwrap();
        registry.register(Box::new(cache_maint_unique_filter_shapes_max.clone())).unwrap();
        registry.register(Box::new(cache_maint_sort_work_items_max.clone())).unwrap();
        registry.register(Box::new(cache_worker_queue_depth.clone())).unwrap();
        registry.register(Box::new(cache_worker_cycle_nanos.clone())).unwrap();
        registry.register(Box::new(cache_worker_cycle_seconds.clone())).unwrap();
        registry.register(Box::new(cache_worker_items_coalesced_total.clone())).unwrap();
        registry.register(Box::new(cache_worker_drops_total.clone())).unwrap();
        registry.register(Box::new(cache_worker_over_budget_total.clone())).unwrap();
        registry.register(Box::new(cache_backpressure_invalidations_total.clone())).unwrap();
        registry.register(Box::new(cache_worker_cycles_total.clone())).unwrap();
        registry.register(Box::new(cache_entries_needs_rebuild.clone())).unwrap();
        registry.register(Box::new(cache_marked_for_rebuild_total.clone())).unwrap();
        registry.register(Box::new(cache_rebuild_completed_total.clone())).unwrap();
        registry.register(Box::new(docstore_put_batch_fast_path_total.clone())).unwrap();
        registry.register(Box::new(docstore_put_batch_slow_path_total.clone())).unwrap();
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
        registry.register(Box::new(range_scan_rejected_total.clone())).unwrap();
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
        registry.register(Box::new(shard_rewrites_total.clone())).unwrap();
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
        registry.register(Box::new(wal_append_duration_seconds.clone())).unwrap();
        registry.register(Box::new(query_op_set_fanout_size.clone())).unwrap();
        registry.register(Box::new(wal_apply_batch_seconds.clone())).unwrap();
        registry.register(Box::new(bitmap_mem_scan_tick_seconds.clone())).unwrap();
        registry.register(Box::new(query_op_set_rejected_total.clone())).unwrap();
        registry.register(Box::new(query_op_set_applied_slots_total.clone())).unwrap();
        registry.register(Box::new(boot_phase_seconds.clone())).unwrap();
        registry.register(Box::new(cache_maint_compound_eval_us.clone())).unwrap();
        registry.register(Box::new(cache_substituted_entries.clone())).unwrap();
        registry.register(Box::new(cache_maint_conservative_total.clone())).unwrap();
        registry.register(Box::new(cache_maint_string_lookup_miss_total.clone())).unwrap();
        registry.register(Box::new(cache_entries_compound_clause_count.clone())).unwrap();

        Self {
            registry,
            alive_documents,
            slot_high_water,
            upsert_total,
            delete_total,
            query_total,
            query_duration_seconds,
            http_response_seconds,
            http_handler_phase_seconds,
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
            flush_publish_nanos,
            flush_timebucket_nanos,
            flush_compact_nanos,
            flush_opslog_nanos,
            flush_sort_promote_nanos,
            cache_maint_unique_filter_shapes,
            cache_maint_sort_work_items,
            cache_maint_unique_filter_shapes_max,
            cache_maint_sort_work_items_max,
            cache_worker_queue_depth,
            cache_worker_cycle_nanos,
            cache_worker_cycle_seconds,
            cache_worker_items_coalesced_total,
            cache_worker_drops_total,
            cache_worker_over_budget_total,
            cache_backpressure_invalidations_total,
            cache_worker_cycles_total,
            cache_entries_needs_rebuild,
            cache_marked_for_rebuild_total,
            cache_rebuild_completed_total,
            docstore_put_batch_fast_path_total,
            docstore_put_batch_slow_path_total,
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
            range_scan_rejected_total,
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
            shard_rewrites_total,
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
            wal_append_duration_seconds,
            query_op_set_fanout_size,
            query_op_set_rejected_total,
            query_op_set_applied_slots_total,
            wal_apply_batch_seconds,
            bitmap_mem_scan_tick_seconds,
            boot_phase_seconds,
            cache_maint_compound_eval_us,
            cache_substituted_entries,
            cache_maint_conservative_total,
            cache_maint_string_lookup_miss_total,
            cache_entries_compound_clause_count,
        }
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn gather(&self) -> String {
        // Sync shard rewrite atomics into gauges at scrape time.
        // These are global monotonic counters written by write_shard_file_atomic().
        use std::sync::atomic::Ordering::Relaxed;
        self.shard_rewrites_total
            .with_label_values(&["compact"])
            .set(crate::shard_store::SHARD_REWRITES_COMPACT.load(Relaxed) as i64);
        self.shard_rewrites_total
            .with_label_values(&["cold_create"])
            .set(crate::shard_store::SHARD_REWRITES_COLD.load(Relaxed) as i64);
        self.shard_rewrites_total
            .with_label_values(&["snapshot"])
            .set(crate::shard_store::SHARD_REWRITES_SNAPSHOT.load(Relaxed) as i64);

        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}
