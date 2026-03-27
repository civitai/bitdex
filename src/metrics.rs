//! Prometheus metrics for BitDex.
//!
//! All metrics are registered in a custom `Registry` and exposed via
//! `gather_metrics()` which returns the Prometheus text exposition format.
//! Gauges for bitmap memory, cache stats, and document counts are refreshed
//! on each scrape (collect-on-scrape pattern).

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
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

    // -- Write pipeline --
    pub flush_last_duration_seconds: IntGaugeVec,
    pub snapshot_publish_total: IntGaugeVec,
    // -- Flush phase timing --
    pub flush_apply_nanos: IntGaugeVec,
    pub flush_cache_nanos: IntGaugeVec,
    pub flush_publish_nanos: IntGaugeVec,
    pub flush_timebucket_nanos: IntGaugeVec,
    pub flush_compact_nanos: IntGaugeVec,

    // -- Tier 2: Lazy loading --
    pub lazy_load_duration_seconds: HistogramVec,
    pub pending_fields: IntGaugeVec,

    // -- Eviction --
    pub eviction_total: IntGaugeVec,
    pub eviction_resident_values: IntGaugeVec,

    // -- Shard compaction --
    pub compaction_total: IntCounterVec,
    pub compaction_duration_seconds: HistogramVec,
    pub compaction_skipped_total: IntGaugeVec,

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

    // V2 sync metrics (unified namespace with source label)
    pub sync_cursor_position: IntGaugeVec,
    pub sync_max_id: IntGaugeVec,
    pub sync_lag_rows: IntGaugeVec,
    pub sync_ops_total: IntCounterVec,
    pub sync_wal_bytes: IntGaugeVec,
    pub sync_cycle_duration_seconds: HistogramVec,
    pub sync_wal_pending_bytes: IntGaugeVec,
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
        registry.register(Box::new(flush_apply_nanos.clone())).unwrap();
        registry.register(Box::new(flush_cache_nanos.clone())).unwrap();
        registry.register(Box::new(flush_publish_nanos.clone())).unwrap();
        registry.register(Box::new(flush_timebucket_nanos.clone())).unwrap();
        registry.register(Box::new(flush_compact_nanos.clone())).unwrap();
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
        registry.register(Box::new(sync_cursor_position.clone())).unwrap();
        registry.register(Box::new(sync_max_id.clone())).unwrap();
        registry.register(Box::new(sync_lag_rows.clone())).unwrap();
        registry.register(Box::new(sync_ops_total.clone())).unwrap();
        registry.register(Box::new(sync_wal_bytes.clone())).unwrap();
        registry.register(Box::new(sync_cycle_duration_seconds.clone())).unwrap();
        registry.register(Box::new(sync_wal_pending_bytes.clone())).unwrap();

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
            flush_apply_nanos,
            flush_cache_nanos,
            flush_publish_nanos,
            flush_timebucket_nanos,
            flush_compact_nanos,
            lazy_load_duration_seconds,
            pending_fields,
            eviction_total,
            eviction_resident_values,
            compaction_total,
            compaction_duration_seconds,
            compaction_skipped_total,
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
            sync_cursor_position,
            sync_max_id,
            sync_lag_rows,
            sync_ops_total,
            sync_wal_bytes,
            sync_cycle_duration_seconds,
            sync_wal_pending_bytes,
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
