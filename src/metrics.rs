//! Prometheus metrics for BitDex.
//!
//! All metrics are registered in a custom `Registry` and exposed via
//! `gather_metrics()` which returns the Prometheus text exposition format.
//! Gauges for bitmap memory, cache stats, and document counts are refreshed
//! on each scrape (collect-on-scrape pattern).

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
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

    // -- Write pipeline --
    pub flush_last_duration_seconds: IntGaugeVec,
    pub snapshot_publish_total: IntGaugeVec,

    // -- Tier 2: Lazy loading --
    pub lazy_load_duration_seconds: HistogramVec,
    pub pending_fields: IntGaugeVec,

    // -- Eviction --
    pub eviction_total: IntGaugeVec,
    pub eviction_resident_values: IntGaugeVec,

    // -- Shard compaction --
    pub compaction_total: IntCounterVec,
    pub compaction_duration_seconds: HistogramVec,
    pub compaction_skipped_total: IntCounterVec,

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

        let query_buckets = vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0,
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
        let compaction_skipped_total = IntCounterVec::new(
            Opts::new("bitdex_compaction_skipped_total", "Compactions skipped (channel full)"),
            &["index"],
        )
        .unwrap();

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

        // Register all metrics
        registry.register(Box::new(alive_documents.clone())).unwrap();
        registry.register(Box::new(slot_high_water.clone())).unwrap();
        registry.register(Box::new(upsert_total.clone())).unwrap();
        registry.register(Box::new(delete_total.clone())).unwrap();
        registry.register(Box::new(query_total.clone())).unwrap();
        registry
            .register(Box::new(query_duration_seconds.clone()))
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

        Self {
            registry,
            alive_documents,
            slot_high_water,
            upsert_total,
            delete_total,
            query_total,
            query_duration_seconds,
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
            lazy_load_duration_seconds,
            pending_fields,
            eviction_total,
            eviction_resident_values,
            compaction_total,
            compaction_duration_seconds,
            compaction_skipped_total,
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
