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

    // -- Cache --
    pub cache_hits_total: IntGaugeVec,
    pub cache_misses_total: IntGaugeVec,
    pub cache_entries: IntGaugeVec,
    pub cache_bytes: IntGaugeVec,
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
            .register(Box::new(cache_hits_total.clone()))
            .unwrap();
        registry
            .register(Box::new(cache_misses_total.clone()))
            .unwrap();
        registry.register(Box::new(cache_entries.clone())).unwrap();
        registry.register(Box::new(cache_bytes.clone())).unwrap();
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

        Self {
            registry,
            alive_documents,
            slot_high_water,
            upsert_total,
            delete_total,
            query_total,
            query_duration_seconds,
            cache_hits_total,
            cache_misses_total,
            cache_entries,
            cache_bytes,
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
