# Prometheus Metrics Plan

## Current State

No Prometheus instrumentation exists. No `prometheus` crate, no `/metrics` endpoint, no metric recording anywhere in the codebase.

The `/api/indexes/{name}/stats` endpoint returns a JSON blob with some useful data (alive count, cache entries/bytes/hits/misses, bound cache stats), but it's a pull-on-demand JSON API — not scrapable by Prometheus.

### Existing Internal APIs (already computed, just not exposed)

| Method | Returns |
|--------|---------|
| `engine.alive_count()` | Live document count |
| `engine.slot_counter()` | High-water slot ID |
| `engine.unified_cache_stats()` | entries, hits, misses, memory_bytes |
| `engine.bound_cache_stats()` | bound entries/bytes, meta-index entries/bytes |
| `engine.bitmap_memory_report()` | slot/filter/sort bytes, per-field breakdowns |

---

## Covered by Kubernetes / cAdvisor

These do NOT need BitDex instrumentation — they come free from the container runtime:

- RSS / memory usage
- CPU usage
- Network I/O
- Pod restarts / OOMKills
- Disk usage
- Container uptime

---

## Tier 1 — Must Have

BitDex-specific metrics that are not observable from outside the process.

### Document Lifecycle

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `bitdex_alive_documents` | Gauge | `index` | Number of live (non-deleted) documents |
| `bitdex_slot_high_water` | Gauge | `index` | High-water slot counter — capacity planning signal |
| `bitdex_upsert_total` | Counter | `index` | Total upsert operations |
| `bitdex_delete_total` | Counter | `index` | Total delete operations |

### Query Performance

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `bitdex_query_total` | Counter | `index` | Total queries served |
| `bitdex_query_duration_seconds` | Histogram | `index` | Query latency distribution. Buckets: 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0 |

### Cache

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `bitdex_cache_hits_total` | Counter | `index` | Unified cache hits |
| `bitdex_cache_misses_total` | Counter | `index` | Unified cache misses |
| `bitdex_cache_entries` | Gauge | `index` | Current unified cache entry count |
| `bitdex_cache_bytes` | Gauge | `index` | Current unified cache memory usage in bytes |
| `bitdex_bound_cache_entries` | Gauge | `index` | Bound cache entry count |
| `bitdex_bound_cache_bytes` | Gauge | `index` | Bound cache memory usage in bytes |
| `bitdex_meta_index_entries` | Gauge | `index` | Meta-index entry count |
| `bitdex_meta_index_bytes` | Gauge | `index` | Meta-index memory usage in bytes |

### Bitmap Memory

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `bitdex_filter_bitmap_bytes` | Gauge | `index`, `field` | Filter bitmap memory per field (lets us see tagIds dominating at 79%) |
| `bitdex_sort_bitmap_bytes` | Gauge | `index`, `field` | Sort layer bitmap memory per field |
| `bitdex_slot_bitmap_bytes` | Gauge | `index` | Alive/slot bitmap memory |
| `bitdex_filter_bitmap_count` | Gauge | `index`, `field` | Number of distinct bitmaps per filter field |

### Write Pipeline

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `bitdex_flush_duration_seconds` | Histogram | `index` | Time spent in flush loop (apply mutations + publish snapshot). Buckets: 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5 |
| `bitdex_snapshot_publish_total` | Counter | `index` | Number of ArcSwap snapshot publishes |

---

## Tier 2 — Very Useful

Operational visibility that's valuable but not critical for day-one deploy.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `bitdex_lazy_load_duration_seconds` | Histogram | `index`, `field` | Time to lazy-load a field's bitmaps on first query |
| `bitdex_pending_fields` | Gauge | `index` | Number of filter+sort fields not yet loaded into memory |
| `bitdex_write_channel_depth` | Gauge | `index` | Crossbeam channel pending mutations — backpressure signal |
| `bitdex_load_status` | Gauge | `index`, `state` | 1.0 for current state (Loading, Saving, Complete) — enum-style gauge |
| `bitdex_docstore_compactions_total` | Counter | `index` | Number of docstore compaction cycles |

---

## Implementation

### Dependencies

Add to `Cargo.toml`:

```toml
prometheus = { version = "0.13", features = ["process"] }
```

The `process` feature gives us `process_cpu_seconds_total`, `process_resident_memory_bytes`, etc. for free — useful as a cross-check against cAdvisor even though K8s covers it.

### New File: `src/metrics.rs`

Lazy-static registry with all metrics defined. Exposed via a public `METRICS` struct or individual statics. Provides:

- `register()` — called once at startup
- `gather()` — returns the text exposition format string for the `/metrics` endpoint

### Endpoint: `GET /metrics`

Added to the axum router (no `{name}` path param — global endpoint). Returns `text/plain; version=0.0.4` with the full Prometheus text exposition.

### Instrumentation Points

| Where | What |
|-------|------|
| `handle_query()` in `server.rs` | Observe `query_duration_seconds`, increment `query_total` |
| `handle_upsert()` in `server.rs` | Increment `upsert_total` |
| `handle_delete_docs()` in `server.rs` | Increment `delete_total` |
| Flush loop in `write_coalescer.rs` | Observe `flush_duration_seconds`, increment `snapshot_publish_total` |
| `ensure_fields_loaded()` in `concurrent_engine.rs` | Observe `lazy_load_duration_seconds` |

### Gauge Refresh Strategy

Gauge values (alive count, cache stats, bitmap memory) are **collected on scrape** — the `/metrics` handler reads the current engine state and sets gauge values before rendering. This avoids a background tick thread and ensures values are always fresh at scrape time.

Per-field bitmap gauges iterate `per_field_bytes()` on each scrape. At our field count (~10 filter fields, ~5 sort fields) this is negligible.

### Histogram Bucket Choices

- **Query latency**: Buckets span 0.1ms to 10s. Our p50 is sub-1ms at 104M, worst case (broad sort) can hit 15-27s, so we cover the range. The 10s+ bucket catches pathological cases.
- **Flush latency**: Tighter buckets (0.1ms to 500ms) — flush should always be fast.

---

## What This Enables

Once wired up, standard Grafana dashboards can show:

- **QPS + latency percentiles** (query_total rate + query_duration_seconds quantiles)
- **Cache hit ratio** (hits / (hits + misses)) — the single most important operational metric
- **Memory breakdown by field** — see tagIds dominating, spot unexpected growth
- **Write throughput** (upsert_total rate) — match against pg-sync pipeline rate
- **Backpressure signals** — channel depth climbing = writes outpacing flushes
- **Lazy load events** — field-level cold-start visibility after restart
