---
status: ACTIVE
updated: 2026-03-28
---

# BitDex Runtime Configuration Reference

This document catalogs every configurable setting in BitDex: startup-only parameters, runtime-patchable knobs, environment variables, and CLI arguments. It also covers persistence behavior, hardcoded constants that should be configurable, and proposed Prometheus gauges for config observability.

---

## Runtime-Patchable Settings

These can be changed without restart via `PATCH /api/indexes/{name}/config`.

### Server-Wide (memory-only -- NOT persisted to disk)

These are server-level settings stored in AtomicBool/AtomicU32/AtomicU64. They take effect immediately but are lost on restart. Re-apply via CLI args (`--max-query-concurrency`, `--enable-traces`, etc.) or `bitdex.toml`.

| Setting | Type | Default | Persisted | What It Controls |
|---------|------|---------|-----------|-----------------|
| `max_query_concurrency` | u32 | `0` (unlimited) | NO | Max concurrent queries before rejection |
| `enable_traces` | bool | `false` | NO | Query trace collection |
| `trace_min_us` | u64 | `0` (all) | NO | Minimum query latency (us) to record trace |
| `trace_buffer_size` | usize | `1000` | NO | In-memory trace ring buffer capacity |
| `enabled_metrics` | string[] | all enabled | YES | Which expensive metric groups to collect: `bitmap_memory`, `eviction_stats`, `boundstore_disk` |

### Unified Cache (persisted via save_yaml)

All cache settings are persisted to `indexes/{name}/config.yaml` on PATCH and survive restarts.

| Setting | Type | Default | Persisted | What It Controls |
|---------|------|---------|-----------|-----------------|
| `cache.max_entries` | usize | `100,000` | YES | Max cached filter result bitmaps |
| `cache.max_bytes` | usize | `512 MB` | YES | Primary eviction trigger |
| `cache.initial_capacity` | usize | `4,000` | YES | Initial bound capacity per entry |
| `cache.max_capacity` | usize | `64,000` | YES | Max bound capacity after expansion |
| `cache.min_filter_size` | usize | `0` | YES | Skip caching if filter result < N docs |
| `cache.decay_rate` | f64 | `0.95` | YES | Exponential decay for hit stats |
| `cache.bound_target_size` | usize | `10,000` | YES | Target cardinality for bound entries |
| `cache.bound_max_size` | usize | `20,000` | YES | Max bound size before rebuild |
| `cache.bound_max_count` | usize | `100` | YES | Max bound cache entries before LRU |
| `cache.prefetch_threshold` | f64 | `0.95` | YES | Background expansion trigger |
| `cache.max_maintenance_work` | usize | `500,000` | YES | Max cache maintenance work per flush |
| `cache.max_maintenance_ms` | u64 | `5` | YES | Time budget for cache maintenance (ms). Do NOT set above 10 -- caused pod lockup. |

### Per-Field (persisted via save_yaml)

| Setting | Type | Default | Persisted | What It Controls |
|---------|------|---------|-----------|-----------------|
| `filter_fields.{name}.eager_load` | bool | `false` | YES | Load field bitmaps on startup vs lazy |
| `sort_fields.{name}.eager_load` | bool | `false` | YES | Load sort layers on startup vs lazy |

### Time Buckets (persisted via save_yaml)

| Setting | Type | Default | Persisted | What It Controls |
|---------|------|---------|-----------|-----------------|
| `time_buckets.range_buckets.{name}.refresh_interval_secs` | u64 | varies | YES | How often to rebuild time range bucket |

---

## Startup-Only Settings

These are set once at startup and cannot be changed without restarting the process.

### CLI Arguments

| Argument | Default | What It Controls |
|----------|---------|-----------------|
| `--port` | `3000` | HTTP listen port |
| `--data-dir` | `./data` | Index data storage directory |
| `--config` | `bitdex.toml` | Config file path |
| `--rebuild` | `false` | Rebuild bitmaps from docstore on startup |
| `--default-format` | `bitdex` | Default query format (bitdex, compact, meilisearch) |
| `--log-level` | `warn` | Logging level |
| `--enable-traces` | `false` | Enable query traces (also patchable at runtime) |
| `--max-query-concurrency` | `0` | Concurrency limit (also patchable at runtime) |

### Engine Settings (config.json)

| Setting | Default | What It Controls |
|---------|---------|-----------------|
| `flush_interval_us` | `50` | Background flush thread interval (microseconds) |
| `merge_interval_ms` | `5000` | Versioned bitmap merge interval |
| `compact_threshold` | `30` | ShardStore compaction trigger (% stale, 0 = disabled) |
| `channel_capacity` | `100,000` | Bounded channel for write coalescer |
| `eviction_sweep_interval` | `1000` | Check idle values every N flush cycles |
| `max_page_size` | `100` | Hard cap on query `limit` parameter |
| `autovac_interval_secs` | `3600` | Autovacuum interval (not yet implemented) |

### Doc Cache (config.json)

| Setting | Default | What It Controls |
|---------|---------|-----------------|
| `doc_cache.max_bytes` | `10 GB` | Doc cache size, LRU eviction trigger. Tune down for pods with <12 GB RAM. |
| `doc_cache.generation_interval_secs` | `60` | Rotate to new generation every N seconds |
| `doc_cache.max_generations` | `30` | Max generations before merging oldest |

### Memory Pressure (config.json / env var)

| Setting | Default | What It Controls |
|---------|---------|-----------------|
| `memory_budget` | auto-detected | RSS-aware eviction budget |
| `memory_pressure_threshold` | `0.80` | Trigger pressure eviction at % of budget |
| `memory_pressure_target` | `0.75` | Evict down to % of budget |

---

## Environment Variables

| Variable | Priority | What It Controls |
|----------|----------|-----------------|
| `BITDEX_ADMIN_TOKEN` | Overrides config | Bearer token for admin endpoints |
| `BITDEX_MEMORY_LIMIT_BYTES` | 2nd (after config) | Memory budget for eviction |
| `BITDEX_POD_MEMORY_LIMIT` | 3rd (K8s downward API) | Cgroup memory limit fallback |
| `RUST_LOG` | Overrides config | Logging level |
| `RAYON_NUM_THREADS` | Process-level | Rayon thread pool size |
| `DATABASE_URL` | pg-sync only | Postgres connection string |
| `CLICKHOUSE_URL` | pg-sync only | ClickHouse connection string |

---

## Config File

**Format:** TOML (`bitdex.toml`) for server settings; JSON (`indexes/{name}/config.json`) for per-index settings.

**Default TOML:** `bitdex.default.toml` in repo root.

**Config source priority:** CLI args > env vars > config file > defaults

---

## Hardcoded Constants (Should Be Configurable)

These are magic numbers embedded in the source code that affect production behavior but cannot be tuned without a code change and redeploy.

### High Priority (affect performance at 107M records)

| Constant | Value | File | What It Controls |
|----------|-------|------|-----------------|
| `COMPACTION_INTERVAL` | 50 cycles | `concurrent_engine.rs:1170` | Filter diff compaction frequency -- affects write throughput |
| `DEFAULT_COMPACT_THRESHOLD` | 500 ops | `shard_store.rs:116` | ShardStore compaction trigger per shard |
| Bucket diff `max_diffs` | 100 | `concurrent_engine.rs:713` | Time bucket history window (~8h at 300s intervals) |
| Bucket diff `compaction_threshold` | 0.3 | `concurrent_engine.rs:722` | Time bucket history compaction trigger |
| `check_interval_cycles` | 100 | `memory_pressure.rs:31` | Memory pressure check frequency |

### Medium Priority (affect operational characteristics)

| Constant | Value | File | What It Controls |
|----------|-------|------|-----------------|
| Compaction channel capacity | 32 | `concurrent_engine.rs:880` | Backpressure queue for shard compaction |
| Eviction channel capacity | 16 | `concurrent_engine.rs:2461` | Backpressure queue for value eviction |
| CSV read batch size | 500,000 | `loader.rs:270` | Bulk loading throughput |
| Dump batch size | 100,000 | `concurrent_engine.rs:6446` | Export/snapshot throughput |
| Parallel rebuild chunk size | 500 | `concurrent_engine.rs:6820,7087,7324` | Parallelism granularity for field rebuilds |

### Low Priority (reasonable defaults, unlikely to need tuning)

| Constant | Value | File | What It Controls |
|----------|-------|------|-----------------|
| WAL reader idle sleep | 50ms | `server.rs:1203` | Polling interval when no new WAL records |
| WAL reader error sleep | 1s | `server.rs:1207` | Backoff on WAL read error |
| Task completion timeout | 5s | `concurrent_engine.rs:3599` | Timeout for async task completion |
| Rebuild timeout | 30s | `concurrent_engine.rs:5797` | Timeout for bitmap rebuild operations |
| Full dump timeout | 600s | `concurrent_engine.rs:5858` | Timeout for full data export |
| Field loading timeout | 60s | `concurrent_engine.rs:6210` | Timeout for lazy field loading |
| Graceful shutdown wait | 2s | `server.rs:1331` | Wait for dump tasks during shutdown |

---

## Proposed Config Metrics

The following Prometheus gauges would expose current runtime config values on every `/metrics` scrape, enabling Grafana panels to track config changes over time.

### High Priority (runtime-patchable, incident-relevant)

| Proposed Metric | Source | Why |
|----------------|--------|-----|
| `bitdex_config_cache_max_bytes` | `cache.max_bytes` | Cache memory pressure is the #1 ops concern |
| `bitdex_config_cache_max_entries` | `cache.max_entries` | Complements max_bytes |
| `bitdex_config_cache_max_maintenance_ms` | `cache.max_maintenance_ms` | Caused pod lockup at 100ms -- must track |
| `bitdex_config_max_query_concurrency` | `max_query_concurrency` | Backpressure tuning |
| `bitdex_config_memory_budget_bytes` | `memory_budget` | RSS eviction target |
| `bitdex_config_memory_pressure_threshold` | `memory_pressure_threshold` | When eviction kicks in |
| `bitdex_config_doc_cache_max_bytes` | `doc_cache.max_bytes` | Doc cache sizing |

### Medium Priority (startup-only, useful for debugging)

| Proposed Metric | Source | Why |
|----------------|--------|-----|
| `bitdex_config_flush_interval_us` | `flush_interval_us` | Flush frequency affects tail latency |
| `bitdex_config_compact_threshold` | `compact_threshold` | ShardStore compaction aggressiveness |
| `bitdex_config_eviction_sweep_interval` | `eviction_sweep_interval` | How often idle values are checked |
| `bitdex_config_merge_interval_ms` | `merge_interval_ms` | Bitmap merge frequency |

### Low Priority (info labels)

| Proposed Metric | Source | Why |
|----------------|--------|-----|
| `bitdex_info` | build version, port, data_dir | Standard info metric with labels |
| `bitdex_config_traces_enabled` | `enable_traces` | Whether tracing is on |
| `bitdex_config_metrics_bitmap_memory_enabled` | `enabled_metrics` | Which metric groups are gated |

---

## Dashboard Section: Runtime Config

A new Grafana row showing current config values as stat panels + time series for config changes:

- **Cache Max Bytes** -- stat panel showing current value, time series showing changes
- **Cache Maintenance Budget** -- stat panel, time series
- **Memory Budget** -- stat panel with RSS overlay
- **Query Concurrency Limit** -- stat panel
- **Doc Cache Max Bytes** -- stat panel

This lets operators see at a glance how the system is configured and correlate config changes with behavior changes in other panels.

---

## Admin Interface Consideration

An admin UI could surface:
- Current config values (read from `GET /api/indexes/{name}`)
- Config change history (requires new endpoint or annotation system)
- One-click runtime tuning (calls `PATCH /api/indexes/{name}/config`)
- Side-by-side config vs metrics view

This is a separate initiative -- discuss scope with Scarlet before starting.
