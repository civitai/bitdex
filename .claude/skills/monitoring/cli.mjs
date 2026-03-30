#!/usr/bin/env node
/**
 * BitDex Monitoring CLI
 *
 * Agent-friendly monitoring commands that parse Prometheus metrics
 * into structured, actionable summaries.
 *
 * Usage: node .claude/skills/monitoring/cli.mjs <command> [args]
 *
 * Commands:
 *   overview         - At-a-glance health summary (QPS, latency, cache, memory)
 *   cache-health     - Unified cache hit rates, eviction, memory pressure
 *   doc-cache        - Document cache stats (hit rate, size, evictions)
 *   memory           - RSS vs tracked memory, bitmap breakdown
 *   query-perf       - Query latency percentiles and phase breakdown
 *   sync-health      - PG-sync V1 + V2 WAL lag, cursor positions
 *   config           - Current runtime config values
 *   alerts           - Check if any thresholds are exceeded
 *   boundstore       - Cache persistence health
 *   backpressure     - Query concurrency and rejection stats
 */
import { execSync } from 'child_process';
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __d = dirname(fileURLToPath(import.meta.url));

// Load .env from dev-server skill (shared admin token)
try {
  const envFile = readFileSync(resolve(__d, '..', 'dev-server', '.env'), 'utf8');
  for (const line of envFile.split('\n')) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    const eq = t.indexOf('=');
    if (eq > 0) {
      const k = t.slice(0, eq).trim(), v = t.slice(eq + 1).trim();
      if (!process.env[k]) process.env[k] = v;
    }
  }
} catch { /* no .env */ }

const ADMIN_TOKEN = process.env.BITDEX_ADMIN_TOKEN || null;

// Source selection: --prod flag queries production via kubectl port-forward,
// otherwise queries local dev server
const args = process.argv.slice(2);
const isProd = args.includes('--prod');
const filteredArgs = args.filter(a => a !== '--prod');

const LOCAL_URL = process.env.BITDEX_URL || 'http://localhost:3001';
const PROD_URL = process.env.BITDEX_PROD_URL || 'https://bitdex.civitai.com';
const BASE_URL = isProd ? PROD_URL : LOCAL_URL;

// --- Helpers ---

function run(cmd) {
  try {
    return execSync(cmd, { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }).trim();
  } catch (e) {
    return null;
  }
}

async function fetchMetrics() {
  const url = `${BASE_URL}/metrics`;
  try {
    const resp = await fetch(url, {
      headers: ADMIN_TOKEN ? { Authorization: `Bearer ${ADMIN_TOKEN}` } : {},
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.text();
  } catch (e) {
    console.error(`Failed to fetch metrics from ${url}: ${e.message}`);
    process.exit(1);
  }
}

async function fetchJson(path) {
  const url = `${BASE_URL}${path}`;
  try {
    const resp = await fetch(url, {
      headers: ADMIN_TOKEN ? { Authorization: `Bearer ${ADMIN_TOKEN}` } : {},
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.json();
  } catch (e) {
    console.error(`Failed to fetch ${url}: ${e.message}`);
    return null;
  }
}

function parseMetrics(text) {
  const metrics = {};
  for (const line of text.split('\n')) {
    if (line.startsWith('#') || !line.trim()) continue;
    // Parse: metric_name{labels} value
    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)\{?([^}]*)\}?\s+(.+)$/);
    if (match) {
      const [, name, labels, value] = match;
      if (!metrics[name]) metrics[name] = [];
      const parsedLabels = {};
      if (labels) {
        for (const pair of labels.split(',')) {
          const [k, v] = pair.split('=');
          if (k && v) parsedLabels[k.trim()] = v.replace(/"/g, '').trim();
        }
      }
      metrics[name].push({ labels: parsedLabels, value: parseFloat(value) });
    } else {
      // No labels
      const simple = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)\s+(.+)$/);
      if (simple) {
        const [, name, value] = simple;
        if (!metrics[name]) metrics[name] = [];
        metrics[name].push({ labels: {}, value: parseFloat(value) });
      }
    }
  }
  return metrics;
}

function getVal(metrics, name, labelFilter = {}) {
  const entries = metrics[name] || [];
  for (const e of entries) {
    let match = true;
    for (const [k, v] of Object.entries(labelFilter)) {
      if (e.labels[k] !== v) { match = false; break; }
    }
    if (match) return e.value;
  }
  return null;
}

function getAllByLabel(metrics, name, labelKey) {
  const entries = metrics[name] || [];
  const result = {};
  for (const e of entries) {
    const key = e.labels[labelKey] || 'unknown';
    result[key] = e.value;
  }
  return result;
}

function fmt(bytes) {
  if (bytes === null || bytes === undefined) return 'N/A';
  if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(2)} GB`;
  if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(1)} MB`;
  if (bytes >= 1e3) return `${(bytes / 1e3).toFixed(1)} KB`;
  return `${bytes} B`;
}

function fmtMs(seconds) {
  if (seconds === null || seconds === undefined) return 'N/A';
  if (seconds < 0.001) return `${(seconds * 1e6).toFixed(0)}us`;
  if (seconds < 1) return `${(seconds * 1000).toFixed(1)}ms`;
  return `${seconds.toFixed(2)}s`;
}

function fmtNum(n) {
  if (n === null || n === undefined) return 'N/A';
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return `${n}`;
}

function pct(n) {
  if (n === null || n === undefined) return 'N/A';
  return `${(n * 100).toFixed(1)}%`;
}

function status(val, warn, crit, invert = false) {
  if (val === null) return 'UNKNOWN';
  if (invert) {
    if (val <= crit) return 'CRITICAL';
    if (val <= warn) return 'WARNING';
    return 'OK';
  }
  if (val >= crit) return 'CRITICAL';
  if (val >= warn) return 'WARNING';
  return 'OK';
}

function json(data) {
  console.log(JSON.stringify(data, null, 2));
}

// --- Commands ---

async function overview() {
  const m = parseMetrics(await fetchMetrics());

  const alive = getVal(m, 'bitdex_alive_documents');
  const rss = getVal(m, 'bitdex_process_rss_bytes');
  const rssPeak = getVal(m, 'bitdex_process_rss_peak_bytes');
  const jemallocAlloc = getVal(m, 'bitdex_jemalloc_allocated_bytes');
  const jemallocRes = getVal(m, 'bitdex_jemalloc_resident_bytes');
  const startupDur = getVal(m, 'bitdex_startup_duration_seconds');
  const cacheHits = getVal(m, 'bitdex_cache_hits_total');
  const cacheMisses = getVal(m, 'bitdex_cache_misses_total');
  const cacheEntries = getVal(m, 'bitdex_cache_entries');
  const cacheBytes = getVal(m, 'bitdex_cache_bytes');
  const docCacheHits = getVal(m, 'bitdex_doc_cache_hit_total');
  const docCacheMisses = getVal(m, 'bitdex_doc_cache_miss_total');
  const queryTotal = getVal(m, 'bitdex_query_total');
  const inFlight = getVal(m, 'bitdex_queries_in_flight');
  const rejected = getVal(m, 'bitdex_queries_rejected_total');
  const pending = getVal(m, 'bitdex_pending_fields');
  const syncErrors = getVal(m, 'bitdex_pgsync_errors_total');

  const cacheHitRate = (cacheHits && cacheMisses != null) ? cacheHits / (cacheHits + cacheMisses) : null;
  const docCacheHitRate = (docCacheHits && docCacheMisses != null) ? docCacheHits / (docCacheHits + docCacheMisses) : null;

  const POD_LIMIT = 32e9;
  const rssPercent = rss ? rss / POD_LIMIT : null;

  json({
    source: isProd ? 'production' : 'local',
    documents: {
      alive: fmtNum(alive),
      raw: alive,
    },
    memory: {
      rss: fmt(rss),
      rss_peak: fmt(rssPeak),
      jemalloc_allocated: fmt(jemallocAlloc),
      jemalloc_resident: fmt(jemallocRes),
      tracking_gap: jemallocAlloc && rss ? `${((1 - jemallocAlloc / rss) * 100).toFixed(0)}% untracked` : 'N/A',
      rss_percent: pct(rssPercent),
      pod_limit: fmt(POD_LIMIT),
      status: status(rssPercent, 0.80, 0.87),
    },
    startup: {
      duration: startupDur ? fmtMs(startupDur) : 'N/A',
    },
    cache: {
      hit_rate: pct(cacheHitRate),
      entries: fmtNum(cacheEntries),
      memory: fmt(cacheBytes),
      status: status(cacheHitRate, 0.80, 0.50, true),
    },
    doc_cache: {
      hit_rate: pct(docCacheHitRate),
      status: status(docCacheHitRate, 0.80, 0.50, true),
    },
    queries: {
      total_served: fmtNum(queryTotal),
      in_flight: inFlight,
      rejected: rejected,
    },
    sync: {
      errors: syncErrors,
    },
    lazy_loading: {
      pending_fields: pending,
    },
  });
}

async function cacheHealth() {
  const m = parseMetrics(await fetchMetrics());

  const hits = getVal(m, 'bitdex_cache_hits_total');
  const misses = getVal(m, 'bitdex_cache_misses_total');
  const entries = getVal(m, 'bitdex_cache_entries');
  const bytes = getVal(m, 'bitdex_cache_bytes');
  const evictions = getVal(m, 'bitdex_cache_evictions_total');
  const invalidations = getVal(m, 'bitdex_cache_invalidations_total');
  const inserts = getVal(m, 'bitdex_cache_inserts_total');
  const updates = getVal(m, 'bitdex_cache_updates_total');
  const extensions = getVal(m, 'bitdex_cache_extensions_total');
  const wallHits = getVal(m, 'bitdex_cache_wall_hits_total');
  const prefetch = getVal(m, 'bitdex_cache_prefetch_total');
  const initial = getVal(m, 'bitdex_cache_entries_initial');
  const expanded = getVal(m, 'bitdex_cache_entries_expanded');

  const hitRate = (hits && misses != null) ? hits / (hits + misses) : null;

  json({
    hit_rate: pct(hitRate),
    status: status(hitRate, 0.80, 0.50, true),
    entries: fmtNum(entries),
    memory: fmt(bytes),
    lifetime: {
      hits: fmtNum(hits),
      misses: fmtNum(misses),
      inserts: fmtNum(inserts),
      updates: fmtNum(updates),
      evictions: fmtNum(evictions),
      invalidations: fmtNum(invalidations),
      extensions: fmtNum(extensions),
      wall_hits: fmtNum(wallHits),
      prefetches: fmtNum(prefetch),
    },
    capacity: {
      initial: fmtNum(initial),
      expanded: fmtNum(expanded),
    },
  });
}

async function docCache() {
  const m = parseMetrics(await fetchMetrics());

  const hits = getVal(m, 'bitdex_doc_cache_hit_total');
  const misses = getVal(m, 'bitdex_doc_cache_miss_total');
  const entries = getVal(m, 'bitdex_doc_cache_entries');
  const bytes = getVal(m, 'bitdex_doc_cache_bytes');
  const evictions = getVal(m, 'bitdex_doc_cache_evictions_total');
  const generations = getVal(m, 'bitdex_doc_cache_generations');
  const backlog = getVal(m, 'bitdex_doc_cache_backlog');

  const hitRate = (hits && misses != null) ? hits / (hits + misses) : null;

  json({
    hit_rate: pct(hitRate),
    status: status(hitRate, 0.80, 0.50, true),
    entries: fmtNum(entries),
    memory: fmt(bytes),
    evictions: fmtNum(evictions),
    generations: generations,
    write_backlog: backlog,
  });
}

async function memory() {
  const m = parseMetrics(await fetchMetrics());

  const rss = getVal(m, 'bitdex_process_rss_bytes');
  const rssPeak = getVal(m, 'bitdex_process_rss_peak_bytes');
  const jemallocAlloc = getVal(m, 'bitdex_jemalloc_allocated_bytes');
  const jemallocRes = getVal(m, 'bitdex_jemalloc_resident_bytes');
  const cacheBytes = getVal(m, 'bitdex_cache_bytes');
  const docCacheBytes = getVal(m, 'bitdex_doc_cache_bytes');

  // Per-field bitmap memory
  const filterByField = getAllByLabel(m, 'bitdex_filter_bitmap_bytes', 'field');
  const sortByField = getAllByLabel(m, 'bitdex_sort_bitmap_bytes', 'field');
  const slotBytes = getVal(m, 'bitdex_slot_bitmap_bytes');

  const totalFilter = Object.values(filterByField).reduce((a, b) => a + b, 0);
  const totalSort = Object.values(sortByField).reduce((a, b) => a + b, 0);
  const totalTracked = totalFilter + totalSort + (slotBytes || 0) + (cacheBytes || 0) + (docCacheBytes || 0);
  const untracked = rss ? rss - totalTracked : null;

  const POD_LIMIT = 32e9;

  json({
    rss: {
      current: fmt(rss),
      peak: fmt(rssPeak),
      percent_of_limit: pct(rss ? rss / POD_LIMIT : null),
      pod_limit: fmt(POD_LIMIT),
      status: status(rss ? rss / POD_LIMIT : null, 0.80, 0.87),
    },
    jemalloc: {
      allocated: fmt(jemallocAlloc),
      resident: fmt(jemallocRes),
      fragmentation: jemallocAlloc && jemallocRes ? pct(1 - jemallocAlloc / jemallocRes) : 'N/A',
      tracking_gap: jemallocAlloc && rss ? `${((1 - jemallocAlloc / rss) * 100).toFixed(0)}% of RSS unaccounted by jemalloc` : 'N/A',
    },
    breakdown: {
      filter_bitmaps: fmt(totalFilter),
      sort_bitmaps: fmt(totalSort),
      slot_bitmaps: fmt(slotBytes),
      unified_cache: fmt(cacheBytes),
      doc_cache: fmt(docCacheBytes),
      untracked: fmt(untracked),
      total_tracked: fmt(totalTracked),
    },
    filter_by_field: Object.fromEntries(
      Object.entries(filterByField)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 10)
        .map(([k, v]) => [k, fmt(v)])
    ),
    sort_by_field: Object.fromEntries(
      Object.entries(sortByField)
        .sort(([,a], [,b]) => b - a)
        .map(([k, v]) => [k, fmt(v)])
    ),
  });
}

async function queryPerf() {
  const m = parseMetrics(await fetchMetrics());

  const queryTotal = getVal(m, 'bitdex_query_total');
  const inFlight = getVal(m, 'bitdex_queries_in_flight');
  const peak = getVal(m, 'bitdex_queries_in_flight_peak');
  const rejected = getVal(m, 'bitdex_queries_rejected_total');

  // Histogram bucket data (cumulative counters — for display only)
  const durationBuckets = (m['bitdex_query_duration_seconds_bucket'] || [])
    .map(e => ({ le: e.labels.le, count: e.value }))
    .sort((a, b) => parseFloat(a.le) - parseFloat(b.le));

  json({
    queries_served: fmtNum(queryTotal),
    in_flight: inFlight,
    peak_concurrent: peak,
    rejected: rejected,
    note: 'For real-time percentiles, use Grafana dashboard or PromQL. Histogram buckets below are cumulative counters since startup.',
    duration_buckets: durationBuckets.slice(0, 15).map(b => ({
      le: b.le === '+Inf' ? '+Inf' : fmtMs(parseFloat(b.le)),
      count: fmtNum(b.count),
    })),
  });
}

async function syncHealth() {
  const m = parseMetrics(await fetchMetrics());

  // V1 PG-Sync
  const v1Cursor = getVal(m, 'bitdex_pgsync_cursor_position');
  const v1RowsFetched = getVal(m, 'bitdex_pgsync_rows_fetched_total');

  // V2 Sync — ingest side (PG → WAL)
  const v2Cursor = getVal(m, 'bitdex_sync_cursor_position');
  const v2MaxId = getVal(m, 'bitdex_sync_max_id');
  const v2Lag = getVal(m, 'bitdex_sync_lag_rows');
  const v2Ops = getVal(m, 'bitdex_sync_ops_total');
  const v2BatchSize = getVal(m, 'bitdex_sync_batch_size');

  // V2 Sync — WAL (storage)
  const walBytes = getVal(m, 'bitdex_sync_wal_bytes');
  const walPending = getVal(m, 'bitdex_sync_wal_pending_bytes');

  // V2 Sync — processing side (WAL → engine)
  const walOpsProcessed = getVal(m, 'bitdex_wal_ops_processed_total');
  const walReadCursor = getVal(m, 'bitdex_wal_read_cursor_bytes');
  const v2Errors = getVal(m, 'bitdex_pgsync_errors_total');

  // V2 is active if ANY of its metrics have been set
  const v2Active = (v2Cursor !== null && v2Cursor > 0) || (v2Ops !== null && v2Ops > 0) || (walBytes !== null && walBytes > 0);

  json({
    v1_pgsync: {
      cursor_position: fmtNum(v1Cursor),
      rows_fetched: fmtNum(v1RowsFetched),
      status: v1Cursor ? 'ACTIVE' : 'INACTIVE',
    },
    v2_ingest: {
      status: v2Active ? 'ACTIVE' : 'INACTIVE',
      pg_cursor: fmtNum(v2Cursor),
      pg_max_id: fmtNum(v2MaxId),
      pg_lag_rows: fmtNum(v2Lag),
      lag_status: v2Lag !== null ? status(v2Lag, 1000, 10000) : (v2Active ? 'OK' : 'INACTIVE'),
      ops_pulled: fmtNum(v2Ops),
      last_batch_size: v2BatchSize,
    },
    v2_wal: {
      wal_size: fmt(walBytes),
      wal_pending: fmt(walPending),
      read_cursor: fmt(walReadCursor),
      lag_status: walPending !== null ? status(walPending, 1048576, 10485760) : 'N/A',
    },
    v2_processing: {
      ops_processed: fmtNum(walOpsProcessed),
      pipeline_complete: (walOpsProcessed !== null && walOpsProcessed > 0) ? 'YES' : 'NO',
      errors: v2Errors,
    },
  });
}

async function configCmd() {
  // Try to get config from the API
  const indexes = await fetchJson('/api/indexes');
  if (!indexes || !Array.isArray(indexes)) {
    console.error('Could not fetch index list');
    process.exit(1);
  }

  for (const idx of indexes) {
    const name = idx.name || idx;
    const detail = await fetchJson(`/api/indexes/${name}`);
    if (!detail) continue;

    const config = detail.config || detail;
    json({
      index: name,
      cache: config.cache || 'N/A',
      doc_cache: config.doc_cache || 'N/A',
      memory_budget: config.memory_budget || 'auto',
      memory_pressure_threshold: config.memory_pressure_threshold,
      memory_pressure_target: config.memory_pressure_target,
      flush_interval_us: config.flush_interval_us,
      merge_interval_ms: config.merge_interval_ms,
      compact_threshold: config.compact_threshold_pct || config.compact_threshold,
      eviction_sweep_interval: config.eviction_sweep_interval,
      max_page_size: config.max_page_size,
      enabled_metrics: config.enabled_metrics,
      filter_fields: config.filter_fields?.length || 0,
      sort_fields: config.sort_fields?.length || 0,
    });
  }
}

async function alerts() {
  const m = parseMetrics(await fetchMetrics());

  const rss = getVal(m, 'bitdex_process_rss_bytes');
  const cacheHits = getVal(m, 'bitdex_cache_hits_total');
  const cacheMisses = getVal(m, 'bitdex_cache_misses_total');
  const docCacheHits = getVal(m, 'bitdex_doc_cache_hit_total');
  const docCacheMisses = getVal(m, 'bitdex_doc_cache_miss_total');
  const rejected = getVal(m, 'bitdex_queries_rejected_total');
  const pending = getVal(m, 'bitdex_pending_fields');
  const v2Lag = getVal(m, 'bitdex_sync_lag_rows');

  const POD_LIMIT = 32e9;
  const rssPercent = rss ? rss / POD_LIMIT : null;
  const cacheHitRate = (cacheHits && cacheMisses != null) ? cacheHits / (cacheHits + cacheMisses) : null;
  const docHitRate = (docCacheHits && docCacheMisses != null) ? docCacheHits / (docCacheHits + docCacheMisses) : null;

  const checks = [];

  // RSS
  if (rssPercent !== null) {
    checks.push({
      name: 'RSS Memory',
      value: `${fmt(rss)} (${pct(rssPercent)} of ${fmt(POD_LIMIT)})`,
      status: status(rssPercent, 0.80, 0.87),
      thresholds: 'warn: 80%, crit: 87%',
    });
  }

  // Cache hit rate
  if (cacheHitRate !== null) {
    checks.push({
      name: 'Unified Cache Hit Rate',
      value: pct(cacheHitRate),
      status: status(cacheHitRate, 0.80, 0.50, true),
      thresholds: 'warn: <80%, crit: <50%',
    });
  }

  // Doc cache hit rate
  if (docHitRate !== null) {
    checks.push({
      name: 'Doc Cache Hit Rate',
      value: pct(docHitRate),
      status: status(docHitRate, 0.80, 0.50, true),
      thresholds: 'warn: <80%, crit: <50%',
    });
  }

  // Rejected queries
  if (rejected !== null && rejected > 0) {
    checks.push({
      name: 'Rejected Queries',
      value: `${rejected} total`,
      status: 'WARNING',
      thresholds: 'any rejection = warning',
    });
  }

  // Pending lazy loads
  if (pending !== null && pending > 0) {
    checks.push({
      name: 'Pending Lazy Loads',
      value: `${pending} fields`,
      status: 'WARNING',
      thresholds: 'any pending = first query may stall',
    });
  }

  // Sync V2 lag
  if (v2Lag !== null) {
    checks.push({
      name: 'Sync V2 Lag',
      value: `${fmtNum(v2Lag)} rows`,
      status: status(v2Lag, 1000, 10000),
      thresholds: 'warn: >1K rows, crit: >10K rows',
    });
  }

  const firing = checks.filter(c => c.status !== 'OK');

  json({
    source: isProd ? 'production' : 'local',
    evaluated_at: new Date().toISOString(),
    total_checks: checks.length,
    firing: firing.length,
    status: firing.some(c => c.status === 'CRITICAL') ? 'CRITICAL' : firing.length > 0 ? 'WARNING' : 'OK',
    checks,
  });
}

async function boundstore() {
  const m = parseMetrics(await fetchMetrics());

  json({
    meta_entries: fmtNum(getVal(m, 'bitdex_boundstore_meta_entries')),
    tombstones: fmtNum(getVal(m, 'bitdex_boundstore_tombstones')),
    pending_shards: fmtNum(getVal(m, 'bitdex_boundstore_pending_shards')),
    disk_bytes: fmt(getVal(m, 'bitdex_boundstore_disk_bytes')),
    shard_loads: fmtNum(getVal(m, 'bitdex_boundstore_shard_loads_total')),
    tombstones_created: fmtNum(getVal(m, 'bitdex_boundstore_tombstones_created')),
    tombstones_cleaned: fmtNum(getVal(m, 'bitdex_boundstore_tombstones_cleaned')),
    entries_restored: fmtNum(getVal(m, 'bitdex_boundstore_entries_restored')),
    bytes_written: fmt(getVal(m, 'bitdex_boundstore_bytes_written')),
    bytes_read: fmt(getVal(m, 'bitdex_boundstore_bytes_read')),
  });
}

async function backpressure() {
  const m = parseMetrics(await fetchMetrics());

  json({
    queries_in_flight: getVal(m, 'bitdex_queries_in_flight'),
    peak_concurrent: getVal(m, 'bitdex_queries_in_flight_peak'),
    rejected_total: getVal(m, 'bitdex_queries_rejected_total'),
    flush_queue_depth: getVal(m, 'bitdex_flush_queue_depth'),
    docstore_concurrent_reads: getVal(m, 'bitdex_docstore_concurrent_reads'),
    shardstore_ops: getAllByLabel(m, 'bitdex_shardstore_ops_count', 'store'),
  });
}

async function healthReport() {
  const m = parseMetrics(await fetchMetrics());

  const POD_LIMIT = 32e9;
  const rss = getVal(m, 'bitdex_process_rss_bytes');
  const rssPeak = getVal(m, 'bitdex_process_rss_peak_bytes');
  const alive = getVal(m, 'bitdex_alive_documents');
  const queryTotal = getVal(m, 'bitdex_query_total');
  const inFlight = getVal(m, 'bitdex_queries_in_flight');
  const rejected = getVal(m, 'bitdex_queries_rejected_total');
  const cacheHits = getVal(m, 'bitdex_cache_hits_total');
  const cacheMisses = getVal(m, 'bitdex_cache_misses_total');
  const cacheEntries = getVal(m, 'bitdex_cache_entries');
  const cacheBytes = getVal(m, 'bitdex_cache_bytes');
  const cacheHitRate = (cacheHits && cacheMisses != null) ? cacheHits / (cacheHits + cacheMisses) : null;
  const docHits = getVal(m, 'bitdex_doc_cache_hit_total');
  const docMisses = getVal(m, 'bitdex_doc_cache_miss_total');
  const docEntries = getVal(m, 'bitdex_doc_cache_entries');
  const docBytes = getVal(m, 'bitdex_doc_cache_bytes');
  const docEvictions = getVal(m, 'bitdex_doc_cache_evictions_total');
  const docHitRate = (docHits && docMisses != null) ? docHits / (docHits + docMisses) : null;
  const evictions = getVal(m, 'bitdex_cache_evictions_total');
  const v1Cursor = getVal(m, 'bitdex_pgsync_cursor_position');
  const v2Cursor = getVal(m, 'bitdex_sync_cursor_position');
  const v2Lag = getVal(m, 'bitdex_sync_lag_rows');
  const walOpsProcessed = getVal(m, 'bitdex_wal_ops_processed_total');
  const rssPercent = rss ? rss / POD_LIMIT : null;

  json({
    timestamp: new Date().toISOString(),
    source: isProd ? 'production' : 'local',
    pod: { rss: fmt(rss), rss_peak: fmt(rssPeak), rss_percent: pct(rssPercent), rss_status: status(rssPercent, 0.80, 0.87), pod_limit: fmt(POD_LIMIT) },
    documents: { alive: fmtNum(alive) },
    queries: { total: fmtNum(queryTotal), in_flight: inFlight, rejected: rejected },
    unified_cache: { hit_rate: pct(cacheHitRate), entries: fmtNum(cacheEntries), memory: fmt(cacheBytes), evictions: fmtNum(evictions) },
    doc_cache: { hit_rate: pct(docHitRate), entries: fmtNum(docEntries), memory: fmt(docBytes), evictions: fmtNum(docEvictions) },
    sync: { v1_cursor: fmtNum(v1Cursor), v2_cursor: fmtNum(v2Cursor), v2_lag: fmtNum(v2Lag), wal_ops_processed: fmtNum(walOpsProcessed) },
  });
}

async function bootTime() {
  const m = parseMetrics(await fetchMetrics());

  const totalBoot = getVal(m, 'bitdex_startup_duration_seconds');
  const phases = getAllByLabel(m, 'bitdex_boot_phase_seconds', 'phase');

  json({
    total_boot_seconds: totalBoot,
    total_boot: totalBoot ? fmtMs(totalBoot) : 'N/A',
    phases: Object.fromEntries(
      Object.entries(phases)
        .sort(([,a], [,b]) => b - a)
        .map(([phase, secs]) => [phase, { seconds: secs, formatted: fmtMs(secs) }])
    ),
    note: phases && Object.keys(phases).length > 0
      ? 'Phases from last boot. Largest phases are optimization targets.'
      : 'Boot phase breakdown not available (requires v1.1.x+)',
  });
}

// --- Main ---

const cmd = filteredArgs[0];
const commands = {
  overview, 'cache-health': cacheHealth, 'doc-cache': docCache,
  memory, 'query-perf': queryPerf, 'sync-health': syncHealth,
  config: configCmd, alerts, boundstore, backpressure,
  'health-report': healthReport, 'boot-time': bootTime,
};

if (!cmd || cmd === 'help' || cmd === '--help') {
  console.log(`BitDex Monitoring CLI

Usage: node .claude/skills/monitoring/cli.mjs <command> [--prod]

Commands:
  overview         At-a-glance health (docs, QPS, cache, memory, alerts)
  cache-health     Unified cache hit rates, eviction, memory
  doc-cache        Document cache stats
  memory           RSS breakdown, bitmap memory by field
  query-perf       Query latency buckets, concurrency
  sync-health      PG-sync V1 + V2 cursor/lag status
  config           Current runtime config values
  alerts           Threshold checks (RSS, cache, sync lag)
  boundstore       Cache persistence health
  backpressure     Concurrency, rejection, queue depth
  health-report    Full health report (RSS, caches, sync, queries — one command)
  boot-time        Boot phase breakdown (where the 85s goes)

Flags:
  --prod           Query production (default: local on port 3001)`);
  process.exit(0);
}

if (!commands[cmd]) {
  console.error(`Unknown command: ${cmd}\nRun with --help for usage.`);
  process.exit(1);
}

commands[cmd]().catch(e => {
  console.error(`Error: ${e.message}`);
  process.exit(1);
});
