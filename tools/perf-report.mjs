#!/usr/bin/env node
/**
 * BitDex Performance Report
 *
 * Collects metrics from production BitDex and generates a report.
 * Designed to be run by agents and sent via Discord.
 *
 * Usage: node tools/perf-report.mjs [--url https://bitdex.civitai.com] [--json] [--store]
 *
 * --store  Append the JSON report to tools/perf-reports.jsonl for historical tracking
 */
import { appendFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

const BITDEX_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'https://bitdex.civitai.com';
const JSON_OUTPUT = process.argv.includes('--json');
const STORE = process.argv.includes('--store');
const REPORT_FILE = join(__dirname, 'perf-reports.jsonl');

async function fetchMetrics() {
  const res = await fetch(`${BITDEX_URL}/metrics`);
  if (!res.ok) throw new Error(`Metrics endpoint: ${res.status}`);
  return res.text();
}

async function fetchIndexes() {
  const res = await fetch(`${BITDEX_URL}/api/indexes`);
  if (!res.ok) throw new Error(`Indexes endpoint: ${res.status}`);
  return res.json();
}

async function timedQuery(filter, sort, limit = 1) {
  const start = Date.now();
  const res = await fetch(`${BITDEX_URL}/api/indexes/civitai/query?format=compact`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ filter, sort, limit }),
  });
  const elapsed = Date.now() - start;
  if (!res.ok) return { error: res.status, elapsed };
  const data = await res.json();
  return { total: data.total_matched, elapsed };
}

function parseMetric(text, name) {
  const re = new RegExp(`^${name.replace(/[{}]/g, '\\$&')}\\{[^}]*\\}\\s+(.+)$`, 'm');
  const m = text.match(re);
  return m ? parseFloat(m[1]) : null;
}

function parseHistogram(text, name) {
  const buckets = {};
  const re = new RegExp(`^${name}_bucket\\{[^}]*le="([^"]+)"\\}\\s+(.+)$`, 'gm');
  let m;
  while ((m = re.exec(text)) !== null) {
    buckets[m[1]] = parseInt(m[2]);
  }
  const sumRe = new RegExp(`^${name}_sum\\{[^}]*\\}\\s+(.+)$`, 'm');
  const countRe = new RegExp(`^${name}_count\\{[^}]*\\}\\s+(.+)$`, 'm');
  const sum = (m = text.match(sumRe)) ? parseFloat(m[1]) : 0;
  const count = (m = text.match(countRe)) ? parseInt(m[1]) : 0;
  return { buckets, sum, count, avg: count > 0 ? sum / count : 0 };
}

function pctFromBuckets(hist, threshold) {
  if (hist.count === 0) return 0;
  const val = hist.buckets[String(threshold)] || 0;
  return ((val / hist.count) * 100).toFixed(1);
}

async function generateReport() {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19) + ' UTC';

  const [metricsText, indexes] = await Promise.all([
    fetchMetrics(),
    fetchIndexes(),
  ]);

  const idx = indexes.indexes.find(i => i.name === 'civitai');
  const aliveCount = idx?.alive_count || 0;

  // Cache metrics
  const cacheHits = parseMetric(metricsText, 'bitdex_cache_hits_total') || 0;
  const cacheMisses = parseMetric(metricsText, 'bitdex_cache_misses_total') || 0;
  const cacheEntries = parseMetric(metricsText, 'bitdex_cache_entries') || 0;
  const cacheBytes = parseMetric(metricsText, 'bitdex_cache_bytes') || 0;
  const cacheInserts = parseMetric(metricsText, 'bitdex_cache_inserts_total') || 0;
  const cacheEvictions = parseMetric(metricsText, 'bitdex_cache_evictions_total') || 0;
  const hitRate = (cacheHits + cacheMisses) > 0
    ? ((cacheHits / (cacheHits + cacheMisses)) * 100).toFixed(1) : '0.0';

  // Query histogram
  const queryHist = parseHistogram(metricsText, 'bitdex_query_duration_seconds');
  const queryTotal = parseMetric(metricsText, 'bitdex_query_total') || 0;

  // Flush
  const flushDuration = parseMetric(metricsText, 'bitdex_flush_last_duration_nanos') || 0;

  // Live query latencies
  const [q1, q2, q3] = await Promise.all([
    timedQuery({ nsfwLevel: 1 }, '-sortAt'),
    timedQuery({ nsfwLevel: 1 }, '-reactionCount'),
    timedQuery({ nsfwLevel: 1, hasMeta: true }, '-sortAt'),
  ]);

  const report = {
    timestamp: ts,
    records: aliveCount,
    queries: {
      total: queryTotal,
      avg_ms: (queryHist.avg * 1000).toFixed(1),
      p50_pct: pctFromBuckets(queryHist, '0.001'),
      p95_pct: pctFromBuckets(queryHist, '0.05'),
      p99_pct: pctFromBuckets(queryHist, '0.5'),
      under_1ms: pctFromBuckets(queryHist, '0.001'),
      under_10ms: pctFromBuckets(queryHist, '0.01'),
      under_50ms: pctFromBuckets(queryHist, '0.05'),
      under_100ms: pctFromBuckets(queryHist, '0.1'),
    },
    cache: {
      entries: cacheEntries,
      bytes_mb: (cacheBytes / (1024 * 1024)).toFixed(1),
      hits: cacheHits,
      misses: cacheMisses,
      hit_rate: hitRate + '%',
      inserts: cacheInserts,
      evictions: cacheEvictions,
    },
    live_latency: {
      sortAt_desc_ms: q1.elapsed,
      reactionCount_desc_ms: q2.elapsed,
      hasMeta_sortAt_ms: q3.elapsed,
    },
    flush_last_ms: (flushDuration / 1_000_000).toFixed(1),
  };

  // Append to JSONL history
  if (STORE) {
    appendFileSync(REPORT_FILE, JSON.stringify(report) + '\n');
    console.error(`Stored to ${REPORT_FILE}`);
  }

  if (JSON_OUTPUT) {
    console.log(JSON.stringify(report, null, 2));
    return report;
  }

  // Text report
  const lines = [
    `📊 **BitDex Performance Report** — ${ts}`,
    '',
    `**Records:** ${aliveCount.toLocaleString()}`,
    `**Queries served:** ${queryTotal.toLocaleString()} (avg ${report.queries.avg_ms}ms)`,
    '',
    '**Query Latency Distribution:**',
    `  <1ms: ${report.queries.under_1ms}% | <10ms: ${report.queries.under_10ms}% | <50ms: ${report.queries.under_50ms}% | <100ms: ${report.queries.under_100ms}%`,
    '',
    '**Cache:**',
    `  Entries: ${cacheEntries} | Size: ${report.cache.bytes_mb} MB | Hit rate: ${report.cache.hit_rate}`,
    `  Hits: ${cacheHits} | Misses: ${cacheMisses} | Inserts: ${cacheInserts} | Evictions: ${cacheEvictions}`,
    '',
    '**Live Query Latency (from this machine):**',
    `  sortAt DESC: ${q1.elapsed}ms | reactionCount DESC: ${q2.elapsed}ms | hasMeta+sortAt: ${q3.elapsed}ms`,
    '',
    `**Flush:** ${report.flush_last_ms}ms last duration`,
  ];

  const text = lines.join('\n');
  console.log(text);
  return report;
}

generateReport().catch(e => {
  console.error('Report failed:', e.message);
  process.exit(1);
});
