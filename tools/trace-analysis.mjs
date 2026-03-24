#!/usr/bin/env node
/**
 * BitDex Trace Analysis — Production Performance Health Check
 *
 * Pulls query traces from a running BitDex server and produces a
 * latency breakdown with percentiles, slow query identification,
 * and phase-by-phase analysis (plan, filter, sort, docs, lazy load).
 *
 * Usage:
 *   node tools/trace-analysis.mjs [--url URL] [--last N] [--threshold-ms N] [--json]
 *
 * Options:
 *   --url <url>          BitDex server URL (default: https://bitdex.civitai.com)
 *   --last <n>           Number of traces to fetch (default: 1000)
 *   --threshold-ms <n>   Flag queries slower than this (default: 1)
 *   --json               Output raw JSON instead of formatted report
 *   --file <path>        Read traces from a local JSON file instead of fetching
 */

const BITDEX_URL = getArg('--url') || 'https://bitdex.civitai.com';
const LAST = parseInt(getArg('--last') || '1000', 10);
const THRESHOLD_US = (parseFloat(getArg('--threshold-ms') || '1') * 1000);
const JSON_OUTPUT = process.argv.includes('--json');
const LOCAL_FILE = getArg('--file');

function getArg(flag) {
  const idx = process.argv.indexOf(flag);
  return idx >= 0 && idx + 1 < process.argv.length ? process.argv[idx + 1] : null;
}

function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  const idx = Math.floor(sorted.length * p);
  return sorted[Math.min(idx, sorted.length - 1)];
}

function formatUs(us) {
  if (us < 1000) return `${us}μs`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}

function computeStats(values) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    count: sorted.length,
    min: sorted[0],
    p50: percentile(sorted, 0.50),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    max: sorted[sorted.length - 1],
    avg: Math.round(sum / sorted.length),
  };
}

async function fetchTraces() {
  if (LOCAL_FILE) {
    const fs = await import('node:fs');
    const data = JSON.parse(fs.readFileSync(LOCAL_FILE, 'utf8'));
    return Array.isArray(data) ? data : data.traces || [];
  }

  const url = `${BITDEX_URL}/api/indexes/civitai/traces?last=${LAST}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Traces endpoint: ${res.status} ${res.statusText}`);
  const data = await res.json();
  return Array.isArray(data) ? data : data.traces || [];
}

function analyzeTraces(traces) {
  const queryTraces = traces.filter(t => t.total_us != null);
  if (queryTraces.length === 0) {
    return { error: 'No query traces found', traceCount: traces.length };
  }

  // Overall latency
  const totalUs = queryTraces.map(t => t.total_us);
  const planUs = queryTraces.map(t => t.plan_us).filter(v => v != null);
  const filterUs = queryTraces.map(t => t.filter_us).filter(v => v != null);
  const sortUs = queryTraces.map(t => t.sort_us).filter(v => v != null);
  const docsUs = queryTraces.map(t => t.docs_us).filter(v => v != null && v > 0);
  const lazyUs = queryTraces.map(t => t.lazy_load_us).filter(v => v != null && v > 0);

  // Cache stats
  const cacheHits = queryTraces.filter(t => t.cache_hit).length;

  // Slow queries (above threshold)
  const slowQueries = queryTraces
    .filter(t => t.total_us > THRESHOLD_US)
    .sort((a, b) => b.total_us - a.total_us)
    .slice(0, 20);

  // Stalls (>500ms)
  const stalls = queryTraces.filter(t => t.total_us > 500_000);

  // Time range
  const timestamps = queryTraces.map(t => t.ts).filter(Boolean).sort();

  return {
    summary: {
      traceCount: queryTraces.length,
      timeRange: timestamps.length > 0
        ? { from: timestamps[0], to: timestamps[timestamps.length - 1] }
        : null,
      cacheHitRate: `${(100 * cacheHits / queryTraces.length).toFixed(1)}%`,
      cacheHits,
      stallCount: stalls.length,
    },
    latency: {
      total: computeStats(totalUs),
      plan: computeStats(planUs),
      filter: computeStats(filterUs),
      sort: computeStats(sortUs),
      docs: docsUs.length > 0 ? computeStats(docsUs) : null,
      lazy_load: lazyUs.length > 0 ? computeStats(lazyUs) : null,
    },
    slowQueries: slowQueries.map(t => ({
      ts: t.ts,
      index: t.index,
      total_us: t.total_us,
      plan_us: t.plan_us,
      filter_us: t.filter_us,
      sort_us: t.sort_us,
      docs_us: t.docs_us,
      lazy_load_us: t.lazy_load_us,
      result_count: t.result_count,
      cache_hit: t.cache_hit,
      clauses: t.clauses?.length || 0,
    })),
    stalls: stalls.map(t => ({
      ts: t.ts,
      total_us: t.total_us,
      result_count: t.result_count,
    })),
  };
}

function printReport(analysis) {
  if (analysis.error) {
    console.log(`⚠ ${analysis.error} (${analysis.traceCount} traces fetched)`);
    return;
  }

  const { summary, latency, slowQueries, stalls } = analysis;
  const s = summary;
  const t = latency.total;

  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║          BitDex Trace Analysis Report            ║');
  console.log('╚══════════════════════════════════════════════════╝');
  console.log();
  console.log(`  Traces: ${s.traceCount}`);
  if (s.timeRange) {
    console.log(`  Window: ${s.timeRange.from} → ${s.timeRange.to}`);
  }
  console.log(`  Cache:  ${s.cacheHitRate} (${s.cacheHits}/${s.traceCount})`);
  console.log(`  Stalls: ${s.stallCount} (>500ms)`);
  console.log();

  console.log('  ── Server-Side Latency ──');
  console.log(`  total:      p50=${formatUs(t.p50)}  p95=${formatUs(t.p95)}  p99=${formatUs(t.p99)}  max=${formatUs(t.max)}`);
  if (latency.plan) {
    const p = latency.plan;
    console.log(`  plan:       p50=${formatUs(p.p50)}  p95=${formatUs(p.p95)}  p99=${formatUs(p.p99)}  max=${formatUs(p.max)}`);
  }
  if (latency.filter) {
    const f = latency.filter;
    console.log(`  filter:     p50=${formatUs(f.p50)}  p95=${formatUs(f.p95)}  p99=${formatUs(f.p99)}  max=${formatUs(f.max)}`);
  }
  if (latency.sort) {
    const s = latency.sort;
    console.log(`  sort:       p50=${formatUs(s.p50)}  p95=${formatUs(s.p95)}  p99=${formatUs(s.p99)}  max=${formatUs(s.max)}`);
  }
  if (latency.docs) {
    const d = latency.docs;
    console.log(`  docs:       p50=${formatUs(d.p50)}  p95=${formatUs(d.p95)}  p99=${formatUs(d.p99)}  max=${formatUs(d.max)}  (${d.count} queries)`);
  }
  if (latency.lazy_load) {
    const l = latency.lazy_load;
    console.log(`  lazy_load:  p50=${formatUs(l.p50)}  p95=${formatUs(l.p95)}  p99=${formatUs(l.p99)}  max=${formatUs(l.max)}  (${l.count} queries)`);
  }
  console.log();

  if (slowQueries.length > 0) {
    console.log(`  ── Slow Queries (>${formatUs(THRESHOLD_US)}, top ${slowQueries.length}) ──`);
    for (const q of slowQueries.slice(0, 10)) {
      const parts = [];
      if (q.filter_us > 0) parts.push(`filter=${formatUs(q.filter_us)}`);
      if (q.sort_us > 0) parts.push(`sort=${formatUs(q.sort_us)}`);
      if (q.docs_us > 0) parts.push(`docs=${formatUs(q.docs_us)}`);
      if (q.lazy_load_us > 0) parts.push(`lazy=${formatUs(q.lazy_load_us)}`);
      console.log(`    ${formatUs(q.total_us).padEnd(10)} ${q.clauses} clauses  ${q.result_count} results  ${q.cache_hit ? 'HIT' : 'MISS'}  ${parts.join(' ')}`);
    }
    console.log();
  }

  if (stalls.length > 0) {
    console.log(`  ── STALLS (>500ms) ──`);
    for (const s of stalls) {
      console.log(`    ${s.ts}  ${formatUs(s.total_us)}  ${s.result_count} results`);
    }
    console.log();
  }

  // Health verdict
  const verdict = t.p99 < 10_000 ? '✓ HEALTHY' :
                  t.p99 < 50_000 ? '⚠ ACCEPTABLE' :
                  '✗ DEGRADED';
  console.log(`  Verdict: ${verdict} (p99=${formatUs(t.p99)})`);
}

async function main() {
  try {
    const traces = await fetchTraces();
    const analysis = analyzeTraces(traces);

    if (JSON_OUTPUT) {
      console.log(JSON.stringify(analysis, null, 2));
    } else {
      printReport(analysis);
    }
  } catch (err) {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
}

main();
