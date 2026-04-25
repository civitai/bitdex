#!/usr/bin/env node
// Phase-validation harness.
//
// Snapshots /metrics + traces ring buffer + stats every interval for the
// duration of a run. Computes histogram quantiles, captures key gauges /
// counters, writes NDJSON time-series + summary + diff vs baseline.
//
// Usage:
//   node scripts/phase-validate.mjs \
//     --label phase1-async-cache-baseline \
//     --duration-min 15 \
//     --interval-sec 5 \
//     --target http://localhost:3002 \
//     --index civitai \
//     [--baseline phase0-pre-bidx-v1] \
//     [--probe none|postid-tail|write-burst] \
//     [--gates always,phase_1]
//
// Output dir: local-prom/runs/<label>/
//   metrics.ndjson      — one prom snapshot per interval (parsed flat keys)
//   traces.ndjson       — trace ring buffer snapshots (every 60s)
//   stats.ndjson        — /api/indexes/<index>/stats snapshots (every 60s)
//   meta.json           — config, git rev, start/end times, args
//   summary.json        — start/end deltas, histogram quantiles, max RSS
//   summary.md          — human readable
//   diff-vs-<baseline>.md — % delta per key metric, regression flags
//
// Exits non-zero if any gate fails.

import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';

// ---------- args ----------

const argv = process.argv.slice(2);
const args = {};
for (let i = 0; i < argv.length; i++) {
  const k = argv[i];
  if (k.startsWith('--')) args[k.slice(2)] = argv[++i];
}
const LABEL = args.label || (() => { console.error('--label required'); process.exit(2); })();
const DUR_MIN = parseFloat(args['duration-min'] || '15');
const INTERVAL_SEC = parseFloat(args['interval-sec'] || '5');
const TARGET = (args.target || 'http://localhost:3002').replace(/\/$/, '');
const INDEX = args.index || 'civitai';
const BASELINE = args.baseline || null;
const PROBE = args.probe || 'none';
const GATES = (args.gates || 'always').split(',').map(s => s.trim()).filter(Boolean);

const RUN_DIR = path.join('local-prom', 'runs', LABEL);
fs.mkdirSync(RUN_DIR, { recursive: true });

// ---------- key metrics ----------

// Histograms — quantiles computed from _bucket series.
const HISTOGRAMS = [
  'bitdex_query_duration_seconds',
  'bitdex_docstore_read_seconds',
  'bitdex_wal_append_duration_seconds', // post-task-#19
];

// Counters / gauges captured per snapshot. Labels with index="<INDEX>" auto.
const SCALAR_METRICS = [
  // Query path
  'bitdex_query_total',
  'bitdex_query_errors_total',
  // Doc cache
  'bitdex_doc_cache_hit_total',
  'bitdex_doc_cache_miss_total',
  'bitdex_doc_cache_entries',
  'bitdex_doc_cache_bytes',
  'bitdex_doc_cache_evictions_total',
  'bitdex_doc_cache_generations',
  'bitdex_doc_cache_backlog',
  // Docstore
  'bitdex_docstore_concurrent_reads',
  'bitdex_docstore_put_batch_fast_path_total',
  'bitdex_docstore_put_batch_slow_path_total',
  // Unified cache
  'bitdex_cache_hit_total',
  'bitdex_cache_miss_total',
  'bitdex_cache_inserts_total',
  'bitdex_cache_evictions_total',
  'bitdex_cache_bytes',
  'bitdex_cache_entries',
  'bitdex_cache_tombstones_created',
  // Flush / merge
  'bitdex_flush_cache_ns_total',
  'bitdex_flush_total',
  'bitdex_merge_total',
  // WAL / ops
  'bitdex_wal_ops_processed_total',
  'bitdex_wal_ops_failed_total',
  // Process
  'process_resident_memory_bytes',
  'process_virtual_memory_bytes',
  'process_cpu_seconds_total',
  // Bitmap memory (if enabled)
  'bitdex_bitmap_memory_bytes_total',
  // Filter indexed-lookup fallback (post-task-#8)
  'bitdex_filter_indexed_lookup_fallback_total',
  // Relay (when active)
  'bitdex_relay_events_total',
  'bitdex_relay_sse_subscribers',
  'bitdex_relay_sse_lagged_events_total',
];

// ---------- gate definitions ----------

const GATES_SPEC = {
  always: {
    // Long-window (cumulative) quantiles — coarse signal.
    'bitdex_query_duration_seconds:p50_ms': { max: 10 },
    'bitdex_query_duration_seconds:p95_ms': { max: 100 },
    'bitdex_query_duration_seconds:p99_ms': { max: 1000 },
    'bitdex_query_duration_seconds:p999_ms': { max: 1500 },
    // SPIKE GATES — max interval quantile + count of intervals over threshold.
    // These catch the "P99 spikes intermittently" pattern Justin called out.
    'bitdex_query_duration_seconds:p99_ms_max': { max: 1000 },
    'bitdex_query_duration_seconds:p999_ms_max': { max: 2000 },
    'bitdex_query_duration_seconds:intervals_over_1000ms': { max: 0 },
    'bitdex_query_duration_seconds:longest_spike_over_1000ms': { max: 0 },
    // Trace ring-buffer outlier count (catches what histograms miss).
    'traces_outliers_over_1s': { max: 0 },
    // Doc cache: overall + worst interval.
    'bitdex_doc_cache_hit_ratio': { regress_pct_max: 2 },
    'bitdex_doc_cache_hit_ratio_min': { min: 90 },
    // RSS: growth + linear trend slope (leak detector).
    'process_resident_memory_bytes:growth_per_min_pct': { max: 1.5 },
  },
  doc_path: {
    'bitdex_docstore_read_seconds:p95_ms': { regress_pct_max: 10 },
    'bitdex_docstore_read_seconds:p99_ms': { regress_pct_max: 10 },
    // Spike gates on doc path too (Justin's load-bearing concern).
    'bitdex_docstore_read_seconds:p99_ms_max': { max: 100 },
    'bitdex_docstore_read_seconds:intervals_over_1000ms': { max: 0 },
    'bitdex_doc_cache_bytes:growth_per_min_pct': { max: 5 },
  },
  phase_1_async_cache: {
    'flush_tombstones_under_backpressure': { min: 1 },
    'bitdex_cache_miss_total:rate_per_min': { regress_pct_max: 20 },
  },
  phase_2_wal: {
    'bitdex_wal_append_duration_seconds:p99_ms': { max: 50 },
    'bitdex_wal_append_duration_seconds:p99_ms_max': { max: 200 },
    'bitdex_wal_ops_failed_total:delta': { max: 0 },
  },
  phase_3_doc_batch: {
    'bitdex_docstore_read_seconds:p95_ms': { regress_pct_max: -10 }, // expect improvement
    'bitdex_docstore_read_seconds:p99_ms_max': { max: 50 }, // tighter spike cap post-batch
  },
};

// ---------- prom parser ----------

// Parses prom text into `{ name, labels: {k:v}, value, isHistogramBucket, le }`.
function parseProm(text) {
  const out = [];
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const sp = line.lastIndexOf(' ');
    if (sp < 0) continue;
    const valStr = line.slice(sp + 1);
    const value = parseFloat(valStr);
    if (!Number.isFinite(value)) continue;
    const head = line.slice(0, sp);
    let name, labels = {};
    const lbrace = head.indexOf('{');
    if (lbrace < 0) {
      name = head;
    } else {
      name = head.slice(0, lbrace);
      const labelStr = head.slice(lbrace + 1, head.lastIndexOf('}'));
      // Naive label parser — handles `k="v",k2="v2"` (no escaped commas).
      const re = /(\w+)="([^"]*)"/g;
      let m;
      while ((m = re.exec(labelStr))) labels[m[1]] = m[2];
    }
    const isHistBucket = name.endsWith('_bucket');
    const baseName = isHistBucket ? name.slice(0, -'_bucket'.length) : name;
    out.push({ name, baseName, labels, value, isHistBucket, le: labels.le ? parseFloat(labels.le) : null });
  }
  return out;
}

// Histogram quantile from cumulative buckets. Linear interpolation.
function histQuantile(buckets, q) {
  if (!buckets.length) return null;
  buckets = buckets.filter(b => Number.isFinite(b.le)).sort((a, b) => a.le - b.le);
  if (!buckets.length) return null;
  const total = buckets[buckets.length - 1].count;
  if (total === 0) return null;
  const target = q * total;
  let prevLe = 0, prevCount = 0;
  for (const b of buckets) {
    if (b.count >= target) {
      if (b.count === prevCount) return b.le;
      const frac = (target - prevCount) / (b.count - prevCount);
      return prevLe + frac * (b.le - prevLe);
    }
    prevLe = b.le;
    prevCount = b.count;
  }
  return buckets[buckets.length - 1].le;
}

// ---------- snapshot capture ----------

async function fetchText(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url}: ${r.status}`);
  return r.text();
}

async function fetchJson(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url}: ${r.status}`);
  return r.json();
}

function nowSec() { return Date.now() / 1000; }

async function captureMetricsSnapshot() {
  const text = await fetchText(`${TARGET}/metrics`);
  const parsed = parseProm(text);
  const snap = { ts: nowSec(), scalars: {}, hist_buckets: {} };
  for (const p of parsed) {
    if (p.isHistBucket && HISTOGRAMS.includes(p.baseName)) {
      const labelKey = JSON.stringify({ ...p.labels, le: undefined });
      const histKey = `${p.baseName}|${labelKey}`;
      if (!snap.hist_buckets[histKey]) snap.hist_buckets[histKey] = [];
      snap.hist_buckets[histKey].push({ le: p.le, count: p.value });
    } else if (SCALAR_METRICS.includes(p.name)) {
      const labelKey = Object.entries(p.labels).map(([k, v]) => `${k}="${v}"`).join(',');
      const key = labelKey ? `${p.name}{${labelKey}}` : p.name;
      snap.scalars[key] = p.value;
    }
  }
  return snap;
}

async function captureTraces() {
  try {
    return await fetchJson(`${TARGET}/api/indexes/${INDEX}/traces?last=1000`);
  } catch (e) {
    return { error: String(e) };
  }
}

async function captureStats() {
  try {
    return await fetchJson(`${TARGET}/api/indexes/${INDEX}/stats`);
  } catch (e) {
    return { error: String(e) };
  }
}

// ---------- probes ----------

async function runProbe(probe, signal) {
  if (probe === 'none') return;
  if (probe === 'postid-tail') {
    const { default: childProcess } = await import('node:child_process');
    childProcess.spawn('node', ['scripts/probe-postid-tail.mjs'], {
      stdio: 'inherit', detached: true, signal,
    });
    return;
  }
  if (probe === 'write-burst') {
    const { default: childProcess } = await import('node:child_process');
    childProcess.spawn('node', ['scripts/ops-loadgen.mjs', '--rate', '500', '--duration-sec', String(DUR_MIN * 60)], {
      stdio: 'inherit', detached: true, signal,
    });
    return;
  }
  console.warn(`unknown probe: ${probe}`);
}

// ---------- summary + gate eval ----------

// Subtract two cumulative bucket arrays element-wise → interval bucket counts.
// `prev` and `curr` may have differently-ordered buckets; we align by `le`.
function diffBuckets(prev, curr) {
  const prevByLe = new Map(prev.map(b => [b.le, b.count]));
  return curr.map(b => ({ le: b.le, count: b.count - (prevByLe.get(b.le) || 0) }));
}

// Linear regression slope (least squares) for time-series y vs x. Returns
// `{ slope_per_sec, intercept, r2 }`.
function linregress(xs, ys) {
  const n = xs.length;
  if (n < 2) return { slope_per_sec: 0, intercept: ys[0] || 0, r2: 0 };
  let sx = 0, sy = 0, sxx = 0, sxy = 0, syy = 0;
  for (let i = 0; i < n; i++) { sx += xs[i]; sy += ys[i]; sxx += xs[i]*xs[i]; sxy += xs[i]*ys[i]; syy += ys[i]*ys[i]; }
  const mx = sx / n, my = sy / n;
  const denom = sxx - n * mx * mx;
  const slope = denom !== 0 ? (sxy - n * mx * my) / denom : 0;
  const intercept = my - slope * mx;
  const ssTot = syy - n * my * my;
  const ssRes = ys.reduce((a, y, i) => a + Math.pow(y - (slope * xs[i] + intercept), 2), 0);
  const r2 = ssTot !== 0 ? 1 - ssRes / ssTot : 0;
  return { slope_per_sec: slope, intercept, r2 };
}

function computeSummary(firstSnap, lastSnap, allSnaps) {
  const out = { duration_sec: lastSnap.ts - firstSnap.ts, snapshots: allSnaps.length, scalars: {}, histograms: {}, timeseries: {}, spikes: {}, trends: {} };

  // Histogram quantiles using LAST snapshot (cumulative since process start).
  // This is the "long-window" view; per-interval below catches the spikes.
  for (const [key, buckets] of Object.entries(lastSnap.hist_buckets)) {
    const q50 = histQuantile(buckets, 0.50);
    const q95 = histQuantile(buckets, 0.95);
    const q99 = histQuantile(buckets, 0.99);
    const q999 = histQuantile(buckets, 0.999);
    out.histograms[key] = {
      p50_ms: q50 === null ? null : q50 * 1000,
      p95_ms: q95 === null ? null : q95 * 1000,
      p99_ms: q99 === null ? null : q99 * 1000,
      p999_ms: q999 === null ? null : q999 * 1000,
    };
  }

  // Per-interval quantiles. For each consecutive snapshot pair, diff the
  // cumulative buckets and compute interval-only quantiles.
  //
  // BUG NOTE: prior version re-cumulated `diffBuckets` output. Prom buckets
  // ARE already cumulative-by-LE (each bucket count = events ≤ LE), and
  // (curr_cum - prev_cum) preserves cumulative-by-LE semantics for the
  // interval window. The re-cumulation step double-counted, pushing P99
  // toward the +Inf bucket boundary. Fix: pass `diffBuckets` output
  // directly to `histQuantile` after sort.
  for (const histKey of Object.keys(lastSnap.hist_buckets)) {
    const series = []; // [{ t_sec_from_start, p50_ms, p95_ms, p99_ms, p999_ms, count }]
    for (let i = 1; i < allSnaps.length; i++) {
      const prev = allSnaps[i - 1].hist_buckets[histKey];
      const curr = allSnaps[i].hist_buckets[histKey];
      if (!prev || !curr) continue;
      const intervalCum = diffBuckets(prev, curr); // cumulative-by-LE for interval
      intervalCum.sort((a, b) => a.le - b.le);
      const total = intervalCum.length ? intervalCum[intervalCum.length - 1].count : 0;
      const q50 = histQuantile(intervalCum, 0.50);
      const q95 = histQuantile(intervalCum, 0.95);
      const q99 = histQuantile(intervalCum, 0.99);
      const q999 = histQuantile(intervalCum, 0.999);
      series.push({
        t_sec_from_start: allSnaps[i].ts - firstSnap.ts,
        count: total,
        p50_ms: q50 === null ? null : q50 * 1000,
        p95_ms: q95 === null ? null : q95 * 1000,
        p99_ms: q99 === null ? null : q99 * 1000,
        p999_ms: q999 === null ? null : q999 * 1000,
      });
    }
    out.timeseries[histKey] = series;

    // Spike summary per histogram. "Spike" = any interval whose quantile
    // exceeds the threshold; longest spike = max contiguous run of spikes.
    const spike = { p99_ms: { max: 0, count_over_100ms: 0, count_over_500ms: 0, count_over_1000ms: 0, longest_contig_over_1000ms: 0 }, p999_ms: { max: 0 }, p95_ms: { max: 0 }, p50_ms: { max: 0 } };
    let contigOver1k = 0, longestOver1k = 0;
    for (const pt of series) {
      for (const q of ['p50_ms', 'p95_ms', 'p99_ms', 'p999_ms']) {
        if (pt[q] !== null && pt[q] > spike[q].max) spike[q].max = pt[q];
      }
      if (pt.p99_ms !== null) {
        if (pt.p99_ms > 100) spike.p99_ms.count_over_100ms++;
        if (pt.p99_ms > 500) spike.p99_ms.count_over_500ms++;
        if (pt.p99_ms > 1000) {
          spike.p99_ms.count_over_1000ms++;
          contigOver1k++;
          longestOver1k = Math.max(longestOver1k, contigOver1k);
        } else {
          contigOver1k = 0;
        }
      }
    }
    spike.p99_ms.longest_contig_over_1000ms = longestOver1k;
    out.spikes[histKey] = spike;

    // Trend regression on P99 (was it stable, rising, falling?).
    const xs = series.filter(p => p.p99_ms !== null).map(p => p.t_sec_from_start);
    const ys = series.filter(p => p.p99_ms !== null).map(p => p.p99_ms);
    out.trends[histKey] = linregress(xs, ys);
    out.trends[histKey].metric = 'p99_ms';
  }

  // Scalar deltas.
  for (const k of Object.keys({ ...firstSnap.scalars, ...lastSnap.scalars })) {
    const start = firstSnap.scalars[k] || 0;
    const end = lastSnap.scalars[k] || 0;
    out.scalars[k] = { start, end, delta: end - start };
  }

  // Per-interval rate-of-change for key counters (catches sudden spikes like
  // cache-miss bursts, eviction storms, error rate changes).
  const rateMetrics = ['bitdex_query_total', 'bitdex_query_errors_total', 'bitdex_cache_miss_total', 'bitdex_doc_cache_miss_total', 'bitdex_doc_cache_evictions_total', 'bitdex_cache_evictions_total', 'bitdex_wal_ops_failed_total'];
  out.rate_timeseries = {};
  out.rate_spikes = {};
  for (const baseName of rateMetrics) {
    const matched = Object.keys(lastSnap.scalars).filter(k => k.startsWith(baseName));
    for (const fullKey of matched) {
      const series = [];
      for (let i = 1; i < allSnaps.length; i++) {
        const dt = allSnaps[i].ts - allSnaps[i - 1].ts;
        if (dt <= 0) continue;
        const dv = (allSnaps[i].scalars[fullKey] || 0) - (allSnaps[i - 1].scalars[fullKey] || 0);
        series.push({ t_sec_from_start: allSnaps[i].ts - firstSnap.ts, rate_per_sec: dv / dt });
      }
      out.rate_timeseries[fullKey] = series;
      const max = series.reduce((a, p) => Math.max(a, p.rate_per_sec), 0);
      const mean = series.length ? series.reduce((a, p) => a + p.rate_per_sec, 0) / series.length : 0;
      out.rate_spikes[fullKey] = { max, mean, ratio_max_to_mean: mean > 0 ? max / mean : null };
    }
  }

  // RSS growth — also build time-series for trend / leak detection.
  const rssKey = Object.keys(out.scalars).find(k => k.startsWith('process_resident_memory_bytes'));
  if (rssKey) {
    const start = out.scalars[rssKey].start;
    const end = out.scalars[rssKey].end;
    const minutes = out.duration_sec / 60;
    out.rss_growth_pct_per_min = minutes > 0 && start > 0 ? ((end - start) / start) * 100 / minutes : 0;
    const rssSeries = allSnaps.map(s => ({ t: s.ts - firstSnap.ts, rss: s.scalars[rssKey] || 0 }));
    out.rss_timeseries = rssSeries;
    out.rss_max = rssSeries.reduce((a, p) => Math.max(a, p.rss), 0);
    const xs = rssSeries.map(p => p.t);
    const ys = rssSeries.map(p => p.rss);
    out.rss_trend = linregress(xs, ys);
  }

  // Doc cache hit ratio (overall + per-interval to catch cache thrash spikes).
  const dh = sumScalar(out.scalars, 'bitdex_doc_cache_hit_total', 'delta');
  const dm = sumScalar(out.scalars, 'bitdex_doc_cache_miss_total', 'delta');
  out.doc_cache_hit_ratio_pct = (dh + dm) > 0 ? (dh / (dh + dm)) * 100 : null;

  // Per-interval hit ratio.
  const ratioSeries = [];
  for (let i = 1; i < allSnaps.length; i++) {
    const ph = sumSnapMatching(allSnaps[i - 1].scalars, 'bitdex_doc_cache_hit_total');
    const pm = sumSnapMatching(allSnaps[i - 1].scalars, 'bitdex_doc_cache_miss_total');
    const ch = sumSnapMatching(allSnaps[i].scalars, 'bitdex_doc_cache_hit_total');
    const cm = sumSnapMatching(allSnaps[i].scalars, 'bitdex_doc_cache_miss_total');
    const dh = ch - ph, dm = cm - pm;
    const ratio = (dh + dm) > 0 ? (dh / (dh + dm)) * 100 : null;
    ratioSeries.push({ t_sec_from_start: allSnaps[i].ts - firstSnap.ts, ratio_pct: ratio });
  }
  out.doc_cache_hit_ratio_timeseries = ratioSeries;
  const validRatios = ratioSeries.filter(p => p.ratio_pct !== null).map(p => p.ratio_pct);
  out.doc_cache_hit_ratio_min = validRatios.length ? Math.min(...validRatios) : null;
  out.doc_cache_hit_ratio_max = validRatios.length ? Math.max(...validRatios) : null;

  return out;
}

function sumSnapMatching(scalars, prefix) {
  let total = 0;
  for (const [k, v] of Object.entries(scalars)) {
    if (k.startsWith(prefix)) total += v || 0;
  }
  return total;
}

function sumScalar(scalars, prefix, field) {
  let total = 0;
  for (const [k, v] of Object.entries(scalars)) {
    if (k.startsWith(prefix)) total += v[field] || 0;
  }
  return total;
}

function evalGates(summary, traces, gateNames) {
  const results = [];
  for (const gateName of gateNames) {
    const spec = GATES_SPEC[gateName];
    if (!spec) { results.push({ gate: gateName, status: 'unknown' }); continue; }
    for (const [metric, rule] of Object.entries(spec)) {
      results.push(evalGate(metric, rule, summary, traces, gateName));
    }
  }
  return results;
}

function evalGate(metric, rule, summary, traces, gateName) {
  let actual = null;
  // Histogram quantile keys.
  // `<name>:p99_ms` — overall (cumulative) quantile across the run.
  // `<name>:p99_ms_max` — maximum interval quantile (catches spikes).
  // `<name>:intervals_over_1000ms` — count of intervals where P99 > 1s.
  // `<name>:longest_spike_over_1000ms` — max contiguous run.
  if (metric.includes(':p') && (metric.endsWith('_ms') || metric.endsWith('_max'))) {
    const parts = metric.split(':');
    const name = parts[0];
    const q = parts[1];
    const histKey = Object.keys(summary.histograms).find(k => k.startsWith(name + '|'));
    if (histKey) {
      if (q.endsWith('_max')) {
        // Spike-aware: max interval value.
        const baseQ = q.replace('_max', '');
        actual = summary.spikes?.[histKey]?.[baseQ]?.max ?? null;
      } else {
        actual = summary.histograms[histKey][q];
      }
    }
  } else if (metric.includes(':intervals_over_')) {
    const [name, suffix] = metric.split(':');
    const ms = parseInt(suffix.replace('intervals_over_', '').replace('ms', ''), 10);
    const histKey = Object.keys(summary.spikes || {}).find(k => k.startsWith(name + '|'));
    if (histKey) actual = summary.spikes[histKey].p99_ms[`count_over_${ms}ms`];
  } else if (metric.includes(':longest_spike_over_')) {
    const [name, suffix] = metric.split(':');
    const ms = parseInt(suffix.replace('longest_spike_over_', '').replace('ms', ''), 10);
    const histKey = Object.keys(summary.spikes || {}).find(k => k.startsWith(name + '|'));
    if (histKey && ms === 1000) actual = summary.spikes[histKey].p99_ms.longest_contig_over_1000ms;
  } else if (metric.includes(':trend_slope_per_min')) {
    const name = metric.split(':')[0];
    const histKey = Object.keys(summary.trends || {}).find(k => k.startsWith(name + '|'));
    if (histKey) actual = (summary.trends[histKey].slope_per_sec || 0) * 60;
  } else if (metric === 'traces_outliers_over_1s') {
    // traces shape varies: array of trace objects, or `{ traces: [...] }`,
    // or `{ error: ... }` if endpoint failed. Normalize.
    const arr = Array.isArray(traces)
      ? traces
      : Array.isArray(traces?.traces)
        ? traces.traces
        : [];
    actual = arr.filter(t => (t?.total_us || 0) > 1_000_000).length;
  } else if (metric === 'bitdex_doc_cache_hit_ratio') {
    actual = summary.doc_cache_hit_ratio_pct;
  } else if (metric === 'bitdex_doc_cache_hit_ratio_min') {
    actual = summary.doc_cache_hit_ratio_min;
  } else if (metric === 'process_resident_memory_bytes:growth_per_min_pct') {
    actual = summary.rss_growth_pct_per_min;
  } else if (metric === 'process_resident_memory_bytes:trend_slope_bytes_per_min') {
    actual = (summary.rss_trend?.slope_per_sec || 0) * 60;
  }
  const pass = checkRule(actual, rule);
  return { gate: gateName, metric, rule, actual, pass };
}

function checkRule(actual, rule) {
  if (actual === null || actual === undefined) return null;
  if (rule.max !== undefined && actual > rule.max) return false;
  if (rule.min !== undefined && actual < rule.min) return false;
  return true;
}

// ---------- main ----------

async function main() {
  console.log(`[phase-validate] label=${LABEL} duration=${DUR_MIN}min interval=${INTERVAL_SEC}s target=${TARGET}`);
  console.log(`[phase-validate] gates=${GATES.join(',')} probe=${PROBE}`);

  // Meta.
  let gitRev = '';
  try { gitRev = execSync('git rev-parse HEAD', { encoding: 'utf8' }).trim(); } catch {}
  let serverConfig = null;
  try { serverConfig = await fetchJson(`${TARGET}/api/config`); } catch (e) { serverConfig = { error: String(e) }; }
  const meta = { label: LABEL, started_at: new Date().toISOString(), duration_min: DUR_MIN, interval_sec: INTERVAL_SEC, target: TARGET, index: INDEX, baseline: BASELINE, probe: PROBE, gates: GATES, git_rev: gitRev, server_config: serverConfig };
  fs.writeFileSync(path.join(RUN_DIR, 'meta.json'), JSON.stringify(meta, null, 2));

  const metricsStream = fs.createWriteStream(path.join(RUN_DIR, 'metrics.ndjson'));
  const tracesStream = fs.createWriteStream(path.join(RUN_DIR, 'traces.ndjson'));
  const statsStream = fs.createWriteStream(path.join(RUN_DIR, 'stats.ndjson'));

  const firstSnap = await captureMetricsSnapshot();
  metricsStream.write(JSON.stringify(firstSnap) + '\n');
  console.log(`[phase-validate] t=0s captured (${Object.keys(firstSnap.scalars).length} scalars, ${Object.keys(firstSnap.hist_buckets).length} histograms)`);

  // Probe.
  const probeAbort = new AbortController();
  if (PROBE !== 'none') {
    runProbe(PROBE, probeAbort.signal).catch(e => console.warn(`probe error: ${e}`));
  }

  // Capture loop.
  const allSnaps = [firstSnap];
  const endTs = nowSec() + DUR_MIN * 60;
  let lastTraceTs = 0, lastStatsTs = 0;
  while (nowSec() < endTs) {
    await new Promise(r => setTimeout(r, INTERVAL_SEC * 1000));
    try {
      const snap = await captureMetricsSnapshot();
      metricsStream.write(JSON.stringify(snap) + '\n');
      allSnaps.push(snap);
      const elapsed = ((snap.ts - firstSnap.ts) / 60).toFixed(1);
      process.stdout.write(`\r[phase-validate] t=${elapsed}min captured ${allSnaps.length} snapshots`);
    } catch (e) {
      console.warn(`\n[phase-validate] snapshot error: ${e}`);
    }
    if (nowSec() - lastTraceTs > 60) {
      try {
        const t = await captureTraces();
        tracesStream.write(JSON.stringify({ ts: nowSec(), traces: t }) + '\n');
        lastTraceTs = nowSec();
      } catch (e) { /* ignore */ }
    }
    if (nowSec() - lastStatsTs > 60) {
      try {
        const s = await captureStats();
        statsStream.write(JSON.stringify({ ts: nowSec(), stats: s }) + '\n');
        lastStatsTs = nowSec();
      } catch (e) { /* ignore */ }
    }
  }
  console.log('');

  probeAbort.abort();
  metricsStream.end(); tracesStream.end(); statsStream.end();

  const lastSnap = allSnaps[allSnaps.length - 1];
  const summary = computeSummary(firstSnap, lastSnap, allSnaps);
  fs.writeFileSync(path.join(RUN_DIR, 'summary.json'), JSON.stringify(summary, null, 2));

  // Time-series CSVs — one per histogram, one for RSS, one for rate counters.
  // Easy to import into Excel / Google Sheets / quickly chart.
  for (const [histKey, series] of Object.entries(summary.timeseries || {})) {
    const safeName = histKey.split('|')[0].replace(/[^a-z0-9_]+/gi, '_');
    const csv = ['t_sec,count,p50_ms,p95_ms,p99_ms,p999_ms'];
    for (const p of series) csv.push(`${p.t_sec_from_start.toFixed(1)},${p.count},${fmtCsv(p.p50_ms)},${fmtCsv(p.p95_ms)},${fmtCsv(p.p99_ms)},${fmtCsv(p.p999_ms)}`);
    fs.writeFileSync(path.join(RUN_DIR, `timeseries-${safeName}.csv`), csv.join('\n'));
  }
  if (summary.rss_timeseries) {
    const csv = ['t_sec,rss_bytes'];
    for (const p of summary.rss_timeseries) csv.push(`${p.t.toFixed(1)},${p.rss}`);
    fs.writeFileSync(path.join(RUN_DIR, 'timeseries-rss.csv'), csv.join('\n'));
  }
  if (summary.doc_cache_hit_ratio_timeseries) {
    const csv = ['t_sec,hit_ratio_pct'];
    for (const p of summary.doc_cache_hit_ratio_timeseries) csv.push(`${p.t_sec_from_start.toFixed(1)},${fmtCsv(p.ratio_pct)}`);
    fs.writeFileSync(path.join(RUN_DIR, 'timeseries-doc-cache-hit-ratio.csv'), csv.join('\n'));
  }
  for (const [k, series] of Object.entries(summary.rate_timeseries || {})) {
    const safeName = k.replace(/[^a-z0-9_]+/gi, '_');
    const csv = ['t_sec,rate_per_sec'];
    for (const p of series) csv.push(`${p.t_sec_from_start.toFixed(1)},${fmtCsv(p.rate_per_sec)}`);
    fs.writeFileSync(path.join(RUN_DIR, `timeseries-rate-${safeName}.csv`), csv.join('\n'));
  }

  // Final traces snapshot for gate eval.
  let finalTraces = [];
  try { finalTraces = await captureTraces(); } catch {}

  const gateResults = evalGates(summary, finalTraces, GATES);
  const fails = gateResults.filter(r => r.pass === false);
  const skipped = gateResults.filter(r => r.pass === null);

  // Markdown summary.
  const md = renderSummaryMd({ meta, summary, gateResults, fails, skipped });
  fs.writeFileSync(path.join(RUN_DIR, 'summary.md'), md);
  console.log('\n' + md);

  // Diff vs baseline.
  if (BASELINE) {
    const baseSummaryPath = path.join('local-prom', 'runs', BASELINE, 'summary.json');
    if (fs.existsSync(baseSummaryPath)) {
      const baseSummary = JSON.parse(fs.readFileSync(baseSummaryPath, 'utf8'));
      const diffMd = renderDiffMd(baseSummary, summary, BASELINE);
      fs.writeFileSync(path.join(RUN_DIR, `diff-vs-${BASELINE}.md`), diffMd);
      console.log(`\n=== DIFF vs ${BASELINE} ===\n`);
      console.log(diffMd);
    } else {
      console.warn(`baseline summary not found: ${baseSummaryPath}`);
    }
  }

  process.exit(fails.length > 0 ? 1 : 0);
}

function renderSummaryMd({ meta, summary, gateResults, fails, skipped }) {
  const lines = [];
  lines.push(`# Phase Validation Run — ${meta.label}`);
  lines.push('');
  lines.push(`- Started: ${meta.started_at}`);
  lines.push(`- Duration: ${(summary.duration_sec / 60).toFixed(1)} min`);
  lines.push(`- Snapshots: ${summary.snapshots}`);
  lines.push(`- Git rev: ${meta.git_rev}`);
  lines.push(`- Probe: ${meta.probe}`);
  lines.push(`- Gates: ${meta.gates.join(', ')}`);
  lines.push('');
  lines.push('## Histogram quantiles — overall (cumulative)');
  lines.push('');
  lines.push('Long-window view. Coarse signal — smooths spikes. Use spike summary below for spike detection.');
  lines.push('');
  lines.push('| Histogram | P50 ms | P95 ms | P99 ms | P99.9 ms |');
  lines.push('|---|---|---|---|---|');
  for (const [k, v] of Object.entries(summary.histograms)) {
    lines.push(`| ${k.split('|')[0]} | ${fmt(v.p50_ms)} | ${fmt(v.p95_ms)} | ${fmt(v.p99_ms)} | ${fmt(v.p999_ms)} |`);
  }
  lines.push('');
  lines.push('## Spike summary — per-interval extremes');
  lines.push('');
  lines.push('Catches transient P99 spikes that the cumulative view masks. Each interval ≈ ' + meta.interval_sec + 's.');
  lines.push('');
  lines.push('| Histogram | Max P50 | Max P95 | Max P99 | Max P99.9 | Intervals >100ms | >500ms | >1000ms | Longest contig >1s (intervals) |');
  lines.push('|---|---|---|---|---|---|---|---|---|');
  for (const [k, sp] of Object.entries(summary.spikes || {})) {
    lines.push(`| ${k.split('|')[0]} | ${fmt(sp.p50_ms.max)} | ${fmt(sp.p95_ms.max)} | ${fmt(sp.p99_ms.max)} | ${fmt(sp.p999_ms.max)} | ${sp.p99_ms.count_over_100ms} | ${sp.p99_ms.count_over_500ms} | ${sp.p99_ms.count_over_1000ms} | ${sp.p99_ms.longest_contig_over_1000ms} |`);
  }
  lines.push('');
  lines.push('## Trend (P99 slope over the run)');
  lines.push('');
  lines.push('Linear regression of per-interval P99 vs time. Slope > 0 = drifting up (potential leak / saturation).');
  lines.push('');
  lines.push('| Histogram | Slope (ms/min) | R² |');
  lines.push('|---|---|---|');
  for (const [k, t] of Object.entries(summary.trends || {})) {
    lines.push(`| ${k.split('|')[0]} | ${fmt((t.slope_per_sec || 0) * 60)} | ${fmt(t.r2)} |`);
  }
  lines.push('');
  lines.push('## RSS time-series');
  lines.push('');
  lines.push(`- Start: ${fmt(summary.scalars[Object.keys(summary.scalars).find(k => k.startsWith('process_resident_memory_bytes')) || '']?.start || 0)} bytes`);
  lines.push(`- End: ${fmt(summary.scalars[Object.keys(summary.scalars).find(k => k.startsWith('process_resident_memory_bytes')) || '']?.end || 0)} bytes`);
  lines.push(`- Max: ${fmt(summary.rss_max || 0)} bytes`);
  lines.push(`- Growth: ${(summary.rss_growth_pct_per_min || 0).toFixed(2)} % / min`);
  lines.push(`- Trend slope: ${fmt((summary.rss_trend?.slope_per_sec || 0) * 60)} bytes/min (R²=${fmt(summary.rss_trend?.r2 || 0)})`);
  lines.push('');
  lines.push('## Counter rate spikes');
  lines.push('');
  lines.push('Per-interval rate-of-change. Catches sudden bursts (cache thrash, eviction storms, error rate jumps).');
  lines.push('');
  lines.push('| Counter | Mean rate/sec | Max rate/sec | Max:Mean |');
  lines.push('|---|---|---|---|');
  for (const [k, sp] of Object.entries(summary.rate_spikes || {})) {
    lines.push(`| ${k} | ${fmt(sp.mean)} | ${fmt(sp.max)} | ${fmt(sp.ratio_max_to_mean)} |`);
  }
  lines.push('');
  lines.push('## Key scalar deltas');
  lines.push('');
  lines.push('| Metric | Start | End | Delta |');
  lines.push('|---|---|---|---|');
  const keyMetrics = Object.keys(summary.scalars).sort();
  for (const k of keyMetrics) {
    const v = summary.scalars[k];
    lines.push(`| ${k} | ${fmt(v.start)} | ${fmt(v.end)} | ${fmt(v.delta)} |`);
  }
  lines.push('');
  lines.push(`- Doc cache hit ratio overall: ${(summary.doc_cache_hit_ratio_pct || 0).toFixed(2)} %`);
  lines.push(`- Doc cache hit ratio min interval: ${summary.doc_cache_hit_ratio_min === null ? 'n/a' : summary.doc_cache_hit_ratio_min.toFixed(2) + ' %'}`);
  lines.push('');
  lines.push('## Gate results');
  lines.push('');
  lines.push('| Gate | Metric | Rule | Actual | Status |');
  lines.push('|---|---|---|---|---|');
  for (const r of gateResults) {
    const status = r.pass === true ? 'PASS' : r.pass === false ? 'FAIL' : 'SKIP';
    lines.push(`| ${r.gate} | ${r.metric} | ${JSON.stringify(r.rule)} | ${fmt(r.actual)} | ${status} |`);
  }
  lines.push('');
  lines.push(`**Verdict**: ${fails.length === 0 ? 'PASS' : `FAIL (${fails.length} regressions)`}`);
  if (fails.length) {
    lines.push('');
    lines.push('### Failing gates');
    for (const f of fails) lines.push(`- ${f.gate} :: ${f.metric} = ${fmt(f.actual)} violates ${JSON.stringify(f.rule)}`);
  }
  if (skipped.length) {
    lines.push('');
    lines.push(`### Skipped (metric missing): ${skipped.length}`);
    for (const s of skipped.slice(0, 10)) lines.push(`- ${s.gate} :: ${s.metric}`);
  }
  return lines.join('\n');
}

function renderDiffMd(base, cur, baseLabel) {
  const lines = [];
  lines.push(`# Diff: ${cur.label || 'current'} vs ${baseLabel}`);
  lines.push('');
  lines.push('## Cumulative histogram quantiles');
  lines.push('');
  lines.push('| Metric | Quantile | Baseline ms | Current ms | Δ % | Flag |');
  lines.push('|---|---|---|---|---|---|');
  for (const k of Object.keys(cur.histograms)) {
    const b = base.histograms[k];
    const c = cur.histograms[k];
    if (!b) continue;
    for (const q of ['p50_ms', 'p95_ms', 'p99_ms', 'p999_ms']) {
      const bv = b[q], cv = c[q];
      if (bv == null || cv == null) continue;
      const pct = bv > 0 ? ((cv - bv) / bv) * 100 : 0;
      const flag = pct > 10 ? 'REGRESS' : pct < -10 ? 'IMPROVE' : '';
      lines.push(`| ${k.split('|')[0]} | ${q} | ${fmt(bv)} | ${fmt(cv)} | ${pct.toFixed(1)} | ${flag} |`);
    }
  }
  lines.push('');
  lines.push('## Spike comparison (per-interval extremes)');
  lines.push('');
  lines.push('| Metric | Quantile | Baseline max | Current max | Δ % | Baseline intervals >1s | Current intervals >1s | Flag |');
  lines.push('|---|---|---|---|---|---|---|---|');
  for (const k of Object.keys(cur.spikes || {})) {
    const b = (base.spikes || {})[k];
    const c = cur.spikes[k];
    if (!b) continue;
    for (const q of ['p99_ms', 'p999_ms']) {
      const bv = b[q]?.max || 0, cv = c[q]?.max || 0;
      const pct = bv > 0 ? ((cv - bv) / bv) * 100 : (cv > 0 ? 100 : 0);
      const baseIntOver1k = q === 'p99_ms' ? b[q].count_over_1000ms : 'n/a';
      const curIntOver1k = q === 'p99_ms' ? c[q].count_over_1000ms : 'n/a';
      const flag = (q === 'p99_ms' && curIntOver1k > baseIntOver1k) ? 'SPIKE-REGRESS' : pct > 10 ? 'REGRESS' : pct < -10 ? 'IMPROVE' : '';
      lines.push(`| ${k.split('|')[0]} | ${q} | ${fmt(bv)} | ${fmt(cv)} | ${pct.toFixed(1)} | ${baseIntOver1k} | ${curIntOver1k} | ${flag} |`);
    }
  }
  lines.push('');
  lines.push('## Trend slope comparison (P99 ms/min)');
  lines.push('');
  lines.push('| Metric | Baseline slope | Current slope | Flag |');
  lines.push('|---|---|---|---|');
  for (const k of Object.keys(cur.trends || {})) {
    const b = (base.trends || {})[k];
    const c = cur.trends[k];
    if (!b) continue;
    const bSlope = (b.slope_per_sec || 0) * 60;
    const cSlope = (c.slope_per_sec || 0) * 60;
    const flag = cSlope > 1 && cSlope > bSlope ? 'DRIFT-UP' : '';
    lines.push(`| ${k.split('|')[0]} | ${fmt(bSlope)} | ${fmt(cSlope)} | ${flag} |`);
  }
  lines.push('');
  lines.push('## Scalar deltas');
  lines.push('');
  lines.push('| Metric | Baseline Δ | Current Δ | Δ ratio | Flag |');
  lines.push('|---|---|---|---|---|');
  for (const k of Object.keys(cur.scalars)) {
    const b = base.scalars[k];
    const c = cur.scalars[k];
    if (!b) continue;
    const ratio = b.delta !== 0 ? c.delta / b.delta : null;
    const flag = ratio !== null && (ratio > 1.5 || ratio < 0.66) ? 'CHANGED' : '';
    lines.push(`| ${k} | ${fmt(b.delta)} | ${fmt(c.delta)} | ${ratio === null ? 'n/a' : ratio.toFixed(2)} | ${flag} |`);
  }
  return lines.join('\n');
}

function fmtCsv(n) {
  if (n === null || n === undefined || !Number.isFinite(n)) return '';
  return String(n);
}

function fmt(n) {
  if (n === null || n === undefined) return '—';
  if (typeof n !== 'number') return String(n);
  if (!Number.isFinite(n)) return String(n);
  if (Math.abs(n) >= 1e6) return n.toExponential(2);
  if (Math.abs(n) >= 100) return n.toFixed(0);
  if (Math.abs(n) >= 1) return n.toFixed(2);
  return n.toFixed(4);
}

main().catch(e => { console.error(e); process.exit(1); });
