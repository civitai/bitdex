#!/usr/bin/env node
// Re-process an existing phase-validate metrics.ndjson with the FIXED
// per-interval quantile logic. Use after fixing a harness bug to get
// corrected numbers without re-running the rig.
//
// Usage:
//   node scripts/reanalyze-run.mjs <run-dir>
//
// Reads metrics.ndjson + meta.json, writes summary-corrected.json + .md.

import fs from 'node:fs';
import path from 'node:path';

const RUN_DIR = process.argv[2];
if (!RUN_DIR) {
  console.error('Usage: reanalyze-run.mjs <run-dir>');
  process.exit(2);
}

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

function diffBuckets(prev, curr) {
  const prevByLe = new Map(prev.map(b => [b.le, b.count]));
  return curr.map(b => ({ le: b.le, count: b.count - (prevByLe.get(b.le) || 0) }));
}

const metricsPath = path.join(RUN_DIR, 'metrics.ndjson');
const lines = fs.readFileSync(metricsPath, 'utf8').split('\n').filter(Boolean);
const snaps = lines.map(l => JSON.parse(l));
console.log(`Loaded ${snaps.length} snapshots from ${metricsPath}`);

const firstSnap = snaps[0];
const lastSnap = snaps[snaps.length - 1];

const histKeys = Object.keys(lastSnap.hist_buckets || {});
console.log(`Histograms: ${histKeys.map(k => k.split('|')[0]).join(', ')}`);

const out = { duration_sec: lastSnap.ts - firstSnap.ts, snapshots: snaps.length, histograms: {}, timeseries: {}, spikes: {} };

// Cumulative quantile from last snapshot.
for (const k of histKeys) {
  const buckets = lastSnap.hist_buckets[k];
  out.histograms[k] = {
    p50_ms: (histQuantile(buckets, 0.50) || 0) * 1000,
    p95_ms: (histQuantile(buckets, 0.95) || 0) * 1000,
    p99_ms: (histQuantile(buckets, 0.99) || 0) * 1000,
    p999_ms: (histQuantile(buckets, 0.999) || 0) * 1000,
  };
}

// Per-interval (CORRECTED — no re-cumulation).
for (const histKey of histKeys) {
  const series = [];
  for (let i = 1; i < snaps.length; i++) {
    const prev = snaps[i - 1].hist_buckets[histKey];
    const curr = snaps[i].hist_buckets[histKey];
    if (!prev || !curr) continue;
    const intervalCum = diffBuckets(prev, curr);
    intervalCum.sort((a, b) => a.le - b.le);
    const total = intervalCum.length ? intervalCum[intervalCum.length - 1].count : 0;
    const q50 = histQuantile(intervalCum, 0.50);
    const q95 = histQuantile(intervalCum, 0.95);
    const q99 = histQuantile(intervalCum, 0.99);
    const q999 = histQuantile(intervalCum, 0.999);
    series.push({
      t: snaps[i].ts - firstSnap.ts,
      count: total,
      p50_ms: q50 === null ? null : q50 * 1000,
      p95_ms: q95 === null ? null : q95 * 1000,
      p99_ms: q99 === null ? null : q99 * 1000,
      p999_ms: q999 === null ? null : q999 * 1000,
    });
  }
  out.timeseries[histKey] = series;

  const spike = { p50_ms: { max: 0 }, p95_ms: { max: 0 }, p99_ms: { max: 0, count_over_100ms: 0, count_over_500ms: 0, count_over_1000ms: 0, longest_contig_over_1000ms: 0 }, p999_ms: { max: 0 } };
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
      } else { contigOver1k = 0; }
    }
  }
  spike.p99_ms.longest_contig_over_1000ms = longestOver1k;
  out.spikes[histKey] = spike;
}

fs.writeFileSync(path.join(RUN_DIR, 'summary-corrected.json'), JSON.stringify(out, null, 2));

// Markdown report.
const md = [];
md.push(`# Re-analysis: ${path.basename(RUN_DIR)}`);
md.push('');
md.push(`Re-processed ${snaps.length} snapshots over ${(out.duration_sec/60).toFixed(1)} min using fixed per-interval quantile logic (no re-cumulation bug).`);
md.push('');
md.push('## Cumulative quantiles');
md.push('');
md.push('| Histogram | P50 | P95 | P99 | P99.9 |');
md.push('|---|---|---|---|---|');
for (const [k, v] of Object.entries(out.histograms)) {
  md.push(`| ${k.split('|')[0]} | ${v.p50_ms.toFixed(2)} ms | ${v.p95_ms.toFixed(2)} ms | ${v.p99_ms.toFixed(2)} ms | ${v.p999_ms.toFixed(2)} ms |`);
}
md.push('');
md.push('## Per-interval spike summary (corrected)');
md.push('');
md.push('| Histogram | Max P50 | Max P95 | Max P99 | Max P99.9 | >100ms | >500ms | >1000ms | Longest contig >1s |');
md.push('|---|---|---|---|---|---|---|---|---|');
for (const [k, sp] of Object.entries(out.spikes)) {
  md.push(`| ${k.split('|')[0]} | ${sp.p50_ms.max.toFixed(2)} | ${sp.p95_ms.max.toFixed(2)} | ${sp.p99_ms.max.toFixed(2)} | ${sp.p999_ms.max.toFixed(2)} | ${sp.p99_ms.count_over_100ms} | ${sp.p99_ms.count_over_500ms} | ${sp.p99_ms.count_over_1000ms} | ${sp.p99_ms.longest_contig_over_1000ms} |`);
}
md.push('');
md.push('## Top 10 worst intervals (by P99) per histogram');
md.push('');
for (const [k, series] of Object.entries(out.timeseries)) {
  md.push(`### ${k.split('|')[0]}`);
  md.push('');
  const sorted = series.slice().filter(p => p.p99_ms !== null).sort((a,b) => b.p99_ms - a.p99_ms).slice(0, 10);
  md.push('| t (s) | count | P50 ms | P95 ms | P99 ms | P99.9 ms |');
  md.push('|---|---|---|---|---|---|');
  for (const p of sorted) md.push(`| ${p.t.toFixed(1)} | ${p.count} | ${p.p50_ms?.toFixed(2)} | ${p.p95_ms?.toFixed(2)} | ${p.p99_ms?.toFixed(2)} | ${p.p999_ms?.toFixed(2)} |`);
  md.push('');
}

fs.writeFileSync(path.join(RUN_DIR, 'summary-corrected.md'), md.join('\n'));
console.log(`Wrote summary-corrected.{json,md} to ${RUN_DIR}`);

// Also write corrected per-histogram CSVs.
for (const [histKey, series] of Object.entries(out.timeseries)) {
  const safeName = histKey.split('|')[0].replace(/[^a-z0-9_]+/gi, '_');
  const csv = ['t_sec,count,p50_ms,p95_ms,p99_ms,p999_ms'];
  for (const p of series) csv.push(`${p.t.toFixed(1)},${p.count},${p.p50_ms ?? ''},${p.p95_ms ?? ''},${p.p99_ms ?? ''},${p.p999_ms ?? ''}`);
  fs.writeFileSync(path.join(RUN_DIR, `timeseries-corrected-${safeName}.csv`), csv.join('\n'));
}
console.log('Wrote timeseries-corrected-*.csv');
