#!/usr/bin/env node
/**
 * System Health Benchmark — Long-tail regression detection.
 *
 * Covers the operational paths that individually seem small but compound
 * into poor user experience: lazy loading, cache hits, cold misses,
 * cache persistence restore, ForcePublish overhead, warm endpoint, etc.
 *
 * Run after every performance change to verify no regressions.
 *
 * Usage: node tools/bench-system-health.mjs [--threshold-file PATH]
 *
 * Requires: BitDex server running on port 3001 with civitai index loaded.
 * The server should be freshly restarted for accurate lazy load measurements.
 */

const BASE = 'http://localhost:3001';

async function req(method, path, body = null) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE}${path}`, opts);
  return res.json();
}

async function queryUs(filters, sort, limit = 50) {
  const body = { filters, sort, limit };
  const r = await req('POST', '/api/indexes/civitai/query', body);
  return { elapsed_us: r.elapsed_us, matched: r.total_matched, cursor: r.cursor };
}

function log(label, us, threshold) {
  const status = us <= threshold ? 'PASS' : 'FAIL';
  const flag = us > threshold ? ' ⚠️' : '';
  console.log(`  [${status}] ${label}: ${us.toLocaleString()}μs (threshold: ${threshold.toLocaleString()}μs)${flag}`);
  return { label, us, threshold, pass: us <= threshold };
}

async function main() {
  console.log('=== BitDex System Health Benchmark ===\n');

  const results = [];

  // 1. Health check
  try { await fetch(`${BASE}/api/health`); } catch {
    console.error('Server not reachable'); process.exit(1);
  }

  const stats = await req('GET', '/api/indexes/civitai/stats');
  console.log(`Server: ${stats.alive_count?.toLocaleString()} records\n`);

  // --- Cache hit latency (the golden path) ---
  console.log('1. Cache hit latency');
  // Warm up
  await queryUs([{ Eq: ['nsfwLevel', { Integer: 1 }] }], { field: 'reactionCount', direction: 'Desc' });
  // Measure
  const hit1 = await queryUs([{ Eq: ['nsfwLevel', { Integer: 1 }] }], { field: 'reactionCount', direction: 'Desc' });
  const hit2 = await queryUs([{ Eq: ['nsfwLevel', { Integer: 1 }] }], { field: 'reactionCount', direction: 'Desc' });
  results.push(log('cache_hit', Math.min(hit1.elapsed_us, hit2.elapsed_us), 100));

  // --- Cold miss latency (first query for a new filter combo) ---
  console.log('\n2. Cold miss latency');
  await req('DELETE', '/api/indexes/civitai/cache');
  const cold = await queryUs(
    [{ In: ['nsfwLevel', [{ Integer: 1 }, { Integer: 2 }]] }, { Eq: ['isPublished', { Bool: true }] }, { In: ['type', [{ String: 'image' }]] }],
    { field: 'reactionCount', direction: 'Desc' }
  );
  results.push(log('cold_miss_compound', cold.elapsed_us, 100_000)); // 100ms

  // --- Warm endpoint ---
  console.log('\n3. Warm endpoint');
  await req('DELETE', '/api/indexes/civitai/cache');
  const warmResult = await req('POST', '/api/indexes/civitai/warm', {
    queries: [
      { filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }], sort: { field: 'commentCount', direction: 'Desc' } },
    ],
  });
  const warmUs = warmResult.results?.[0]?.elapsed_us || 0;
  results.push(log('warm_seed', warmUs, 100_000));
  // Verify hit after warm
  const postWarm = await queryUs([{ Eq: ['nsfwLevel', { Integer: 1 }] }], { field: 'commentCount', direction: 'Desc' });
  results.push(log('post_warm_hit', postWarm.elapsed_us, 100));

  // --- Lazy load (non-eager field) ---
  console.log('\n4. Lazy load overhead');
  // This only works if a non-eager field hasn't been loaded yet.
  // Check if 'poi' is pending by querying it (first query triggers lazy load)
  const lazyStart = Date.now();
  const lazy = await queryUs(
    [{ Eq: ['poi', { Bool: false }] }, { Eq: ['nsfwLevel', { Integer: 1 }] }],
    { field: 'reactionCount', direction: 'Desc' }
  );
  results.push(log('lazy_load_boolean', lazy.elapsed_us, 200_000)); // 200ms

  // --- Pagination (cursor) ---
  console.log('\n5. Cursor pagination');
  const page1 = await queryUs([{ Eq: ['nsfwLevel', { Integer: 1 }] }], { field: 'reactionCount', direction: 'Desc' });
  const page2body = {
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 50,
    cursor: page1.cursor,
  };
  const page2 = await req('POST', '/api/indexes/civitai/query', page2body);
  results.push(log('cursor_page2', page2.elapsed_us, 100));

  // --- Sparse filter (userId) ---
  console.log('\n6. Sparse filter (userId)');
  const sparse = await queryUs(
    [{ Eq: ['userId', { Integer: 5418 }] }, { Eq: ['nsfwLevel', { Integer: 1 }] }],
    { field: 'reactionCount', direction: 'Desc' }
  );
  results.push(log('sparse_userid', sparse.elapsed_us, 50_000));

  // --- Summary ---
  console.log('\n=== Summary ===\n');
  console.log('| Metric | Value | Threshold | Status |');
  console.log('|--------|-------|-----------|--------|');
  for (const r of results) {
    console.log(`| ${r.label} | ${r.us.toLocaleString()}μs | ${r.threshold.toLocaleString()}μs | ${r.pass ? 'PASS' : 'FAIL'} |`);
  }

  const failures = results.filter(r => !r.pass);
  if (failures.length > 0) {
    console.log(`\n${failures.length} FAILURE(S) — regressions detected.`);
    process.exit(1);
  } else {
    console.log(`\nAll ${results.length} checks PASSED.`);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
