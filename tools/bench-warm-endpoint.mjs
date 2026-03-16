#!/usr/bin/env node
/**
 * Benchmark: Cache warming endpoint effectiveness.
 *
 * Proves that POST /api/indexes/{name}/warm eliminates cold miss latency
 * for pre-warmed query patterns. Measures before/after latency for
 * multiple query combinations.
 *
 * Usage: node tools/bench-warm-endpoint.mjs
 *
 * Requires: BitDex server running on port 3001 with the civitai index loaded.
 */

const BASE = 'http://localhost:3001';

const QUERIES = [
  {
    name: 'nsfwLevel[1,2] + isPublished + type[image] → reactionCount Desc',
    filters: [
      { In: ['nsfwLevel', [{ Integer: 1 }, { Integer: 2 }]] },
      { Eq: ['isPublished', { Bool: true }] },
      { In: ['type', [{ String: 'image' }]] },
    ],
    sort: { field: 'reactionCount', direction: 'Desc' },
  },
  {
    name: 'nsfwLevel=1 → commentCount Desc',
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'commentCount', direction: 'Desc' },
  },
  {
    name: 'nsfwLevel[1,2] + isPublished + type[image] → collectedCount Desc',
    filters: [
      { In: ['nsfwLevel', [{ Integer: 1 }, { Integer: 2 }]] },
      { Eq: ['isPublished', { Bool: true }] },
      { In: ['type', [{ String: 'image' }]] },
    ],
    sort: { field: 'collectedCount', direction: 'Desc' },
  },
];

async function req(method, path, body = null) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE}${path}`, opts);
  return res.json();
}

async function queryLatency(q) {
  const body = { filters: q.filters, sort: q.sort, limit: 50 };
  const result = await req('POST', '/api/indexes/civitai/query', body);
  return result.elapsed_us;
}

async function main() {
  console.log('=== Cache Warming Benchmark ===\n');

  // Verify server
  try {
    await fetch(`${BASE}/api/health`);
  } catch {
    console.error('Server not reachable at ' + BASE);
    process.exit(1);
  }

  // Step 1: Purge all caches
  console.log('Purging all caches (RAM + disk)...');
  await req('DELETE', '/api/indexes/civitai/cache/persistent');

  // Step 2: Measure cold miss latency for each query
  console.log('Measuring cold miss latency...\n');
  const coldResults = [];
  for (const q of QUERIES) {
    const us = await queryLatency(q);
    coldResults.push(us);
    console.log(`  COLD: ${us.toLocaleString()}μs — ${q.name}`);
  }

  // Step 3: Purge again
  console.log('\nPurging caches again...');
  await req('DELETE', '/api/indexes/civitai/cache/persistent');

  // Step 4: Warm the cache
  console.log('Warming cache via POST /warm...\n');
  const warmBody = {
    queries: QUERIES.map(q => ({ filters: q.filters, sort: q.sort })),
  };
  const warmResult = await req('POST', '/api/indexes/civitai/warm', warmBody);
  for (const r of warmResult.results) {
    console.log(`  WARM: ${r.elapsed_us.toLocaleString()}μs (${r.status}) — ${QUERIES[r.query_index].name}`);
  }

  // Step 5: Measure cache hit latency
  console.log('\nMeasuring post-warm latency...\n');
  const warmHitResults = [];
  for (const q of QUERIES) {
    // Run twice, take second (first might have shard overhead)
    await queryLatency(q);
    const us = await queryLatency(q);
    warmHitResults.push(us);
    console.log(`  HIT:  ${us.toLocaleString()}μs — ${q.name}`);
  }

  // Summary
  console.log('\n=== Results ===\n');
  console.log('| Query | Cold Miss | Warm Hit | Speedup |');
  console.log('|-------|-----------|----------|---------|');
  for (let i = 0; i < QUERIES.length; i++) {
    const cold = coldResults[i];
    const hit = warmHitResults[i];
    const speedup = (cold / hit).toFixed(0);
    console.log(`| ${QUERIES[i].name.slice(0, 50)} | ${cold.toLocaleString()}μs | ${hit.toLocaleString()}μs | ${speedup}x |`);
  }

  // Pass/fail
  const allFaster = warmHitResults.every((hit, i) => hit < coldResults[i] * 0.1);
  if (allFaster) {
    console.log('\nPASS: All warmed queries are >10x faster than cold misses.');
  } else {
    console.log('\nWARN: Some warmed queries did not achieve 10x speedup.');
  }
}

main().catch(e => { console.error(e); process.exit(1); });
