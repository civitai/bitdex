#!/usr/bin/env node
/**
 * BitDex Write-Path Stress Test
 *
 * Pushes cache entry count from 0 to 1M to find breaking points.
 * At each level: measures write throughput, concurrent write scaling,
 * and verifies cache correctness after high-throughput mutations.
 *
 * Key questions answered:
 *   1. At what cache count do writes become unacceptably slow (>200ms)?
 *   2. Are cached results still correct after thousands of writes/sec?
 *   3. How many concurrent writers can we sustain?
 *   4. What's the memory cost of N cache entries?
 *
 * Usage:
 *   node tests/e2e/bench-write-stress.mjs [--url http://localhost:3100] [--verbose]
 *   node tests/e2e/bench-write-stress.mjs --max-caches 100000   # stop at 100K
 *   node tests/e2e/bench-write-stress.mjs --skip-to 10000       # skip to 10K level
 */

import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PROJECT_ROOT = resolve(__dirname, '..', '..');

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3100';
const VERBOSE = process.argv.includes('--verbose');
const MAX_CACHES = Number(process.argv.includes('--max-caches')
  ? process.argv[process.argv.indexOf('--max-caches') + 1]
  : '1000000');
const SKIP_TO = Number(process.argv.includes('--skip-to')
  ? process.argv[process.argv.indexOf('--skip-to') + 1]
  : '0');

const INDEX = 'write-stress';
const DOC_COUNT = 5000; // enough docs for diverse filter results

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [v]', ...args); }

// ── HTTP helpers ──

async function apiPost(path, body) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  try { return { status: res.status, data: JSON.parse(text) }; }
  catch { return { status: res.status, data: { _raw: text } }; }
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  const text = await res.text();
  try { return { status: res.status, data: JSON.parse(text) }; }
  catch { return { status: res.status, data: { _raw: text } }; }
}

async function apiDelete(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const text = await res.text();
  try { return { status: res.status, data: JSON.parse(text) }; }
  catch { return { status: res.status, data: { _raw: text } }; }
}

const getStats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const doQuery = (filters, sort, limit = 20) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, sort, limit }).then(r => r.data);
const doUpsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const clearCache = () => apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForFlush(expectedAlive, maxWaitMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await getStats();
    if (s.alive_count >= expectedAlive) return s;
    await sleep(20);
  }
  return await getStats();
}

async function waitForCycle(startCycle, maxWaitMs = 10000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await getStats();
    if (s.flush_cycle > startCycle) return s;
    await sleep(5);
  }
  return await getStats();
}

function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  const idx = Math.ceil(sorted.length * p / 100) - 1;
  return sorted[Math.max(0, idx)];
}

function fmtStats(latencies) {
  const sorted = [...latencies].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    count: sorted.length,
    mean: +(sum / sorted.length).toFixed(2),
    p50: +percentile(sorted, 50).toFixed(2),
    p95: +percentile(sorted, 95).toFixed(2),
    p99: +percentile(sorted, 99).toFixed(2),
    min: +sorted[0].toFixed(2),
    max: +sorted[sorted.length - 1].toFixed(2),
  };
}

function fmtMs(ms) { return ms < 1 ? `${(ms * 1000).toFixed(0)}μs` : `${ms.toFixed(1)}ms`; }
function fmtBytes(b) {
  if (b < 1024) return `${b}B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)}KB`;
  return `${(b / (1024 * 1024)).toFixed(1)}MB`;
}

// ── Setup ──

async function setup() {
  log('\n=== Setup ===');

  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(300);
  }

  // High-cardinality fields to enable millions of unique cache keys
  // category(1-50) × priority(1-20) × region(1-20) × tier(1-10) × active(2) × 3 sorts = 1.2M+
  const config = {
    filter_fields: [
      { name: 'category', field_type: 'single_value' },
      { name: 'priority', field_type: 'single_value' },
      { name: 'region', field_type: 'single_value' },
      { name: 'tier', field_type: 'single_value' },
      { name: 'active', field_type: 'boolean' },
      { name: 'tagIds', field_type: 'multi_value' },
    ],
    sort_fields: [
      { name: 'score', bits: 32 },
      { name: 'createdAt', bits: 32 },
      { name: 'viewCount', bits: 32 },
    ],
    flush_interval_us: 50,
    max_page_size: 200,
  };

  const data_schema = {
    id_field: 'id',
    fields: [
      { source: 'category', target: 'category', value_type: 'integer' },
      { source: 'priority', target: 'priority', value_type: 'integer' },
      { source: 'region', target: 'region', value_type: 'integer' },
      { source: 'tier', target: 'tier', value_type: 'integer' },
      { source: 'active', target: 'active', value_type: 'boolean' },
      { source: 'tagIds', target: 'tagIds', value_type: 'integer_array' },
      { source: 'score', target: 'score', value_type: 'integer' },
      { source: 'createdAt', target: 'createdAt', value_type: 'integer' },
      { source: 'viewCount', target: 'viewCount', value_type: 'integer' },
    ],
  };

  const createResult = await apiPost('/api/indexes', { name: INDEX, config, data_schema });
  if (createResult.status !== 200 && createResult.status !== 201) {
    throw new Error(`Failed to create index: ${JSON.stringify(createResult.data)}`);
  }

  // Insert documents in batches
  log(`  Inserting ${DOC_COUNT} documents...`);
  const BATCH_SIZE = 500;
  for (let i = 0; i < DOC_COUNT; i += BATCH_SIZE) {
    const batch = [];
    for (let j = i; j < Math.min(i + BATCH_SIZE, DOC_COUNT); j++) {
      const id = j + 1;
      batch.push({
        id,
        category: (id % 50) + 1,
        priority: (id % 20) + 1,
        region: (id % 20) + 1,
        tier: (id % 10) + 1,
        active: id % 3 !== 0,
        tagIds: [id % 100, (id % 100) + 100],
        score: Math.floor(Math.random() * 1000000),
        createdAt: 1700000000 + id * 60,
        viewCount: Math.floor(Math.random() * 500000),
      });
    }
    await doUpsert(batch);
  }
  const s = await waitForFlush(DOC_COUNT);
  log(`  ${s.alive_count} documents alive`);
}

// ── Cache Population ──

/**
 * Populate cache entries by firing queries with increasing field combinations.
 * Uses a generator approach to efficiently create large numbers of unique keys.
 */
async function populateCachesTo(targetEntries) {
  const SORT_CONFIGS = [
    { field: 'score', direction: 'Desc' },
    { field: 'score', direction: 'Asc' },
    { field: 'createdAt', direction: 'Desc' },
    { field: 'viewCount', direction: 'Desc' },
  ];

  let totalPopulated = 0;
  const batchSize = 50; // concurrent query batch size

  // Generator: yields filter clause arrays in increasing complexity
  function* filterGenerator() {
    // 1-field: category(50)
    for (let c = 1; c <= 50; c++)
      yield [{ Eq: ['category', { Integer: c }] }];
    // 1-field: priority(20)
    for (let p = 1; p <= 20; p++)
      yield [{ Eq: ['priority', { Integer: p }] }];
    // 1-field: region(20)
    for (let r = 1; r <= 20; r++)
      yield [{ Eq: ['region', { Integer: r }] }];
    // 2-field: category × priority (1000)
    for (let c = 1; c <= 50; c++)
      for (let p = 1; p <= 20; p++)
        yield [{ Eq: ['category', { Integer: c }] }, { Eq: ['priority', { Integer: p }] }];
    // 2-field: category × region (1000)
    for (let c = 1; c <= 50; c++)
      for (let r = 1; r <= 20; r++)
        yield [{ Eq: ['category', { Integer: c }] }, { Eq: ['region', { Integer: r }] }];
    // 2-field: priority × region (400)
    for (let p = 1; p <= 20; p++)
      for (let r = 1; r <= 20; r++)
        yield [{ Eq: ['priority', { Integer: p }] }, { Eq: ['region', { Integer: r }] }];
    // 3-field: category × priority × region (20000)
    for (let c = 1; c <= 50; c++)
      for (let p = 1; p <= 20; p++)
        for (let r = 1; r <= 20; r++)
          yield [{ Eq: ['category', { Integer: c }] }, { Eq: ['priority', { Integer: p }] }, { Eq: ['region', { Integer: r }] }];
    // 3-field: category × priority × tier (10000)
    for (let c = 1; c <= 50; c++)
      for (let p = 1; p <= 20; p++)
        for (let t = 1; t <= 10; t++)
          yield [{ Eq: ['category', { Integer: c }] }, { Eq: ['priority', { Integer: p }] }, { Eq: ['tier', { Integer: t }] }];
    // 4-field: category × priority × region × tier (200000)
    for (let c = 1; c <= 50; c++)
      for (let p = 1; p <= 20; p++)
        for (let r = 1; r <= 20; r++)
          for (let t = 1; t <= 10; t++)
            yield [{ Eq: ['category', { Integer: c }] }, { Eq: ['priority', { Integer: p }] }, { Eq: ['region', { Integer: r }] }, { Eq: ['tier', { Integer: t }] }];
    // 5-field: add active (400000)
    for (let c = 1; c <= 50; c++)
      for (let p = 1; p <= 20; p++)
        for (let r = 1; r <= 20; r++)
          for (let t = 1; t <= 10; t++)
            for (let a = 0; a <= 1; a++)
              yield [{ Eq: ['category', { Integer: c }] }, { Eq: ['priority', { Integer: p }] }, { Eq: ['region', { Integer: r }] }, { Eq: ['tier', { Integer: t }] }, { Eq: ['active', { Bool: a === 1 }] }];
  }

  const gen = filterGenerator();
  let sortIdx = 0;
  const startTime = Date.now();
  let lastLog = startTime;

  while (totalPopulated < targetEntries) {
    // Fire a batch of queries concurrently
    const promises = [];
    for (let i = 0; i < batchSize && totalPopulated + promises.length < targetEntries; i++) {
      const { value: filters, done } = gen.next();
      if (done) {
        // Exhausted all combos — cycle through sorts with same filters
        break;
      }
      const sort = SORT_CONFIGS[sortIdx % SORT_CONFIGS.length];
      sortIdx++;
      promises.push(doQuery(filters, sort));
    }

    if (promises.length === 0) break; // exhausted generator
    await Promise.all(promises);
    totalPopulated += promises.length;

    // Log progress every 5 seconds
    if (Date.now() - lastLog > 5000) {
      const s = await getStats();
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = (totalPopulated / (Date.now() - startTime) * 1000).toFixed(0);
      log(`    ... ${s.unified_cache_entries} entries (${rate}/s, ${elapsed}s elapsed, ${fmtBytes(s.unified_cache_bytes)} cache mem)`);
      lastLog = Date.now();
    }
  }

  const s = await getStats();
  return s;
}

// ── Benchmark Functions ──

/**
 * Measure write throughput: fire upserts for `durationMs` and count ops.
 * Each upsert changes the doc's category (triggers filter cache maintenance).
 */
async function benchWriteThroughput(durationMs, concurrency) {
  const latencies = [];
  let totalOps = 0;
  const start = Date.now();

  const workers = [];
  for (let w = 0; w < concurrency; w++) {
    workers.push((async () => {
      let ops = 0;
      const workerLatencies = [];
      while (Date.now() - start < durationMs) {
        const docId = Math.floor(Math.random() * DOC_COUNT) + 1;
        const t0 = performance.now();
        await doUpsert([{
          id: docId,
          category: Math.floor(Math.random() * 50) + 1,
          priority: (docId % 20) + 1,
          region: (docId % 20) + 1,
          tier: (docId % 10) + 1,
          active: docId % 3 !== 0,
          tagIds: [docId % 100, (docId % 100) + 100],
          score: Math.floor(Math.random() * 1000000),
          createdAt: 1700000000 + docId * 60,
          viewCount: Math.floor(Math.random() * 500000),
        }]);
        workerLatencies.push(performance.now() - t0);
        ops++;
      }
      return { ops, latencies: workerLatencies };
    })());
  }

  const results = await Promise.all(workers);
  for (const r of results) {
    totalOps += r.ops;
    latencies.push(...r.latencies);
  }

  const elapsed = Date.now() - start;
  return {
    total_ops: totalOps,
    duration_ms: elapsed,
    ops_per_sec: Math.round(totalOps / (elapsed / 1000)),
    latency: fmtStats(latencies),
  };
}

/**
 * Correctness check: populate cache, do writes, verify cached results are correct.
 * Compares cache-hit results against a fresh (cache-miss) query.
 */
async function verifyCorrectness(writeCount) {
  const SORT = { field: 'score', direction: 'Desc' };

  // Step 1: Pick a category and populate its cache entry
  const testCat = Math.floor(Math.random() * 50) + 1;
  const filters = [{ Eq: ['category', { Integer: testCat }] }];

  // First query = miss (populates cache)
  await doQuery(filters, SORT, 50);
  // Second query = hit (verify cache works)
  await doQuery(filters, SORT, 50);

  // Step 2: Fire N random writes (some will change category)
  for (let i = 0; i < writeCount; i++) {
    const docId = Math.floor(Math.random() * DOC_COUNT) + 1;
    await doUpsert([{
      id: docId,
      category: Math.floor(Math.random() * 50) + 1, // may move into or out of testCat
      priority: (docId % 20) + 1,
      region: (docId % 20) + 1,
      tier: (docId % 10) + 1,
      active: docId % 3 !== 0,
      tagIds: [docId % 100, (docId % 100) + 100],
      score: Math.floor(Math.random() * 1000000),
      createdAt: 1700000000 + docId * 60,
      viewCount: Math.floor(Math.random() * 500000),
    }]);
  }

  // Wait for all flushes
  await sleep(500);

  // Step 3: Query again — this should be a cache hit with maintained results
  const hitsBefore = (await getStats()).unified_cache_hits;
  const cachedResult = await doQuery(filters, SORT, 50);
  const hitsAfter = (await getStats()).unified_cache_hits;
  const wasCacheHit = hitsAfter > hitsBefore;

  // Step 4: Clear cache and query again — this gives the ground truth
  await clearCache();
  await sleep(100);
  const freshResult = await doQuery(filters, SORT, 50);

  // Step 5: Compare
  const cachedIds = JSON.stringify(cachedResult.ids);
  const freshIds = JSON.stringify(freshResult.ids);
  const correct = cachedIds === freshIds;

  if (!correct) {
    log(`    MISMATCH! category=${testCat}`);
    log(`      Cached (${cachedResult.ids.length}): [${cachedResult.ids.slice(0, 10)}...]`);
    log(`      Fresh  (${freshResult.ids.length}): [${freshResult.ids.slice(0, 10)}...]`);
    // Find diffs
    const cachedSet = new Set(cachedResult.ids);
    const freshSet = new Set(freshResult.ids);
    const inCachedNotFresh = cachedResult.ids.filter(id => !freshSet.has(id));
    const inFreshNotCached = freshResult.ids.filter(id => !cachedSet.has(id));
    if (inCachedNotFresh.length) log(`      In cached but not fresh: [${inCachedNotFresh}]`);
    if (inFreshNotCached.length) log(`      In fresh but not cached: [${inFreshNotCached}]`);
  }

  return { correct, was_cache_hit: wasCacheHit, cached_count: cachedResult.ids.length, fresh_count: freshResult.ids.length };
}

// ── Main ──

async function main() {
  log('╔══════════════════════════════════════════════════════╗');
  log('║   BitDex Write-Path Stress Test                     ║');
  log('╚══════════════════════════════════════════════════════╝');
  log(`Server: ${BASE_URL}`);
  log(`Documents: ${DOC_COUNT}`);
  log(`Max caches: ${MAX_CACHES.toLocaleString()}`);
  log(`Timestamp: ${new Date().toISOString()}`);

  await setup();

  const allResults = [];

  // ── Cache scaling levels ──
  // Exponential: 0, 100, 1K, 5K, 10K, 50K, 100K, 500K, 1M
  const levels = [0, 100, 1000, 5000, 10000, 50000, 100000, 500000, 1000000]
    .filter(n => n <= MAX_CACHES);

  for (const target of levels) {
    if (target < SKIP_TO) continue;

    log(`\n${'═'.repeat(60)}`);
    log(`  TARGET: ${target.toLocaleString()} cache entries`);
    log(`${'═'.repeat(60)}`);

    // Populate caches (incremental — don't clear between levels)
    let s;
    if (target === 0) {
      await clearCache();
      await sleep(100);
      s = await getStats();
    } else {
      log(`  Populating caches to ${target.toLocaleString()}...`);
      const popStart = Date.now();
      s = await populateCachesTo(target);
      const popElapsed = ((Date.now() - popStart) / 1000).toFixed(1);
      log(`  Populated: ${s.unified_cache_entries.toLocaleString()} entries in ${popElapsed}s`);
    }

    const cacheEntries = s.unified_cache_entries;
    const cacheBytes = s.unified_cache_bytes;
    const metaEntries = s.unified_cache_meta_entries;
    const metaBytes = s.unified_cache_meta_bytes;

    log(`  Cache: ${cacheEntries.toLocaleString()} entries (${fmtBytes(cacheBytes)}), meta: ${metaEntries.toLocaleString()} entries (${fmtBytes(metaBytes)})`);

    // ── Test 1: Single-writer throughput (3s) ──
    log(`\n  [1] Single-writer throughput (3s):`);
    const single = await benchWriteThroughput(3000, 1);
    log(`      ${single.ops_per_sec}/s  p50=${fmtMs(single.latency.p50)}  p95=${fmtMs(single.latency.p95)}  max=${fmtMs(single.latency.max)}`);

    // ── Test 2: Concurrent writers (3s each, 1/2/4/8 writers) ──
    log(`  [2] Concurrent writer scaling (3s each):`);
    const concurrencyLevels = [1, 2, 4, 8];
    const concResults = {};
    for (const c of concurrencyLevels) {
      const r = await benchWriteThroughput(3000, c);
      concResults[c] = r;
      log(`      c=${c}: ${r.ops_per_sec}/s  p50=${fmtMs(r.latency.p50)}  p95=${fmtMs(r.latency.p95)}  max=${fmtMs(r.latency.max)}`);
    }

    // ── Test 3: Correctness verification ──
    // Re-populate caches (writes above may have triggered rebuilds)
    if (target > 0 && target <= 10000) {
      // Only re-populate for levels we can do quickly
      await populateCachesTo(target);
    }

    log(`  [3] Correctness check (50 writes, verify cache):`)
    const CORRECTNESS_ROUNDS = 5;
    let allCorrect = true;
    for (let round = 0; round < CORRECTNESS_ROUNDS; round++) {
      const check = await verifyCorrectness(50);
      if (!check.correct) {
        allCorrect = false;
        log(`      Round ${round + 1}: FAIL (cached=${check.cached_count}, fresh=${check.fresh_count}, hit=${check.was_cache_hit})`);
      }
    }
    if (allCorrect) {
      log(`      ${CORRECTNESS_ROUNDS}/${CORRECTNESS_ROUNDS} rounds CORRECT`);
    }

    // ── Test 4: Memory snapshot ──
    const sAfter = await getStats();
    log(`  [4] Memory: cache=${fmtBytes(sAfter.unified_cache_bytes)}, meta=${fmtBytes(sAfter.unified_cache_meta_bytes)}`);

    // Record results
    allResults.push({
      target_cache_entries: target,
      actual_cache_entries: cacheEntries,
      cache_bytes: cacheBytes,
      meta_entries: metaEntries,
      meta_bytes: metaBytes,
      single_writer: {
        ops_per_sec: single.ops_per_sec,
        latency: single.latency,
      },
      concurrent_writers: Object.fromEntries(
        Object.entries(concResults).map(([c, r]) => [c, {
          ops_per_sec: r.ops_per_sec,
          latency: r.latency,
        }])
      ),
      correctness: allCorrect ? 'PASS' : 'FAIL',
      memory_after: {
        cache_bytes: sAfter.unified_cache_bytes,
        meta_bytes: sAfter.unified_cache_meta_bytes,
        cache_entries: sAfter.unified_cache_entries,
      },
    });

    // Bail if writes are too slow
    if (single.latency.p50 > 500) {
      log(`\n  ⚠ p50 write latency exceeds 500ms — stopping escalation`);
      break;
    }
  }

  // ── Final Summary ──

  log(`\n${'═'.repeat(70)}`);
  log('  SUMMARY');
  log(`${'═'.repeat(70)}`);

  // Table 1: Write throughput vs cache count
  log('\n  Write Throughput (single writer):');
  log('  Cache Entries |  ops/s | p50      | p95      | p99      | max');
  log('  -------------|--------|----------|----------|----------|----------');
  for (const r of allResults) {
    const l = r.single_writer.latency;
    log(`  ${String(r.actual_cache_entries.toLocaleString()).padStart(13)} | ${String(r.single_writer.ops_per_sec).padStart(6)} | ${fmtMs(l.p50).padStart(8)} | ${fmtMs(l.p95).padStart(8)} | ${fmtMs(l.p99).padStart(8)} | ${fmtMs(l.max).padStart(8)}`);
  }

  // Table 2: Concurrent scaling at each level
  log('\n  Concurrent Writer Scaling (ops/s):');
  const header = '  Cache Entries |' + [1, 2, 4, 8].map(c => ` c=${c}`.padStart(8)).join(' |') + ' |';
  log(header);
  log('  ' + '-'.repeat(header.length - 2));
  for (const r of allResults) {
    const cells = [1, 2, 4, 8].map(c => {
      const cr = r.concurrent_writers[c];
      return cr ? String(cr.ops_per_sec).padStart(7) : '    N/A';
    });
    log(`  ${String(r.actual_cache_entries.toLocaleString()).padStart(13)} | ${cells.join(' | ')} |`);
  }

  // Table 3: Memory
  log('\n  Memory:');
  log('  Cache Entries | Cache Mem  | Meta Mem   | Correctness');
  log('  -------------|------------|------------|------------');
  for (const r of allResults) {
    log(`  ${String(r.actual_cache_entries.toLocaleString()).padStart(13)} | ${fmtBytes(r.cache_bytes).padStart(10)} | ${fmtBytes(r.meta_bytes).padStart(10)} | ${r.correctness}`);
  }

  // Write JSON results
  const resultsDir = resolve(PROJECT_ROOT, 'docs', 'benchmarks', 'results');
  mkdirSync(resultsDir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const outFile = resolve(resultsDir, `bench-write-stress-${ts}.json`);
  writeFileSync(outFile, JSON.stringify({
    timestamp: new Date().toISOString(),
    doc_count: DOC_COUNT,
    max_caches: MAX_CACHES,
    results: allResults,
  }, null, 2));
  log(`\nResults: ${outFile}`);

  // Cleanup
  await apiDelete(`/api/indexes/${INDEX}`);
  log('Done.');
}

main().catch(err => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
