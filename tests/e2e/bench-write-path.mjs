#!/usr/bin/env node
/**
 * BitDex Write-Path Benchmark
 *
 * Measures the end-to-end cost of writes as cache entry count and fan-out
 * scale up. The key question: is cache maintenance O(1) per affected entry,
 * or does it degrade as the number of entries grows?
 *
 * Phases:
 *   1. Setup: create index with many filter fields to enable high cache counts
 *   2. Populate: insert N base documents
 *   3. Warm caches: fire queries to populate C cache entries
 *   4. Benchmark: measure upsert latency (write → flush → cache maintained)
 *      at varying cache counts and fan-out levels
 *
 * Metrics per scenario:
 *   - Upsert wall-clock latency (HTTP round-trip)
 *   - Flush cycle time (time for mutation to be fully processed)
 *   - Write throughput (upserts/sec sustained)
 *   - Cache entry count before/after
 *   - Memory (RSS via stats)
 *
 * Usage:
 *   node tests/e2e/bench-write-path.mjs [--url http://localhost:3100] [--docs 1000] [--verbose]
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
const DOC_COUNT = Number(process.argv.includes('--docs')
  ? process.argv[process.argv.indexOf('--docs') + 1]
  : '1000');
const VERBOSE = process.argv.includes('--verbose');

const INDEX = 'write-bench';

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [v]', ...args); }

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
const query = (filters, sort, limit = 50) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, sort, limit }).then(r => r.data);
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const clearCache = () => apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForFlush(expectedAlive, maxWaitMs = 10000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await getStats();
    if (s.alive_count >= expectedAlive) return s;
    await sleep(20);
  }
  return await getStats();
}

async function waitForCycle(startCycle, maxWaitMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await getStats();
    if (s.flush_cycle > startCycle) return s;
    await sleep(10);
  }
  return await getStats();
}

function percentile(sorted, p) {
  const idx = Math.ceil(sorted.length * p / 100) - 1;
  return sorted[Math.max(0, idx)];
}

function formatStats(latencies) {
  const sorted = [...latencies].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    count: sorted.length,
    mean: (sum / sorted.length).toFixed(2),
    p50: percentile(sorted, 50).toFixed(2),
    p95: percentile(sorted, 95).toFixed(2),
    p99: percentile(sorted, 99).toFixed(2),
    min: sorted[0].toFixed(2),
    max: sorted[sorted.length - 1].toFixed(2),
  };
}

// ── Setup ──

async function setup() {
  log('\n=== Setup ===');

  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(200);
  }

  // Create index with multiple filter fields to enable many distinct cache keys
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
    ],
    flush_interval_us: 50, // fast flush for benchmarking
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
    ],
  };

  const createResult = await apiPost('/api/indexes', { name: INDEX, config, data_schema });
  if (createResult.status !== 200 && createResult.status !== 201) {
    throw new Error(`Failed to create index: ${JSON.stringify(createResult.data)}`);
  }
  log(`  Index created`);

  // Insert base documents in batches
  log(`  Inserting ${DOC_COUNT} documents...`);
  const BATCH_SIZE = 200;
  for (let i = 0; i < DOC_COUNT; i += BATCH_SIZE) {
    const batch = [];
    for (let j = i; j < Math.min(i + BATCH_SIZE, DOC_COUNT); j++) {
      const id = j + 1;
      batch.push({
        id,
        category: (id % 10) + 1,     // 1-10
        priority: (id % 5) + 1,      // 1-5
        region: (id % 8) + 1,        // 1-8
        tier: (id % 4) + 1,          // 1-4
        active: id % 3 !== 0,        // 2/3 active
        tagIds: [id % 20, (id % 20) + 20, (id % 20) + 40],
        score: Math.floor(Math.random() * 100000),
        createdAt: 1700000000 + id * 60,
      });
    }
    await upsert(batch);
  }
  const s = await waitForFlush(DOC_COUNT);
  log(`  ${s.alive_count} documents alive, flush_cycle=${s.flush_cycle}`);
}

// ── Cache Population ──

/**
 * Populate N cache entries by firing diverse queries.
 * Returns the number of entries created.
 */
async function populateCaches(targetEntries) {
  await clearCache();
  await sleep(100);

  const SORT_DESC = { field: 'score', direction: 'Desc' };
  const SORT_ASC = { field: 'score', direction: 'Asc' };
  const SORT_CREATED = { field: 'createdAt', direction: 'Desc' };

  let queriesRun = 0;

  // Single-field queries: 10 categories × 3 sorts = 30
  for (let cat = 1; cat <= 10; cat++) {
    await query([{ Eq: ['category', { Integer: cat }] }], SORT_DESC);
    await query([{ Eq: ['category', { Integer: cat }] }], SORT_ASC);
    await query([{ Eq: ['category', { Integer: cat }] }], SORT_CREATED);
    queriesRun += 3;
  }

  if (queriesRun >= targetEntries) {
    const s = await getStats();
    return s.unified_cache_entries;
  }

  // Two-field queries: category × priority = 50 × 3 sorts = 150
  for (let cat = 1; cat <= 10 && queriesRun < targetEntries; cat++) {
    for (let prio = 1; prio <= 5 && queriesRun < targetEntries; prio++) {
      await query([
        { Eq: ['category', { Integer: cat }] },
        { Eq: ['priority', { Integer: prio }] },
      ], SORT_DESC);
      queriesRun++;
    }
  }

  if (queriesRun >= targetEntries) {
    const s = await getStats();
    return s.unified_cache_entries;
  }

  // Three-field queries: category × priority × region
  for (let cat = 1; cat <= 10 && queriesRun < targetEntries; cat++) {
    for (let prio = 1; prio <= 5 && queriesRun < targetEntries; prio++) {
      for (let reg = 1; reg <= 8 && queriesRun < targetEntries; reg++) {
        await query([
          { Eq: ['category', { Integer: cat }] },
          { Eq: ['priority', { Integer: prio }] },
          { Eq: ['region', { Integer: reg }] },
        ], SORT_DESC);
        queriesRun++;
      }
    }
  }

  if (queriesRun >= targetEntries) {
    const s = await getStats();
    return s.unified_cache_entries;
  }

  // Four-field queries: add tier
  for (let cat = 1; cat <= 10 && queriesRun < targetEntries; cat++) {
    for (let prio = 1; prio <= 5 && queriesRun < targetEntries; prio++) {
      for (let reg = 1; reg <= 8 && queriesRun < targetEntries; reg++) {
        for (let tier = 1; tier <= 4 && queriesRun < targetEntries; tier++) {
          await query([
            { Eq: ['category', { Integer: cat }] },
            { Eq: ['priority', { Integer: prio }] },
            { Eq: ['region', { Integer: reg }] },
            { Eq: ['tier', { Integer: tier }] },
          ], SORT_DESC);
          queriesRun++;
        }
      }
    }
  }

  const s = await getStats();
  return s.unified_cache_entries;
}

// ── Benchmark Scenarios ──

/**
 * Measure single-upsert latency: HTTP → flush → cache maintained.
 * Returns array of latencies in ms.
 */
async function benchUpsertLatency(iterations, label) {
  const latencies = [];

  for (let i = 0; i < iterations; i++) {
    // Pick a random doc to upsert (change its category to trigger filter cache maintenance)
    const docId = Math.floor(Math.random() * DOC_COUNT) + 1;
    const newCategory = Math.floor(Math.random() * 10) + 1;
    const newScore = Math.floor(Math.random() * 100000);

    const start = performance.now();
    const cycle = (await getStats()).flush_cycle;

    await upsert([{
      id: docId,
      category: newCategory,
      priority: (docId % 5) + 1,
      region: (docId % 8) + 1,
      tier: (docId % 4) + 1,
      active: docId % 3 !== 0,
      tagIds: [docId % 20, (docId % 20) + 20, (docId % 20) + 40],
      score: newScore,
      createdAt: 1700000000 + docId * 60,
    }]);

    // Wait for flush to process (cache maintenance happens here)
    await waitForCycle(cycle);
    const elapsed = performance.now() - start;
    latencies.push(elapsed);
  }

  return latencies;
}

/**
 * Measure sustained write throughput: fire N upserts as fast as possible,
 * then wait for all flushes to complete.
 */
async function benchSustainedThroughput(count, label) {
  const docs = [];
  for (let i = 0; i < count; i++) {
    const docId = Math.floor(Math.random() * DOC_COUNT) + 1;
    docs.push({
      id: docId,
      category: Math.floor(Math.random() * 10) + 1,
      priority: (docId % 5) + 1,
      region: (docId % 8) + 1,
      tier: (docId % 4) + 1,
      active: docId % 3 !== 0,
      tagIds: [docId % 20, (docId % 20) + 20, (docId % 20) + 40],
      score: Math.floor(Math.random() * 100000),
      createdAt: 1700000000 + docId * 60,
    });
  }

  const startCycle = (await getStats()).flush_cycle;
  const start = performance.now();

  // Fire all upserts (one doc at a time for realistic single-write simulation)
  for (const doc of docs) {
    await upsert([doc]);
  }

  // Wait for flush to catch up
  const afterSend = performance.now();
  await sleep(200); // let flush thread process remaining
  const s = await getStats();
  const end = performance.now();

  const sendDuration = afterSend - start;
  const totalDuration = end - start;
  const cyclesProcessed = s.flush_cycle - startCycle;

  return {
    count,
    send_duration_ms: sendDuration.toFixed(1),
    total_duration_ms: totalDuration.toFixed(1),
    send_rate: (count / (sendDuration / 1000)).toFixed(0),
    flush_cycles: cyclesProcessed,
    cache_entries: s.unified_cache_entries,
  };
}

/**
 * Measure concurrent read + write: writers upsert while readers query.
 * Measures whether cache maintenance under write load degrades read latency.
 */
async function benchMixedReadWrite(writeDurationMs, writerCount, readerCount) {
  const writeLatencies = [];
  const readLatencies = [];
  let writersDone = false;

  const SORT_DESC = { field: 'score', direction: 'Desc' };

  // Writer tasks
  const writers = [];
  for (let w = 0; w < writerCount; w++) {
    writers.push((async () => {
      const start = Date.now();
      let ops = 0;
      while (Date.now() - start < writeDurationMs) {
        const docId = Math.floor(Math.random() * DOC_COUNT) + 1;
        const t0 = performance.now();
        await upsert([{
          id: docId,
          category: Math.floor(Math.random() * 10) + 1,
          priority: (docId % 5) + 1,
          region: (docId % 8) + 1,
          tier: (docId % 4) + 1,
          active: docId % 3 !== 0,
          tagIds: [docId % 20, (docId % 20) + 20, (docId % 20) + 40],
          score: Math.floor(Math.random() * 100000),
          createdAt: 1700000000 + docId * 60,
        }]);
        writeLatencies.push(performance.now() - t0);
        ops++;
      }
      return ops;
    })());
  }

  // Reader tasks
  const readers = [];
  for (let r = 0; r < readerCount; r++) {
    readers.push((async () => {
      let ops = 0;
      while (!writersDone) {
        const cat = Math.floor(Math.random() * 10) + 1;
        const t0 = performance.now();
        await query([{ Eq: ['category', { Integer: cat }] }], SORT_DESC);
        readLatencies.push(performance.now() - t0);
        ops++;
      }
      return ops;
    })());
  }

  // Wait for writers, then stop readers
  const writeOps = await Promise.all(writers);
  writersDone = true;
  const readOps = await Promise.all(readers);

  return {
    write_ops: writeOps.reduce((a, b) => a + b, 0),
    read_ops: readOps.reduce((a, b) => a + b, 0),
    write_stats: formatStats(writeLatencies),
    read_stats: formatStats(readLatencies),
  };
}

// ── Main ──

async function main() {
  log('=== BitDex Write-Path Benchmark ===');
  log(`Server: ${BASE_URL}`);
  log(`Documents: ${DOC_COUNT}`);
  log(`Timestamp: ${new Date().toISOString()}`);

  await setup();

  const results = [];

  // Phase 1: Upsert latency at varying cache entry counts
  log('\n=== Phase 1: Upsert Latency vs Cache Entry Count ===');
  log('Measures: time from HTTP upsert → flush cycle complete (cache maintained)');
  log('');

  const cacheLevels = [0, 30, 100, 500, 1000, 2000, 5000];

  for (const target of cacheLevels) {
    const actualEntries = target === 0 ? 0 : await populateCaches(target);
    const s = await getStats();
    const cacheEntries = s.unified_cache_entries;
    const cacheBytes = s.unified_cache_bytes;
    const metaEntries = s.unified_cache_meta_entries;

    log(`\n  Cache entries: ${cacheEntries} (target ${target}), meta=${metaEntries}, bytes=${cacheBytes}`);

    const ITERATIONS = 30;
    const latencies = await benchUpsertLatency(ITERATIONS, `cache=${target}`);
    const stats = formatStats(latencies);

    log(`  Upsert latency (${ITERATIONS} ops): p50=${stats.p50}ms  p95=${stats.p95}ms  p99=${stats.p99}ms  mean=${stats.mean}ms`);

    const sAfter = await getStats();
    results.push({
      phase: 'upsert_latency',
      target_cache_entries: target,
      actual_cache_entries: cacheEntries,
      meta_entries: metaEntries,
      cache_bytes: cacheBytes,
      iterations: ITERATIONS,
      ...stats,
      cache_entries_after: sAfter.unified_cache_entries,
      cache_hits_after: sAfter.unified_cache_hits,
    });

    // Don't clear between levels to avoid compounding population time
    // But do clear at 0 explicitly
    if (target === 0) {
      await clearCache();
    }
  }

  // Phase 2: Sustained write throughput at different cache levels
  log('\n\n=== Phase 2: Sustained Write Throughput ===');
  log('Measures: fire N upserts sequentially, measure send rate + total time');
  log('');

  const throughputLevels = [0, 100, 1000, 5000];
  const BURST_SIZE = 100;

  for (const target of throughputLevels) {
    const actualEntries = target === 0 ? 0 : await populateCaches(target);
    const s = await getStats();
    log(`\n  Cache entries: ${s.unified_cache_entries} (target ${target})`);

    const tp = await benchSustainedThroughput(BURST_SIZE, `cache=${target}`);
    log(`  ${BURST_SIZE} upserts: send_rate=${tp.send_rate}/s  send=${tp.send_duration_ms}ms  total=${tp.total_duration_ms}ms  flush_cycles=${tp.flush_cycles}`);

    results.push({
      phase: 'sustained_throughput',
      target_cache_entries: target,
      actual_cache_entries: s.unified_cache_entries,
      burst_size: BURST_SIZE,
      ...tp,
    });

    if (target === 0) await clearCache();
  }

  // Phase 3: Mixed read/write under load
  log('\n\n=== Phase 3: Mixed Read/Write Under Load ===');
  log('Measures: concurrent readers + writers, cache maintenance impact on reads');
  log('');

  const mixedLevels = [0, 500, 2000];
  const MIXED_DURATION_MS = 5000;
  const WRITERS = 2;
  const READERS = 4;

  for (const target of mixedLevels) {
    const actualEntries = target === 0 ? 0 : await populateCaches(target);
    const s = await getStats();
    log(`\n  Cache entries: ${s.unified_cache_entries} (target ${target})`);

    const mixed = await benchMixedReadWrite(MIXED_DURATION_MS, WRITERS, READERS);
    log(`  ${MIXED_DURATION_MS}ms, ${WRITERS}W/${READERS}R:`);
    log(`    Writes: ${mixed.write_ops} ops, p50=${mixed.write_stats.p50}ms  p95=${mixed.write_stats.p95}ms`);
    log(`    Reads:  ${mixed.read_ops} ops, p50=${mixed.read_stats.p50}ms  p95=${mixed.read_stats.p95}ms`);

    const sAfter = await getStats();
    results.push({
      phase: 'mixed_read_write',
      target_cache_entries: target,
      actual_cache_entries: s.unified_cache_entries,
      duration_ms: MIXED_DURATION_MS,
      writers: WRITERS,
      readers: READERS,
      write_ops: mixed.write_ops,
      read_ops: mixed.read_ops,
      write_latency: mixed.write_stats,
      read_latency: mixed.read_stats,
      cache_entries_after: sAfter.unified_cache_entries,
    });

    if (target === 0) await clearCache();
  }

  // Phase 4: Fan-out measurement — single mutation touching N cache entries
  log('\n\n=== Phase 4: Fan-Out Cost ===');
  log('Measures: how flush time scales with number of cache entries affected by one mutation');
  log('');

  // For each fan-out level, populate caches that all reference "category" field,
  // then mutate one doc's category and measure the flush cycle time.
  const fanoutLevels = [1, 10, 50, 100, 500, 1000];

  for (const fanout of fanoutLevels) {
    await clearCache();
    await sleep(100);

    // Populate exactly `fanout` cache entries that all reference category field
    const SORT_DESC = { field: 'score', direction: 'Desc' };
    let populated = 0;

    // Single-category queries: 10 categories × sorts
    for (let cat = 1; cat <= 10 && populated < fanout; cat++) {
      await query([{ Eq: ['category', { Integer: cat }] }], SORT_DESC);
      populated++;
    }
    // Two-field combos
    for (let cat = 1; cat <= 10 && populated < fanout; cat++) {
      for (let prio = 1; prio <= 5 && populated < fanout; prio++) {
        await query([
          { Eq: ['category', { Integer: cat }] },
          { Eq: ['priority', { Integer: prio }] },
        ], SORT_DESC);
        populated++;
      }
    }
    // Three-field combos
    for (let cat = 1; cat <= 10 && populated < fanout; cat++) {
      for (let prio = 1; prio <= 5 && populated < fanout; prio++) {
        for (let reg = 1; reg <= 8 && populated < fanout; reg++) {
          await query([
            { Eq: ['category', { Integer: cat }] },
            { Eq: ['priority', { Integer: prio }] },
            { Eq: ['region', { Integer: reg }] },
          ], SORT_DESC);
          populated++;
        }
      }
    }
    // Four-field combos
    for (let cat = 1; cat <= 10 && populated < fanout; cat++) {
      for (let prio = 1; prio <= 5 && populated < fanout; prio++) {
        for (let reg = 1; reg <= 8 && populated < fanout; reg++) {
          for (let tier = 1; tier <= 4 && populated < fanout; tier++) {
            await query([
              { Eq: ['category', { Integer: cat }] },
              { Eq: ['priority', { Integer: prio }] },
              { Eq: ['region', { Integer: reg }] },
              { Eq: ['tier', { Integer: tier }] },
            ], SORT_DESC);
            populated++;
          }
        }
      }
    }

    const s = await getStats();
    const actualEntries = s.unified_cache_entries;
    vlog(`Populated ${actualEntries} entries for fanout=${fanout}`);

    // Now measure: mutate one doc's category (touches all entries referencing "category")
    const FANOUT_ITERS = 20;
    const flushTimes = [];

    for (let i = 0; i < FANOUT_ITERS; i++) {
      const docId = Math.floor(Math.random() * DOC_COUNT) + 1;
      const newCat = Math.floor(Math.random() * 10) + 1;
      const cycle = (await getStats()).flush_cycle;
      const t0 = performance.now();

      await upsert([{
        id: docId,
        category: newCat,
        priority: (docId % 5) + 1,
        region: (docId % 8) + 1,
        tier: (docId % 4) + 1,
        active: docId % 3 !== 0,
        tagIds: [docId % 20, (docId % 20) + 20, (docId % 20) + 40],
        score: Math.floor(Math.random() * 100000),
        createdAt: 1700000000 + docId * 60,
      }]);

      await waitForCycle(cycle);
      flushTimes.push(performance.now() - t0);
    }

    const fStats = formatStats(flushTimes);
    log(`  Fan-out ${String(fanout).padStart(5)}: entries=${actualEntries}  p50=${fStats.p50}ms  p95=${fStats.p95}ms  mean=${fStats.mean}ms`);

    results.push({
      phase: 'fanout',
      target_fanout: fanout,
      actual_cache_entries: actualEntries,
      iterations: FANOUT_ITERS,
      ...fStats,
    });
  }

  // ── Summary ──

  log('\n\n=== Summary ===');
  log('');

  // Upsert latency table
  log('Upsert Latency vs Cache Entries:');
  log('  Cache Entries | p50 (ms) | p95 (ms) | mean (ms)');
  log('  -------------|----------|----------|----------');
  for (const r of results.filter(r => r.phase === 'upsert_latency')) {
    log(`  ${String(r.actual_cache_entries).padStart(13)} | ${String(r.p50).padStart(8)} | ${String(r.p95).padStart(8)} | ${String(r.mean).padStart(9)}`);
  }

  log('');
  log('Fan-Out Cost:');
  log('  Cache Entries | p50 (ms) | p95 (ms) | mean (ms)');
  log('  -------------|----------|----------|----------');
  for (const r of results.filter(r => r.phase === 'fanout')) {
    log(`  ${String(r.actual_cache_entries).padStart(13)} | ${String(r.p50).padStart(8)} | ${String(r.p95).padStart(8)} | ${String(r.mean).padStart(9)}`);
  }

  log('');
  log('Sustained Throughput:');
  log('  Cache Entries | Send Rate (/s) | Total (ms)');
  log('  -------------|----------------|----------');
  for (const r of results.filter(r => r.phase === 'sustained_throughput')) {
    log(`  ${String(r.actual_cache_entries).padStart(13)} | ${String(r.send_rate).padStart(14)} | ${String(r.total_duration_ms).padStart(10)}`);
  }

  log('');
  log('Mixed Read/Write:');
  log('  Cache Entries | Write p50 | Write p95 | Read p50 | Read p95 | Write ops | Read ops');
  log('  -------------|-----------|-----------|----------|----------|-----------|--------');
  for (const r of results.filter(r => r.phase === 'mixed_read_write')) {
    log(`  ${String(r.actual_cache_entries).padStart(13)} | ${String(r.write_latency.p50).padStart(9)} | ${String(r.write_latency.p95).padStart(9)} | ${String(r.read_latency.p50).padStart(8)} | ${String(r.read_latency.p95).padStart(8)} | ${String(r.write_ops).padStart(9)} | ${String(r.read_ops).padStart(8)}`);
  }

  // Write JSON results
  const resultsDir = resolve(PROJECT_ROOT, 'docs', 'benchmarks', 'results');
  mkdirSync(resultsDir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const outFile = resolve(resultsDir, `bench-write-path-${ts}.json`);
  writeFileSync(outFile, JSON.stringify({ timestamp: new Date().toISOString(), doc_count: DOC_COUNT, results }, null, 2));
  log(`\nResults written to: ${outFile}`);

  // Cleanup
  await apiDelete(`/api/indexes/${INDEX}`);
  log('Index deleted. Done.');
}

main().catch(err => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
