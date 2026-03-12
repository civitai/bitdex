#!/usr/bin/env node
/**
 * Mixed Workload Benchmark for BitDex Unified Cache
 *
 * Generates realistic random queries from a weighted config, mixes in upserts
 * and deletes, runs concurrent workers, and reports percentile latencies.
 *
 * Usage:
 *   node tools/mixed-workload.mjs [options]
 *
 * Options:
 *   --url <url>          Server URL (default: http://localhost:3000)
 *   --index <name>       Index name (default: civitai)
 *   --config <path>      Workload config (default: tools/workload.json)
 *   --requests <n>       Override total_requests from config
 *   --concurrency <n>    Override concurrency from config
 *   --verbose            Print every request
 *   --dry-run            Print generated queries without sending
 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

// --- CLI args ---
const args = process.argv.slice(2);
function arg(name, def) {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const flag = (name) => args.includes(`--${name}`);

const BASE_URL = arg('url', 'http://localhost:3000');
const INDEX = arg('index', 'civitai');
const CONFIG_PATH = resolve(arg('config', 'tools/workload.json'));
const VERBOSE = flag('verbose');
const DRY_RUN = flag('dry-run');

const config = JSON.parse(readFileSync(CONFIG_PATH, 'utf-8'));

const TOTAL = Number(arg('requests', config.total_requests || 5000));
const CONCURRENCY = Number(arg('concurrency', config.concurrency || 4));
const WARMUP = config.warmup_requests || 200;
const LIMIT = config.limit || 20;

// --- Weighted random ---
function weightedPick(items, weightKey = 'weight') {
  const total = items.reduce((s, i) => s + (i[weightKey] || 0), 0);
  let r = Math.random() * total;
  for (const item of items) {
    r -= item[weightKey] || 0;
    if (r <= 0) return item;
  }
  return items[items.length - 1];
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// --- Query generator ---
function generateQuery() {
  const filters = [];

  // For each filter field, roll whether to include it
  for (const [field, spec] of Object.entries(config.filters)) {
    if (Math.random() * 100 >= spec.weight) continue;

    if (spec.type === 'eq') {
      if (spec.values) {
        const picked = weightedPick(spec.values);
        filters.push({ Eq: [field, { Integer: picked.v }] });
      } else if (spec.range) {
        const v = randInt(spec.range[0], spec.range[1]);
        filters.push({ Eq: [field, { Integer: v }] });
      }
    } else if (spec.type === 'eq_bool') {
      filters.push({ Eq: [field, { Bool: spec.value }] });
    } else if (spec.type === 'in') {
      const count = randInt(spec.count[0], spec.count[1]);
      const values = [];
      for (let i = 0; i < count; i++) {
        values.push({ Integer: randInt(spec.range[0], spec.range[1]) });
      }
      filters.push({ In: [field, values] });
    }
  }

  // If no filters were selected (very rare), force nsfwLevel
  if (filters.length === 0) {
    filters.push({ Eq: ['nsfwLevel', { Integer: 1 }] });
  }

  const sort = weightedPick(config.sorts);

  return {
    filters,
    sort: { field: sort.field, direction: sort.direction },
    limit: LIMIT,
  };
}

function generateUpsert() {
  const id = randInt(config.write_mix.id_range[0], config.write_mix.id_range[1]);
  return {
    type: 'upsert',
    body: {
      documents: [{
        id,
        nsfwLevel: weightedPick(config.filters.nsfwLevel.values).v,
        reactionCount: randInt(0, 5000),
        type: 'image',
      }],
    },
  };
}

function generateDelete() {
  const id = randInt(config.write_mix.id_range[0], config.write_mix.id_range[1]);
  return {
    type: 'delete',
    body: { ids: [id] },
  };
}

function pickOperation() {
  const r = Math.random() * 100;
  if (r < (config.write_mix?.upsert_pct || 0)) return 'upsert';
  if (r < (config.write_mix?.upsert_pct || 0) + (config.write_mix?.delete_pct || 0)) return 'delete';
  return 'query';
}

// Hot pool: pre-defined common queries that get repeated
function pickHotQuery() {
  const pool = config.hot_pool?.queries;
  if (!pool || pool.length === 0) return null;
  const picked = weightedPick(pool);
  return {
    filters: picked.filters,
    sort: picked.sort,
    limit: LIMIT,
  };
}

function generateQueryMixed() {
  const hotPct = config.hot_pool?.hot_pct || 0;
  if (Math.random() * 100 < hotPct) {
    const q = pickHotQuery();
    if (q) return { query: q, source: 'hot' };
  }
  return { query: generateQuery(), source: 'cold' };
}

// --- HTTP helpers ---
const api = (path) => `${BASE_URL}/api/indexes/${INDEX}${path}`;

async function sendQuery(q) {
  const res = await fetch(api('/query'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(q),
  });
  return res.json();
}

async function sendUpsert(body) {
  const res = await fetch(api('/documents/upsert'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return res.json();
}

async function sendDelete(body) {
  const res = await fetch(api('/documents'), {
    method: 'DELETE',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return res.json();
}

async function getStats() {
  const res = await fetch(api('/stats'));
  return res.json();
}

async function clearCache() {
  const res = await fetch(api('/cache'), { method: 'DELETE' });
  return res.json();
}

// --- Formatting ---
function fmt(us) {
  if (us >= 1000000) return `${(us / 1000000).toFixed(2)}s`;
  if (us >= 1000) return `${(us / 1000).toFixed(2)}ms`;
  return `${us}us`;
}

function percentile(sorted, p) {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function printPercentiles(label, samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const p50 = percentile(sorted, 50);
  const p80 = percentile(sorted, 80);
  const p95 = percentile(sorted, 95);
  const p99 = percentile(sorted, 99);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  const mean = Math.round(samples.reduce((a, b) => a + b, 0) / samples.length);
  console.log(`  ${label} (n=${samples.length})`);
  console.log(`    min=${fmt(min)}  p50=${fmt(p50)}  p80=${fmt(p80)}  p95=${fmt(p95)}  p99=${fmt(p99)}  max=${fmt(max)}  mean=${fmt(mean)}`);
  return { p50, p80, p95, p99, min, max, mean };
}

// --- Worker ---
async function runWorker(workerId, tasks, results) {
  // Each worker keeps a small cursor cache for pagination continuations
  const cursorCache = new Map(); // queryKey -> { cursor, pagesLeft }

  for (const task of tasks) {
    const start = performance.now();

    if (task.op === 'query') {
      let q = task.query;

      // Check if we should continue paginating a previous query
      const qKey = JSON.stringify({ f: q.filters, s: q.sort });
      const cached = cursorCache.get(qKey);
      if (cached && cached.pagesLeft > 0 && Math.random() * 100 < (config.pagination?.continuation_pct || 0)) {
        q = { ...q, cursor: cached.cursor };
        cached.pagesLeft--;
      }

      const r = await sendQuery(q);
      const elapsed = performance.now() - start;

      if (r.cursor && !q.cursor) {
        // First page — store cursor for potential continuation
        cursorCache.set(qKey, {
          cursor: r.cursor,
          pagesLeft: (config.pagination?.max_pages || 3) - 1,
        });
      } else if (r.cursor && q.cursor) {
        // Continuation page — update cursor
        const c = cursorCache.get(qKey);
        if (c) c.cursor = r.cursor;
      }

      const isContinuation = !!q.cursor;
      results.push({
        type: isContinuation ? 'query_page2+' : 'query_page1',
        elapsed_us: r.elapsed_us,
        wall_ms: elapsed,
        total_matched: r.total_matched,
        ids: r.ids?.length || 0,
      });

      if (VERBOSE) {
        console.log(`  [w${workerId}] query ${isContinuation ? 'p2+' : 'p1 '} ${fmt(r.elapsed_us).padStart(10)} server / ${elapsed.toFixed(1)}ms wall  matched=${r.total_matched}  ids=${r.ids?.length}`);
      }
    } else if (task.op === 'upsert') {
      const r = await sendUpsert(task.body);
      const elapsed = performance.now() - start;
      results.push({ type: 'upsert', wall_ms: elapsed, upserted: r.upserted });
      if (VERBOSE) console.log(`  [w${workerId}] upsert ${elapsed.toFixed(1)}ms`);
    } else if (task.op === 'delete') {
      const r = await sendDelete(task.body);
      const elapsed = performance.now() - start;
      results.push({ type: 'delete', wall_ms: elapsed, deleted: r.deleted });
      if (VERBOSE) console.log(`  [w${workerId}] delete ${elapsed.toFixed(1)}ms`);
    }
  }
}

// --- Main ---
async function main() {
  console.log('BitDex Mixed Workload Benchmark');
  console.log(`  Server: ${BASE_URL}`);
  console.log(`  Index: ${INDEX}`);
  console.log(`  Config: ${CONFIG_PATH}`);
  console.log(`  Requests: ${TOTAL} (+ ${WARMUP} warmup)`);
  console.log(`  Concurrency: ${CONCURRENCY}`);
  console.log(`  Write mix: ${config.write_mix?.upsert_pct || 0}% upsert, ${config.write_mix?.delete_pct || 0}% delete`);
  console.log();

  // Verify server
  try {
    const s = await getStats();
    console.log(`  Server alive: ${s.alive_count.toLocaleString()} docs`);
  } catch (e) {
    console.log(`ERROR: Cannot reach server at ${BASE_URL}: ${e.message}`);
    process.exit(1);
  }

  // Generate all operations upfront
  console.log('\nGenerating workload...');
  const allOps = [];
  const queryFingerprints = new Set();

  let hotCount = 0, coldCount = 0;
  for (let i = 0; i < WARMUP + TOTAL; i++) {
    const op = pickOperation();
    if (op === 'query') {
      const { query: q, source } = generateQueryMixed();
      if (source === 'hot') hotCount++; else coldCount++;
      const fingerprint = JSON.stringify({ f: q.filters, s: q.sort });
      queryFingerprints.add(fingerprint);
      allOps.push({ op: 'query', query: q });
    } else if (op === 'upsert') {
      const u = generateUpsert();
      allOps.push({ op: 'upsert', body: u.body });
    } else {
      const d = generateDelete();
      allOps.push({ op: 'delete', body: d.body });
    }
  }

  const queryCount = allOps.filter(o => o.op === 'query').length;
  const upsertCount = allOps.filter(o => o.op === 'upsert').length;
  const deleteCount = allOps.filter(o => o.op === 'delete').length;
  console.log(`  Generated: ${queryCount} queries (${hotCount} hot, ${coldCount} cold), ${upsertCount} upserts, ${deleteCount} deletes`);
  console.log(`  Unique query fingerprints: ${queryFingerprints.size}`);

  if (DRY_RUN) {
    console.log('\nSample queries (first 10):');
    for (const op of allOps.slice(0, 10)) {
      if (op.op === 'query') {
        console.log(`  Q: filters=${JSON.stringify(op.query.filters)} sort=${op.query.sort.field}:${op.query.sort.direction}`);
      } else {
        console.log(`  ${op.op.toUpperCase()}: ${JSON.stringify(op.body).slice(0, 100)}`);
      }
    }
    process.exit(0);
  }

  // Clear cache for clean start
  await clearCache();
  console.log('  Cache cleared\n');

  // --- Warmup phase ---
  console.log(`Warmup: ${WARMUP} requests (sequential)...`);
  const warmupOps = allOps.splice(0, WARMUP);
  for (const task of warmupOps) {
    if (task.op === 'query') {
      await sendQuery(task.query);
    } else if (task.op === 'upsert') {
      await sendUpsert(task.body);
    } else {
      await sendDelete(task.body);
    }
  }

  const statsAfterWarmup = await getStats();
  console.log(`  Cache after warmup: ${statsAfterWarmup.unified_cache_entries} entries, ${statsAfterWarmup.unified_cache_hits} hits, ${statsAfterWarmup.unified_cache_misses} misses`);
  console.log();

  // --- Timed phase ---
  console.log(`Running ${allOps.length} requests across ${CONCURRENCY} workers...`);
  const startTime = performance.now();

  // Distribute ops round-robin to workers
  const workerTasks = Array.from({ length: CONCURRENCY }, () => []);
  for (let i = 0; i < allOps.length; i++) {
    workerTasks[i % CONCURRENCY].push(allOps[i]);
  }

  const allResults = [];
  const workers = workerTasks.map((tasks, i) => {
    const results = [];
    allResults.push(results);
    return runWorker(i, tasks, results);
  });

  await Promise.all(workers);
  const totalTime = performance.now() - startTime;

  // --- Results ---
  const results = allResults.flat();
  const queryP1 = results.filter(r => r.type === 'query_page1');
  const queryP2 = results.filter(r => r.type === 'query_page2+');
  const upserts = results.filter(r => r.type === 'upsert');
  const deletes = results.filter(r => r.type === 'delete');
  const allQueries = [...queryP1, ...queryP2];

  const finalStats = await getStats();

  console.log(`\nCompleted in ${(totalTime / 1000).toFixed(1)}s (${(results.length / (totalTime / 1000)).toFixed(0)} ops/s)`);

  console.log(`\n${'='.repeat(80)}`);
  console.log('Latency (server-reported elapsed_us)');
  console.log(`${'='.repeat(80)}`);

  if (queryP1.length > 0) {
    // Split by sub-millisecond (cache hit) vs >= 1ms (cache miss / sort traversal)
    const fast = queryP1.filter(r => r.elapsed_us < 1000);
    const slow = queryP1.filter(r => r.elapsed_us >= 1000);
    printPercentiles('All page-1 queries', queryP1.map(r => r.elapsed_us));
    if (fast.length > 0) printPercentiles('  Sub-ms (cache hits)', fast.map(r => r.elapsed_us));
    if (slow.length > 0) printPercentiles('  >= 1ms (misses/traversal)', slow.map(r => r.elapsed_us));
  }

  if (queryP2.length > 0) {
    printPercentiles('Pagination (page 2+)', queryP2.map(r => r.elapsed_us));
  }

  console.log(`\n${'='.repeat(80)}`);
  console.log('Wall-clock latency (includes HTTP round-trip)');
  console.log(`${'='.repeat(80)}`);

  if (allQueries.length > 0) printPercentiles('All queries (wall)', allQueries.map(r => r.wall_ms * 1000));
  if (upserts.length > 0) printPercentiles('Upserts (wall)', upserts.map(r => r.wall_ms * 1000));
  if (deletes.length > 0) printPercentiles('Deletes (wall)', deletes.map(r => r.wall_ms * 1000));

  console.log(`\n${'='.repeat(80)}`);
  console.log('Summary');
  console.log(`${'='.repeat(80)}`);
  console.log(`  Total ops:          ${results.length}`);
  console.log(`  Queries (page 1):   ${queryP1.length}`);
  console.log(`  Queries (page 2+):  ${queryP2.length}`);
  console.log(`  Upserts:            ${upserts.length}`);
  console.log(`  Deletes:            ${deletes.length}`);
  console.log(`  Wall time:          ${(totalTime / 1000).toFixed(1)}s`);
  console.log(`  Throughput:         ${(results.length / (totalTime / 1000)).toFixed(0)} ops/s`);
  console.log(`  Unified cache entries: ${finalStats.unified_cache_entries}`);
  console.log(`  Unified cache hits:   ${finalStats.unified_cache_hits.toLocaleString()}`);
  console.log(`  Unified cache misses: ${finalStats.unified_cache_misses.toLocaleString()}`);
  console.log(`  Unified cache hit rate: ${((finalStats.unified_cache_hits / Math.max(1, finalStats.unified_cache_hits + finalStats.unified_cache_misses)) * 100).toFixed(1)}%`);
  console.log(`  Unified cache memory: ${(finalStats.unified_cache_bytes / 1024).toFixed(1)} KB`);
  console.log(`  Meta-index entries:   ${finalStats.unified_cache_meta_entries}`);
  console.log(`  Meta-index memory:    ${(finalStats.unified_cache_meta_bytes / 1024).toFixed(1)} KB`);
  console.log(`  Alive docs:           ${finalStats.alive_count.toLocaleString()}`);
  console.log(`${'='.repeat(80)}`);
}

main().catch(e => { console.error(e); process.exit(1); });
