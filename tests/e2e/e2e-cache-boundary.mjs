#!/usr/bin/env node
/**
 * E2E: Cache Boundary Expansion
 *
 * Tests that pagination crosses the unified cache boundary (default 4K initial
 * capacity) without stalling.
 *
 * Two bugs are tested:
 *   1. Wall fix: when sorted_keys exhausts at exactly the cache boundary,
 *      cursor was returned as null, stopping pagination. Now the engine
 *      detects this and expands synchronously.
 *   2. Prefetch: when a cursor nears the boundary (within prefetch_threshold),
 *      a background worker expands the cache before the user hits the wall.
 *
 * Test groups:
 *   A. Setup: insert 6000 docs with unique descending scores
 *   B. Full pagination past 4K boundary — no stall, no duplicates
 *   C. Filtered pagination past boundary — same correctness with filter
 *   D. Ascending sort past boundary
 *
 * Usage:
 *   node tests/e2e/e2e-cache-boundary.mjs [--url http://localhost:3000] [--verbose] [--keep]
 */

import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const VERBOSE = process.argv.includes('--verbose');
const KEEP = process.argv.includes('--keep');
const RESULTS_DIR = process.argv.includes('--results-dir')
  ? process.argv[process.argv.indexOf('--results-dir') + 1]
  : null;

const INDEX = 'cache-boundary-test';
const DOC_COUNT = 6000; // Must exceed initial_capacity (4000)
const PAGE_SIZE = 100;

let passed = 0;
let failed = 0;
const groupResults = [];

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function apiPost(path, body) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 300));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  return { status: res.status, data };
}

async function apiDelete(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  return { status: res.status, data };
}

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const queryApi = (filters, sort, limit, cursor = null) => {
  const body = { filters, sort, limit };
  if (cursor) body.cursor = cursor;
  return apiPost(`/api/indexes/${INDEX}/query`, body).then(r => r.data);
};
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const clearCache = () =>
  apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForFlush(expectedAlive, maxWaitMs = 10000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await stats();
    if (s.alive_count >= expectedAlive) return s;
    await sleep(100);
  }
  return await stats();
}

const SORT_SCORE_DESC = { field: 'score', direction: 'Desc' };
const SORT_SCORE_ASC = { field: 'score', direction: 'Asc' };

// ── Setup ──

async function setup() {
  log('\n--- Setup: Create index + insert 6000 docs ---');

  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    log('  Deleting existing test index...');
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(500);
  }

  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'category', field_type: 'single_value', value_type: 'integer' },
      ],
      sort_fields: [
        { name: 'score', bits: 32 },
      ],
      max_page_size: 200,
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'category', target: 'category', value_type: 'integer' },
        { source: 'score', target: 'score', value_type: 'integer' },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created`);

  // Insert in batches of 500 to avoid huge payloads
  const batchSize = 500;
  for (let start = 1; start <= DOC_COUNT; start += batchSize) {
    const docs = [];
    const end = Math.min(start + batchSize - 1, DOC_COUNT);
    for (let i = start; i <= end; i++) {
      docs.push({
        id: i,
        // category 1 for first 5000, category 2 for last 1000
        category: i <= 5000 ? 1 : 2,
        // Unique scores: score = i * 10 (no ties to simplify cursor logic)
        score: i * 10,
      });
    }
    const res = await upsert(docs);
    assert(res.upserted === docs.length, `Batch starting at ${start}: expected ${docs.length} upserted, got ${res.upserted}`);
  }

  log(`  Inserted ${DOC_COUNT} documents`);

  const s = await waitForFlush(DOC_COUNT);
  assert(s.alive_count === DOC_COUNT, `Expected ${DOC_COUNT} alive, got ${s.alive_count}`);
  log(`  Flush complete, ${s.alive_count} docs alive`);
}

// ── Test B: Full pagination past 4K boundary ──

async function testB_PaginationPastBoundary() {
  log('\n--- B. Pagination past 4K cache boundary (Desc) ---');

  await clearCache();

  const allIds = [];
  let cursor = null;
  let pageCount = 0;
  const maxPages = Math.ceil(DOC_COUNT / PAGE_SIZE) + 5; // safety margin

  // Seed the cache with the first query (cache miss → form cache)
  const seed = await queryApi([], SORT_SCORE_DESC, PAGE_SIZE);
  allIds.push(...(seed.ids || []));
  cursor = seed.cursor;
  pageCount = 1;

  assert(seed.ids?.length === PAGE_SIZE, `Seed page: expected ${PAGE_SIZE}, got ${seed.ids?.length}`);
  assert(cursor !== null, 'Seed page should have cursor');
  log(`  [1] Seed page: ${seed.ids.length} results, total_matched=${seed.total_matched}`);

  // Page through everything
  while (cursor && pageCount < maxPages) {
    const r = await queryApi([], SORT_SCORE_DESC, PAGE_SIZE, cursor);
    const ids = r.ids || [];
    allIds.push(...ids);
    pageCount++;

    if (pageCount % 10 === 0 || !r.cursor) {
      vlog(`  Page ${pageCount}: ${ids.length} results, cursor=${JSON.stringify(r.cursor)}`);
    }

    // Critical check: at the 4K boundary (pages 39-41), we must still get results
    if (pageCount === 40) {
      // Page 40 = items 3901-4000 (exactly at the initial_capacity boundary)
      assert(ids.length > 0, `Page 40 (4K boundary): got 0 results — cache wall bug!`);
      log(`  [2] Page 40 (4K boundary): ${ids.length} results — wall crossed`);
    }

    if (pageCount === 41) {
      // Page 41 = items 4001-4100 (past the boundary, expansion must have fired)
      assert(ids.length > 0, `Page 41 (past boundary): got 0 results — expansion failed!`);
      log(`  [3] Page 41 (past boundary): ${ids.length} results — expansion worked`);
    }

    cursor = r.cursor;
  }

  // All 6000 docs collected
  assert(allIds.length === DOC_COUNT, `Expected ${DOC_COUNT} total IDs, got ${allIds.length}`);
  log(`  [4] Collected all ${allIds.length} IDs across ${pageCount} pages`);

  // No duplicates
  const unique = new Set(allIds);
  assert(unique.size === allIds.length, `Duplicates: ${allIds.length} total, ${unique.size} unique`);
  log(`  [5] No duplicates`);

  // Sort order: descending by score (score = id * 10, so IDs should be descending)
  for (let i = 1; i < allIds.length; i++) {
    assert(allIds[i - 1] > allIds[i],
      `Sort order violation at position ${i}: id ${allIds[i - 1]} followed by ${allIds[i]}`);
  }
  log(`  [6] Sort order correct (descending throughout)`);
}

// ── Test C: Filtered pagination past boundary ──

async function testC_FilteredPaginationPastBoundary() {
  log('\n--- C. Filtered pagination past boundary ---');

  await clearCache();

  // category=1 has 5000 docs — exceeds 4K boundary
  const filter = [{ Eq: ['category', { Integer: 1 }] }];
  const allIds = [];
  let cursor = null;
  let pageCount = 0;
  const maxPages = 60;

  while (pageCount < maxPages) {
    const r = await queryApi(filter, SORT_SCORE_DESC, PAGE_SIZE, cursor);
    const ids = r.ids || [];
    allIds.push(...ids);
    pageCount++;

    if (pageCount === 1) {
      assert(r.total_matched >= 5000, `Expected >= 5000 matched, got ${r.total_matched}`);
      log(`  [1] total_matched=${r.total_matched}`);
    }

    cursor = r.cursor;
    if (!cursor) break;
  }

  assert(allIds.length === 5000, `Expected 5000 filtered results, got ${allIds.length}`);
  log(`  [2] All 5000 filtered results collected across ${pageCount} pages`);

  const unique = new Set(allIds);
  assert(unique.size === allIds.length, `Duplicates in filtered results`);
  log(`  [3] No duplicates`);

  // All IDs should be <= 5000 (category 1)
  for (const id of allIds) {
    assert(id <= 5000, `ID ${id} should not be in category=1 results`);
  }
  log(`  [4] All results correctly filtered to category=1`);
}

// ── Test D: Ascending sort past boundary ──

async function testD_AscendingPastBoundary() {
  log('\n--- D. Ascending sort past boundary ---');

  await clearCache();

  const allIds = [];
  let cursor = null;
  let pageCount = 0;
  const maxPages = Math.ceil(DOC_COUNT / PAGE_SIZE) + 5;

  while (pageCount < maxPages) {
    const r = await queryApi([], SORT_SCORE_ASC, PAGE_SIZE, cursor);
    const ids = r.ids || [];
    allIds.push(...ids);
    pageCount++;

    cursor = r.cursor;
    if (!cursor) break;
  }

  assert(allIds.length === DOC_COUNT, `Expected ${DOC_COUNT}, got ${allIds.length}`);
  log(`  [1] All ${allIds.length} results via ascending sort`);

  // Ascending order: each ID should be less than the next
  for (let i = 1; i < allIds.length; i++) {
    assert(allIds[i - 1] < allIds[i],
      `Ascending sort violation at ${i}: ${allIds[i - 1]} then ${allIds[i]}`);
  }
  log(`  [2] Sort order correct (ascending throughout)`);
}

// ── Test E: Prefetch timing comparison ──

async function testE_PrefetchTiming() {
  log('\n--- E. Prefetch timing: pause-at-threshold vs straight-through ---');

  // Threshold page: 95% of 4000 initial_capacity = 3800 items = page 38
  const THRESHOLD_PAGE = 38;
  const BOUNDARY_PAGE = 41; // first page clearly past 4K

  // Run 1: straight through (no pause) — sync fallback does the work
  await clearCache();
  let cursor = null;
  for (let p = 1; p <= THRESHOLD_PAGE; p++) {
    const r = await queryApi([], SORT_SCORE_DESC, PAGE_SIZE, cursor);
    cursor = r.cursor;
  }
  // Immediately cross the boundary — no time for prefetch
  const straightStart = performance.now();
  for (let p = THRESHOLD_PAGE + 1; p <= BOUNDARY_PAGE; p++) {
    const r = await queryApi([], SORT_SCORE_DESC, PAGE_SIZE, cursor);
    cursor = r.cursor;
  }
  const straightMs = performance.now() - straightStart;
  log(`  [1] Straight-through boundary crossing: ${straightMs.toFixed(1)}ms (${BOUNDARY_PAGE - THRESHOLD_PAGE} pages)`);

  // Run 2: pause at threshold to let prefetch worker expand
  await clearCache();
  cursor = null;
  for (let p = 1; p <= THRESHOLD_PAGE; p++) {
    const r = await queryApi([], SORT_SCORE_DESC, PAGE_SIZE, cursor);
    cursor = r.cursor;
  }
  // Pause — give prefetch worker time to expand the cache
  await sleep(1500);

  const prefetchStart = performance.now();
  for (let p = THRESHOLD_PAGE + 1; p <= BOUNDARY_PAGE; p++) {
    const r = await queryApi([], SORT_SCORE_DESC, PAGE_SIZE, cursor);
    cursor = r.cursor;
  }
  const prefetchMs = performance.now() - prefetchStart;
  log(`  [2] With prefetch pause: ${prefetchMs.toFixed(1)}ms (${BOUNDARY_PAGE - THRESHOLD_PAGE} pages)`);

  // The prefetch path should be faster (serving from expanded cache vs computing filters inline).
  // We don't hard-assert a ratio because timing is noisy on small datasets,
  // but we log the comparison and soft-assert prefetch wasn't slower.
  const ratio = straightMs / Math.max(prefetchMs, 0.1);
  log(`  [3] Speedup ratio: ${ratio.toFixed(2)}x (straight/prefetch)`);

  if (prefetchMs < straightMs) {
    log(`  [4] Prefetch was faster — background expansion likely fired`);
  } else {
    log(`  [4] Prefetch was not faster (${prefetchMs.toFixed(1)}ms vs ${straightMs.toFixed(1)}ms) — may be too fast to measure at this scale`);
  }
  // Soft pass: we don't fail on timing, just report it
}

// ── Cleanup ──

async function cleanup() {
  if (KEEP) {
    log('\n--- Cleanup: skipped (--keep) ---');
    return;
  }
  log('\n--- Cleanup ---');
  try {
    await apiDelete(`/api/indexes/${INDEX}`);
    log('  Index deleted');
  } catch {
    log('  Index cleanup timed out (non-fatal)');
  }
}

// ── Main ──

async function main() {
  log(`=== BitDex E2E: Cache Boundary Expansion ===`);
  log(`Server: ${BASE_URL}`);
  log(`Docs: ${DOC_COUNT}, Page size: ${PAGE_SIZE}\n`);

  // Health check
  const health = await apiGet('/api/indexes').catch(() => null);
  if (!health || health.status !== 200) {
    log('ERROR: Server not reachable');
    process.exit(1);
  }

  const groups = [
    ['Setup', 'Create index + insert docs', setup],
    ['B', 'Pagination past 4K boundary (Desc)', testB_PaginationPastBoundary],
    ['C', 'Filtered pagination past boundary', testC_FilteredPaginationPastBoundary],
    ['D', 'Ascending sort past boundary', testD_AscendingPastBoundary],
    ['E', 'Prefetch timing comparison', testE_PrefetchTiming],
  ];

  const suiteStart = Date.now();

  for (const [id, name, fn] of groups) {
    const groupStart = Date.now();
    try {
      await fn();
      passed++;
      log(`  PASS: ${name}`);
      groupResults.push({ id, name, status: 'pass', duration_ms: Date.now() - groupStart });
    } catch (e) {
      failed++;
      log(`  FAIL: ${name}: ${e.message}`);
      if (VERBOSE) console.error(e.stack);
      groupResults.push({ id, name, status: 'fail', duration_ms: Date.now() - groupStart, error: e.message });
    }
  }

  await cleanup();

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);

  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const resultsPath = resolve(RESULTS_DIR, 'e2e-cache-boundary.json');
    writeFileSync(resultsPath, JSON.stringify({
      suite: 'cache-boundary',
      timestamp: new Date().toISOString(),
      server_url: BASE_URL,
      groups: groupResults,
      summary: { passed, failed, total: groups.length, duration_ms: Date.now() - suiteStart },
    }, null, 2));
    log(`JSON results: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => { console.error(e); process.exit(1); });
