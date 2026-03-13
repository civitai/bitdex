#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Query Operators
 *
 * Tests query operators that have zero unit test coverage through the HTTP API path:
 * range filters (Gt, Gte, Lt, Lte), NotEq, and combined range+filter queries.
 *
 * Test groups:
 *   A. Range filters (Gt, Gte, Lt, Lte) on integer field
 *   B. NotEq filter on string field
 *   C. Range + filter combination with sorted output
 *
 * Usage:
 *   node tests/e2e/e2e-query-operators.mjs [--url http://localhost:3000] [--verbose] [--keep]
 *
 * Prerequisites:
 *   Server running with NO existing index (or use --keep to skip cleanup):
 *     cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-query-data
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

const INDEX = 'query-ops-test';

let passed = 0;
let failed = 0;

// Per-group results tracking for JSON output
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
  const data = await res.json();
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  const data = await res.json();
  vlog(`GET ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiDelete(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const data = await res.json();
  vlog(`DELETE ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const query = (filters, sort, limit = 100) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, sort, limit }).then(r => r.data);
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const clearCache = () =>
  apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/** Wait for flush to process mutations. Polls stats until alive_count matches expected. */
async function waitForFlush(expectedAlive, maxWaitMs = 3000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await stats();
    if (s.alive_count === expectedAlive) return s;
    await sleep(50);
  }
  const s = await stats();
  vlog(`waitForFlush timeout: expected ${expectedAlive}, got ${s.alive_count}`);
  return s;
}

/** Query with cache cleared to ensure we hit the bitmap path. */
async function freshQuery(filters, sort, limit = 100) {
  await clearCache();
  return query(filters, sort, limit);
}

const SORT_SCORE_DESC = { field: 'score', direction: 'Desc' };
const SORT_SCORE_ASC = { field: 'score', direction: 'Asc' };

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create test index ---');

  // Delete existing test index if present
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
        { name: 'category', field_type: 'single_value' },
        { name: 'score', field_type: 'single_value' },
      ],
      sort_fields: [
        { name: 'score', bits: 32 },
      ],
      max_page_size: 100,
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
}

// ----- Test A: Range Filters (Gt, Gte, Lt, Lte) -----

async function testA_RangeFilters() {
  log('\n--- A. Range Filters (Gt, Gte, Lt, Lte) ---');

  // Insert 10 docs with scores 10, 20, ..., 100. Category 1 = range-test group.
  const docs = [];
  for (let i = 1; i <= 10; i++) {
    docs.push({
      id: i,
      category: 1,
      score: i * 10,
    });
  }

  const res = await upsert(docs);
  assert(res.upserted === 10, `Expected 10 upserted, got ${res.upserted}`);
  log(`  [1] Inserted 10 documents with scores 10-100`);

  // Wait for flush
  await waitForFlush(10);

  // Test Gt: score > 50 => scores 60,70,80,90,100 => ids 10,9,8,7,6 (desc)
  const rGt = await freshQuery(
    [{ Gt: ['score', { Integer: 50 }] }],
    SORT_SCORE_DESC,
  );
  assert(rGt.ids && rGt.ids.length === 5, `Gt: expected 5 results, got ${rGt.ids?.length}`);
  assert(rGt.ids.join(',') === '10,9,8,7,6', `Gt: expected [10,9,8,7,6], got [${rGt.ids.join(',')}]`);
  log(`  [2] Gt score>50: [${rGt.ids.join(',')}] (correct)`);

  // Test Gte: score >= 50 => scores 50..100 => ids 10,9,8,7,6,5 (desc)
  const rGte = await freshQuery(
    [{ Gte: ['score', { Integer: 50 }] }],
    SORT_SCORE_DESC,
  );
  assert(rGte.ids && rGte.ids.length === 6, `Gte: expected 6 results, got ${rGte.ids?.length}`);
  assert(rGte.ids.join(',') === '10,9,8,7,6,5', `Gte: expected [10,9,8,7,6,5], got [${rGte.ids.join(',')}]`);
  log(`  [3] Gte score>=50: [${rGte.ids.join(',')}] (correct)`);

  // Test Lt: score < 30 => scores 10,20 => ids 1,2 (asc)
  const rLt = await freshQuery(
    [{ Lt: ['score', { Integer: 30 }] }],
    SORT_SCORE_ASC,
  );
  assert(rLt.ids && rLt.ids.length === 2, `Lt: expected 2 results, got ${rLt.ids?.length}`);
  assert(rLt.ids.join(',') === '1,2', `Lt: expected [1,2], got [${rLt.ids.join(',')}]`);
  log(`  [4] Lt score<30: [${rLt.ids.join(',')}] (correct)`);

  // Test Lte: score <= 30 => scores 10,20,30 => ids 1,2,3 (asc)
  const rLte = await freshQuery(
    [{ Lte: ['score', { Integer: 30 }] }],
    SORT_SCORE_ASC,
  );
  assert(rLte.ids && rLte.ids.length === 3, `Lte: expected 3 results, got ${rLte.ids?.length}`);
  assert(rLte.ids.join(',') === '1,2,3', `Lte: expected [1,2,3], got [${rLte.ids.join(',')}]`);
  log(`  [5] Lte score<=30: [${rLte.ids.join(',')}] (correct)`);

  // Edge case: range completely outside data bounds => empty results
  const rEmpty = await freshQuery(
    [{ Gt: ['score', { Integer: 9999 }] }],
    SORT_SCORE_DESC,
  );
  assert(rEmpty.ids && rEmpty.ids.length === 0, `Out-of-bounds Gt: expected 0 results, got ${rEmpty.ids?.length}`);
  log(`  [6] Gt score>9999: [] (correct — empty range)`);
}

// ----- Test B: NotEq Filter -----

async function testB_NotEqFilter() {
  log('\n--- B. NotEq Filter ---');

  // Insert 5 docs with different categories (ID range 201-205 to avoid collisions)
  // Category 2 = A, Category 3 = B, Category 4 = C
  const docs = [
    { id: 201, category: 2, score: 10 },
    { id: 202, category: 3, score: 20 },
    { id: 203, category: 2, score: 30 },
    { id: 204, category: 4, score: 40 },
    { id: 205, category: 3, score: 50 },
  ];

  const res = await upsert(docs);
  assert(res.upserted === 5, `Expected 5 upserted, got ${res.upserted}`);
  log(`  [1] Inserted 5 documents: cat=2(201,203), cat=3(202,205), cat=4(204)`);

  await waitForFlush(15); // 10 from Test A + 5 from Test B

  // NotEq category != 2 => ids 202, 204, 205 (those with category 3 or 4)
  // Also returns Group A docs (category=1), so filter to B-group IDs
  const r1 = await freshQuery(
    [{ NotEq: ['category', { Integer: 2 }] }],
    SORT_SCORE_DESC,
  );
  assert(r1.ids && r1.ids.length >= 3, `NotEq 2: expected at least 3 results from B group, got ${r1.ids?.length}`);
  const bGroupIds = r1.ids.filter(id => id >= 201 && id <= 205);
  assert(bGroupIds.length === 3, `NotEq 2: expected 3 B-group results, got ${bGroupIds.length}`);
  assert(bGroupIds.includes(202), `NotEq 2: expected id 202`);
  assert(bGroupIds.includes(204), `NotEq 2: expected id 204`);
  assert(bGroupIds.includes(205), `NotEq 2: expected id 205`);
  assert(!bGroupIds.includes(201), `NotEq 2: id 201 should NOT be in results (category=2)`);
  assert(!bGroupIds.includes(203), `NotEq 2: id 203 should NOT be in results (category=2)`);
  log(`  [2] NotEq category!=2: B-group ids [${bGroupIds.join(',')}] (correct)`);

  // NotEq category != 99 => all 5 B-group docs (no doc has category 99)
  const r2 = await freshQuery(
    [{ NotEq: ['category', { Integer: 99 }] }],
    SORT_SCORE_DESC,
  );
  const bGroupIds2 = r2.ids.filter(id => id >= 201 && id <= 205);
  assert(bGroupIds2.length === 5, `NotEq 99: expected 5 B-group results, got ${bGroupIds2.length}`);
  for (const id of [201, 202, 203, 204, 205]) {
    assert(bGroupIds2.includes(id), `NotEq 99: expected id ${id}`);
  }
  log(`  [3] NotEq category!=99: all 5 B-group docs present (correct)`);
}

// ----- Test C: Range + Filter Combination -----

async function testC_RangeFilterCombination() {
  log('\n--- C. Range + Filter Combination ---');

  // Use docs from Group A (ids 1-10, scores 10-100).
  // Isolate with category=1 to avoid pollution from Group B docs.
  // And: category=1 AND score >= 30 AND score <= 70 => ids 3,4,5,6,7
  const rangeFilter = [
    { Eq: ['category', { Integer: 1 }] },
    {
      And: [
        { Gte: ['score', { Integer: 30 }] },
        { Lte: ['score', { Integer: 70 }] },
      ],
    },
  ];

  const r1 = await freshQuery(rangeFilter, SORT_SCORE_DESC);
  assert(r1.ids && r1.ids.length === 5, `And Gte+Lte: expected 5 results, got ${r1.ids?.length}`);
  assert(r1.ids.join(',') === '7,6,5,4,3', `And Gte+Lte: expected [7,6,5,4,3], got [${r1.ids.join(',')}]`);
  log(`  [1] And(Gte 30, Lte 70): [${r1.ids.join(',')}] (correct)`);

  // Same query but limit 3 Desc: should get 70, 60, 50 => ids 7, 6, 5
  const r2 = await freshQuery(rangeFilter, SORT_SCORE_DESC, 3);
  assert(r2.ids && r2.ids.length === 3, `Sorted top-3: expected 3 results, got ${r2.ids?.length}`);
  assert(r2.ids.join(',') === '7,6,5', `Sorted top-3: expected [7,6,5], got [${r2.ids.join(',')}]`);
  log(`  [2] Top-3 Desc: [${r2.ids.join(',')}] (correct)`);

  // Sort Asc, limit 3: should get 30, 40, 50 => ids 3, 4, 5
  const r3 = await freshQuery(rangeFilter, SORT_SCORE_ASC, 3);
  assert(r3.ids && r3.ids.length === 3, `Sorted bottom-3: expected 3 results, got ${r3.ids?.length}`);
  assert(r3.ids.join(',') === '3,4,5', `Sorted bottom-3: expected [3,4,5], got [${r3.ids.join(',')}]`);
  log(`  [3] Bottom-3 Asc: [${r3.ids.join(',')}] (correct)`);
}

// ----- Cleanup -----

async function cleanup() {
  if (KEEP) {
    log(`\n  --keep specified, leaving index '${INDEX}' in place`);
    return;
  }
  log(`\n--- Cleanup: Deleting test index ---`);
  await apiDelete(`/api/indexes/${INDEX}`);
  log(`  Index '${INDEX}' deleted`);
}

// ----- Runner -----

const groups = [
  ['Setup', 'Create test index', setup],
  ['A', 'Range filters (Gt, Gte, Lt, Lte)', testA_RangeFilters],
  ['B', 'NotEq filter', testB_NotEqFilter],
  ['C', 'Range + filter combination', testC_RangeFilterCombination],
];

async function main() {
  log('BitDex Query Operators E2E Tests');
  log(`Server: ${BASE_URL}`);
  log(`Index:  ${INDEX}`);

  // Verify server is reachable
  try {
    const res = await fetch(`${BASE_URL}/api/indexes`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    log('Server is reachable');
  } catch (e) {
    log(`\nERROR: Cannot reach server at ${BASE_URL}`);
    log(`  ${e.message}`);
    log(`\nStart the server first:`);
    log(`  cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-query-data`);
    process.exit(1);
  }

  const suiteStart = Date.now();

  for (const [id, name, fn] of groups) {
    const groupStart = Date.now();
    try {
      await fn();
      passed++;
      log(`  PASS: ${id}. ${name}`);
      groupResults.push({
        id,
        name,
        status: 'pass',
        duration_ms: Date.now() - groupStart,
        assertions: [],
      });
    } catch (e) {
      failed++;
      log(`  FAIL: ${id}. ${name}`);
      log(`    ${e.message}`);
      if (VERBOSE) console.error(e.stack);
      groupResults.push({
        id,
        name,
        status: 'fail',
        duration_ms: Date.now() - groupStart,
        assertions: [{ check: e.message, passed: false }],
      });
    }
  }

  await cleanup();

  log(`\n${'='.repeat(50)}`);
  log(`Results: ${passed} passed, ${failed} failed out of ${groups.length}`);
  log(`${'='.repeat(50)}`);

  // Write JSON results if --results-dir provided
  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const resultsJson = {
      suite: 'query-operators',
      timestamp: new Date().toISOString(),
      server_url: BASE_URL,
      groups: groupResults,
      summary: {
        passed,
        failed,
        total: groups.length,
        duration_ms: Date.now() - suiteStart,
      },
    };
    const resultsPath = resolve(RESULTS_DIR, 'query-operators.json');
    writeFileSync(resultsPath, JSON.stringify(resultsJson, null, 2));
    log(`JSON results written to: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
