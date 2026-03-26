#!/usr/bin/env node
/**
 * E2E Validation Suite for Computed Sort Fields
 *
 * Tests that computed sort fields (sortAt = GREATEST(existedAt, publishedAt))
 * produce correct sort order through the full pipeline: insert → flush → query.
 *
 * Test groups:
 *   A. Insert with both source fields → computed sort order correct
 *   B. Update one source field → computed sort recalculates
 *   C. Update the other source field → GREATEST changes winner
 *   D. PATCH with only one source field → computed uses old + new
 *
 * Usage:
 *   node tests/e2e/e2e-computed-sort.mjs [--url http://localhost:3000] [--verbose] [--keep]
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

const INDEX = 'computed-sort-test';

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
  const data = await res.json();
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiPatch(path, body) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  vlog(`PATCH ${path}:`, JSON.stringify(data).slice(0, 500));
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
const patch = (documents) =>
  apiPatch(`/api/indexes/${INDEX}/documents/patch`, { documents }).then(r => r.data);
const clearCache = () =>
  apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForFlush(expectedAlive, maxWaitMs = 3000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await stats();
    if (s.alive_count === expectedAlive) return s;
    await sleep(50);
  }
  return await stats();
}

async function freshQuery(filters, sort, limit = 100) {
  await clearCache();
  return query(filters, sort, limit);
}

const SORT_AT_DESC = { field: 'sortAt', direction: 'Desc' };
const SORT_AT_ASC = { field: 'sortAt', direction: 'Asc' };

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create test index with computed sort field ---');

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
        { name: 'nsfwLevel', field_type: 'single_value' },
      ],
      sort_fields: [
        { name: 'existedAt', bits: 32 },
        { name: 'publishedAt', bits: 32 },
        {
          name: 'sortAt',
          bits: 32,
          computed: {
            op: 'greatest',
            source_fields: ['existedAt', 'publishedAt'],
          },
        },
      ],
      max_page_size: 100,
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'existedAt', target: 'existedAt', value_type: 'integer' },
        { source: 'publishedAt', target: 'publishedAt', value_type: 'integer' },
        // sortAt is computed — no source mapping needed
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created with computed sortAt = GREATEST(existedAt, publishedAt)`);
}

// ----- Test A: Insert with both source fields -----

async function testA_InsertComputedSort() {
  log('\n--- A. Insert with Both Source Fields → Computed Sort Order ---');
  const groupStart = Date.now();
  const tests = [];

  // Insert 5 docs with different existedAt and publishedAt values
  // Doc 1: existedAt=100, publishedAt=500 → sortAt=500
  // Doc 2: existedAt=400, publishedAt=200 → sortAt=400
  // Doc 3: existedAt=300, publishedAt=300 → sortAt=300
  // Doc 4: existedAt=600, publishedAt=100 → sortAt=600
  // Doc 5: existedAt=200, publishedAt=700 → sortAt=700
  await upsert([
    { id: 1, nsfwLevel: 1, existedAt: 100, publishedAt: 500 },
    { id: 2, nsfwLevel: 1, existedAt: 400, publishedAt: 200 },
    { id: 3, nsfwLevel: 1, existedAt: 300, publishedAt: 300 },
    { id: 4, nsfwLevel: 1, existedAt: 600, publishedAt: 100 },
    { id: 5, nsfwLevel: 1, existedAt: 200, publishedAt: 700 },
  ]);

  await waitForFlush(5);

  // Query sorted by sortAt Desc → expected order: 5(700), 4(600), 1(500), 2(400), 3(300)
  const result = await freshQuery([], SORT_AT_DESC);

  try {
    assert(result.ids.length === 5, `Expected 5 results, got ${result.ids.length}`);
    assert(result.ids[0] === 5, `First should be id=5 (sortAt=700), got ${result.ids[0]}`);
    assert(result.ids[1] === 4, `Second should be id=4 (sortAt=600), got ${result.ids[1]}`);
    assert(result.ids[2] === 1, `Third should be id=1 (sortAt=500), got ${result.ids[2]}`);
    assert(result.ids[3] === 2, `Fourth should be id=2 (sortAt=400), got ${result.ids[3]}`);
    assert(result.ids[4] === 3, `Fifth should be id=3 (sortAt=300), got ${result.ids[4]}`);
    tests.push({ name: 'Computed sort order correct on insert', passed: true });
    passed++;
    log('  PASS: Computed sort order correct on insert');
  } catch (e) {
    tests.push({ name: 'Computed sort order correct on insert', passed: false, error: e.message });
    failed++;
    log('  FAIL:', e.message);
  }

  // Also check Asc order
  const resultAsc = await freshQuery([], SORT_AT_ASC);
  try {
    assert(resultAsc.ids[0] === 3, `Asc first should be id=3 (sortAt=300), got ${resultAsc.ids[0]}`);
    assert(resultAsc.ids[4] === 5, `Asc last should be id=5 (sortAt=700), got ${resultAsc.ids[4]}`);
    tests.push({ name: 'Computed sort Asc order correct', passed: true });
    passed++;
    log('  PASS: Computed sort Asc order correct');
  } catch (e) {
    tests.push({ name: 'Computed sort Asc order correct', passed: false, error: e.message });
    failed++;
    log('  FAIL:', e.message);
  }

  groupResults.push({ group: 'A', name: 'Insert Computed Sort', tests, duration_ms: Date.now() - groupStart });
}

// ----- Test B: Update existedAt → sortAt recalculates -----

async function testB_UpdateExistedAt() {
  log('\n--- B. Update existedAt → sortAt Recalculates ---');
  const groupStart = Date.now();
  const tests = [];

  // Doc 3 was: existedAt=300, publishedAt=300 → sortAt=300 (lowest)
  // Update existedAt to 900 → sortAt should become 900 (highest)
  await upsert([{ id: 3, nsfwLevel: 1, existedAt: 900, publishedAt: 300 }]);
  await sleep(300);

  const result = await freshQuery([], SORT_AT_DESC);

  try {
    assert(result.ids[0] === 3, `Doc 3 should now be first (sortAt=900), got ${result.ids[0]}`);
    tests.push({ name: 'existedAt update recalculates sortAt', passed: true });
    passed++;
    log('  PASS: existedAt update recalculates sortAt');
  } catch (e) {
    tests.push({ name: 'existedAt update recalculates sortAt', passed: false, error: e.message });
    failed++;
    log('  FAIL:', e.message);
  }

  groupResults.push({ group: 'B', name: 'Update existedAt', tests, duration_ms: Date.now() - groupStart });
}

// ----- Test C: Update publishedAt → GREATEST changes winner -----

async function testC_UpdatePublishedAt() {
  log('\n--- C. Update publishedAt → GREATEST Changes Winner ---');
  const groupStart = Date.now();
  const tests = [];

  // Doc 2 was: existedAt=400, publishedAt=200 → sortAt=400
  // Update publishedAt to 1000 → sortAt should become 1000 (highest of all)
  await upsert([{ id: 2, nsfwLevel: 1, existedAt: 400, publishedAt: 1000 }]);
  await sleep(300);

  const result = await freshQuery([], SORT_AT_DESC);

  try {
    assert(result.ids[0] === 2, `Doc 2 should now be first (sortAt=1000), got ${result.ids[0]}`);
    tests.push({ name: 'publishedAt update changes sortAt winner', passed: true });
    passed++;
    log('  PASS: publishedAt update changes sortAt winner');
  } catch (e) {
    tests.push({ name: 'publishedAt update changes sortAt winner', passed: false, error: e.message });
    failed++;
    log('  FAIL:', e.message);
  }

  groupResults.push({ group: 'C', name: 'Update publishedAt', tests, duration_ms: Date.now() - groupStart });
}

// ----- Test D: PATCH with only one source field -----

async function testD_PatchSingleSource() {
  log('\n--- D. PATCH Single Source Field → Computed Uses Old + New ---');
  const groupStart = Date.now();
  const tests = [];

  // Doc 1: currently existedAt=100, publishedAt=500 → sortAt=500
  // PATCH only existedAt to 2000 → sortAt should become GREATEST(2000, 500) = 2000
  await patch([{ id: 1, existedAt: 2000 }]);
  await sleep(300);

  const result = await freshQuery([], SORT_AT_DESC);

  try {
    assert(result.ids[0] === 1, `Doc 1 should now be first (sortAt=2000), got ${result.ids[0]}`);
    tests.push({ name: 'PATCH single source recalculates computed', passed: true });
    passed++;
    log('  PASS: PATCH single source recalculates computed');
  } catch (e) {
    tests.push({ name: 'PATCH single source recalculates computed', passed: false, error: e.message });
    failed++;
    log('  FAIL:', e.message);
  }

  groupResults.push({ group: 'D', name: 'PATCH Single Source', tests, duration_ms: Date.now() - groupStart });
}

// ----- Main -----

async function main() {
  try {
    await setup();
    await testA_InsertComputedSort();
    await testB_UpdateExistedAt();
    await testC_UpdatePublishedAt();
    await testD_PatchSingleSource();
  } catch (e) {
    log('\nFATAL:', e.message);
    failed++;
  }

  // Cleanup
  if (!KEEP) {
    log('\n--- Cleanup ---');
    await apiDelete(`/api/indexes/${INDEX}`);
    log(`  Index '${INDEX}' deleted`);
  }

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);

  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    writeFileSync(
      resolve(RESULTS_DIR, 'e2e-computed-sort.json'),
      JSON.stringify({ suite: 'computed-sort', passed, failed, groups: groupResults }, null, 2),
    );
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
