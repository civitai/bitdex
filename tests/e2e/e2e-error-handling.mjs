#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Error Handling & Edge Cases
 *
 * Tests HTTP error handling and edge cases that have zero server-level test coverage.
 *
 * Test groups:
 *   A. Invalid JSON / Malformed Requests
 *   B. Unknown Index Name
 *   C. Empty Index Queries
 *   D. Slot Recycling (Delete → Reinsert Same ID)
 *
 * Usage:
 *   node tests/e2e/e2e-error-handling.mjs [--url http://localhost:3000] [--verbose] [--keep] [--results-dir ./results]
 *
 * Prerequisites:
 *   Server running:
 *     cargo run --release --features server --bin bitdex-server -- --port 3000 --data-dir ./test-error-data
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

const INDEX = 'error-test';

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
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { _raw: text };
  }
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

/** POST with raw string body (not JSON.stringify'd) for malformed request testing. */
async function apiPostRaw(path, rawBody) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: rawBody,
  });
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { _raw: text };
  }
  vlog(`POST (raw) ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  let data;
  try {
    data = await res.json();
  } catch {
    data = { _raw: await res.text() };
  }
  vlog(`GET ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiDelete(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  let data;
  try {
    data = await res.json();
  } catch {
    data = { _raw: await res.text() };
  }
  vlog(`DELETE ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const query = (filters, sort, limit = 100) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, sort, limit }).then(r => r.data);
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const deleteDocs = (ids) =>
  apiDelete(`/api/indexes/${INDEX}/documents`, { ids }).then(r => r.data);
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

const SORT_DESC = { field: 'score', direction: 'Desc' };

// ----- Setup: Create test index (needed by Groups A, C, D) -----

async function setup() {
  log('\n--- Setup: Create test index ---');

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

// ----- Group A: Invalid JSON / Malformed Requests -----

async function testA_InvalidJsonRequests() {
  log('\n--- A. Invalid JSON / Malformed Requests ---');

  // A1: POST raw garbage string to query endpoint (index exists, so this tests JSON parsing)
  const r1 = await apiPostRaw(`/api/indexes/${INDEX}/query`, 'not json at all');
  assert(
    r1.status === 400 || r1.status === 422,
    `A1: Expected 400/422 for garbage body, got ${r1.status}`
  );
  log(`  [1] Garbage string body: ${r1.status} (correct)`);

  // A2: POST empty object {} to query endpoint
  const r2 = await apiPost(`/api/indexes/${INDEX}/query`, {});
  // Should be 400 (missing required fields) or succeed with empty results — NOT 500
  assert(
    r2.status < 500,
    `A2: Expected non-5xx for empty object, got ${r2.status}`
  );
  log(`  [2] Empty object {}: ${r2.status} (not 5xx — correct)`);

  // A3: POST { "filters": "not an object" } — wrong type for filters field
  const r3 = await apiPost(`/api/indexes/${INDEX}/query`, { filters: 'not an object' });
  assert(
    r3.status === 400 || r3.status === 422,
    `A3: Expected 400/422 for bad filters type, got ${r3.status}`
  );
  log(`  [3] filters as string: ${r3.status} (correct)`);
}

// ----- Group B: Unknown Index Name -----

async function testB_UnknownIndexName() {
  log('\n--- B. Unknown Index Name ---');

  const bogus = 'nonexistent-index-12345';

  // B1: GET stats for nonexistent index
  const r1 = await apiGet(`/api/indexes/${bogus}/stats`);
  assert(r1.status === 404, `B1: Expected 404 for unknown index stats, got ${r1.status}`);
  log(`  [1] GET stats for unknown index: ${r1.status} (correct)`);

  // B2: POST query to nonexistent index
  const r2 = await apiPost(`/api/indexes/${bogus}/query`, {
    filters: [],
    sort: { field: 'score', direction: 'Desc' },
    limit: 10,
  });
  assert(r2.status === 404, `B2: Expected 404 for unknown index query, got ${r2.status}`);
  log(`  [2] POST query to unknown index: ${r2.status} (correct)`);

  // B3: POST upsert to nonexistent index
  const r3 = await apiPost(`/api/indexes/${bogus}/documents/upsert`, {
    documents: [{ id: 1, category: 'test', score: 1 }],
  });
  assert(r3.status === 404, `B3: Expected 404 for unknown index upsert, got ${r3.status}`);
  log(`  [3] POST upsert to unknown index: ${r3.status} (correct)`);
}

// ----- Group C: Empty Index Queries -----

async function testC_EmptyIndexQueries() {
  log('\n--- C. Empty Index Queries ---');

  // Index already created by setup(). It has 0 documents.

  // C1: Query with no filters, no sort — empty index
  const r1 = await apiPost(`/api/indexes/${INDEX}/query`, {
    filters: [],
    limit: 10,
  });
  assert(r1.status === 200, `C1: Expected 200 for empty index query, got ${r1.status}`);
  assert(
    r1.data.total_matched === 0 || r1.data.ids?.length === 0,
    `C1: Expected 0 results, got total_matched=${r1.data.total_matched}, ids=${r1.data.ids?.length}`
  );
  log(`  [1] Query empty index (no filters): total=${r1.data.total_matched} (correct)`);

  // C2: Query with sort on empty index
  const r2 = await apiPost(`/api/indexes/${INDEX}/query`, {
    filters: [],
    sort: SORT_DESC,
    limit: 10,
  });
  assert(r2.status === 200, `C2: Expected 200 for sorted query on empty index, got ${r2.status}`);
  assert(
    r2.data.total_matched === 0 || r2.data.ids?.length === 0,
    `C2: Expected 0 sorted results, got total_matched=${r2.data.total_matched}, ids=${r2.data.ids?.length}`
  );
  log(`  [2] Query empty index (with sort): total=${r2.data.total_matched} (correct)`);

  // C3: Query with filter on empty index — no matching value
  const r3 = await apiPost(`/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['category', { Integer: 999 }] }],
    limit: 10,
  });
  assert(r3.status === 200, `C3: Expected 200 for filter query on empty index, got ${r3.status}`);
  assert(
    r3.data.total_matched === 0 || r3.data.ids?.length === 0,
    `C3: Expected 0 filtered results, got total_matched=${r3.data.total_matched}, ids=${r3.data.ids?.length}`
  );
  log(`  [3] Query empty index (Eq filter): total=${r3.data.total_matched} (correct)`);
}

// ----- Group D: Slot Recycling (Delete → Reinsert Same ID) -----

async function testD_SlotRecycling() {
  log('\n--- D. Slot Recycling (Delete → Reinsert Same ID) ---');

  // D1: Insert doc id=1 (score=100) and anchor doc id=2 (score=150)
  // The anchor doc makes sort-order assertions meaningful — without it,
  // a single doc is always "first" regardless of stale sort bits.
  // Category: 1=original, 2=anchor, 3=recycled
  await upsert([
    { id: 1, category: 1, score: 100 },
    { id: 2, category: 2, score: 150 },
  ]);
  log(`  [1] Inserted doc 1 (score=100) and anchor doc 2 (score=150)`);

  // D2: Wait for flush, verify sort order: [2, 1] (150 > 100)
  await waitForFlush(2);
  const r1 = await freshQuery([], SORT_DESC);
  assert(r1.ids && r1.ids.length === 2, `D2: Expected 2 results, got ${r1.ids?.length}`);
  assert(r1.ids[0] === 2, `D2: Expected doc 2 first (score=150), got ${r1.ids[0]}`);
  assert(r1.ids[1] === 1, `D2: Expected doc 1 second (score=100), got ${r1.ids[1]}`);
  log(`  [2] Sort Desc: [${r1.ids}] = [2,1] (correct — 150 > 100)`);

  // D3: Delete doc id=1
  await deleteDocs([1]);
  log(`  [3] Deleted doc 1`);

  // D4: Wait for flush, verify doc 1 gone from filter AND sort
  await waitForFlush(1);
  const r2 = await freshQuery(
    [{ Eq: ['category', { Integer: 1 }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 0, `D4: Expected 0 results for category=1 after delete, got ${r2.ids?.length}`);
  log(`  [4] Query category=1 after delete: [] (correct — clean delete)`);

  // D5: Reinsert same id with new values (score=200, higher than anchor)
  await upsert([{ id: 1, category: 3, score: 200 }]);
  log(`  [5] Reinserted doc 1 (category=3, score=200)`);

  // D6: Wait for flush, verify new value appears
  await waitForFlush(2);
  const r3 = await freshQuery(
    [{ Eq: ['category', { Integer: 3 }] }],
    SORT_DESC,
  );
  assert(r3.ids && r3.ids.length === 1, `D6: Expected 1 result for category=3, got ${r3.ids?.length}`);
  assert(r3.ids[0] === 1, `D6: Expected doc id=1, got ${r3.ids[0]}`);
  log(`  [6] Query category=3: [${r3.ids}] (correct)`);

  // D7: Old value fully gone
  const r4 = await freshQuery(
    [{ Eq: ['category', { Integer: 1 }] }],
    SORT_DESC,
  );
  assert(r4.ids && r4.ids.length === 0, `D7: Expected 0 results for category=1 after recycle, got ${r4.ids?.length}`);
  log(`  [7] Query category=1 after recycle: [] (correct — old value fully gone)`);

  // D8: Sort order proves clean delete — doc 1 (score=200) must be ABOVE anchor (score=150)
  // If old score=100 bits leaked, doc 1 would appear below anchor.
  const r5 = await freshQuery([], SORT_DESC);
  assert(r5.ids && r5.ids.length === 2, `D8: Expected 2 results for sort query, got ${r5.ids?.length}`);
  assert(r5.ids[0] === 1, `D8: Expected doc 1 first (score=200), got ${r5.ids[0]}`);
  assert(r5.ids[1] === 2, `D8: Expected doc 2 second (score=150), got ${r5.ids[1]}`);
  log(`  [8] Sort Desc: [${r5.ids}] = [1,2] (correct — recycled doc 200 > anchor 150, no stale bits)`);
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
  ['A', 'Invalid JSON / Malformed requests', testA_InvalidJsonRequests],
  ['B', 'Unknown index name', testB_UnknownIndexName],
  ['C', 'Empty index queries', testC_EmptyIndexQueries],
  ['D', 'Slot recycling (delete + reinsert same ID)', testD_SlotRecycling],
];

async function main() {
  log('BitDex Error Handling E2E Tests');
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
    log(`  cargo run --release --features server --bin bitdex-server -- --port 3000 --data-dir ./test-error-data`);
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
      suite: 'error-handling',
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
    const resultsPath = resolve(RESULTS_DIR, 'error-handling.json');
    writeFileSync(resultsPath, JSON.stringify(resultsJson, null, 2));
    log(`JSON results written to: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
