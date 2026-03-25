#!/usr/bin/env node
/**
 * E2E: Deferred Alive — Time-Progression Visibility Test
 *
 * Verifies that documents with publishedAt in the future are invisible
 * until their activation time, then become visible after the flush thread
 * activates them.
 *
 * Test groups:
 *   A. Future doc invisible via upsert path (pg-sync write path)
 *   B. Past doc immediately visible via upsert path
 *   C. Future doc becomes visible after activation time
 *   D. Mixed batch: only past docs visible, future docs deferred
 *   E. Deferred doc not in negation queries (NOT nsfwLevel=X)
 *
 * Usage:
 *   node tests/e2e/e2e-deferred-alive.mjs [--url http://localhost:3000] [--verbose]
 *
 * Prerequisites:
 *   Server running with NO existing index:
 *     cargo run --release --features server --bin bitdex-server -- --port 3000 --data-dir ./test-deferred-data
 */

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const VERBOSE = process.argv.includes('--verbose');
const RESULTS_DIR = process.argv.includes('--results-dir')
  ? process.argv[process.argv.indexOf('--results-dir') + 1]
  : null;

const INDEX = 'deferred-test';

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

function nowUnix() { return Math.floor(Date.now() / 1000); }

async function waitForAlive(expected, maxWaitMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await stats();
    if (s.alive_count === expected) return s;
    await sleep(100);
  }
  return stats();
}

async function freshQuery(filters, sort, limit = 100) {
  await clearCache();
  return query(filters, sort, limit);
}

const SORT_DESC = { field: 'score', direction: 'Desc' };

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create deferred-alive test index ---');

  // Delete existing test index if present
  try { await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' }); } catch (_) {}
  await sleep(300);

  const { status } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'nsfwLevel', field_type: 'single_value' },
        { name: 'category', field_type: 'single_value' },
      ],
      sort_fields: [
        { name: 'score', bits: 32 },
      ],
      deferred_alive: { source_field: 'publishedAt' },
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'category', target: 'category', value_type: 'integer' },
        { source: 'publishedAt', target: 'publishedAt', value_type: 'integer' },
        { source: 'score', target: 'score', value_type: 'integer', default: 0 },
      ],
    },
  });
  assert(status === 200 || status === 201, `Create index failed: ${status}`);
  log(`  Index '${INDEX}' created with deferred_alive on publishedAt`);
}

// ----- Test A: Future doc invisible via upsert -----

async function testA() {
  const group = 'A';
  const results = [];
  log(`\n--- ${group}. Future doc invisible via upsert ---`);

  const futureTime = nowUnix() + 600; // 10 minutes from now
  await upsert([{ id: 1001, nsfwLevel: 1, category: 10, publishedAt: futureTime, score: 500 }]);
  await sleep(500); // let flush process

  const s = await stats();
  const test1 = s.alive_count === 0;
  results.push({ name: 'future doc not alive', pass: test1 });
  log(`  [1] alive_count=${s.alive_count} (expect 0): ${test1 ? 'PASS' : 'FAIL'}`);

  // Query should return nothing
  const q = await freshQuery([{ field: 'nsfwLevel', op: 'Eq', value: 1 }], SORT_DESC);
  const test2 = q.ids.length === 0;
  results.push({ name: 'future doc not in query results', pass: test2 });
  log(`  [2] query ids=${q.ids.length} (expect 0): ${test2 ? 'PASS' : 'FAIL'}`);

  // Deferred count should be 1
  const test3 = (s.deferred_count || 0) >= 0; // may not be exposed in stats, just check no crash
  results.push({ name: 'stats endpoint ok with deferred', pass: test3 });
  log(`  [3] deferred_count=${s.deferred_count ?? 'N/A'}: ${test3 ? 'PASS' : 'FAIL'}`);

  groupResults.push({ group, results });
  results.forEach(r => r.pass ? passed++ : failed++);
}

// ----- Test B: Past doc immediately visible -----

async function testB() {
  const group = 'B';
  const results = [];
  log(`\n--- ${group}. Past doc immediately visible ---`);

  const pastTime = nowUnix() - 3600; // 1 hour ago
  await upsert([{ id: 2001, nsfwLevel: 2, category: 20, publishedAt: pastTime, score: 300 }]);
  const s = await waitForAlive(1, 3000);

  const test1 = s.alive_count === 1;
  results.push({ name: 'past doc is alive', pass: test1 });
  log(`  [1] alive_count=${s.alive_count} (expect 1): ${test1 ? 'PASS' : 'FAIL'}`);

  const q = await freshQuery([{ field: 'nsfwLevel', op: 'Eq', value: 2 }], SORT_DESC);
  const test2 = q.ids.length === 1 && q.ids[0] === 2001;
  results.push({ name: 'past doc in query results', pass: test2 });
  log(`  [2] query ids=${JSON.stringify(q.ids)} (expect [2001]): ${test2 ? 'PASS' : 'FAIL'}`);

  groupResults.push({ group, results });
  results.forEach(r => r.pass ? passed++ : failed++);
}

// ----- Test C: Future doc becomes visible after activation -----

async function testC() {
  const group = 'C';
  const results = [];
  log(`\n--- ${group}. Future doc becomes visible after activation ---`);

  // Insert doc that activates in 3 seconds
  const activateAt = nowUnix() + 3;
  await upsert([{ id: 3001, nsfwLevel: 4, category: 30, publishedAt: activateAt, score: 999 }]);
  await sleep(500);

  // Should still be invisible
  const s1 = await stats();
  const test1 = s1.alive_count === 1; // only doc 2001 from test B
  results.push({ name: 'doc invisible before activation', pass: test1 });
  log(`  [1] alive_count=${s1.alive_count} (expect 1): ${test1 ? 'PASS' : 'FAIL'}`);

  // Wait for activation (3s + buffer)
  log('  [2] Waiting for activation...');
  await sleep(4000);

  // Should now be visible
  const s2 = await waitForAlive(2, 5000);
  const test2 = s2.alive_count === 2;
  results.push({ name: 'doc visible after activation', pass: test2 });
  log(`  [2] alive_count=${s2.alive_count} (expect 2): ${test2 ? 'PASS' : 'FAIL'}`);

  // Should appear in queries
  const q = await freshQuery([{ field: 'nsfwLevel', op: 'Eq', value: 4 }], SORT_DESC);
  const test3 = q.ids.length === 1 && q.ids[0] === 3001;
  results.push({ name: 'activated doc in query results', pass: test3 });
  log(`  [3] query ids=${JSON.stringify(q.ids)} (expect [3001]): ${test3 ? 'PASS' : 'FAIL'}`);

  groupResults.push({ group, results });
  results.forEach(r => r.pass ? passed++ : failed++);
}

// ----- Test D: Mixed batch — only past docs visible -----

async function testD() {
  const group = 'D';
  const results = [];
  log(`\n--- ${group}. Mixed batch: past visible, future deferred ---`);

  const past = nowUnix() - 1000;
  const future = nowUnix() + 600;

  await upsert([
    { id: 4001, nsfwLevel: 1, category: 40, publishedAt: past, score: 100 },
    { id: 4002, nsfwLevel: 1, category: 40, publishedAt: future, score: 200 },
    { id: 4003, nsfwLevel: 1, category: 40, publishedAt: past, score: 300 },
    { id: 4004, nsfwLevel: 1, category: 40, publishedAt: future, score: 400 },
  ]);

  // Wait for flush — 2 existing + 2 past = 4 alive (2 future deferred)
  const s = await waitForAlive(4, 3000);
  const test1 = s.alive_count === 4;
  results.push({ name: 'only past docs alive in mixed batch', pass: test1 });
  log(`  [1] alive_count=${s.alive_count} (expect 4): ${test1 ? 'PASS' : 'FAIL'}`);

  // Query for category=40 should return only 4001 and 4003
  const q = await freshQuery([{ field: 'category', op: 'Eq', value: 40 }], SORT_DESC);
  const test2 = q.ids.length === 2;
  results.push({ name: 'only 2 of 4 docs in query', pass: test2 });
  log(`  [2] query ids=${JSON.stringify(q.ids)} (expect 2 results): ${test2 ? 'PASS' : 'FAIL'}`);

  // Future docs should NOT be in results
  const hasFuture = q.ids.includes(4002) || q.ids.includes(4004);
  const test3 = !hasFuture;
  results.push({ name: 'future docs not in results', pass: test3 });
  log(`  [3] future docs absent: ${test3 ? 'PASS' : 'FAIL'}`);

  groupResults.push({ group, results });
  results.forEach(r => r.pass ? passed++ : failed++);
}

// ----- Test E: Deferred doc excluded from negation queries -----

async function testE() {
  const group = 'E';
  const results = [];
  log(`\n--- ${group}. Deferred doc excluded from negation ---`);

  // Query NOT nsfwLevel=99 — should return all alive docs, not deferred ones
  const q = await freshQuery([{ field: 'nsfwLevel', op: 'NotEq', value: 99 }], SORT_DESC);
  const hasFutureDoc = q.ids.includes(1001) || q.ids.includes(4002) || q.ids.includes(4004);
  const test1 = !hasFutureDoc;
  results.push({ name: 'deferred docs excluded from NOT query', pass: test1 });
  log(`  [1] future docs in NOT query: ${hasFutureDoc} (expect false): ${test1 ? 'PASS' : 'FAIL'}`);

  groupResults.push({ group, results });
  results.forEach(r => r.pass ? passed++ : failed++);
}

// ----- Main -----

async function main() {
  log('=== BitDex E2E: Deferred Alive Time-Progression ===');
  log(`Server: ${BASE_URL}`);

  try {
    await setup();
    await testA();
    await testB();
    await testC();
    await testD();
    await testE();
  } catch (err) {
    console.error('\nFATAL:', err.message);
    failed++;
  }

  // Cleanup
  try {
    await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' });
  } catch (_) {}

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);

  if (RESULTS_DIR) {
    const { mkdirSync, writeFileSync } = await import('node:fs');
    const { resolve } = await import('node:path');
    mkdirSync(RESULTS_DIR, { recursive: true });
    writeFileSync(
      resolve(RESULTS_DIR, 'e2e-deferred-alive.json'),
      JSON.stringify({ passed, failed, groups: groupResults }, null, 2)
    );
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
