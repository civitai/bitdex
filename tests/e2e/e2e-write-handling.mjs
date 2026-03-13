#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Write Handling
 *
 * Tests that writes (upserts, updates, deletes) correctly update bitmaps
 * and that queries reflect changes after the flush cycle.
 *
 * Test groups:
 *   A. Fresh insert appears in queries
 *   B. Upsert updates filter values (old value gone, new value present)
 *   C. Upsert updates sort values (sort order changes)
 *   D. Delete removes document from queries
 *   E. Concurrent reads during writes return consistent results
 *   F. Multi-value field upsert (add/remove tags)
 *
 * Usage:
 *   node tests/e2e/e2e-write-handling.mjs [--url http://localhost:3000] [--verbose] [--keep]
 *
 * Prerequisites:
 *   Server running with NO existing index (or use --keep to skip cleanup):
 *     cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-write-data
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

const INDEX = 'write-test';

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

const SORT_DESC = { field: 'reactionCount', direction: 'Desc' };
const SORT_ASC = { field: 'reactionCount', direction: 'Asc' };

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
        { name: 'nsfwLevel', field_type: 'single_value' },
        { name: 'tagIds', field_type: 'multi_value' },
        { name: 'status', field_type: 'single_value' },
      ],
      sort_fields: [
        { name: 'reactionCount', bits: 32 },
      ],
      max_page_size: 100,
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'tagIds', target: 'tagIds', value_type: 'integer_array' },
        { source: 'status', target: 'status', value_type: 'integer' },
        { source: 'reactionCount', target: 'reactionCount', value_type: 'integer' },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created`);
}

// ----- Test A: Fresh insert appears in queries -----

async function testA_InsertAppearsInQuery() {
  log('\n--- A. Fresh Insert Appears in Query ---');

  // Insert 10 documents
  const docs = [];
  for (let i = 1; i <= 10; i++) {
    docs.push({
      id: i,
      nsfwLevel: 1,
      tagIds: [10, 20],
      status: 1,
      reactionCount: i * 100,
    });
  }

  const res = await upsert(docs);
  assert(res.upserted === 10, `Expected 10 upserted, got ${res.upserted}`);
  log(`  [1] Inserted 10 documents`);

  // Wait for flush
  await waitForFlush(10);

  // Query by nsfwLevel=1 — should return all 10
  const r = await freshQuery(
    [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    SORT_DESC,
  );
  assert(r.ids && r.ids.length === 10, `Expected 10 results, got ${r.ids?.length}`);
  log(`  [2] Query nsfwLevel=1: ${r.ids.length} results (correct)`);

  // Verify sort order: highest reactionCount first
  assert(r.ids[0] === 10, `Expected doc 10 first (highest reactions), got ${r.ids[0]}`);
  assert(r.ids[9] === 1, `Expected doc 1 last (lowest reactions), got ${r.ids[9]}`);
  log(`  [3] Sort order correct: [${r.ids.slice(0, 3).join(', ')}, ..., ${r.ids[9]}]`);

  // Query by tagIds IN [10] — should also return all 10
  const r2 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 10 }]] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 10, `Expected 10 results for tag 10, got ${r2.ids?.length}`);
  log(`  [4] Query tagIds IN [10]: ${r2.ids.length} results (correct)`);
}

// ----- Test B: Upsert updates filter values -----

async function testB_UpsertUpdatesFilterValues() {
  log('\n--- B. Upsert Updates Filter Values ---');

  // Doc 5 currently has nsfwLevel=1. Change it to nsfwLevel=2.
  const res = await upsert([{
    id: 5,
    nsfwLevel: 2,
    tagIds: [10, 20],
    status: 1,
    reactionCount: 500,
  }]);
  assert(res.upserted === 1, `Expected 1 upserted, got ${res.upserted}`);
  log(`  [1] Updated doc 5: nsfwLevel 1→2`);

  // Wait for flush
  await sleep(300);

  // Query nsfwLevel=1 — should return 9 (doc 5 no longer matches)
  const r1 = await freshQuery(
    [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.length === 9, `Expected 9 results for nsfwLevel=1, got ${r1.ids?.length}`);
  assert(!r1.ids.includes(5), `Doc 5 should NOT appear in nsfwLevel=1 results`);
  log(`  [2] Query nsfwLevel=1: ${r1.ids.length} results, doc 5 absent (correct)`);

  // Query nsfwLevel=2 — should return 1 (doc 5 only)
  const r2 = await freshQuery(
    [{ Eq: ['nsfwLevel', { Integer: 2 }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 1, `Expected 1 result for nsfwLevel=2, got ${r2.ids?.length}`);
  assert(r2.ids[0] === 5, `Expected doc 5, got ${r2.ids[0]}`);
  log(`  [3] Query nsfwLevel=2: ${r2.ids.length} result, doc 5 present (correct)`);

  // Restore doc 5 to nsfwLevel=1 for subsequent tests
  await upsert([{ id: 5, nsfwLevel: 1, tagIds: [10, 20], status: 1, reactionCount: 500 }]);
  await sleep(200);
}

// ----- Test C: Upsert updates sort values -----

async function testC_UpsertUpdatesSortValues() {
  log('\n--- C. Upsert Updates Sort Values ---');

  // Doc 1 currently has reactionCount=100 (lowest). Boost it to 9999 (highest).
  await upsert([{
    id: 1,
    nsfwLevel: 1,
    tagIds: [10, 20],
    status: 1,
    reactionCount: 9999,
  }]);
  log(`  [1] Updated doc 1: reactionCount 100→9999`);

  await sleep(300);

  // Query sorted DESC — doc 1 should now be first
  const r = await freshQuery(
    [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    SORT_DESC,
  );
  assert(r.ids[0] === 1, `Expected doc 1 first (reactionCount=9999), got ${r.ids[0]}`);
  log(`  [2] Sort DESC: doc 1 is first (correct)`);

  // Query sorted ASC — doc 1 should be last
  const r2 = await freshQuery(
    [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    SORT_ASC,
  );
  assert(r2.ids[r2.ids.length - 1] === 1, `Expected doc 1 last in ASC, got ${r2.ids[r2.ids.length - 1]}`);
  log(`  [3] Sort ASC: doc 1 is last (correct)`);

  // Restore doc 1 to original value
  await upsert([{ id: 1, nsfwLevel: 1, tagIds: [10, 20], status: 1, reactionCount: 100 }]);
  await sleep(200);
}

// ----- Test D: Delete removes document from queries -----

async function testD_DeleteRemovesFromQuery() {
  log('\n--- D. Delete Removes Document from Query ---');

  // Delete doc 3
  const res = await deleteDocs([3]);
  log(`  [1] Deleted doc 3: ${JSON.stringify(res)}`);

  await waitForFlush(9);

  // Query — doc 3 should be absent
  const r = await freshQuery(
    [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    SORT_DESC,
  );
  assert(r.ids && r.ids.length === 9, `Expected 9 results after delete, got ${r.ids?.length}`);
  assert(!r.ids.includes(3), `Doc 3 should NOT appear after deletion`);
  log(`  [2] Query: ${r.ids.length} results, doc 3 absent (correct)`);

  // Also verify tag query doesn't return doc 3
  const r2 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 10 }]] }],
    SORT_DESC,
  );
  assert(!r2.ids.includes(3), `Doc 3 should NOT appear in tag query after deletion`);
  log(`  [3] Tag query: doc 3 absent (correct)`);

  // Re-insert doc 3 for subsequent tests
  await upsert([{ id: 3, nsfwLevel: 1, tagIds: [10, 20], status: 1, reactionCount: 300 }]);
  await waitForFlush(10);
  log(`  [4] Re-inserted doc 3`);
}

// ----- Test E: Concurrent reads during writes -----

async function testE_ConcurrentReadsAndWrites() {
  log('\n--- E. Concurrent Reads During Writes ---');

  // Fire off rapid writes while also querying. No query should panic or
  // return an error. Results should always be consistent (no mix of old/new
  // that violates correctness — e.g., a doc appears but with wrong filter).

  const writePromises = [];
  const readPromises = [];

  // 20 rapid upserts changing status field
  for (let i = 0; i < 20; i++) {
    writePromises.push(
      upsert([{
        id: 1,
        nsfwLevel: 1,
        tagIds: [10, 20],
        status: (i % 2 === 0) ? 1 : 2,
        reactionCount: 100 + i,
      }])
    );
  }

  // 20 concurrent queries
  for (let i = 0; i < 20; i++) {
    readPromises.push(
      query(
        [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
        SORT_DESC,
      )
    );
  }

  const [writes, reads] = await Promise.all([
    Promise.all(writePromises),
    Promise.all(readPromises),
  ]);

  // All writes should succeed
  const writeErrors = writes.filter(w => w.upserted !== 1);
  assert(writeErrors.length === 0, `${writeErrors.length} writes failed`);
  log(`  [1] All 20 writes succeeded`);

  // All reads should return valid results (no errors, valid arrays)
  const readErrors = reads.filter(r => !r.ids || !Array.isArray(r.ids));
  assert(readErrors.length === 0, `${readErrors.length} reads returned invalid data`);
  log(`  [2] All 20 reads returned valid results`);

  // Each read should return 9 or 10 documents (doc count is consistent)
  const counts = reads.map(r => r.ids.length);
  const badCounts = counts.filter(c => c < 9 || c > 10);
  assert(badCounts.length === 0, `Some reads returned unexpected counts: ${JSON.stringify(badCounts)}`);
  log(`  [3] All reads returned 9-10 docs (expected during concurrent writes)`);

  // Wait for all mutations to settle
  await sleep(500);

  // Final state: doc 1 should exist with the last write's values
  const finalR = await freshQuery(
    [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    SORT_DESC,
  );
  assert(finalR.ids.includes(1), `Doc 1 should be present after concurrent writes`);
  log(`  [4] Final state consistent: doc 1 present, ${finalR.ids.length} total docs`);

  // Restore doc 1
  await upsert([{ id: 1, nsfwLevel: 1, tagIds: [10, 20], status: 1, reactionCount: 100 }]);
  await sleep(200);
}

// ----- Test F: Multi-value field upsert -----

async function testF_MultiValueFieldUpdate() {
  log('\n--- F. Multi-Value Field Update ---');

  // Doc 7 currently has tagIds=[10, 20]. Change to [10, 30].
  await upsert([{
    id: 7,
    nsfwLevel: 1,
    tagIds: [10, 30],
    status: 1,
    reactionCount: 700,
  }]);
  log(`  [1] Updated doc 7: tagIds [10,20]→[10,30]`);

  await sleep(300);

  // Query tagIds IN [20] — doc 7 should NOT appear (tag 20 removed)
  const r1 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 20 }]] }],
    SORT_DESC,
  );
  assert(!r1.ids.includes(7), `Doc 7 should NOT appear in tagIds IN [20] (tag removed)`);
  log(`  [2] Query tagIds IN [20]: doc 7 absent (correct, ${r1.ids.length} results)`);

  // Query tagIds IN [30] — doc 7 SHOULD appear (tag 30 added)
  const r2 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 30 }]] }],
    SORT_DESC,
  );
  assert(r2.ids.includes(7), `Doc 7 should appear in tagIds IN [30] (tag added)`);
  assert(r2.ids.length === 1, `Expected only doc 7 with tag 30, got ${r2.ids.length}`);
  log(`  [3] Query tagIds IN [30]: doc 7 present (correct)`);

  // Query tagIds IN [10] — doc 7 should still appear (tag 10 kept)
  const r3 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 10 }]] }],
    SORT_DESC,
  );
  assert(r3.ids.includes(7), `Doc 7 should still appear in tagIds IN [10]`);
  log(`  [4] Query tagIds IN [10]: doc 7 still present (correct, ${r3.ids.length} results)`);

  // Restore doc 7
  await upsert([{ id: 7, nsfwLevel: 1, tagIds: [10, 20], status: 1, reactionCount: 700 }]);
  await sleep(200);
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
  ['A', 'Fresh insert appears in query', testA_InsertAppearsInQuery],
  ['B', 'Upsert updates filter values', testB_UpsertUpdatesFilterValues],
  ['C', 'Upsert updates sort values', testC_UpsertUpdatesSortValues],
  ['D', 'Delete removes from query', testD_DeleteRemovesFromQuery],
  ['E', 'Concurrent reads during writes', testE_ConcurrentReadsAndWrites],
  ['F', 'Multi-value field update', testF_MultiValueFieldUpdate],
];

async function main() {
  log('BitDex Write Handling E2E Tests');
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
    log(`  cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-write-data`);
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
      suite: 'write-handling',
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
    const resultsPath = resolve(RESULTS_DIR, 'write-handling.json');
    writeFileSync(resultsPath, JSON.stringify(resultsJson, null, 2));
    log(`JSON results written to: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
