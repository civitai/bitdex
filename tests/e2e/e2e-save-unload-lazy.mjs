#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Save-Snapshot + Lazy Reload Lifecycle
 *
 * Creates a self-contained test index, inserts 100 docs, saves a snapshot,
 * and verifies query correctness before and after snapshot. Also tests that
 * mutations after snapshot are reflected in queries, and that stats remain
 * consistent throughout.
 *
 * Usage:
 *   node tests/e2e/e2e-save-unload-lazy.mjs [--url http://localhost:3000] [--verbose] [--keep] [--results-dir ./results]
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

const INDEX = 'save-unload-test';

const api = (path) => `${BASE_URL}/api/indexes/${INDEX}${path}`;

let passed = 0;
let failed = 0;
const groupResults = [];

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

function arraysEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

async function post(path, body) {
  const res = await fetch(api(path), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  const data = JSON.parse(text);
  vlog(`POST ${path} [${res.status}]:`, JSON.stringify(data).slice(0, 300));
  return { status: res.status, data };
}

async function get(path) {
  const res = await fetch(api(path));
  const text = await res.text();
  const data = JSON.parse(text);
  vlog(`GET ${path} [${res.status}]:`, JSON.stringify(data).slice(0, 300));
  return { status: res.status, data };
}

async function del(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(api(path), opts);
  const text = await res.text();
  const data = JSON.parse(text);
  vlog(`DELETE ${path} [${res.status}]:`, JSON.stringify(data).slice(0, 300));
  return { status: res.status, data };
}

async function stats() {
  const { data } = await get('/stats');
  return data;
}

async function clearCache() {
  return del('/cache');
}

async function query(filters, sort, limit = 20) {
  const body = { filters, limit };
  if (sort) body.sort = sort;
  const { data } = await post('/query', body);
  return data;
}

/**
 * Poll stats until alive_count reaches expectedAlive (or timeout).
 */
async function waitForFlush(expectedAlive, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const s = await stats();
    if (s.alive_count >= expectedAlive && s.flush_cycle > 0) return s;
    await new Promise(r => setTimeout(r, 50));
  }
  // Return last stats even if not reached
  return stats();
}

// ---------------------------------------------------------------------------
// Setup: Create index + insert 100 docs
// ---------------------------------------------------------------------------

async function setup() {
  log('\n--- Setup: Create index + insert 100 docs ---');

  // Delete if leftover from previous run
  try {
    const res = await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' });
    if (res.ok) log('  Cleaned up leftover index');
  } catch (_) { /* ignore */ }

  // Wait a moment for cleanup
  await new Promise(r => setTimeout(r, 200));

  // Create index
  const createRes = await fetch(`${BASE_URL}/api/indexes`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: INDEX,
      config: {
        filter_fields: [
          { name: 'category', field_type: 'single_value' },
          { name: 'tags', field_type: 'multi_value' },
        ],
        sort_fields: [
          { name: 'score', bits: 32 },
        ],
        flush_interval_us: 50,
      },
      data_schema: {
        id_field: 'id',
        fields: [
          { source: 'category', target: 'category', value_type: 'integer' },
          { source: 'tags', target: 'tags', value_type: 'integer_array' },
          { source: 'score', target: 'score', value_type: 'integer' },
        ],
      },
    }),
  });
  const createText = await createRes.text();
  const createData = JSON.parse(createText);
  assert(createRes.status === 200 || createRes.status === 201, `create index failed: ${createRes.status} ${JSON.stringify(createData)}`);
  log(`  Index '${INDEX}' created`);

  // Insert 100 docs in batches of 25
  const docs = [];
  for (let i = 1; i <= 100; i++) {
    docs.push({
      id: i,
      category: i <= 50 ? 1 : 2,
      tags: [i % 10, i % 5],
      score: i * 10,
    });
  }

  for (let batch = 0; batch < 4; batch++) {
    const slice = docs.slice(batch * 25, (batch + 1) * 25);
    const { status, data } = await post('/documents/upsert', { documents: slice });
    assert(status === 200, `upsert batch ${batch} failed: ${status} ${JSON.stringify(data)}`);
    vlog(`  Batch ${batch}: upserted ${data.upserted}`);
  }

  // Wait for all docs to be flushed
  const s = await waitForFlush(100);
  assert(s.alive_count === 100, `expected 100 alive, got ${s.alive_count}`);
  log(`  100 docs inserted, alive_count=${s.alive_count}, flush_cycle=${s.flush_cycle}`);
}

// ---------------------------------------------------------------------------
// A. Snapshot Save + Stats Verification
// ---------------------------------------------------------------------------

async function testA_SnapshotSave() {
  log('\n--- A. Snapshot Save + Stats Verification ---');

  // Warm up bitmaps with a query
  const warmup = await query(
    [{ Eq: ['category', { Integer: 1 }] }],
    { field: 'score', direction: 'Desc' },
    10,
  );
  assert(warmup.ids && warmup.ids.length > 0, 'warmup query should return results');
  log(`  [1] Warmup query returned ${warmup.ids.length} results`);

  // Record stats before snapshot
  const sBefore = await stats();
  log(`  [2] Pre-snapshot: alive=${sBefore.alive_count}, flush_cycle=${sBefore.flush_cycle}`);

  // Save snapshot
  const { status, data } = await post('/snapshot', {});
  assert(status === 200, `snapshot failed: ${status} ${JSON.stringify(data)}`);
  assert(data.status === 'saved', `expected status=saved, got ${data.status}`);
  assert(typeof data.elapsed_secs === 'number', 'response should have elapsed_secs');
  log(`  [3] Snapshot saved in ${data.elapsed_secs.toFixed(3)}s`);

  // Verify stats unchanged after snapshot
  const sAfter = await stats();
  assert(sAfter.alive_count === 100, `alive_count changed after snapshot: ${sAfter.alive_count}`);
  log(`  [4] Post-snapshot: alive=${sAfter.alive_count}, flush_cycle=${sAfter.flush_cycle}`);
}

// ---------------------------------------------------------------------------
// B. Query Correctness Before and After Snapshot
// ---------------------------------------------------------------------------

async function testB_QueryCorrectness() {
  log('\n--- B. Query Correctness Before and After Snapshot ---');

  // Clear cache so we get fresh bitmap traversals
  await clearCache();

  // Query 1: category=1, sort score DESC, limit 10
  // IDs 1-50 have category=1, scores 10-500. Top 10 by score = IDs 50,49,...,41
  const r1 = await query(
    [{ Eq: ['category', { Integer: 1 }] }],
    { field: 'score', direction: 'Desc' },
    10,
  );
  assert(r1.ids && r1.ids.length === 10, `expected 10 results, got ${r1.ids?.length}`);
  const expected1 = [50, 49, 48, 47, 46, 45, 44, 43, 42, 41];
  assert(arraysEqual(r1.ids, expected1), `category=1 top 10: expected [50..41], got [${r1.ids}]`);
  log(`  [1] category=1, score DESC, limit 10: [${r1.ids.slice(0, 5)}...] -- correct`);

  // Query 2: tags IN [0], sort score DESC, limit 5
  // Tags [id%10, id%5]: tag 0 appears for all multiples of 5 (id%5==0 OR id%10==0).
  // That's IDs 5,10,15,20,...,100 = 20 docs. Top 5 by score: 100,95,90,85,80
  const r2 = await query(
    [{ In: ['tags', [{ Integer: 0 }]] }],
    { field: 'score', direction: 'Desc' },
    5,
  );
  assert(r2.ids && r2.ids.length === 5, `expected 5 results, got ${r2.ids?.length}`);
  const expected2 = [100, 95, 90, 85, 80];
  assert(arraysEqual(r2.ids, expected2), `tags IN [0] top 5: expected [100,95,90,85,80], got [${r2.ids}]`);
  log(`  [2] tags IN [0], score DESC, limit 5: [${r2.ids}] -- correct`);

  // Save snapshot
  const { status } = await post('/snapshot', {});
  assert(status === 200, 'snapshot should succeed');
  log(`  [3] Snapshot saved`);

  // Clear cache again for post-snapshot queries
  await clearCache();

  // Re-run same queries after snapshot
  const r1b = await query(
    [{ Eq: ['category', { Integer: 1 }] }],
    { field: 'score', direction: 'Desc' },
    10,
  );
  assert(arraysEqual(r1b.ids, expected1), `post-snapshot category=1: expected [50..41], got [${r1b.ids}]`);
  log(`  [4] Post-snapshot category=1 query: identical results -- correct`);

  const r2b = await query(
    [{ In: ['tags', [{ Integer: 0 }]] }],
    { field: 'score', direction: 'Desc' },
    5,
  );
  assert(arraysEqual(r2b.ids, expected2), `post-snapshot tags IN [0]: expected [100,95,90,85,80], got [${r2b.ids}]`);
  log(`  [5] Post-snapshot tags IN [0] query: identical results -- correct`);
}

// ---------------------------------------------------------------------------
// C. Upsert After Snapshot (Mutation Survival)
// ---------------------------------------------------------------------------

async function testC_MutationAfterSnapshot() {
  log('\n--- C. Upsert After Snapshot (Mutation Survival) ---');

  // Upsert doc 50: change score from 500 to 99999
  const { status: uStatus, data: uData } = await post('/documents/upsert', {
    documents: [{ id: 50, category: 1, tags: [0, 0], score: 99999 }],
  });
  assert(uStatus === 200, `upsert failed: ${uStatus}`);
  log(`  [1] Upserted doc 50 with score=99999`);

  // Wait for flush
  await waitForFlush(100);
  // Extra wait for mutation to propagate
  await new Promise(r => setTimeout(r, 200));

  // Clear cache to ensure fresh traversal
  await clearCache();

  // Query category=1, sort score DESC, limit 1 -- doc 50 should be first (score 99999)
  const r = await query(
    [{ Eq: ['category', { Integer: 1 }] }],
    { field: 'score', direction: 'Desc' },
    1,
  );
  assert(r.ids && r.ids.length === 1, `expected 1 result, got ${r.ids?.length}`);
  assert(r.ids[0] === 50, `expected doc 50 first (score 99999), got doc ${r.ids[0]}`);
  log(`  [2] After upsert: top result is doc ${r.ids[0]} -- correct`);

  // Restore doc 50 to original score
  const { status: rStatus } = await post('/documents/upsert', {
    documents: [{ id: 50, category: 1, tags: [0, 0], score: 500 }],
  });
  assert(rStatus === 200, 'restore upsert failed');
  await waitForFlush(100);
  await new Promise(r => setTimeout(r, 200));
  log(`  [3] Restored doc 50 to score=500`);

  // Verify restoration
  await clearCache();
  const r2 = await query(
    [{ Eq: ['category', { Integer: 1 }] }],
    { field: 'score', direction: 'Desc' },
    1,
  );
  assert(r2.ids[0] === 50, `expected doc 50 still first at score=500, got ${r2.ids[0]}`);
  log(`  [4] Verified doc 50 back at top with score=500`);
}

// ---------------------------------------------------------------------------
// D. Stats Integrity
// ---------------------------------------------------------------------------

async function testD_StatsIntegrity() {
  log('\n--- D. Stats Integrity ---');

  const s = await stats();

  // alive_count should be 100
  assert(s.alive_count === 100, `expected alive_count=100, got ${s.alive_count}`);
  log(`  [1] alive_count=${s.alive_count} -- correct`);

  // flush_cycle should be > 0 (we've been running mutations)
  assert(s.flush_cycle > 0, `expected flush_cycle > 0, got ${s.flush_cycle}`);
  log(`  [2] flush_cycle=${s.flush_cycle} -- correct`);

  // slot_count should be >= 100 (may be higher due to slot allocation)
  assert(s.slot_count >= 100, `expected slot_count >= 100, got ${s.slot_count}`);
  log(`  [3] slot_count=${s.slot_count}`);

  // Cache stats should be numeric
  assert(typeof s.unified_cache_entries === 'number', 'unified_cache_entries should be numeric');
  assert(typeof s.unified_cache_bytes === 'number', 'unified_cache_bytes should be numeric');
  log(`  [4] cache: entries=${s.unified_cache_entries}, bytes=${s.unified_cache_bytes}`);
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

async function cleanup() {
  if (KEEP) {
    log(`\n--- Cleanup: SKIPPED (--keep flag) ---`);
    return;
  }
  log('\n--- Cleanup: Delete test index ---');
  try {
    const res = await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' });
    const text = await res.text();
    const data = JSON.parse(text);
    if (res.ok) {
      log(`  Index '${INDEX}' deleted`);
    } else {
      log(`  Warning: delete returned ${res.status}: ${JSON.stringify(data)}`);
    }
  } catch (e) {
    log(`  Warning: cleanup failed: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

const groups = [
  ['A', 'Snapshot Save + Stats Verification', testA_SnapshotSave],
  ['B', 'Query Correctness Before and After Snapshot', testB_QueryCorrectness],
  ['C', 'Upsert After Snapshot (Mutation Survival)', testC_MutationAfterSnapshot],
  ['D', 'Stats Integrity', testD_StatsIntegrity],
];

async function main() {
  log('BitDex Save-Unload-Lazy E2E Tests');
  log(`Server: ${BASE_URL}`);
  log(`Index: ${INDEX}`);

  // Verify server is reachable
  try {
    const res = await fetch(`${BASE_URL}/api/indexes`);
    if (!res.ok) throw new Error(`server returned ${res.status}`);
    log('Server reachable');
  } catch (e) {
    log(`\nERROR: Cannot reach server at ${BASE_URL}`);
    log(`  ${e.message}`);
    log(`\nStart the server first:`);
    log(`  cargo run --release --bin server -- --port 3000`);
    process.exit(1);
  }

  const suiteStart = Date.now();

  // Setup
  try {
    await setup();
  } catch (e) {
    log(`\nFATAL: Setup failed: ${e.message}`);
    if (VERBOSE) console.error(e.stack);
    await cleanup();
    process.exit(1);
  }

  // Run test groups
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

  // Cleanup
  await cleanup();

  log(`\nResults: ${passed} passed, ${failed} failed out of ${groups.length}`);

  // Write JSON results if --results-dir provided
  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const resultsJson = {
      suite: 'save-unload-lazy',
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
    const resultsPath = resolve(RESULTS_DIR, 'save-unload-lazy.json');
    writeFileSync(resultsPath, JSON.stringify(resultsJson, null, 2));
    log(`JSON results written to: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
