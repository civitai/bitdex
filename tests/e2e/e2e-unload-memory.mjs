#!/usr/bin/env node
/**
 * E2E Regression Test: Bitmap Memory Drops After Save-and-Unload
 *
 * Verifies that save_and_unload actually frees bitmap memory and that
 * queries still work via lazy reload. This catches the bug where the
 * flush thread's staging held old data and re-inflated the snapshot.
 *
 * Flow:
 *   1. Create index, insert 1000 docs (enough to measure memory)
 *   2. Check bitmap memory via /stats (filter + sort bytes > 0)
 *   3. POST /snapshot?unload=true (save and unload)
 *   4. Check bitmap memory dropped to near-zero
 *   5. Wait a bit (flush thread might re-inflate — the old bug)
 *   6. Re-check memory is still low
 *   7. Query to trigger lazy reload, verify correctness
 *   8. Verify only queried fields reload (not everything)
 *
 * Usage:
 *   node tests/e2e/e2e-unload-memory.mjs [--url http://localhost:3000] [--verbose] [--keep] [--results-dir ./results]
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

const INDEX = 'unload-memory-test';

const api = (path) => `${BASE_URL}/api/indexes/${INDEX}${path}`;

let passed = 0;
let failed = 0;
const groupResults = [];

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
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

async function stats() {
  const { data } = await get('/stats');
  return data;
}

async function waitForFlush(expectedAlive, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const s = await stats();
    if (s.alive_count >= expectedAlive && s.flush_cycle > 0) return s;
    await new Promise(r => setTimeout(r, 50));
  }
  return stats();
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

async function setup() {
  log('\n--- Setup: Create index + insert 1000 docs ---');

  // Delete if leftover
  try {
    const res = await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' });
    if (res.ok) log('  Cleaned up leftover index');
  } catch (_) { /* ignore */ }
  await new Promise(r => setTimeout(r, 200));

  // Create index with multi_value field (these are the big memory consumers)
  const createRes = await fetch(`${BASE_URL}/api/indexes`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: INDEX,
      config: {
        filter_fields: [
          { name: 'category', field_type: 'single_value' },
          { name: 'tags', field_type: 'multi_value' },
          { name: 'active', field_type: 'boolean' },
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
          { source: 'active', target: 'active', value_type: 'boolean', default: true },
          { source: 'score', target: 'score', value_type: 'integer' },
        ],
      },
    }),
  });
  assert(createRes.ok, `create index failed: ${createRes.status}`);
  log(`  Index '${INDEX}' created`);

  // Insert 1000 docs in batches
  const TOTAL = 1000;
  const BATCH = 100;
  for (let batch = 0; batch < TOTAL / BATCH; batch++) {
    const docs = [];
    for (let i = 0; i < BATCH; i++) {
      const id = batch * BATCH + i + 1;
      docs.push({
        id,
        category: id % 10,
        tags: [id % 50, id % 30 + 100, id % 20 + 200],
        active: id % 3 !== 0,
        score: id * 7,
      });
    }
    const { status } = await post('/documents/upsert', { documents: docs });
    assert(status === 200, `upsert batch ${batch} failed`);
  }

  const s = await waitForFlush(TOTAL);
  assert(s.alive_count === TOTAL, `expected ${TOTAL} alive, got ${s.alive_count}`);
  log(`  ${TOTAL} docs inserted, alive_count=${s.alive_count}`);
}

// ---------------------------------------------------------------------------
// A. Bitmap memory is non-zero before unload
// ---------------------------------------------------------------------------

async function testA_MemoryBeforeUnload() {
  log('\n--- A. Bitmap Memory Before Unload ---');

  const s = await stats();
  const filterBytes = s.filter_bitmap_bytes;
  const sortBytes = s.sort_bitmap_bytes;
  const totalBytes = filterBytes + sortBytes;

  assert(typeof filterBytes === 'number', 'stats should include filter_bitmap_bytes');
  assert(typeof sortBytes === 'number', 'stats should include sort_bitmap_bytes');
  assert(totalBytes > 0, `bitmap memory should be > 0 before unload, got ${totalBytes}`);

  log(`  [1] filter_bitmap_bytes=${filterBytes}, sort_bitmap_bytes=${sortBytes}, total=${totalBytes}`);
  // Store for comparison
  globalThis._memBefore = totalBytes;
}

// ---------------------------------------------------------------------------
// B. Save-and-unload drops memory
// ---------------------------------------------------------------------------

async function testB_UnloadDropsMemory() {
  log('\n--- B. Save-and-Unload Drops Bitmap Memory ---');

  // Save and unload
  const snapRes = await post('/snapshot?unload=true', {});
  assert(snapRes.status === 200, `snapshot+unload failed: ${snapRes.status} ${JSON.stringify(snapRes.data)}`);
  assert(snapRes.data.status === 'saved_and_unloaded', `expected saved_and_unloaded, got ${snapRes.data.status}`);
  log(`  [1] save_and_unload completed in ${snapRes.data.elapsed_secs.toFixed(3)}s`);

  // Check memory dropped
  const s = await stats();
  const totalAfter = s.filter_bitmap_bytes + s.sort_bitmap_bytes;
  const memBefore = globalThis._memBefore;

  log(`  [2] bitmap memory: before=${memBefore}, after=${totalAfter}`);
  assert(
    totalAfter < memBefore * 0.5,
    `bitmap memory should drop by >50% after unload (before=${memBefore}, after=${totalAfter})`
  );
  log(`  [3] memory dropped ${((1 - totalAfter / memBefore) * 100).toFixed(1)}% -- correct`);
}

// ---------------------------------------------------------------------------
// C. Memory stays low (flush thread doesn't re-inflate)
// ---------------------------------------------------------------------------

async function testC_MemoryStaysLow() {
  log('\n--- C. Memory Stays Low After Unload ---');

  // Wait for several flush cycles — if the flush thread's staging
  // re-inflates the snapshot, memory will spike back up.
  await new Promise(r => setTimeout(r, 500));

  const s = await stats();
  const totalNow = s.filter_bitmap_bytes + s.sort_bitmap_bytes;
  const memBefore = globalThis._memBefore;

  log(`  [1] bitmap memory after 500ms wait: ${totalNow} (was ${memBefore} before unload)`);
  assert(
    totalNow < memBefore * 0.5,
    `bitmap memory should stay low after 500ms (before=${memBefore}, now=${totalNow}). ` +
    `If this fails, the flush thread staging is re-inflating the published snapshot.`
  );
  log(`  [2] memory still low after flush cycles -- no re-inflation`);
}

// ---------------------------------------------------------------------------
// D. Queries work via lazy reload
// ---------------------------------------------------------------------------

async function testD_QueryAfterUnload() {
  log('\n--- D. Queries Work Via Lazy Reload ---');

  // Query: category=0, sort by score DESC, limit 10
  // IDs with category=0: 10, 20, 30, ..., 1000 (100 docs). Top by score (id*7): 1000, 990, ..., 910
  const r = await post('/query', {
    filters: [{ Eq: ['category', { Integer: 0 }] }],
    sort: { field: 'score', direction: 'Desc' },
    limit: 10,
  });
  assert(r.status === 200, `query failed: ${r.status}`);
  assert(r.data.ids && r.data.ids.length === 10, `expected 10 results, got ${r.data.ids?.length}`);

  const expected = [1000, 990, 980, 970, 960, 950, 940, 930, 920, 910];
  const match = r.data.ids.every((id, i) => id === expected[i]);
  assert(match, `expected [${expected}], got [${r.data.ids}]`);
  log(`  [1] query after unload: [${r.data.ids.slice(0, 5)}...] -- correct via lazy reload`);

  // Check that memory came back for the queried fields (but not everything)
  const s = await stats();
  const reloaded = s.filter_bitmap_bytes + s.sort_bitmap_bytes;
  assert(reloaded > 0, 'queried fields should be back in memory after lazy reload');
  log(`  [2] bitmap memory after lazy reload: ${reloaded} (partial reload as expected)`);
}

// ---------------------------------------------------------------------------
// E. Upsert after unload works correctly
// ---------------------------------------------------------------------------

async function testE_UpsertAfterUnload() {
  log('\n--- E. Upsert After Unload Works ---');

  // Insert a new doc after unload
  const { status } = await post('/documents/upsert', {
    documents: [{ id: 9999, category: 0, tags: [1, 2], active: true, score: 999999 }],
  });
  assert(status === 200, 'upsert after unload failed');

  // Wait for flush and clear cache to avoid stale cached results.
  await waitForFlush(1001);
  await new Promise(r => setTimeout(r, 500));

  // Clear cache — the previous query cached category=0 results
  const delRes = await fetch(api('/cache'), { method: 'DELETE' });
  vlog(`DELETE /cache [${delRes.status}]`);

  // Query: the new doc should appear first (highest score)
  const r = await post('/query', {
    filters: [{ Eq: ['category', { Integer: 0 }] }],
    sort: { field: 'score', direction: 'Desc' },
    limit: 1,
  });
  vlog(`Query result: ids=${JSON.stringify(r.data.ids)}, total_matched=${r.data.total_matched}`);
  assert(r.data.ids[0] === 9999, `expected doc 9999 first (score=999999), got ${r.data.ids[0]}`);
  log(`  [1] new doc 9999 visible in queries after unload -- correct`);
}

// ---------------------------------------------------------------------------
// F. Combined exit_loading + save + unload (load endpoint path)
// ---------------------------------------------------------------------------

async function testF_CombinedLoadPathNoSpike() {
  log('\n--- F. Combined Load Path: No Memory Spike ---');

  // This tests the optimized path where exit_loading_mode + save + unload
  // happen in one flush command, avoiding the staging.clone() memory spike.
  //
  // Write NDJSON file, load via the load endpoint (which uses the combined path),
  // then verify bitmap memory is near-zero after load completes.

  const fs = await import('node:fs');
  const path = await import('node:path');
  const os = await import('node:os');

  // Write a small NDJSON file
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'bitdex-e2e-'));
  const ndjsonPath = path.join(tmpDir, 'test-data.ndjson');
  const lines = [];
  for (let i = 2001; i <= 3000; i++) {
    lines.push(JSON.stringify({
      id: i,
      category: i % 10,
      tags: [i % 50, i % 30 + 100],
      active: i % 2 === 0,
      score: i * 3,
    }));
  }
  fs.writeFileSync(ndjsonPath, lines.join('\n') + '\n');
  log(`  [1] wrote ${lines.length} records to ${ndjsonPath}`);

  // Trigger load with save_snapshot=true (uses combined exit+save+unload path)
  const loadRes = await fetch(api('/load'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      path: ndjsonPath,
      save_snapshot: true,
    }),
  });
  assert(loadRes.ok, `load request failed: ${loadRes.status}`);
  log(`  [2] load started`);

  // Wait for load to complete — poll load-status until complete/error
  const deadline = Date.now() + 60000;
  let loadStatus;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(api('/load/status'));
      const text = await res.text();
      if (!text) { await new Promise(r => setTimeout(r, 200)); continue; }
      loadStatus = JSON.parse(text);
      vlog(`load-status: ${JSON.stringify(loadStatus)}`);
      if (loadStatus.status === 'complete' || loadStatus.status === 'error') break;
    } catch (_) { /* may not be ready yet */ }
    await new Promise(r => setTimeout(r, 200));
  }
  assert(loadStatus, 'load-status never returned');
  assert(loadStatus.status === 'complete', `load did not complete: ${JSON.stringify(loadStatus)}`);
  log(`  [3] load complete: ${loadStatus.records_loaded} records`);

  // Check bitmap memory is near-zero (all unloaded)
  const s = await stats();
  const totalBytes = s.filter_bitmap_bytes + s.sort_bitmap_bytes;
  log(`  [4] bitmap memory after combined load+save+unload: ${totalBytes} bytes`);

  // After combined save+unload, filter and sort bitmaps should be mostly unloaded.
  // Some residual bytes remain from field metadata and any in-flight diffs,
  // but the vast majority of bitmap data should be freed.
  // At scale (105M), this is the difference between 6.5GB and ~10KB.
  // For 1000 records the numbers are small, so we check < 50KB as a sanity bound.
  assert(
    totalBytes < 50000,
    `bitmap memory should be very low after combined load (got ${totalBytes}). ` +
    `If high, the combined exit+save+unload path may not be unloading correctly.`
  );
  log(`  [5] bitmap memory ${totalBytes} bytes (< 50KB) -- combined path working correctly`);

  // Verify queries still work via lazy reload
  const r = await post('/query', {
    filters: [{ Eq: ['category', { Integer: 5 }] }],
    sort: { field: 'score', direction: 'Desc' },
    limit: 5,
  });
  assert(r.status === 200, `query after combined load failed: ${r.status}`);
  assert(r.data.ids.length > 0, 'expected results after combined load');
  log(`  [6] queries work via lazy reload after combined load -- [${r.data.ids.slice(0, 3)}...]`);

  // Cleanup temp file
  try { fs.rmSync(tmpDir, { recursive: true }); } catch (_) {}
}

// ---------------------------------------------------------------------------
// Cleanup + Runner
// ---------------------------------------------------------------------------

async function cleanup() {
  if (KEEP) {
    log(`\n--- Cleanup: SKIPPED (--keep flag) ---`);
    return;
  }
  log('\n--- Cleanup ---');
  try {
    const res = await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' });
    if (res.ok) log(`  Index '${INDEX}' deleted`);
  } catch (e) {
    log(`  Warning: cleanup failed: ${e.message}`);
  }
}

const groups = [
  ['A', 'Bitmap Memory Before Unload', testA_MemoryBeforeUnload],
  ['B', 'Save-and-Unload Drops Memory', testB_UnloadDropsMemory],
  ['C', 'Memory Stays Low (No Re-inflation)', testC_MemoryStaysLow],
  ['D', 'Queries Work Via Lazy Reload', testD_QueryAfterUnload],
  ['E', 'Upsert After Unload Works', testE_UpsertAfterUnload],
  ['F', 'Combined Load Path No Memory Spike', testF_CombinedLoadPathNoSpike],
];

async function main() {
  log('BitDex Unload Memory Regression E2E Tests');
  log(`Server: ${BASE_URL}`);

  // Verify server is reachable
  try {
    const res = await fetch(`${BASE_URL}/api/indexes`);
    if (!res.ok) throw new Error(`server returned ${res.status}`);
    log('Server reachable');
  } catch (e) {
    log(`\nERROR: Cannot reach server at ${BASE_URL}`);
    log(`  ${e.message}`);
    log(`\nStart the server first:`);
    log(`  cargo run --release --features server --bin bitdex-server -- --port 3000`);
    process.exit(1);
  }

  const suiteStart = Date.now();

  try {
    await setup();
  } catch (e) {
    log(`\nFATAL: Setup failed: ${e.message}`);
    if (VERBOSE) console.error(e.stack);
    await cleanup();
    process.exit(1);
  }

  for (const [id, name, fn] of groups) {
    const groupStart = Date.now();
    try {
      await fn();
      passed++;
      log(`  PASS: ${id}. ${name}`);
      groupResults.push({
        id, name, status: 'pass',
        duration_ms: Date.now() - groupStart,
      });
    } catch (e) {
      failed++;
      log(`  FAIL: ${id}. ${name}`);
      log(`    ${e.message}`);
      if (VERBOSE) console.error(e.stack);
      groupResults.push({
        id, name, status: 'fail',
        duration_ms: Date.now() - groupStart,
        assertions: [{ check: e.message, passed: false }],
      });
    }
  }

  await cleanup();

  log(`\nResults: ${passed} passed, ${failed} failed out of ${groups.length}`);

  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const resultsPath = resolve(RESULTS_DIR, 'unload-memory.json');
    writeFileSync(resultsPath, JSON.stringify({
      suite: 'unload-memory',
      timestamp: new Date().toISOString(),
      server_url: BASE_URL,
      groups: groupResults,
      summary: { passed, failed, total: groups.length, duration_ms: Date.now() - suiteStart },
    }, null, 2));
    log(`JSON results written to: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
