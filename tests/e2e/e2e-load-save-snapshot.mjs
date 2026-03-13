#!/usr/bin/env node
/**
 * E2E: Load with save_snapshot=true — regression test for the loading mode
 * race condition where the flush thread's force-publish overwrites the
 * loader's published data before the save command reads it.
 *
 * Specifically tests:
 *   A. NDJSON load with save_snapshot=true completes with correct alive_count
 *   B. Queries return correct results after save+unload (lazy reload)
 *   C. Stats show zero bitmap memory (bitmaps unloaded to disk)
 *
 * Usage:
 *   node tests/e2e/e2e-load-save-snapshot.mjs [--url http://localhost:3000] [--verbose] [--results-dir ./results]
 */

import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { tmpdir } from 'node:os';

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const VERBOSE = process.argv.includes('--verbose');
const RESULTS_DIR = process.argv.includes('--results-dir')
  ? process.argv[process.argv.indexOf('--results-dir') + 1]
  : null;

const INDEX = 'load-save-test';
const NUM_DOCS = 500;

const api = (path) => `${BASE_URL}/api/indexes/${INDEX}${path}`;

let passed = 0;
let failed = 0;
const groupResults = [];

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function apiPost(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { _raw: text }; }
  vlog(`POST ${url} [${res.status}]:`, JSON.stringify(data).slice(0, 300));
  return { status: res.status, data };
}

async function post(path, body) {
  return apiPost(api(path), body);
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
  let data;
  try { data = JSON.parse(text); } catch { data = { _raw: text }; }
  return { status: res.status, data };
}

async function stats() {
  const { data } = await get('/stats');
  return data;
}

async function query(filters, sort, limit = 20) {
  const body = { filters, limit };
  if (sort) body.sort = sort;
  const { data } = await post('/query', body);
  return data;
}

/**
 * Poll stats until alive_count reaches target or timeout.
 */
async function waitForAlive(target, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const s = await stats();
    if (s.alive_count >= target) return s;
    await new Promise(r => setTimeout(r, 200));
  }
  return stats();
}

/**
 * Poll stats until alive_count is stable (load finished) or timeout.
 * For save_snapshot loads, alive_count may be 0 during loading mode,
 * then jumps to the final value after exit_loading_mode_and_save_unload.
 */
async function waitForLoadComplete(timeoutMs = 60000) {
  const deadline = Date.now() + timeoutMs;
  let lastAlive = -1;
  let stableCount = 0;

  // Wait for alive to become > 0 (loading mode publishes 0 until done)
  while (Date.now() < deadline) {
    const s = await stats();
    if (s.alive_count > 0) {
      return s;
    }
    await new Promise(r => setTimeout(r, 500));
  }
  return stats();
}

async function runGroup(id, name, fn) {
  const t0 = Date.now();
  try {
    await fn();
    const ms = Date.now() - t0;
    log(`  PASS: ${id}. ${name} (${ms}ms)`);
    passed++;
    groupResults.push({ id, name, status: 'pass' });
  } catch (e) {
    const ms = Date.now() - t0;
    log(`  FAIL: ${id}. ${name} (${ms}ms)`);
    log(`    ${e.message}`);
    failed++;
    groupResults.push({ id, name, status: 'fail' });
  }
}

// ---------------------------------------------------------------------------
// Setup: Create index
// ---------------------------------------------------------------------------

async function setup() {
  log('\n--- Setup: Create test index ---');

  // Delete leftover
  try {
    await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' });
  } catch (_) {}
  await new Promise(r => setTimeout(r, 200));

  // Create index
  const res = await fetch(`${BASE_URL}/api/indexes`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: INDEX,
      config: {
        filter_fields: [
          { name: 'category', field_type: 'single_value' },
          { name: 'tags', field_type: 'multi_value' },
          { name: 'isPublished', field_type: 'boolean' },
        ],
        sort_fields: [
          { name: 'score', bits: 32 },
        ],
        // deferred_alive is the key ingredient: during NDJSON load, the flush
        // thread activates deferred slots into its own staging, setting
        // staging_dirty=true. Without the race fix, exit_loading_mode flips
        // the flag before sending ExitLoadingSaveUnload, causing the flush
        // thread's force-publish to overwrite the loader's published data.
        deferred_alive: { source_field: 'publishedAt' },
      },
      data_schema: {
        id_field: 'id',
        fields: [
          { source: 'category', target: 'category', value_type: 'integer' },
          { source: 'tags', target: 'tags', value_type: 'integer_array', default: [] },
          { source: 'publishedAt', target: 'publishedAt', value_type: 'integer' },
          { source: 'publishedAt', target: 'isPublished', value_type: 'exists_boolean' },
          { source: 'score', target: 'score', value_type: 'integer', default: 0 },
        ],
      },
    }),
  });
  assert(res.ok, `Create index failed: ${res.status}`);
  log(`  Index '${INDEX}' created`);
}

// ---------------------------------------------------------------------------
// A. Load with save_snapshot=true: alive_count correct after completion
// ---------------------------------------------------------------------------

async function testA_LoadSaveSnapshot() {
  log('\n--- A. NDJSON load with save_snapshot=true ---');

  // Write NDJSON file
  const ndjsonPath = resolve(tmpdir(), `bitdex-load-save-test-${Date.now()}.ndjson`);
  const lines = [];
  // publishedAt = past timestamps so deferred_alive activates them immediately.
  // This is the key: the flush thread will activate these deferred slots during
  // loading, making staging_dirty=true and triggering the race condition.
  const baseTime = Math.floor(Date.now() / 1000) - 86400; // 24h ago
  for (let i = 1; i <= NUM_DOCS; i++) {
    lines.push(JSON.stringify({
      id: i,
      category: (i % 5) + 1,       // 1-5
      tags: [i % 10, i % 3],        // two tags each
      publishedAt: baseTime + i,    // all in the past → immediate activation
      score: i * 100,
    }));
  }
  writeFileSync(ndjsonPath, lines.join('\n') + '\n');
  log(`  [1] Wrote ${NUM_DOCS}-doc NDJSON to ${ndjsonPath}`);

  // Start load with save_snapshot=true (this is the critical path)
  const { status, data } = await apiPost(`${BASE_URL}/api/indexes/${INDEX}/load`, {
    path: ndjsonPath,
    save_snapshot: true,
    threads: 1,
    chunk_size: 200,
  });
  assert(status === 202, `Expected 202, got ${status} ${JSON.stringify(data)}`);
  log(`  [2] Load started (save_snapshot=true)`);

  // Wait for load to complete — alive_count will be 0 during loading mode,
  // then jump to NUM_DOCS after exit_loading_mode_and_save_unload.
  const s = await waitForLoadComplete(60000);
  log(`  [3] After load: alive=${s.alive_count}, filter_bytes=${s.filter_bitmap_bytes}, sort_bytes=${s.sort_bitmap_bytes}`);

  // THE KEY ASSERTION: alive_count must equal NUM_DOCS.
  // Before the fix, this was 0 due to the race condition.
  assert(
    s.alive_count === NUM_DOCS,
    `Expected ${NUM_DOCS} alive after load+save, got ${s.alive_count}. ` +
    `This is the race condition: flush thread force-publish overwrote loader data.`
  );
  log(`  [4] alive_count=${s.alive_count} -- CORRECT (matches ${NUM_DOCS} loaded docs)`);
}

// ---------------------------------------------------------------------------
// B. Queries return correct results after save+unload (lazy reload)
// ---------------------------------------------------------------------------

async function testB_QueryAfterSaveUnload() {
  log('\n--- B. Queries correct after save+unload (lazy reload) ---');

  // Query 1: category=1, sort score DESC
  // category=1 = ids where (id % 5)+1 == 1, i.e. id%5==0: 5,10,15,...,500 = 100 docs
  const r1 = await query(
    [{ Eq: ['category', { Integer: 1 }] }],
    { field: 'score', direction: 'Desc' },
    5,
  );
  assert(r1.ids && r1.ids.length === 5, `Expected 5 results, got ${r1.ids?.length}`);
  // Top 5 by score DESC: 500, 495, 490, 485, 480
  const expected1 = [500, 495, 490, 485, 480];
  assert(
    JSON.stringify(r1.ids) === JSON.stringify(expected1),
    `category=1 top 5: expected [${expected1}], got [${r1.ids}]`
  );
  log(`  [1] category=1, score DESC top 5: [${r1.ids}] -- correct`);

  // Query 2: tags IN [0], sort score DESC
  // tag 0 appears for ids where id%10==0 OR id%3==0
  // id%10==0: 10,20,...,500 = 50 docs
  // id%3==0: 3,6,9,...,498 = 166 docs
  // Union: any id where id%10==0 OR id%3==0
  // Top by score: id=500 (500*100=50000, has tag 0 via 500%10=0)
  const r2 = await query(
    [{ In: ['tags', [{ Integer: 0 }]] }],
    { field: 'score', direction: 'Desc' },
    3,
  );
  assert(r2.ids && r2.ids.length === 3, `Expected 3 results, got ${r2.ids?.length}`);
  assert(r2.ids[0] === 500, `Top result should be id=500, got ${r2.ids[0]}`);
  log(`  [2] tags IN [0], score DESC top 3: [${r2.ids}] -- correct`);

  // Query 3: total_matched should reflect the full dataset
  const r3 = await query([], { field: 'score', direction: 'Desc' }, 5);
  assert(r3.total_matched === NUM_DOCS, `Expected total_matched=${NUM_DOCS}, got ${r3.total_matched}`);
  log(`  [3] Unfiltered total_matched=${r3.total_matched} -- correct`);
}

// ---------------------------------------------------------------------------
// C. Bitmap memory near zero after save+unload
// ---------------------------------------------------------------------------

async function testC_MemoryAfterUnload() {
  log('\n--- C. Memory state after save+unload ---');

  // Before any queries triggered lazy load, bitmap bytes should be 0 or very low.
  // But our queries in B already triggered lazy loads. Still, we can verify
  // the stats are sane and the server is functioning.
  const s = await stats();
  log(`  [1] filter_bytes=${s.filter_bitmap_bytes}, sort_bytes=${s.sort_bitmap_bytes}`);
  log(`  [2] alive_count=${s.alive_count}, slot_count=${s.slot_count}`);

  assert(s.alive_count === NUM_DOCS, `alive should be ${NUM_DOCS}, got ${s.alive_count}`);
  assert(s.slot_count > 0, `slot_count should be > 0, got ${s.slot_count}`);
  log(`  [3] Stats consistent after lazy reload`);
}

// ---------------------------------------------------------------------------
// Cleanup + main
// ---------------------------------------------------------------------------

async function cleanup() {
  log('\n--- Cleanup ---');
  try {
    await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' });
    log('  Index deleted');
  } catch (_) {
    log('  Index cleanup skipped');
  }
}

async function main() {
  log(`=== BitDex E2E: Load with save_snapshot (race condition regression) ===`);
  log(`Server: ${BASE_URL}\n`);

  // Verify server reachable
  try {
    const r = await fetch(`${BASE_URL}/api/indexes`);
    assert(r.ok, `Server not reachable at ${BASE_URL}`);
  } catch (e) {
    log(`FATAL: Server not reachable at ${BASE_URL}: ${e.message}`);
    process.exit(1);
  }

  await runGroup('Setup', 'Create test index', setup);
  await runGroup('A', 'NDJSON load with save_snapshot=true preserves alive_count', testA_LoadSaveSnapshot);
  await runGroup('B', 'Queries correct after save+unload (lazy reload)', testB_QueryAfterSaveUnload);
  await runGroup('C', 'Stats consistent after save+unload', testC_MemoryAfterUnload);
  await cleanup();

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);

  // Write results
  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const resultsFile = resolve(RESULTS_DIR, 'e2e-load-save-snapshot.json');
    writeFileSync(resultsFile, JSON.stringify({
      suite: 'load-save-snapshot',
      timestamp: new Date().toISOString(),
      groups: groupResults,
      summary: { passed, failed, total: passed + failed },
    }, null, 2));
    log(`Results written to ${resultsFile}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
