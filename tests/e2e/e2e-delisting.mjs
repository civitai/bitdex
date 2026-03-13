#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Document Delisting / Visibility
 *
 * Tests that document visibility (availability field) works correctly:
 * filtering by availability status, excluding private/blocked content,
 * upsert-based status transitions, and combined visibility + content filters.
 *
 * This mirrors how Civitai uses BitDex to control search visibility —
 * documents aren't deleted, they're filtered at query time via availability.
 *
 * Test groups:
 *   A. Visibility filtering: Public/Private/Unsearchable correctly partitioned
 *   B. Delist (upsert availability change): Public → Private removes from search
 *   C. Relist (upsert availability change): Private → Public restores to search
 *   D. blockedFor moderation: blocked docs excluded from queries
 *   E. Combined filters: visibility + content filter + sort
 *
 * Usage:
 *   node tests/e2e/e2e-delisting.mjs [--url http://localhost:3000] [--verbose] [--keep]
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

const INDEX = 'delist-test';

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
const query = (filters, sort, limit = 100, opts = {}) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, sort, limit, ...opts }).then(r => r.data);
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const deleteDocs = (ids) =>
  apiDelete(`/api/indexes/${INDEX}/documents`, { ids }).then(r => r.data);
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
  const s = await stats();
  vlog(`waitForFlush timeout: expected ${expectedAlive}, got ${s.alive_count}`);
  return s;
}

async function freshQuery(filters, sort, limit = 100, opts = {}) {
  await clearCache();
  return query(filters, sort, limit, opts);
}

const SORT_DESC = { field: 'score', direction: 'Desc' };

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create index with availability + blockedFor fields ---');

  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    log('  Deleting existing test index...');
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(500);
  }

  // Use mapped_string for availability (like the Civitai config) to test
  // the predefined string_map approach to delisting.
  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'availability', field_type: 'single_value' },
        { name: 'blockedFor', field_type: 'single_value' },
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
        {
          source: 'availability', target: 'availability', value_type: 'mapped_string',
          string_map: { 'Public': 1, 'Unsearchable': 2, 'Private': 3, 'EarlyAccess': 4 },
        },
        {
          source: 'blockedFor', target: 'blockedFor', value_type: 'mapped_string',
          string_map: { 'tos': 1, 'moderated': 2, 'CSAM': 3 },
        },
        { source: 'category', target: 'category', value_type: 'integer' },
        { source: 'score', target: 'score', value_type: 'integer' },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created`);
}

// ----- Test A: Visibility filtering -----

async function testA_VisibilityFiltering() {
  log('\n--- A. Visibility Filtering: Public/Private/Unsearchable partitioned ---');

  const docs = [
    { id: 1, availability: 'Public', category: 1, score: 500 },
    { id: 2, availability: 'Public', category: 1, score: 400 },
    { id: 3, availability: 'Public', category: 2, score: 300 },
    { id: 4, availability: 'Private', category: 1, score: 200 },
    { id: 5, availability: 'Private', category: 2, score: 100 },
    { id: 6, availability: 'Unsearchable', category: 1, score: 600 },
    { id: 7, availability: 'EarlyAccess', category: 2, score: 700 },
  ];

  await upsert(docs);
  log('  [1] Inserted 7 documents: 3 Public, 2 Private, 1 Unsearchable, 1 EarlyAccess');

  await waitForFlush(7);

  // Public only
  const r1 = await freshQuery(
    [{ Eq: ['availability', { String: 'Public' }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.length === 3, `Expected 3 Public, got ${r1.ids?.length}`);
  log(`  [2] Query availability="Public": ${r1.ids.length} results (correct)`);

  // Exclude Private (typical search query pattern: NOT Private)
  const r2 = await freshQuery(
    [{ Not: { Eq: ['availability', { String: 'Private' }] } }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 5, `Expected 5 non-Private, got ${r2.ids?.length}`);
  assert(!r2.ids.includes(4) && !r2.ids.includes(5), `Private docs should be excluded`);
  log(`  [3] Query NOT availability="Private": ${r2.ids.length} results (correct — excludes ids 4,5)`);

  // EarlyAccess only
  const r3 = await freshQuery(
    [{ Eq: ['availability', { String: 'EarlyAccess' }] }],
    SORT_DESC,
  );
  assert(r3.ids && r3.ids.length === 1, `Expected 1 EarlyAccess, got ${r3.ids?.length}`);
  assert(r3.ids[0] === 7, `Expected id 7, got ${r3.ids[0]}`);
  log(`  [4] Query availability="EarlyAccess": ${r3.ids.length} result (correct)`);
}

// ----- Test B: Delist — Public → Private hides from search -----

async function testB_DelistPublicToPrivate() {
  log('\n--- B. Delist: Upsert availability Public → Private ---');

  // Doc 1 was Public — change to Private
  await upsert([{ id: 1, availability: 'Private', category: 1, score: 500 }]);
  log('  [1] Upserted doc 1: Public → Private');

  await waitForFlush(7); // still 7 alive

  // Public should now be 2
  const r1 = await freshQuery(
    [{ Eq: ['availability', { String: 'Public' }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.length === 2, `Expected 2 Public after delist, got ${r1.ids?.length}`);
  assert(!r1.ids.includes(1), `Doc 1 should no longer be Public`);
  log(`  [2] Query Public: ${r1.ids.length} results (doc 1 removed — correct)`);

  // Private should now be 3
  const r2 = await freshQuery(
    [{ Eq: ['availability', { String: 'Private' }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 3, `Expected 3 Private after delist, got ${r2.ids?.length}`);
  assert(r2.ids.includes(1), `Doc 1 should now be Private`);
  log(`  [3] Query Private: ${r2.ids.length} results (doc 1 added — correct)`);
}

// ----- Test C: Relist — Private → Public restores visibility -----

async function testC_RelistPrivateToPublic() {
  log('\n--- C. Relist: Upsert availability Private → Public ---');

  // Doc 1 was Private (from test B) — change back to Public
  await upsert([{ id: 1, availability: 'Public', category: 1, score: 500 }]);
  log('  [1] Upserted doc 1: Private → Public');

  await waitForFlush(7);

  // Public should be back to 3
  const r1 = await freshQuery(
    [{ Eq: ['availability', { String: 'Public' }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.length === 3, `Expected 3 Public after relist, got ${r1.ids?.length}`);
  assert(r1.ids.includes(1), `Doc 1 should be Public again`);
  log(`  [2] Query Public: ${r1.ids.length} results (doc 1 restored — correct)`);

  // Private should be back to 2
  const r2 = await freshQuery(
    [{ Eq: ['availability', { String: 'Private' }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 2, `Expected 2 Private after relist, got ${r2.ids?.length}`);
  log(`  [3] Query Private: ${r2.ids.length} results (doc 1 removed — correct)`);
}

// ----- Test D: blockedFor moderation -----

async function testD_BlockedForModeration() {
  log('\n--- D. blockedFor Moderation: Blocked docs excluded from search ---');

  // Add a blocked doc and block an existing one
  await upsert([
    { id: 8, availability: 'Public', blockedFor: 'tos', category: 1, score: 800 },
    { id: 9, availability: 'Public', blockedFor: 'moderated', category: 2, score: 900 },
  ]);
  log('  [1] Inserted 2 blocked Public docs (id 8: tos, id 9: moderated)');

  await waitForFlush(9);

  // All Public docs (including blocked ones)
  const r1 = await freshQuery(
    [{ Eq: ['availability', { String: 'Public' }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.length === 5, `Expected 5 Public (including blocked), got ${r1.ids?.length}`);
  log(`  [2] Query Public (no blockedFor filter): ${r1.ids.length} results`);

  // Query for blocked=tos
  const r2 = await freshQuery(
    [{ Eq: ['blockedFor', { String: 'tos' }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 1, `Expected 1 blocked-for-tos, got ${r2.ids?.length}`);
  assert(r2.ids[0] === 8, `Expected id 8, got ${r2.ids[0]}`);
  log(`  [3] Query blockedFor="tos": ${r2.ids.length} result (correct)`);

  // Typical search pattern: Public AND NOT blocked (NotEq blockedFor)
  // This requires that non-blocked docs DON'T have a blockedFor value set.
  // Docs without blockedFor won't match any blockedFor bitmap, so
  // we use: Public AND NOT(blockedFor=tos) AND NOT(blockedFor=moderated)
  const r3 = await freshQuery(
    [
      { Eq: ['availability', { String: 'Public' }] },
      { Not: { Eq: ['blockedFor', { String: 'tos' }] } },
      { Not: { Eq: ['blockedFor', { String: 'moderated' }] } },
    ],
    SORT_DESC,
  );
  // Public=5, minus tos=1, minus moderated=1 = 3
  assert(r3.ids && r3.ids.length === 3, `Expected 3 unblocked Public, got ${r3.ids?.length}`);
  assert(!r3.ids.includes(8) && !r3.ids.includes(9), `Blocked docs should be excluded`);
  log(`  [4] Query Public AND NOT blocked: ${r3.ids.length} results (correct — blocked excluded)`);

  // Unblock doc 8 by clearing blockedFor (set to null/empty — will remove from blockedFor bitmap)
  await upsert([{ id: 8, availability: 'Public', category: 1, score: 800 }]);
  log('  [5] Upserted doc 8: cleared blockedFor');

  await waitForFlush(9);

  const r4 = await freshQuery(
    [
      { Eq: ['availability', { String: 'Public' }] },
      { Not: { Eq: ['blockedFor', { String: 'tos' }] } },
      { Not: { Eq: ['blockedFor', { String: 'moderated' }] } },
    ],
    SORT_DESC,
  );
  assert(r4.ids && r4.ids.length === 4, `Expected 4 unblocked Public after clearing, got ${r4.ids?.length}`);
  assert(r4.ids.includes(8), `Doc 8 should be unblocked now`);
  log(`  [6] Query Public AND NOT blocked (after unblock): ${r4.ids.length} results (correct)`);
}

// ----- Test E: Combined visibility + content filter + sort -----

async function testE_CombinedFilters() {
  log('\n--- E. Combined: Visibility + Content Filter + Sort ---');

  // Public category=1 docs sorted by score desc
  const r1 = await freshQuery(
    [
      { Eq: ['availability', { String: 'Public' }] },
      { Eq: ['category', { Integer: 1 }] },
    ],
    SORT_DESC,
  );
  // Public cat=1: ids 1(500), 2(400), 8(800) = 3 docs
  assert(r1.ids && r1.ids.length === 3, `Expected 3 Public+cat=1, got ${r1.ids?.length}`);
  // Sort: 8(800), 1(500), 2(400)
  assert(r1.ids[0] === 8, `Expected doc 8 first (score=800), got ${r1.ids[0]}`);
  log(`  [1] Public+cat=1 sorted: [${r1.ids.join(', ')}] (correct)`);

  // In filter: Public or EarlyAccess
  const r2 = await freshQuery(
    [{ In: ['availability', [{ String: 'Public' }, { String: 'EarlyAccess' }]] }],
    SORT_DESC,
  );
  // Public=5, EarlyAccess=1 = 6
  assert(r2.ids && r2.ids.length === 6, `Expected 6 Public+EarlyAccess, got ${r2.ids?.length}`);
  log(`  [2] Query In [Public, EarlyAccess]: ${r2.ids.length} results (correct)`);

  // Verify include_docs returns correct availability string
  const r3 = await freshQuery(
    [{ Eq: ['availability', { String: 'EarlyAccess' }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r3.documents && r3.documents.length === 1, `Expected 1 document`);
  const avail = r3.documents[0].availability;
  assert(typeof avail === 'string', `Expected availability as string, got ${typeof avail}`);
  assert(avail === 'EarlyAccess', `Expected "EarlyAccess", got "${avail}"`);
  log(`  [3] include_docs availability="${avail}" (correct string, not integer)`);
}

// ----- Main -----

async function runGroup(name, fn) {
  const start = Date.now();
  const results = { group: name, tests: [], pass: 0, fail: 0 };
  try {
    await fn();
    results.pass++;
    passed++;
    log(`  PASS: ${name}`);
  } catch (e) {
    results.fail++;
    failed++;
    results.tests.push({ name, error: e.message });
    log(`  FAIL: ${name}: ${e.message}`);
  }
  results.elapsed_ms = Date.now() - start;
  groupResults.push(results);
}

async function main() {
  log('=== BitDex E2E: Document Delisting / Visibility ===');
  log(`Server: ${BASE_URL}`);

  try {
    await setup();

    await runGroup('A. Visibility Filtering', testA_VisibilityFiltering);
    await runGroup('B. Delist (Public→Private)', testB_DelistPublicToPrivate);
    await runGroup('C. Relist (Private→Public)', testC_RelistPrivateToPublic);
    await runGroup('D. blockedFor Moderation', testD_BlockedForModeration);
    await runGroup('E. Combined Filters', testE_CombinedFilters);
  } catch (e) {
    log(`\nFATAL: ${e.message}`);
    failed++;
  }

  // Cleanup
  if (!KEEP) {
    log('\n--- Cleanup ---');
    await apiDelete(`/api/indexes/${INDEX}`);
    log('  Index deleted');
  }

  // JSON results
  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const report = {
      suite: 'delisting',
      timestamp: new Date().toISOString(),
      server: BASE_URL,
      passed,
      failed,
      groups: groupResults,
    };
    const path = resolve(RESULTS_DIR, 'e2e-delisting.json');
    writeFileSync(path, JSON.stringify(report, null, 2));
    log(`\nResults written to ${path}`);
  }

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main();
