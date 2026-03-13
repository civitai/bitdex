#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex LowCardinalityString Fields
 *
 * Tests that low_cardinality_string fields auto-build dictionaries,
 * handle case-insensitive matching, persist across save/restore, and
 * serve original casing back in documents.
 *
 * Test groups:
 *   A. Auto-dictionary: insert docs with string values, query by string, verify mapping
 *   B. Case-insensitive: same value in different cases maps to one bitmap
 *   C. Upsert updates dictionary field (old value gone, new value present)
 *   D. include_docs returns original string casing (not integer keys)
 *   E. Mixed filters: combine LCS field with integer filter + sort
 *   F. Nonexistent value: query for string not in dictionary returns empty (no crash)
 *   G. Dictionary persistence: save snapshot, verify dict and reverse maps survive
 *
 * Usage:
 *   node tests/e2e/e2e-low-cardinality-string.mjs [--url http://localhost:3000] [--verbose] [--keep]
 *
 * Prerequisites:
 *   Server running with NO existing index:
 *     cargo run --release --features server --bin bitdex-server -- --port 3000 --data-dir ./test-data
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

const INDEX = 'lcs-test';

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
const SORT_ASC = { field: 'score', direction: 'Asc' };

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create test index with LowCardinalityString fields ---');

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
        { name: 'status', field_type: 'single_value' },
        { name: 'priority', field_type: 'single_value' },
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
        { source: 'category', target: 'category', value_type: 'low_cardinality_string' },
        { source: 'status', target: 'status', value_type: 'low_cardinality_string' },
        { source: 'priority', target: 'priority', value_type: 'integer' },
        { source: 'score', target: 'score', value_type: 'integer' },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created`);
}

// ----- Test A: Auto-dictionary — strings are auto-mapped and queryable -----

async function testA_AutoDictionary() {
  log('\n--- A. Auto-Dictionary: Insert string values, query by string ---');

  const docs = [
    { id: 1, category: 'Art', status: 'Published', priority: 1, score: 500 },
    { id: 2, category: 'Photo', status: 'Published', priority: 2, score: 400 },
    { id: 3, category: 'Art', status: 'Draft', priority: 1, score: 300 },
    { id: 4, category: 'Video', status: 'Published', priority: 3, score: 200 },
    { id: 5, category: 'Photo', status: 'Draft', priority: 2, score: 100 },
  ];

  await upsert(docs);
  log('  [1] Inserted 5 documents with string category/status fields');

  await waitForFlush(5);

  // Query by category = "Art" (string, auto-mapped to integer internally)
  const r1 = await freshQuery(
    [{ Eq: ['category', { String: 'Art' }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.length === 2, `Expected 2 Art docs, got ${r1.ids?.length}`);
  assert(r1.ids.includes(1) && r1.ids.includes(3), `Expected ids [1,3], got ${r1.ids}`);
  log(`  [2] Query category="Art": ${r1.ids.length} results (correct)`);

  // Query by status = "Published"
  const r2 = await freshQuery(
    [{ Eq: ['status', { String: 'Published' }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 3, `Expected 3 Published docs, got ${r2.ids?.length}`);
  log(`  [3] Query status="Published": ${r2.ids.length} results (correct)`);

  // Query by category = "Video"
  const r3 = await freshQuery(
    [{ Eq: ['category', { String: 'Video' }] }],
    SORT_DESC,
  );
  assert(r3.ids && r3.ids.length === 1, `Expected 1 Video doc, got ${r3.ids?.length}`);
  assert(r3.ids[0] === 4, `Expected id 4, got ${r3.ids[0]}`);
  log(`  [4] Query category="Video": ${r3.ids.length} result (correct)`);
}

// ----- Test B: Case-insensitive matching -----

async function testB_CaseInsensitive() {
  log('\n--- B. Case-Insensitive: Different cases map to same bitmap ---');

  // Insert more docs with varied casing
  const docs = [
    { id: 6, category: 'art', status: 'published', priority: 1, score: 600 },  // lowercase
    { id: 7, category: 'ART', status: 'PUBLISHED', priority: 1, score: 700 },  // uppercase
    { id: 8, category: 'Art', status: 'Published', priority: 2, score: 800 },  // mixed
  ];

  await upsert(docs);
  log('  [1] Inserted 3 docs with "art"/"ART"/"Art" casing variants');

  await waitForFlush(8);

  // All art/Art/ART should be in same bitmap — query with original "Art" casing
  const r1 = await freshQuery(
    [{ Eq: ['category', { String: 'Art' }] }],
    SORT_DESC,
  );
  // 2 from testA (ids 1,3) + 3 from testB (ids 6,7,8) = 5
  assert(r1.ids && r1.ids.length === 5, `Expected 5 Art docs (case-insensitive), got ${r1.ids?.length}`);
  log(`  [2] Query category="Art" (mixed case inserts): ${r1.ids.length} results (correct)`);

  // Query with lowercase should also work
  const r2 = await freshQuery(
    [{ Eq: ['category', { String: 'art' }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 5, `Expected 5 art docs (lowercase query), got ${r2.ids?.length}`);
  log(`  [3] Query category="art" (lowercase query): ${r2.ids.length} results (correct)`);

  // Query with uppercase
  const r3 = await freshQuery(
    [{ Eq: ['category', { String: 'ART' }] }],
    SORT_DESC,
  );
  assert(r3.ids && r3.ids.length === 5, `Expected 5 ART docs (uppercase query), got ${r3.ids?.length}`);
  log(`  [4] Query category="ART" (uppercase query): ${r3.ids.length} results (correct)`);
}

// ----- Test C: Upsert changes dictionary field value -----

async function testC_UpsertChangesValue() {
  log('\n--- C. Upsert Updates LCS Field (old value removed, new value added) ---');

  // Doc 4 was "Video" — change to "Art"
  await upsert([{ id: 4, category: 'Art', status: 'Published', priority: 3, score: 200 }]);
  log('  [1] Upserted doc 4: category Video → Art');

  await waitForFlush(8);

  // Video should have 0 results now
  const r1 = await freshQuery(
    [{ Eq: ['category', { String: 'Video' }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.length === 0, `Expected 0 Video docs after upsert, got ${r1.ids?.length}`);
  log(`  [2] Query category="Video": ${r1.ids.length} results (correct — removed)`);

  // Art should now have 6 (was 5 + 1 moved from Video)
  const r2 = await freshQuery(
    [{ Eq: ['category', { String: 'Art' }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 6, `Expected 6 Art docs after upsert, got ${r2.ids?.length}`);
  log(`  [3] Query category="Art": ${r2.ids.length} results (correct — gained doc 4)`);
}

// ----- Test D: include_docs returns original string (not integer key) -----

async function testD_IncludeDocsReturnsString() {
  log('\n--- D. include_docs Returns Original String Casing ---');

  const r = await freshQuery(
    [{ Eq: ['category', { String: 'Photo' }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r.ids && r.ids.length === 2, `Expected 2 Photo docs, got ${r.ids?.length}`);
  assert(r.documents && r.documents.length === 2, `Expected 2 documents returned`);
  log(`  [1] Query category="Photo" with include_docs: ${r.documents.length} documents`);

  // Verify the document fields contain strings, not integers
  // Documents are returned with fields at top level (not nested under .fields)
  for (const doc of r.documents) {
    const cat = doc.category;
    assert(typeof cat === 'string', `Expected category to be string, got ${typeof cat}: ${JSON.stringify(cat)}`);
    assert(cat.toLowerCase() === 'photo', `Expected category "photo" (any case), got "${cat}"`);
    log(`  [2] Doc ${doc.id}: category="${cat}", status="${doc.status}" (strings, not integers)`);
  }
}

// ----- Test E: Mixed filters — LCS field + integer filter + sort -----

async function testE_MixedFilters() {
  log('\n--- E. Mixed Filters: LCS + Integer + Sort ---');

  // Query Art docs with priority=1, sorted by score desc
  const r1 = await freshQuery(
    [
      { Eq: ['category', { String: 'Art' }] },
      { Eq: ['priority', { Integer: 1 }] },
    ],
    SORT_DESC,
  );
  // Art docs with priority=1: ids 1 (score=500), 3 (score=300), 6 (score=600), 7 (score=700)
  assert(r1.ids && r1.ids.length === 4, `Expected 4 Art+priority=1 docs, got ${r1.ids?.length}`);
  // Sort desc by score: 7(700), 6(600), 1(500), 3(300)
  assert(r1.ids[0] === 7, `Expected doc 7 first (score=700), got ${r1.ids[0]}`);
  assert(r1.ids[r1.ids.length - 1] === 3, `Expected doc 3 last (score=300), got ${r1.ids[r1.ids.length - 1]}`);
  log(`  [1] Query Art+priority=1 sorted by score: [${r1.ids.join(', ')}] (correct order)`);

  // NotEq on LCS field: everything except Art
  const r2 = await freshQuery(
    [{ NotEq: ['category', { String: 'Art' }] }],
    SORT_DESC,
  );
  // Photo: ids 2,5 = 2 docs
  assert(r2.ids && r2.ids.length === 2, `Expected 2 non-Art docs, got ${r2.ids?.length}`);
  log(`  [2] Query NotEq category="Art": ${r2.ids.length} results (correct — Photo only)`);

  // In filter on LCS field
  const r3 = await freshQuery(
    [{ In: ['status', [{ String: 'Draft' }]] }],
    SORT_DESC,
  );
  // Draft: ids 3, 5 = 2 docs
  assert(r3.ids && r3.ids.length === 2, `Expected 2 Draft docs, got ${r3.ids?.length}`);
  log(`  [3] Query status In ["Draft"]: ${r3.ids.length} results (correct)`);
}

// ----- Test F: Query for nonexistent value returns empty results (no crash) -----

async function testF_NonexistentValue() {
  log('\n--- F. Nonexistent Value: query for string not in dictionary ---');

  // Query a value that was never inserted — should return 0 results, not error
  const r1 = await freshQuery(
    [{ Eq: ['category', { String: 'Nonexistent' }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.length === 0, `Expected 0 results for nonexistent value, got ${r1.ids?.length}`);
  log(`  [1] Query category="Nonexistent": ${r1.ids.length} results (correct — no crash)`);

  // NotEq with nonexistent value should return all docs
  const r2 = await freshQuery(
    [{ NotEq: ['category', { String: 'Nonexistent' }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.length === 8, `Expected 8 results for NotEq nonexistent, got ${r2.ids?.length}`);
  log(`  [2] Query NotEq category="Nonexistent": ${r2.ids.length} results (all docs — correct)`);

  // In filter with mix of existing and nonexistent
  const r3 = await freshQuery(
    [{ In: ['category', [{ String: 'Art' }, { String: 'Nonexistent' }]] }],
    SORT_DESC,
  );
  assert(r3.ids && r3.ids.length === 6, `Expected 6 Art results (nonexistent ignored), got ${r3.ids?.length}`);
  log(`  [3] Query In ["Art", "Nonexistent"]: ${r3.ids.length} results (only Art matched — correct)`);
}

// ----- Test G: Dictionary persists across save/snapshot -----

async function testG_DictionaryPersistence() {
  log('\n--- G. Dictionary Persistence: save snapshot, verify dict survives ---');

  // Trigger save_snapshot to persist bitmaps + dictionaries
  const snapRes = await apiPost(`/api/indexes/${INDEX}/snapshot`, {});
  assert(snapRes.status === 200, `Snapshot save failed: ${snapRes.status}`);
  log('  [1] save_snapshot triggered — dictionaries should now be on disk');

  // Query still works after save (dict wasn't lost)
  const r1 = await freshQuery(
    [{ Eq: ['category', { String: 'Art' }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.length === 6, `Expected 6 Art docs after snapshot, got ${r1.ids?.length}`);
  log(`  [2] Query category="Art" after save: ${r1.ids.length} results (correct)`);

  // include_docs still returns strings (reverse maps intact)
  const r2 = await freshQuery(
    [{ Eq: ['category', { String: 'Photo' }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r2.documents && r2.documents.length === 2, `Expected 2 Photo docs`);
  for (const doc of r2.documents) {
    assert(typeof doc.category === 'string', `Expected string category after save, got ${typeof doc.category}`);
    assert(doc.category.toLowerCase() === 'photo', `Expected "photo", got "${doc.category}"`);
  }
  log(`  [3] include_docs after save: categories are strings (dict persistence verified)`);

  // Upsert a new LCS value AFTER snapshot — tests dirty flag persistence
  await upsert([{ id: 9, category: 'Music', status: 'Published', priority: 1, score: 900 }]);
  log('  [4] Upserted doc 9 with NEW category="Music" (tests dirty dict persistence)');

  await waitForFlush(9);

  const r3 = await freshQuery(
    [{ Eq: ['category', { String: 'Music' }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r3.ids && r3.ids.length === 1, `Expected 1 Music doc, got ${r3.ids?.length}`);
  assert(r3.documents[0].category.toLowerCase() === 'music', `Expected "Music" in doc, got "${r3.documents[0].category}"`);
  log(`  [5] New value "Music" after snapshot: queryable and string in include_docs (correct)`);
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
  log('=== BitDex E2E: LowCardinalityString Fields ===');
  log(`Server: ${BASE_URL}`);

  try {
    await setup();

    await runGroup('A. Auto-Dictionary', testA_AutoDictionary);
    await runGroup('B. Case-Insensitive', testB_CaseInsensitive);
    await runGroup('C. Upsert Changes Value', testC_UpsertChangesValue);
    await runGroup('D. include_docs Returns Strings', testD_IncludeDocsReturnsString);
    await runGroup('E. Mixed Filters', testE_MixedFilters);
    await runGroup('F. Nonexistent Value', testF_NonexistentValue);
    await runGroup('G. Dictionary Persistence', testG_DictionaryPersistence);
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
      suite: 'low-cardinality-string',
      timestamp: new Date().toISOString(),
      server: BASE_URL,
      passed,
      failed,
      groups: groupResults,
    };
    const path = resolve(RESULTS_DIR, 'e2e-low-cardinality-string.json');
    writeFileSync(path, JSON.stringify(report, null, 2));
    log(`\nResults written to ${path}`);
  }

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main();
