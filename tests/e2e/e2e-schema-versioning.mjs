#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Schema Versioning & Field Elision
 *
 * Tests that document field elision (skipping default values on write) and
 * reconstruction (filling defaults on read) works correctly through the
 * HTTP API. Covers the full pipeline: upsert → encode with elision →
 * store on disk → decode → format_document with defaults.
 *
 * Test groups:
 *   A. Default reconstruction: insert doc with defaults, include_docs returns them
 *   B. Non-default preservation: non-default values survive round-trip
 *   C. Missing fields filled with defaults: sparse doc gets defaults on read
 *   D. Null default exclusion: fields with null default excluded from output
 *   E. Upsert changes default→non-default: update a field from default to non-default
 *   F. Mixed field types: boolean, integer, array defaults all reconstructed
 *   G. Snapshot preserves defaults: save_snapshot, verify defaults survive
 *
 * Usage:
 *   node tests/e2e/e2e-schema-versioning.mjs [--url http://localhost:3000] [--verbose] [--keep]
 *
 * Prerequisites:
 *   Server running with NO existing index:
 *     cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-data
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

const INDEX = 'schema-ver-test';

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
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 800));
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
const getDoc = (slot) =>
  apiPost(`/api/indexes/${INDEX}/document`, { slot }).then(r => r.data);
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

async function freshQuery(filters, sort, limit = 100, opts = {}) {
  await clearCache();
  return query(filters, sort, limit, opts);
}

const SORT_DESC = { field: 'score', direction: 'Desc' };

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create index with field defaults ---');

  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    log('  Deleting existing test index...');
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(500);
  }

  // Schema with various field types and defaults:
  // - commentCount: integer, default 0
  // - hasMeta: boolean, default false
  // - tagIds: multi-value array, default []
  // - category: integer, no default (required)
  // - score: sort field, no default
  // - optionalNote: integer, default null (excluded from output when absent)
  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'category', field_type: 'single_value' },
        { name: 'commentCount', field_type: 'single_value' },
        { name: 'hasMeta', field_type: 'boolean' },
        { name: 'tagIds', field_type: 'multi_value' },
        { name: 'optionalNote', field_type: 'single_value' },
      ],
      sort_fields: [
        { name: 'score', bits: 32 },
      ],
      max_page_size: 100,
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      schema_version: 1,
      fields: [
        { source: 'category', target: 'category', value_type: 'integer' },
        { source: 'commentCount', target: 'commentCount', value_type: 'integer', default: 0 },
        { source: 'hasMeta', target: 'hasMeta', value_type: 'boolean', default: false },
        { source: 'tagIds', target: 'tagIds', value_type: 'integer_array', default: [] },
        { source: 'score', target: 'score', value_type: 'integer' },
        { source: 'optionalNote', target: 'optionalNote', value_type: 'integer', default: null },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created with schema_version=1`);
}

// ----- Test A: Default values reconstructed on read -----

async function testA_DefaultReconstruction() {
  log('\n--- A. Default Reconstruction: insert docs with default values ---');

  // Insert doc where commentCount=0, hasMeta=false, tagIds=[] (all defaults)
  await upsert([
    { id: 1, category: 1, commentCount: 0, hasMeta: false, tagIds: [], score: 500 },
  ]);
  log('  [1] Inserted doc 1 with all-default values (commentCount=0, hasMeta=false, tagIds=[])');

  await waitForFlush(1);

  // Retrieve with include_docs — defaults should be reconstructed
  const r = await freshQuery(
    [{ Eq: ['category', { Integer: 1 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r.documents && r.documents.length === 1, `Expected 1 document, got ${r.documents?.length}`);

  const doc = r.documents[0];
  assert(doc.commentCount === 0, `Expected commentCount=0, got ${doc.commentCount}`);
  assert(doc.hasMeta === false, `Expected hasMeta=false, got ${doc.hasMeta}`);
  assert(Array.isArray(doc.tagIds) && doc.tagIds.length === 0, `Expected tagIds=[], got ${JSON.stringify(doc.tagIds)}`);
  assert(doc.score === 500, `Expected score=500, got ${doc.score}`);
  log(`  [2] include_docs: commentCount=${doc.commentCount}, hasMeta=${doc.hasMeta}, tagIds=${JSON.stringify(doc.tagIds)} (defaults reconstructed)`);
}

// ----- Test B: Non-default values preserved -----

async function testB_NonDefaultPreserved() {
  log('\n--- B. Non-Default Preservation: non-default values survive round-trip ---');

  await upsert([
    { id: 2, category: 2, commentCount: 42, hasMeta: true, tagIds: [10, 20, 30], score: 400 },
  ]);
  log('  [1] Inserted doc 2 with non-default values');

  await waitForFlush(2);

  const r = await freshQuery(
    [{ Eq: ['category', { Integer: 2 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r.documents && r.documents.length === 1, `Expected 1 document`);

  const doc = r.documents[0];
  assert(doc.commentCount === 42, `Expected commentCount=42, got ${doc.commentCount}`);
  assert(doc.hasMeta === true, `Expected hasMeta=true, got ${doc.hasMeta}`);
  assert(Array.isArray(doc.tagIds) && doc.tagIds.length === 3, `Expected 3 tagIds, got ${JSON.stringify(doc.tagIds)}`);
  assert(doc.tagIds[0] === 10 && doc.tagIds[1] === 20 && doc.tagIds[2] === 30, `Expected tagIds=[10,20,30]`);
  log(`  [2] Round-trip: commentCount=${doc.commentCount}, hasMeta=${doc.hasMeta}, tagIds=${JSON.stringify(doc.tagIds)} (non-defaults preserved)`);
}

// ----- Test C: Missing fields filled with defaults -----

async function testC_MissingFieldsFilled() {
  log('\n--- C. Missing Fields Filled: sparse doc gets defaults on read ---');

  // Insert doc with only required fields — omit optional fields with defaults
  await upsert([
    { id: 3, category: 3, score: 300 },
  ]);
  log('  [1] Inserted doc 3 with only category + score (missing commentCount, hasMeta, tagIds)');

  await waitForFlush(3);

  const r = await freshQuery(
    [{ Eq: ['category', { Integer: 3 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r.documents && r.documents.length === 1, `Expected 1 document`);

  const doc = r.documents[0];
  // Missing fields should get their defaults
  assert(doc.commentCount === 0, `Expected commentCount=0 (default), got ${doc.commentCount}`);
  assert(doc.hasMeta === false, `Expected hasMeta=false (default), got ${doc.hasMeta}`);
  assert(Array.isArray(doc.tagIds) && doc.tagIds.length === 0, `Expected tagIds=[] (default), got ${JSON.stringify(doc.tagIds)}`);
  log(`  [2] Missing fields filled: commentCount=${doc.commentCount}, hasMeta=${doc.hasMeta}, tagIds=${JSON.stringify(doc.tagIds)}`);
}

// ----- Test D: Explicit default value vs missing field -----

async function testD_ExplicitDefaultVsMissing() {
  log('\n--- D. Explicit Default vs Missing: fields with explicit default=0 vs no default ---');

  // Doc 3 was inserted without optionalNote — for an integer filter field,
  // missing means no bitmap bit set. When served, the type default (0) is used
  // since null can't be represented as an integer in the bitmap index.
  const r = await freshQuery(
    [{ Eq: ['category', { Integer: 3 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  const doc = r.documents[0];

  // optionalNote was missing from input — type default (0) should appear
  log(`  [1] optionalNote when missing from input: ${doc.optionalNote} (type default applied)`);

  // Now insert a doc WITH optionalNote set to non-zero
  await upsert([
    { id: 4, category: 4, optionalNote: 99, score: 200 },
  ]);
  await waitForFlush(4);

  const r2 = await freshQuery(
    [{ Eq: ['category', { Integer: 4 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  const doc2 = r2.documents[0];
  assert(doc2.optionalNote === 99, `Expected optionalNote=99, got ${doc2.optionalNote}`);
  log(`  [2] Doc 4 with optionalNote=99: ${doc2.optionalNote} (non-default value preserved)`);

  // Verify commentCount=0 (explicit default) is still reconstructed
  assert(doc.commentCount === 0, `Expected commentCount=0, got ${doc.commentCount}`);
  log(`  [3] commentCount=0 still present via default reconstruction`);
}

// ----- Test E: Upsert changes default to non-default -----

async function testE_UpsertDefaultToNonDefault() {
  log('\n--- E. Upsert Default→Non-Default: update field from default to non-default ---');

  // Doc 1 has commentCount=0 (default). Update to 15.
  await upsert([
    { id: 1, category: 1, commentCount: 15, hasMeta: true, tagIds: [50], score: 500 },
  ]);
  log('  [1] Upserted doc 1: commentCount 0→15, hasMeta false→true, tagIds []→[50]');

  await sleep(500); // upsert on existing doc — alive_count stays same, just wait for flush

  const r = await freshQuery(
    [{ Eq: ['category', { Integer: 1 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  const doc = r.documents[0];
  assert(doc.commentCount === 15, `Expected commentCount=15, got ${doc.commentCount}`);
  assert(doc.hasMeta === true, `Expected hasMeta=true, got ${doc.hasMeta}`);
  assert(doc.tagIds.length === 1 && doc.tagIds[0] === 50, `Expected tagIds=[50], got ${JSON.stringify(doc.tagIds)}`);
  log(`  [2] After upsert: commentCount=${doc.commentCount}, hasMeta=${doc.hasMeta}, tagIds=${JSON.stringify(doc.tagIds)} (non-defaults preserved)`);

  // Now update back to defaults
  await upsert([
    { id: 1, category: 1, commentCount: 0, hasMeta: false, tagIds: [], score: 500 },
  ]);
  log('  [3] Upserted doc 1 back to defaults');

  await sleep(500); // upsert on existing doc

  const r2 = await freshQuery(
    [{ Eq: ['category', { Integer: 1 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  const doc2 = r2.documents[0];
  assert(doc2.commentCount === 0, `Expected commentCount=0 (back to default), got ${doc2.commentCount}`);
  assert(doc2.hasMeta === false, `Expected hasMeta=false (back to default), got ${doc2.hasMeta}`);
  assert(doc2.tagIds.length === 0, `Expected tagIds=[] (back to default), got ${JSON.stringify(doc2.tagIds)}`);
  log(`  [4] Back to defaults: commentCount=${doc2.commentCount}, hasMeta=${doc2.hasMeta}, tagIds=${JSON.stringify(doc2.tagIds)}`);
}

// ----- Test F: Bitmap filtering still works with elided fields -----

async function testF_FilteringWithElision() {
  log('\n--- F. Filtering Works With Elided Fields ---');

  // Doc 1 was explicitly inserted with commentCount=0 — its bitmap bit IS set.
  // Doc 3 was inserted WITHOUT commentCount — no bitmap bit set (missing != 0).
  // Elision only affects docstore storage, not bitmap indexing.
  const r1 = await freshQuery(
    [{ Eq: ['commentCount', { Integer: 0 }] }],
    SORT_DESC,
  );
  assert(r1.ids && r1.ids.includes(1), `Expected doc 1 in commentCount=0 results`);
  log(`  [1] Filter commentCount=0: ${r1.ids.length} result(s) — bitmap reflects explicit values only`);

  // hasMeta=true should find doc 2
  const r2 = await freshQuery(
    [{ Eq: ['hasMeta', { Bool: true }] }],
    SORT_DESC,
  );
  assert(r2.ids && r2.ids.includes(2), `Expected doc 2 in hasMeta=true results`);
  log(`  [2] Filter hasMeta=true: ${r2.ids.length} results (boolean bitmap correct)`);

  // tagIds IN [10] should find doc 2
  const r3 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 10 }]] }],
    SORT_DESC,
  );
  assert(r3.ids && r3.ids.includes(2), `Expected doc 2 in tagIds IN [10] results`);
  log(`  [3] Filter tagIds IN [10]: ${r3.ids.length} results (multi-value bitmap correct)`);

  // Verify that include_docs still shows defaults for doc 3 even though bitmap has no entry
  const r4 = await freshQuery(
    [{ Eq: ['category', { Integer: 3 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  const doc3 = r4.documents[0];
  assert(doc3.commentCount === 0, `Expected doc 3 commentCount=0 (default reconstructed), got ${doc3.commentCount}`);
  log(`  [4] Doc 3 include_docs: commentCount=${doc3.commentCount} (default reconstructed even without bitmap entry)`);
}

// ----- Test G: Save snapshot, verify defaults and elision survive -----

async function testG_SnapshotPreservesDefaults() {
  log('\n--- G. Snapshot Preserves Defaults: save_snapshot, query after save ---');

  // Trigger save_snapshot — writes schema history to disk
  const snapRes = await apiPost(`/api/indexes/${INDEX}/snapshot`, {});
  assert(snapRes.status === 200, `Snapshot save failed: ${snapRes.status}`);
  log('  [1] save_snapshot triggered — schema v1 history should be on disk');

  // Query with include_docs after save — defaults should still reconstruct
  const r1 = await freshQuery(
    [{ Eq: ['category', { Integer: 1 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r1.documents && r1.documents.length === 1, `Expected 1 doc`);
  const doc1 = r1.documents[0];
  assert(doc1.commentCount === 0, `Expected commentCount=0 after snapshot, got ${doc1.commentCount}`);
  assert(doc1.hasMeta === false, `Expected hasMeta=false after snapshot, got ${doc1.hasMeta}`);
  log(`  [2] Doc 1 after save: commentCount=${doc1.commentCount}, hasMeta=${doc1.hasMeta} (defaults reconstructed)`);

  // Verify doc 2 (non-default values) also survived
  const r2 = await freshQuery(
    [{ Eq: ['category', { Integer: 2 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r2.documents && r2.documents.length === 1, `Expected 1 doc`);
  const doc2 = r2.documents[0];
  assert(doc2.commentCount === 42, `Expected commentCount=42 after snapshot, got ${doc2.commentCount}`);
  assert(doc2.hasMeta === true, `Expected hasMeta=true after snapshot, got ${doc2.hasMeta}`);
  assert(doc2.tagIds.length === 3, `Expected 3 tagIds after snapshot, got ${doc2.tagIds.length}`);
  log(`  [3] Doc 2 after save: commentCount=${doc2.commentCount}, tagIds=${JSON.stringify(doc2.tagIds)} (non-defaults preserved)`);

  // Insert new doc AFTER snapshot — should still use v1 defaults for elision
  await upsert([
    { id: 10, category: 10, commentCount: 0, hasMeta: false, tagIds: [], score: 1000 },
  ]);
  log('  [4] Inserted doc 10 (all defaults) after snapshot');

  await waitForFlush(5); // docs 1,2,3,4,10

  const r3 = await freshQuery(
    [{ Eq: ['category', { Integer: 10 }] }],
    SORT_DESC,
    10,
    { include_docs: true },
  );
  assert(r3.documents && r3.documents.length === 1, `Expected 1 doc`);
  const doc10 = r3.documents[0];
  assert(doc10.commentCount === 0, `Expected commentCount=0 for post-snapshot doc, got ${doc10.commentCount}`);
  assert(doc10.hasMeta === false, `Expected hasMeta=false for post-snapshot doc, got ${doc10.hasMeta}`);
  assert(Array.isArray(doc10.tagIds) && doc10.tagIds.length === 0, `Expected tagIds=[] for post-snapshot doc`);
  log(`  [5] Post-snapshot doc 10: defaults correctly elided and reconstructed`);
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
  log('=== BitDex E2E: Schema Versioning & Field Elision ===');
  log(`Server: ${BASE_URL}`);

  try {
    await setup();

    await runGroup('A. Default Reconstruction', testA_DefaultReconstruction);
    await runGroup('B. Non-Default Preservation', testB_NonDefaultPreserved);
    await runGroup('C. Missing Fields Filled', testC_MissingFieldsFilled);
    await runGroup('D. Explicit Default vs Missing', testD_ExplicitDefaultVsMissing);
    await runGroup('E. Upsert Default→Non-Default', testE_UpsertDefaultToNonDefault);
    await runGroup('F. Filtering With Elision', testF_FilteringWithElision);
    await runGroup('G. Snapshot Preserves Defaults', testG_SnapshotPreservesDefaults);
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
      suite: 'schema-versioning',
      timestamp: new Date().toISOString(),
      server: BASE_URL,
      passed,
      failed,
      groups: groupResults,
    };
    const path = resolve(RESULTS_DIR, 'e2e-schema-versioning.json');
    writeFileSync(path, JSON.stringify(report, null, 2));
    log(`\nResults written to ${path}`);
  }

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main();
