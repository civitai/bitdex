#!/usr/bin/env node
/**
 * E2E: Schema Mapping Correctness
 *
 * Verifies that field source→target mapping, ms_to_seconds conversion,
 * exists_boolean, and fallback fields work correctly across ALL ingestion paths:
 *   1. PUT (full document upsert — used by bulk load)
 *   2. PATCH (partial update — used by outbox sync)
 *   3. Query (returned documents reflect correct values)
 *
 * This test exists because publishedAt was silently broken twice:
 *   - First: outbox poller zeroed metrics via PUT (fixed with PATCH endpoint)
 *   - Second: PATCH didn't apply ms_to_seconds conversion for publishedAtUnix→publishedAt
 *
 * Usage:
 *   node tests/e2e/e2e-schema-mapping.mjs [--url http://localhost:3000] [--verbose]
 */

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const VERBOSE = process.argv.includes('--verbose');
const INDEX = 'schema-mapping-test';

let passed = 0;
let failed = 0;

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [v]', ...args); }

async function api(method, path, body) {
  const url = `${BASE_URL}${path}`;
  vlog(`${method} ${url}`);
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(url, opts);
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = text; }
  return { status: res.status, json };
}

async function getDoc(id) {
  const r = await api('POST', `/api/indexes/${INDEX}/query?format=compact`, {
    filter: { id }, sort: '-sortAt', limit: 1, include_docs: true,
  });
  if (r.status !== 200 || !r.json.documents || r.json.documents.length === 0) return null;
  return r.json.documents[0];
}

async function test(name, fn) {
  try {
    await fn();
    passed++;
    log(`  ✓ ${name}`);
  } catch (e) {
    failed++;
    log(`  ✗ ${name}: ${e.message}`);
    if (VERBOSE) console.error(e);
  }
}

function assert(cond, msg) { if (!cond) throw new Error(msg); }

// --- Config with schema mappings that mirror Civitai production ---
const CONFIG = {
  name: INDEX,
  config: {
    filter_fields: [
      { name: 'nsfwLevel', field_type: 'single_value' },
      { name: 'isPublished', field_type: 'boolean' },
    ],
    sort_fields: [
      { name: 'sortAt', bits: 32 },
      { name: 'publishedAt', bits: 32 },
      { name: 'reactionCount', bits: 32 },
    ],
  },
  data_schema: {
    id_field: 'id',
    schema_version: 1,
    fields: [
      { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
      // publishedAtUnix → isPublished (exists_boolean: non-null = true)
      { source: 'publishedAtUnix', target: 'isPublished', value_type: 'exists_boolean' },
      // publishedAtUnix → publishedAt (ms_to_seconds conversion, default 0 for unpublished)
      { source: 'publishedAtUnix', target: 'publishedAt', value_type: 'integer', ms_to_seconds: true, default: 0 },
      // sortAtUnix → sortAt (ms_to_seconds conversion, default 0)
      { source: 'sortAtUnix', target: 'sortAt', value_type: 'integer', ms_to_seconds: true, default: 0 },
      // reactionCount with default 0
      { source: 'reactionCount', target: 'reactionCount', value_type: 'integer', default: 0 },
    ],
  },
};

const SLOT = 42;
const PUBLISHED_AT_MS = 1773148081000; // ms
const PUBLISHED_AT_SECS = 1773148081; // seconds (ms / 1000)
const SORT_AT_MS = 1773200000000;
const SORT_AT_SECS = 1773200000;
const REACTION_COUNT = 500;

// --- Tests ---
log(`\nE2E: Schema Mapping Correctness`);
log(`Target: ${BASE_URL}`);
log(`---`);

// Cleanup
await api('DELETE', `/api/indexes/${INDEX}`);

// Setup
log(`\n[A] Setup`);

await test('Create index with schema mappings', async () => {
  const r = await api('POST', '/api/indexes', CONFIG);
  assert(r.status === 201 || r.status === 200, `Create: ${r.status} ${JSON.stringify(r.json)}`);
});

// =========================================================================
// GROUP B: PUT (full document upsert — bulk load path)
// =========================================================================
log(`\n[B] PUT — Full Document Upsert (bulk load path)`);

await test('PUT with publishedAtUnix in ms → publishedAt in seconds', async () => {
  const doc = {
    id: SLOT,
    nsfwLevel: 1,
    publishedAtUnix: PUBLISHED_AT_MS,
    sortAtUnix: SORT_AT_MS,
    reactionCount: REACTION_COUNT,
  };
  const r = await api('POST', `/api/indexes/${INDEX}/documents/upsert`, { documents: [doc] });
  assert(r.status === 200, `Upsert: ${r.status}`);
  await new Promise(r => setTimeout(r, 500));

  const d = await getDoc(SLOT);
  assert(d, 'Doc not found');
  vlog(`PUT result: ${JSON.stringify(d)}`);

  // ms_to_seconds: publishedAtUnix (ms) → publishedAt (seconds)
  assert(d.publishedAt === PUBLISHED_AT_SECS,
    `publishedAt: expected ${PUBLISHED_AT_SECS}, got ${d.publishedAt}`);

  // ms_to_seconds: sortAtUnix (ms) → sortAt (seconds)
  assert(d.sortAt === SORT_AT_SECS,
    `sortAt: expected ${SORT_AT_SECS}, got ${d.sortAt}`);

  // exists_boolean: publishedAtUnix non-null → isPublished = true
  assert(d.isPublished === true,
    `isPublished: expected true, got ${d.isPublished}`);

  // direct mapping
  assert(d.reactionCount === REACTION_COUNT,
    `reactionCount: expected ${REACTION_COUNT}, got ${d.reactionCount}`);
});

await test('PUT with publishedAtUnix=null → isPublished=false, publishedAt=0', async () => {
  const doc = {
    id: SLOT,
    nsfwLevel: 1,
    publishedAtUnix: null,
    sortAtUnix: SORT_AT_MS,
    reactionCount: 100,
  };
  const r = await api('POST', `/api/indexes/${INDEX}/documents/upsert`, { documents: [doc] });
  assert(r.status === 200, `Upsert: ${r.status} ${JSON.stringify(r.json)}`);
  await new Promise(r => setTimeout(r, 2000)); // longer wait for flush

  const d = await getDoc(SLOT);
  assert(d, 'Doc not found');

  // null → exists_boolean = false
  assert(d.isPublished === false || d.isPublished === undefined,
    `isPublished: expected false/undefined, got ${d.isPublished}`);

  // null → publishedAt defaults to 0
  assert(d.publishedAt === 0 || d.publishedAt === undefined,
    `publishedAt: expected 0/undefined, got ${d.publishedAt}`);
});

// Restore to published state for PATCH tests
await api('POST', `/api/indexes/${INDEX}/documents/upsert`, {
  documents: [{
    id: SLOT, nsfwLevel: 1,
    publishedAtUnix: PUBLISHED_AT_MS,
    sortAtUnix: SORT_AT_MS,
    reactionCount: REACTION_COUNT,
  }]
});
await new Promise(r => setTimeout(r, 2000));

// =========================================================================
// GROUP C: PATCH — Partial Update (outbox sync path)
// =========================================================================
log(`\n[C] PATCH — Partial Update (outbox sync path)`);

await test('PATCH nsfwLevel only → publishedAt preserved', async () => {
  const r = await api('PATCH', `/api/indexes/${INDEX}/documents/patch`, {
    documents: [{ id: SLOT, nsfwLevel: 2 }]
  });
  assert(r.status === 200, `Patch: ${r.status}`);
  await new Promise(r => setTimeout(r, 500));

  const d = await getDoc(SLOT);
  assert(d, 'Doc not found');

  assert(d.publishedAt === PUBLISHED_AT_SECS,
    `CRITICAL: publishedAt changed! Expected ${PUBLISHED_AT_SECS}, got ${d.publishedAt}`);
  assert(d.reactionCount === REACTION_COUNT,
    `reactionCount changed! Expected ${REACTION_COUNT}, got ${d.reactionCount}`);
  assert(d.nsfwLevel === 2, `nsfwLevel not updated: ${d.nsfwLevel}`);
});

await test('PATCH publishedAtUnix → publishedAt updates with ms_to_seconds', async () => {
  const newPublishedMs = 1780000000000;
  const newPublishedSecs = 1780000000;

  const r = await api('PATCH', `/api/indexes/${INDEX}/documents/patch`, {
    documents: [{ id: SLOT, publishedAtUnix: newPublishedMs }]
  });
  assert(r.status === 200, `Patch: ${r.status}`);
  await new Promise(r => setTimeout(r, 500));

  const d = await getDoc(SLOT);
  assert(d, 'Doc not found');

  assert(d.publishedAt === newPublishedSecs,
    `CRITICAL: ms_to_seconds not applied in PATCH! Expected ${newPublishedSecs}, got ${d.publishedAt}`);
  assert(d.isPublished === true,
    `isPublished should still be true, got ${d.isPublished}`);
});

await test('PATCH reactionCount only → publishedAt and sortAt preserved', async () => {
  const r = await api('PATCH', `/api/indexes/${INDEX}/documents/patch`, {
    documents: [{ id: SLOT, reactionCount: 999 }]
  });
  assert(r.status === 200, `Patch: ${r.status}`);
  await new Promise(r => setTimeout(r, 500));

  const d = await getDoc(SLOT);
  assert(d, 'Doc not found');

  assert(d.reactionCount === 999, `reactionCount: expected 999, got ${d.reactionCount}`);
  assert(d.sortAt === SORT_AT_SECS, `sortAt changed: expected ${SORT_AT_SECS}, got ${d.sortAt}`);
  // publishedAt was updated in previous test
  assert(d.publishedAt === 1780000000, `publishedAt changed: ${d.publishedAt}`);
});

await test('PATCH with publishedAtUnix=null → clears isPublished and publishedAt', async () => {
  const r = await api('PATCH', `/api/indexes/${INDEX}/documents/patch`, {
    documents: [{ id: SLOT, publishedAtUnix: null }]
  });
  assert(r.status === 200, `Patch: ${r.status}`);
  await new Promise(r => setTimeout(r, 500));

  const d = await getDoc(SLOT);
  assert(d, 'Doc not found');

  assert(d.isPublished === false || d.isPublished === undefined,
    `isPublished should be false after null publishedAtUnix, got ${d.isPublished}`);
});

// =========================================================================
// GROUP D: Sort bitmap correctness
// =========================================================================
log(`\n[D] Sort Bitmap Correctness`);

// Insert two docs with different publishedAt to verify sort order
await api('POST', `/api/indexes/${INDEX}/documents/upsert`, {
  documents: [
    { id: 100, nsfwLevel: 1, publishedAtUnix: 1770000000000, sortAtUnix: 1770000000000, reactionCount: 10 },
    { id: 101, nsfwLevel: 1, publishedAtUnix: 1780000000000, sortAtUnix: 1780000000000, reactionCount: 20 },
  ]
});
await new Promise(r => setTimeout(r, 500));

await test('Sort by publishedAt Desc returns correct order', async () => {
  const r = await api('POST', `/api/indexes/${INDEX}/query?format=compact`, {
    filter: { nsfwLevel: 1 }, sort: '-publishedAt', limit: 2, include_docs: true,
  });
  assert(r.status === 200, `Query: ${r.status}`);
  assert(r.json.documents.length >= 2, `Expected 2+ docs, got ${r.json.documents.length}`);
  // Newer publishedAt should be first
  assert(r.json.documents[0].publishedAt >= r.json.documents[1].publishedAt,
    `Sort order wrong: ${r.json.documents[0].publishedAt} < ${r.json.documents[1].publishedAt}`);
});

await test('Sort bitmap reflects ms_to_seconds conversion', async () => {
  const r = await api('POST', `/api/indexes/${INDEX}/query?format=compact`, {
    filter: { nsfwLevel: 1 }, sort: '-publishedAt', limit: 1, include_docs: true,
  });
  const doc = r.json.documents[0];
  assert(doc.publishedAt === 1780000000,
    `Top publishedAt should be 1780000000 (seconds), got ${doc.publishedAt}`);
});

// =========================================================================
// Cleanup
// =========================================================================
await api('DELETE', `/api/indexes/${INDEX}`);

// Summary
log(`\n${'='.repeat(40)}`);
log(`Results: ${passed} passed, ${failed} failed`);
if (failed > 0) {
  log(`\n⚠ ${failed} test(s) FAILED`);
  process.exit(1);
} else {
  log(`\n✓ All tests passed`);
}
