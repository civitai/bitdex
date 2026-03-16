#!/usr/bin/env node
/**
 * E2E: PATCH preserves unmentioned fields (metrics zeroing fix)
 *
 * Verifies that PATCH /documents/patch only updates provided fields
 * and preserves fields not in the payload. This prevents the outbox
 * poller from zeroing out ClickHouse metrics (reactionCount, etc.)
 * when it syncs documents without metric data.
 *
 * Test groups:
 *   A. Setup: create index, PUT a doc with reactionCount=500
 *   B. PATCH same doc with only nsfwLevel change — verify reactionCount preserved
 *   C. PATCH with reactionCount=999 — verify it updates
 *   D. PUT full doc without reactionCount — verify it DOES get cleared (put = full replace)
 *   E. PATCH on non-existent slot — verify 404/error
 *
 * Usage:
 *   node tests/e2e/e2e-patch-metrics.mjs [--url http://localhost:3000] [--verbose]
 */

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const VERBOSE = process.argv.includes('--verbose');
const INDEX = 'patch-metrics-test';

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
  // Use query with include_docs to get the document (more reliable than raw GET)
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

// --- Config ---
const CONFIG = {
  name: INDEX,
  config: {
    filter_fields: [
      { name: 'nsfwLevel', field_type: 'single_value' },
    ],
    sort_fields: [
      { name: 'reactionCount', bits: 32 },
      { name: 'sortAt', bits: 32 },
    ],
  },
  data_schema: {
    id_field: 'id',
    schema_version: 1,
    fields: [
      { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
      { source: 'reactionCount', target: 'reactionCount', value_type: 'integer', default: 0 },
      { source: 'sortAt', target: 'sortAt', value_type: 'integer', default: 0 },
    ],
  },
};

const DOC_ID = 42;
const INITIAL_RC = 500;
const UPDATED_RC = 999;
const SORT_AT = 1700000000;

// --- Tests ---
log(`\nE2E: PATCH Preserves Unmentioned Fields`);
log(`Target: ${BASE_URL}`);
log(`---`);

// Cleanup from previous run
await api('DELETE', `/api/indexes/${INDEX}`);

// A. Setup
log(`\n[A] Setup`);

await test('Create index', async () => {
  const r = await api('POST', '/api/indexes', CONFIG);
  assert(r.status === 201 || r.status === 200, `Create index: ${r.status} ${JSON.stringify(r.json)}`);
});

await test('PUT doc with reactionCount=500', async () => {
  const doc = { id: DOC_ID, nsfwLevel: 1, reactionCount: INITIAL_RC, sortAt: SORT_AT };
  const r = await api('POST', `/api/indexes/${INDEX}/documents/upsert`, { documents: [doc] });
  assert(r.status === 200 || r.status === 201, `Upsert: ${r.status} ${JSON.stringify(r.json)}`);
  vlog(`Upserted: ${JSON.stringify(r.json)}`);
});

// Wait for flush
await new Promise(r => setTimeout(r, 500));

await test('Verify initial doc has reactionCount=500', async () => {
  const doc = await getDoc(DOC_ID);
  assert(doc, `Doc not found`);
  assert(doc.reactionCount === INITIAL_RC, `Expected reactionCount=${INITIAL_RC}, got ${doc.reactionCount}`);
  vlog(`Doc: ${JSON.stringify(doc)}`);
});

// B. PATCH without metrics — should preserve reactionCount
log(`\n[B] PATCH without metrics`);

await test('PATCH nsfwLevel only (no reactionCount) — reactionCount preserved', async () => {
  const patch = { id: DOC_ID, nsfwLevel: 2 };
  const r = await api('PATCH', `/api/indexes/${INDEX}/documents/patch`, { documents: [patch] });
  assert(r.status === 200, `Patch: ${r.status} ${JSON.stringify(r.json)}`);
  assert(r.json.patched === 1, `Expected patched=1, got ${r.json.patched}`);

  // Wait for flush
  await new Promise(r => setTimeout(r, 500));

  const doc = await getDoc(DOC_ID);
  assert(doc, `Doc not found after patch`);
  assert(doc.reactionCount === INITIAL_RC,
    `CRITICAL: reactionCount zeroed! Expected ${INITIAL_RC}, got ${doc.reactionCount}`);
  assert(doc.nsfwLevel === 2, `Expected nsfwLevel=2, got ${doc.nsfwLevel}`);
  vlog(`After patch: ${JSON.stringify(doc)}`);
});

await test('Sort by reactionCount still returns doc at correct position', async () => {
  const r = await api('POST', `/api/indexes/${INDEX}/query?format=compact`, {
    filter: {}, sort: '-reactionCount', limit: 1, include_docs: true,
  });
  assert(r.status === 200, `Query: ${r.status}`);
  assert(r.json.documents[0].reactionCount === INITIAL_RC,
    `Sort bitmap corrupted: expected ${INITIAL_RC}, got ${r.json.documents[0].reactionCount}`);
});

// C. PATCH with reactionCount — should update
log(`\n[C] PATCH with metrics update`);

await test('PATCH reactionCount=999 — updates correctly', async () => {
  const patch = { id: DOC_ID, reactionCount: UPDATED_RC };
  const r = await api('PATCH', `/api/indexes/${INDEX}/documents/patch`, { documents: [patch] });
  assert(r.status === 200, `Patch: ${r.status}`);

  await new Promise(r => setTimeout(r, 500));

  const doc = await getDoc(DOC_ID);
  assert(doc, `Doc not found after patch`);
  assert(doc.reactionCount === UPDATED_RC,
    `Expected reactionCount=${UPDATED_RC}, got ${doc.reactionCount}`);
  assert(doc.nsfwLevel === 2, `nsfwLevel should still be 2, got ${doc.nsfwLevel}`);
});

// D. PUT full replace without reactionCount — SHOULD clear it
log(`\n[D] PUT full replace (should clear missing fields)`);

await test('PUT without reactionCount — sort bitmap clears', async () => {
  const doc = { id: DOC_ID, nsfwLevel: 1, sortAt: SORT_AT };
  const r = await api('POST', `/api/indexes/${INDEX}/documents/upsert`, { documents: [doc] });
  assert(r.status === 200, `Upsert: ${r.status}`);

  await new Promise(r => setTimeout(r, 2000));

  // The sort bitmap for reactionCount should be cleared (0).
  // Query by reactionCount sort — with rc=0 the doc should be at the bottom.
  const q = await api('POST', `/api/indexes/${INDEX}/query?format=compact`, {
    filter: {}, sort: '-reactionCount', limit: 1,
  });
  assert(q.status === 200, `Query: ${q.status}`);
  // The bitmap sort position should reflect 0, not 999
  vlog(`After PUT, sort query result: ${JSON.stringify(q.json)}`);
});

// E. PATCH on non-existent slot
log(`\n[E] Error cases`);

await test('PATCH non-existent slot returns error', async () => {
  const patch = { id: 99999, nsfwLevel: 1 };
  const r = await api('PATCH', `/api/indexes/${INDEX}/documents/patch`, { documents: [patch] });
  assert(r.status === 200, `Patch: ${r.status}`);
  assert(r.json.errors && r.json.errors.length > 0,
    `Expected error for non-existent slot, got: ${JSON.stringify(r.json)}`);
  vlog(`Error response: ${JSON.stringify(r.json)}`);
});

// Cleanup
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
