#!/usr/bin/env node
// Gate 5 — Sync V2 Regression Suite
//
// Automated regression tests for the full sync pipeline.
// Validates dump loading, ops mutations (all field types),
// docstore correctness, and doc cache consistency.
//
// Self-contained: starts its own server, loads test data,
// runs all tests, prints PASS/FAIL per test, exits non-zero on failure.
//
// Prerequisites:
//   cargo build --profile fast --features "server,pg-sync" --bin bitdex-server
//   Test CSVs in .test-data/gate5-csvs/ (images.csv, tags.csv, posts.csv)
//
// Usage:
//   node tests/gate5-regression.mjs [--port 3007] [--binary target/fast/bitdex-server.exe]

import { spawn } from 'child_process';
import { mkdirSync, rmSync, existsSync } from 'fs';
import path from 'path';

const PORT = parseInt(process.argv.find((_, i) => process.argv[i - 1] === '--port') || '3007');
const BINARY = process.argv.find((_, i) => process.argv[i - 1] === '--binary') || 'target/fast/bitdex-server.exe';
const BASE = `http://127.0.0.1:${PORT}`;
const INDEX = 'regression-test';
const DATA_DIR = path.resolve('.test-data/regression-data');
const CSV_DIR = path.resolve('.test-data/gate5-csvs');

let passed = 0, failed = 0, serverProc = null;

// ── Helpers ──

function check(name, condition, detail) {
  if (condition) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} — ${detail || 'FAILED'}`); failed++; }
}

async function req(method, urlPath, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  try {
    const res = await fetch(`${BASE}${urlPath}`, opts);
    const text = await res.text();
    try { return { status: res.status, body: JSON.parse(text) }; }
    catch { return { status: res.status, body: text }; }
  } catch (e) {
    return { status: 0, body: e.message };
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForServer(maxMs = 10000) {
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    try {
      const res = await fetch(`${BASE}/api/indexes`);
      if (res.ok) return true;
    } catch {}
    await sleep(500);
  }
  return false;
}

function startServer() {
  console.log(`  Starting server: ${BINARY} --port ${PORT} --data-dir ${DATA_DIR}`);
  serverProc = spawn(BINARY, [
    '--port', String(PORT),
    '--data-dir', DATA_DIR,
    '--enable-traces',
  ], { stdio: ['ignore', 'pipe', 'pipe'] });
  serverProc.stderr.on('data', d => {
    const line = d.toString().trim();
    if (line.includes('error') || line.includes('panic') || line.includes('WARN')) {
      console.log(`    [server] ${line}`);
    }
  });
}

function killServer() {
  if (serverProc) {
    serverProc.kill('SIGTERM');
    try { serverProc.kill('SIGKILL'); } catch {}
    serverProc = null;
  }
}

async function postOps(ops, meta) {
  return req('POST', `/api/indexes/${INDEX}/ops`, { ops, meta: meta || { source: 'regression-test' } });
}

async function getDoc(id) {
  const r = await req('GET', `/api/indexes/${INDEX}/documents/${id}`);
  return r.body;
}

async function query(filters, sort, limit = 100) {
  return req('POST', `/api/indexes/${INDEX}/query`, {
    filters,
    sort: sort || { field: 'id', direction: 'Desc' },
    limit,
    skip_cache: true,
  });
}

async function waitForDump(taskId, timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    await sleep(500);
    const task = await req('GET', `/api/tasks/${taskId}`);
    if (task.body?.status === 'complete' || task.body?.status === 'completed') return true;
    if (task.body?.status === 'error' || task.body?.status === 'failed') return false;
  }
  return false;
}

// ── Index Config ──

const INDEX_CONFIG = {
  name: INDEX,
  config: {
    filter_fields: [
      { name: 'nsfwLevel', field_type: 'single_value' },
      { name: 'type', field_type: 'single_value' },
      { name: 'userId', field_type: 'single_value' },
      { name: 'tagIds', field_type: 'multi_value' },
      { name: 'hasMeta', field_type: 'single_value' },
      { name: 'onSite', field_type: 'single_value' },
      { name: 'postId', field_type: 'single_value' },
    ],
    sort_fields: [
      { name: 'existedAt', source_type: 'uint32', encoding: 'linear', bits: 32 },
      { name: 'publishedAt', source_type: 'uint32', encoding: 'linear', bits: 32 },
      { name: 'sortAt', source_type: 'uint32', encoding: 'linear', bits: 32,
        computed: { op: 'greatest', source_fields: ['existedAt', 'publishedAt'] } },
      { name: 'id', source_type: 'uint32', encoding: 'linear', bits: 32 },
    ],
    deferred_alive: { source_field: 'publishedAt', ms_to_seconds: false },
  },
  data_schema: {
    id_field: 'id',
    fields: [
      { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
      { source: 'type', target: 'type', value_type: 'string' },
      { source: 'userId', target: 'userId', value_type: 'integer' },
      { source: 'postId', target: 'postId', value_type: 'integer' },
      { source: 'hasMeta', target: 'hasMeta', value_type: 'integer' },
      { source: 'publishedAt', target: 'publishedAt', value_type: 'integer' },
      { source: 'existedAt', target: 'existedAt', value_type: 'integer' },
      { source: 'sortAt', target: 'sortAt', value_type: 'integer' },
      { source: 'url', target: 'url', value_type: 'string', doc_only: true },
      { source: 'hash', target: 'hash', value_type: 'string', doc_only: true },
      { source: 'width', target: 'width', value_type: 'integer' },
      { source: 'height', target: 'height', value_type: 'integer' },
    ],
  },
};

// ══════════════════════════════════════════════════════════════════
// Suite 1: Dump Validation
// ══════════════════════════════════════════════════════════════════

async function suite1_dump_validation() {
  console.log('\n═══ Suite 1: Dump Validation ═══');

  // Images dump
  const imagesPath = path.resolve(CSV_DIR, 'images.csv').replace(/\\/g, '/');
  const imagesDump = {
    name: 'images',
    csv_path: imagesPath,
    format: 'csv',
    slot_field: 'id',
    sets_alive: true,
    fields: [
      'nsfwLevel',
      { column: 'type', target: 'type' },
      'userId', 'postId',
      { column: 'url', target: 'url' },
      { column: 'hash', target: 'hash' },
      'width', 'height',
    ],
    computed_fields: [
      { target: 'hasMeta', expression: '(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0' },
      { target: 'onSite', expression: '(flags >> 14) & 1 == 1' },
      { target: 'existedAt', expression: 'max(scannedAtSecs, createdAtSecs)' },
      { target: 'id', expression: 'id' },
    ],
    enrichment: [{
      csv_path: path.resolve(CSV_DIR, 'posts.csv').replace(/\\/g, '/'),
      key: 'id',
      join_on: 'postId',
      fields: [{ column: 'publishedAtSecs', target: 'publishedAt' }],
      computed_fields: [{ target: 'postedToId', expression: 'lookup_key' }],
      enrichment: [],
    }],
  };

  const r1 = await req('PUT', `/api/indexes/${INDEX}/dumps`, imagesDump);
  check('Images dump accepted', r1.status === 200 || r1.status === 201,
    `status=${r1.status} body=${JSON.stringify(r1.body).slice(0, 200)}`);
  if (r1.body?.task_id) {
    const ok = await waitForDump(r1.body.task_id);
    check('Images dump completed', ok);
  }

  // Tags dump
  const tagsDump = {
    name: 'tags',
    csv_path: path.resolve(CSV_DIR, 'tags.csv').replace(/\\/g, '/'),
    format: 'csv',
    slot_field: 'imageId',
    sets_alive: false,
    fields: [{ column: 'tagId', target: 'tagIds' }],
    filter: '(attributes >> 10) & 1 = 0',
    computed_fields: [],
    enrichment: [],
  };

  const r2 = await req('PUT', `/api/indexes/${INDEX}/dumps`, tagsDump);
  check('Tags dump accepted', r2.status === 200 || r2.status === 201,
    `status=${r2.status} body=${JSON.stringify(r2.body).slice(0, 200)}`);
  if (r2.body?.task_id) {
    const ok = await waitForDump(r2.body.task_id);
    check('Tags dump completed', ok);
  }

  // Restart to load bitmaps from disk
  console.log('  Restarting server to load dump bitmaps...');
  killServer();
  await sleep(2000);
  startServer();
  const ready = await waitForServer(15000);
  check('Server restarted after dump', ready);
  if (!ready) { console.log('FATAL: Server did not restart'); process.exit(1); }
  await sleep(1000);

  // Verify stats
  const stats = await req('GET', `/api/indexes/${INDEX}/stats`);
  const alive = stats.body?.alive_count || stats.body?.alive || 0;
  check(`Alive count > 0 after dump (got ${alive})`, alive > 0);

  // Verify filter queries work
  const q1 = await query([{ Eq: ['nsfwLevel', { Integer: 1 }] }]);
  check(`nsfwLevel=1 filter works (${q1.body?.ids?.length || 0} results)`, (q1.body?.ids?.length || 0) >= 1);

  const q2 = await query([{ In: ['tagIds', [{ Integer: 42 }]] }]);
  check(`tagIds=42 filter works (${q2.body?.ids?.length || 0} results)`, (q2.body?.ids?.length || 0) >= 1);

  // Verify sortAt was computed (GREATEST(existedAt, publishedAt))
  const q3 = await query([], { field: 'sortAt', direction: 'Desc' }, 5);
  check('sortAt sort works after dump', (q3.body?.ids?.length || 0) >= 1,
    `ids=${JSON.stringify(q3.body?.ids)} err=${JSON.stringify(q3.body?.error || q3.body)}`);

  // Verify existedAt in docstore
  if (q3.body?.ids?.length > 0) {
    const doc = await getDoc(q3.body.ids[0]);
    check('existedAt in docstore after dump', doc.existedAt != null && doc.existedAt > 0,
      `existedAt=${doc.existedAt}`);
    check('sortAt in docstore after dump', doc.sortAt != null && doc.sortAt > 0,
      `sortAt=${doc.sortAt}`);
  }
}

// ══════════════════════════════════════════════════════════════════
// Suite 2: Ops Field Coverage (all 8 field types)
// ══════════════════════════════════════════════════════════════════

async function suite2_ops_field_coverage() {
  console.log('\n═══ Suite 2: Ops Field Coverage ═══');

  // Find a test entity from the dump
  const stats = await req('GET', `/api/indexes/${INDEX}/stats`);
  const alive = stats.body?.alive_count || stats.body?.alive || 0;
  if (alive === 0) { console.log('  SKIP: no alive entities'); return; }

  // Use entity 1 as test subject
  const ENTITY = 1;
  const baseline = await getDoc(ENTITY);
  console.log(`  Test entity: ${ENTITY}, baseline nsfwLevel=${baseline.nsfwLevel}`);

  // ── Test 2.1: Scalar filter (nsfwLevel) ──
  console.log('  --- 2.1: Scalar filter (nsfwLevel) ---');
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'remove', field: 'nsfwLevel', value: baseline.nsfwLevel || 1 },
    { op: 'set', field: 'nsfwLevel', value: 77 },
  ]}]);
  await sleep(500);
  const q21 = await query([{ Eq: ['nsfwLevel', { Integer: 77 }] }]);
  check('2.1 scalar set: nsfwLevel=77 bitmap', q21.body?.ids?.includes(ENTITY),
    `ids=${JSON.stringify(q21.body?.ids)}`);

  // ── Test 2.2: Multi-value add (tagIds) ──
  console.log('  --- 2.2: Multi-value add (tagIds) ---');
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'add', field: 'tagIds', value: 99999 },
  ]}]);
  await sleep(500);
  const q22 = await query([{ In: ['tagIds', [{ Integer: 99999 }]] }]);
  check('2.2 multi-value add: tagIds=99999', q22.body?.ids?.includes(ENTITY),
    `ids=${JSON.stringify(q22.body?.ids)}`);

  // ── Test 2.3: Multi-value remove (tagIds) ──
  console.log('  --- 2.3: Multi-value remove (tagIds) ---');
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'remove', field: 'tagIds', value: 99999 },
  ]}]);
  await sleep(500);
  const q23 = await query([{ In: ['tagIds', [{ Integer: 99999 }]] }]);
  check('2.3 multi-value remove: tagIds=99999 gone', !q23.body?.ids?.includes(ENTITY),
    `ids=${JSON.stringify(q23.body?.ids)}`);

  // ── Test 2.4: LCS string field (type) ──
  console.log('  --- 2.4: LCS string (type) ---');
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'set', field: 'type', value: 'audio' },
  ]}]);
  await sleep(500);
  // LCS fields: new values not in dump dictionary may not appear in bitmap queries.
  // Verify docstore reflects the mutation instead.
  const doc24 = await getDoc(ENTITY);
  check('2.4 LCS string: type=audio in docstore', doc24.type === 'audio',
    `got ${doc24.type}`);

  // ── Test 2.5: Boolean field (hasMeta) ──
  console.log('  --- 2.5: Boolean (hasMeta) ---');
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'set', field: 'hasMeta', value: 0 },
  ]}]);
  await sleep(500);
  const q25 = await query([{ Eq: ['hasMeta', { Integer: 0 }] }]);
  check('2.5 boolean: hasMeta=0 bitmap', q25.body?.ids?.includes(ENTITY),
    `ids=${JSON.stringify(q25.body?.ids)}`);

  // ── Test 2.6: Sort field (publishedAt) ──
  console.log('  --- 2.6: Sort field (publishedAt) ---');
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'set', field: 'publishedAt', value: 1900000000 },
  ]}]);
  await sleep(500);
  const doc26 = await getDoc(ENTITY);
  check('2.6 sort field: publishedAt updated in docstore', doc26.publishedAt === 1900000000,
    `got ${doc26.publishedAt}`);

  // ── Test 2.7: Computed sort (sortAt = GREATEST(existedAt, publishedAt)) ──
  console.log('  --- 2.7: Computed sort (sortAt) ---');
  // After setting publishedAt=1900000000, sortAt should be GREATEST(existedAt, 1900000000)
  const doc27 = await getDoc(ENTITY);
  const expectedSortAt = Math.max(doc27.existedAt || 0, 1900000000);
  check('2.7 computed sort: sortAt = GREATEST(existedAt, publishedAt)',
    doc27.sortAt === expectedSortAt,
    `sortAt=${doc27.sortAt} expected=${expectedSortAt} existedAt=${doc27.existedAt}`);

  // ── Test 2.8: Delete ──
  console.log('  --- 2.8: Delete ---');
  // Use entity 2 for delete so we don't destroy our primary test subject
  const DEL = 2;
  await postOps([{ entity_id: DEL, creates_slot: false, ops: [{ op: 'delete' }] }]);
  await sleep(500);
  // After delete, entity should not appear in any broad query
  const q28 = await query([]);
  check('2.8 delete: entity gone from results', !q28.body?.ids?.includes(DEL),
    `entity ${DEL} still in ids`);
}

// ══════════════════════════════════════════════════════════════════
// Suite 3: Docstore Correctness
// ══════════════════════════════════════════════════════════════════

async function suite3_docstore() {
  console.log('\n═══ Suite 3: Docstore Correctness ═══');

  const ENTITY = 1;
  const doc = await getDoc(ENTITY);

  // After suite 2 mutations, verify docstore reflects all changes
  check('3.1 nsfwLevel in docstore matches op', doc.nsfwLevel === 77,
    `got ${doc.nsfwLevel}`);
  check('3.2 type in docstore matches op', doc.type === 'audio',
    `got ${doc.type}`);
  check('3.3 hasMeta in docstore matches op', doc.hasMeta === 0,
    `got ${doc.hasMeta}`);
  check('3.4 publishedAt in docstore matches op', doc.publishedAt === 1900000000,
    `got ${doc.publishedAt}`);
  check('3.5 existedAt persisted from dump', doc.existedAt != null && doc.existedAt > 0,
    `got ${doc.existedAt}`);
  check('3.6 sortAt = GREATEST(existedAt, publishedAt)',
    doc.sortAt === Math.max(doc.existedAt || 0, doc.publishedAt || 0),
    `sortAt=${doc.sortAt} expected=${Math.max(doc.existedAt || 0, doc.publishedAt || 0)}`);

  // Verify deleted entity returns empty/null
  const deleted = await getDoc(2);
  // After delete, document may still exist on disk but should not be format_document'd
  // (depends on implementation — check that query doesn't return it)
  const q = await query([]);
  check('3.7 deleted entity not in query results', !q.body?.ids?.includes(2));
}

// ══════════════════════════════════════════════════════════════════
// Suite 4: Doc Cache Consistency
// ══════════════════════════════════════════════════════════════════

async function suite4_doc_cache() {
  console.log('\n═══ Suite 4: Doc Cache Consistency ═══');

  const ENTITY = 3; // Use entity 3 for cache tests
  const baseline = await getDoc(ENTITY);
  if (!baseline || baseline.error) {
    console.log(`  SKIP: entity ${ENTITY} not found`);
    return;
  }
  console.log(`  Entity ${ENTITY} baseline userId=${baseline.userId}`);

  // Read doc to warm cache
  const cached1 = await getDoc(ENTITY);
  check('4.1 cache warm: initial read succeeds', cached1.userId != null);

  // Mutate via ops
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'set', field: 'nsfwLevel', value: 33 },
  ]}]);
  await sleep(500);

  // Immediately read — cache should be invalidated and reflect new value
  const cached2 = await getDoc(ENTITY);
  check('4.2 cache invalidated: nsfwLevel reflects mutation', cached2.nsfwLevel === 33,
    `got ${cached2.nsfwLevel} expected 33`);

  // Mutate sort field
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'set', field: 'publishedAt', value: 1800000000 },
  ]}]);
  await sleep(500);

  const cached3 = await getDoc(ENTITY);
  check('4.3 cache reflects publishedAt sort mutation', cached3.publishedAt === 1800000000,
    `got ${cached3.publishedAt}`);

  // Verify sortAt recomputation visible through cache
  const expectedSortAt = Math.max(cached3.existedAt || 0, 1800000000);
  check('4.4 cache reflects computed sortAt', cached3.sortAt === expectedSortAt,
    `sortAt=${cached3.sortAt} expected=${expectedSortAt}`);

  // Multi-value add then read
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'add', field: 'tagIds', value: 77777 },
  ]}]);
  await sleep(500);

  const q = await query([{ In: ['tagIds', [{ Integer: 77777 }]] }]);
  check('4.5 cache+bitmap: tagIds add visible in query', q.body?.ids?.includes(ENTITY),
    `ids=${JSON.stringify(q.body?.ids)}`);

  // Clean up
  await postOps([{ entity_id: ENTITY, creates_slot: false, ops: [
    { op: 'remove', field: 'tagIds', value: 77777 },
  ]}]);
}

// ══════════════════════════════════════════════════════════════════
// Main
// ══════════════════════════════════════════════════════════════════

async function main() {
  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║  Gate 5 — Sync V2 Regression Suite              ║');
  console.log('╚══════════════════════════════════════════════════╝');
  console.log(`Binary: ${BINARY}`);
  console.log(`Port: ${PORT}, Data: ${DATA_DIR}`);
  console.log(`CSVs: ${CSV_DIR}`);

  if (!existsSync(BINARY)) {
    console.log(`\nERROR: Binary not found: ${BINARY}`);
    console.log('Build with: cargo build --profile fast --features "server,pg-sync" --bin bitdex-server');
    process.exit(1);
  }
  if (!existsSync(CSV_DIR)) {
    console.log(`\nERROR: CSV dir not found: ${CSV_DIR}`);
    process.exit(1);
  }

  // Clean data dir
  if (existsSync(DATA_DIR)) rmSync(DATA_DIR, { recursive: true });
  mkdirSync(DATA_DIR, { recursive: true });

  try {
    // Start server
    startServer();
    const ready = await waitForServer();
    check('Server started and healthy', ready);
    if (!ready) { console.log('FATAL: Server did not start'); process.exit(1); }

    // Create index
    const r = await req('POST', '/api/indexes', INDEX_CONFIG);
    check('Index created', r.status === 200 || r.status === 201,
      `status=${r.status} body=${JSON.stringify(r.body).slice(0, 200)}`);

    await suite1_dump_validation();
    await suite2_ops_field_coverage();
    await suite3_docstore();
    await suite4_doc_cache();
  } finally {
    killServer();
    await sleep(1000);
  }

  // Summary
  console.log('\n════════════════════════════');
  console.log(`  PASS: ${passed}`);
  console.log(`  FAIL: ${failed}`);
  console.log(`  Total: ${passed + failed}`);
  console.log('════════════════════════════');

  if (failed > 0) {
    console.log(`\nFAILED — ${failed} test(s) failed`);
    process.exit(1);
  } else {
    console.log('\nALL TESTS PASSED ✓');
  }
}

main().catch(e => {
  console.error('Unhandled error:', e);
  killServer();
  process.exit(1);
});
