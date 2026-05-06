#!/usr/bin/env node
/**
 * E2E: blockedFor null transition (low_cardinality_string scalar nullable)
 *
 * Repros the bug seen on prod slot 129764603: blockedFor cleared in DB but
 * BitDex still reports the old value. Root cause was the docstore writer
 * dropping null payloads (json_to_packed null → None), so the prior tuple
 * remained the LIFO winner on read. Fix added PackedValue::Null with
 * field-clear semantics on apply.
 *
 * Drives the steady-state /ops path (POST /api/indexes/{name}/ops) directly,
 * matching how the PG triggers send {Remove(old) + Set(new=null)} pairs.
 *
 * Test groups:
 *   A. Insert blockedFor="CSAM", then UPDATE to null — bitmap clears + doc clears
 *   B. Round-trip value → null → value — doc tracks latest
 *
 * Usage:
 *   node tests/e2e/e2e-blockedfor-null-transition.mjs [--url http://localhost:3001] [--verbose]
 *
 * Server must be built with `--features server,pg-sync`.
 */

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3001';
const VERBOSE = process.argv.includes('--verbose');
const RESULTS_DIR = process.argv.includes('--results-dir')
  ? process.argv[process.argv.indexOf('--results-dir') + 1]
  : null;

const INDEX = 'blockedfor-test';

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
  let data = null;
  try { data = await res.json(); } catch (_) {}
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  let data = null;
  try { data = await res.json(); } catch (_) {}
  vlog(`GET ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiDelete(path) {
  const res = await fetch(`${BASE_URL}${path}`, { method: 'DELETE' });
  let data = null;
  try { data = await res.json(); } catch (_) {}
  return { status: res.status, data };
}

const query = (filters, limit = 100) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, limit }).then(r => r.data);
const sendOps = (ops) =>
  apiPost(`/api/indexes/${INDEX}/ops`, { ops }).then(r => r);
const getDoc = (slot) =>
  apiGet(`/api/indexes/${INDEX}/documents/${slot}`).then(r => r);
const clearCache = () =>
  apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function freshQuery(filters, limit = 100) {
  await clearCache();
  return query(filters, limit);
}

async function setup() {
  log('\n--- Setup: blockedFor null transition test index ---');
  try { await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' }); } catch (_) {}
  await sleep(300);

  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'nsfwLevel', field_type: 'single_value' },
        { name: 'blockedFor', field_type: 'single_value' },
      ],
      sort_fields: [],
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'blockedFor', target: 'blockedFor', value_type: 'low_cardinality_string', nullable: true },
      ],
    },
  });
  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created`);
}

function recordTest(group, results) {
  groupResults.push({ group, results });
  results.forEach(r => r.pass ? passed++ : failed++);
}

function getDocField(docResp, field) {
  const d = docResp.data;
  return d?.document?.[field] ?? d?.fields?.[field] ?? d?.[field];
}

async function testA() {
  const group = 'A';
  const results = [];
  log(`\n--- ${group}. value → null transition (the prod bug) ---`);

  // Step 1: create slot with blockedFor="CSAM"
  await sendOps([
    {
      entity_id: 200,
      creates_slot: true,
      ops: [
        { op: 'alive' },
        { op: 'set', field: 'nsfwLevel', value: 8 },
        { op: 'set', field: 'blockedFor', value: 'CSAM' },
      ],
    },
  ]);
  await sleep(400);

  const before = await getDoc(200);
  vlog('before doc:', JSON.stringify(before.data));
  const beforeVal = getDocField(before, 'blockedFor');
  const t0 = beforeVal === 'CSAM';
  results.push({ name: 'pre-state: doc has CSAM', pass: t0 });
  log(`  [0] doc.blockedFor=${JSON.stringify(beforeVal)} (expect "CSAM"): ${t0 ? 'PASS' : 'FAIL'}`);

  // Step 2: UPDATE to null (mimics PG trigger emitting Remove(old) + Set(null))
  await sendOps([
    {
      entity_id: 200,
      creates_slot: false,
      ops: [
        { op: 'remove', field: 'blockedFor', value: 'CSAM' },
        { op: 'set', field: 'blockedFor', value: null },
      ],
    },
  ]);
  await sleep(400);

  const isNullQ = await freshQuery([{ IsNull: 'blockedFor' }]);
  const t1 = Array.isArray(isNullQ?.ids) && isNullQ.ids.includes(200);
  results.push({ name: 'after null update: IsNull bitmap contains slot', pass: t1 });
  log(`  [1] IsNull ids=${JSON.stringify(isNullQ?.ids)}: ${t1 ? 'PASS' : 'FAIL'}`);

  const isNotNullQ = await freshQuery([{ IsNotNull: 'blockedFor' }]);
  const t2 = Array.isArray(isNotNullQ?.ids) && !isNotNullQ.ids.includes(200);
  results.push({ name: 'after null update: IsNotNull excludes slot', pass: t2 });
  log(`  [2] IsNotNull ids=${JSON.stringify(isNotNullQ?.ids)}: ${t2 ? 'PASS' : 'FAIL'}`);

  const after = await getDoc(200);
  vlog('after doc:', JSON.stringify(after.data));
  const afterVal = getDocField(after, 'blockedFor');
  const t3 = afterVal == null;
  results.push({ name: 'after null update: doc.blockedFor cleared', pass: t3 });
  log(`  [3] doc.blockedFor=${JSON.stringify(afterVal)} (expect null/undefined): ${t3 ? 'PASS' : 'FAIL'}`);

  recordTest(group, results);
}

async function testB() {
  const group = 'B';
  const results = [];
  log(`\n--- ${group}. Round-trip value → null → value ---`);

  await sendOps([
    {
      entity_id: 300,
      creates_slot: true,
      ops: [
        { op: 'alive' },
        { op: 'set', field: 'nsfwLevel', value: 4 },
        { op: 'set', field: 'blockedFor', value: 'TOS' },
      ],
    },
  ]);
  await sleep(200);

  await sendOps([
    {
      entity_id: 300,
      creates_slot: false,
      ops: [
        { op: 'remove', field: 'blockedFor', value: 'TOS' },
        { op: 'set', field: 'blockedFor', value: null },
      ],
    },
  ]);
  await sleep(200);

  await sendOps([
    {
      entity_id: 300,
      creates_slot: false,
      ops: [
        { op: 'set', field: 'blockedFor', value: 'TOS' },
      ],
    },
  ]);
  await sleep(400);

  const isNotNullQ = await freshQuery([{ IsNotNull: 'blockedFor' }]);
  const t1 = Array.isArray(isNotNullQ?.ids) && isNotNullQ.ids.includes(300);
  results.push({ name: 'round-trip: IsNotNull contains slot', pass: t1 });
  log(`  [1] IsNotNull ids=${JSON.stringify(isNotNullQ?.ids)}: ${t1 ? 'PASS' : 'FAIL'}`);

  const doc = await getDoc(300);
  const v = getDocField(doc, 'blockedFor');
  const t2 = v === 'TOS';
  results.push({ name: 'round-trip: doc has latest value', pass: t2 });
  log(`  [2] doc.blockedFor=${JSON.stringify(v)} (expect "TOS"): ${t2 ? 'PASS' : 'FAIL'}`);

  recordTest(group, results);
}

async function main() {
  log('=== BitDex E2E: blockedFor null transition ===');
  log(`Server: ${BASE_URL}`);

  try {
    await setup();
    await testA();
    await testB();
  } catch (err) {
    console.error('\nFATAL:', err.message);
    failed++;
  }

  try { await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' }); } catch (_) {}

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);

  if (RESULTS_DIR) {
    const { mkdirSync, writeFileSync } = await import('node:fs');
    const { resolve } = await import('node:path');
    mkdirSync(RESULTS_DIR, { recursive: true });
    writeFileSync(
      resolve(RESULTS_DIR, 'e2e-blockedfor-null-transition.json'),
      JSON.stringify({ passed, failed, groups: groupResults }, null, 2)
    );
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
