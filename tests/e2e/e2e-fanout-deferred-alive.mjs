#!/usr/bin/env node
/**
 * E2E: queryOpSet fan-out honors deferred_alive
 *
 * Repros the scheduled-post leak: a Post created with publishedAt in the
 * future fans out a queryOpSet to all images with that postId, immediately
 * setting publishedAt sort layers and flipping the isPublished shadow on
 * already-alive image slots. The fix routes deferred fan-out through the
 * deferred map: doc gets future field values, bitmap stays at prior state,
 * activate_due replays at activation time.
 *
 * Test groups:
 *   A. Pre-existing image, future-scheduled Post fan-out — invisible until activation
 *   B. Past-scheduled Post fan-out — immediately visible
 *
 * Usage:
 *   node tests/e2e/e2e-fanout-deferred-alive.mjs [--url http://localhost:3001] [--verbose]
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

const INDEX = 'fanout-deferred-test';

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

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const query = (filters, limit = 100) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, limit }).then(r => r.data);
const sendOps = (ops) =>
  apiPost(`/api/indexes/${INDEX}/ops`, { ops }).then(r => r);
const clearCache = () =>
  apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function nowUnix() { return Math.floor(Date.now() / 1000); }

async function freshQuery(filters, limit = 100) {
  await clearCache();
  return query(filters, limit);
}

async function setup() {
  log('\n--- Setup: queryOpSet deferred fan-out test index ---');
  try { await fetch(`${BASE_URL}/api/indexes/${INDEX}`, { method: 'DELETE' }); } catch (_) {}
  await sleep(300);

  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'postId', field_type: 'single_value' },
        { name: 'isPublished', field_type: 'boolean' },
      ],
      sort_fields: [
        { name: 'publishedAt', bits: 32 },
      ],
      deferred_alive: { source_field: 'publishedAt' },
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'postId', target: 'postId', value_type: 'integer' },
        { source: 'publishedAt', target: 'publishedAt', value_type: 'integer' },
        { source: 'publishedAt', target: 'isPublished', value_type: 'exists_boolean' },
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

async function testA() {
  const group = 'A';
  const results = [];
  log(`\n--- ${group}. Pre-existing image, future Post fan-out ---`);

  // Step 1: insert image slot 5001 with postId=999, no publishedAt yet
  await sendOps([
    {
      entity_id: 5001,
      creates_slot: true,
      ops: [
        { op: 'alive' },
        { op: 'set', field: 'postId', value: 999 },
      ],
    },
  ]);
  await sleep(300);

  const sBefore = await stats();
  const t0 = sBefore?.alive_count >= 1;
  results.push({ name: 'image alive after initial insert', pass: t0 });
  log(`  [0] alive_count=${sBefore?.alive_count}: ${t0 ? 'PASS' : 'FAIL'}`);

  // Step 2: simulate Post 999 created with publishedAt=now+600 → fan-out queryOpSet
  const futureTs = nowUnix() + 600;
  await sendOps([
    {
      entity_id: 999,  // Post.id, not image slot
      creates_slot: false,
      ops: [
        {
          op: 'queryOpSet',
          query: 'postId eq 999',
          ops: [
            { op: 'set', field: 'publishedAt', value: futureTs },
          ],
        },
      ],
    },
  ]);
  await sleep(500);

  // Image slot 5001 must NOT be visible to isPublished=true queries
  const pubQ = await freshQuery([{ Eq: ['isPublished', { Bool: true }] }]);
  const t1 = Array.isArray(pubQ?.ids) && !pubQ.ids.includes(5001);
  results.push({ name: 'image not in isPublished=true before activation', pass: t1 });
  log(`  [1] isPublished=true ids=${JSON.stringify(pubQ?.ids)}: ${t1 ? 'PASS' : 'FAIL'}`);

  // postId=999 query should still find the image (slot is alive, postId set)
  const pidQ = await freshQuery([{ Eq: ['postId', { Integer: 999 }] }]);
  const t2 = Array.isArray(pidQ?.ids) && pidQ.ids.includes(5001);
  results.push({ name: 'image still found by postId during defer window', pass: t2 });
  log(`  [2] postId=999 ids=${JSON.stringify(pidQ?.ids)}: ${t2 ? 'PASS' : 'FAIL'}`);

  recordTest(group, results);
}

async function testB() {
  const group = 'B';
  const results = [];
  log(`\n--- ${group}. Past-scheduled Post fan-out — immediately visible ---`);

  await sendOps([
    {
      entity_id: 6001,
      creates_slot: true,
      ops: [
        { op: 'alive' },
        { op: 'set', field: 'postId', value: 1000 },
      ],
    },
  ]);
  await sleep(300);

  const pastTs = nowUnix() - 3600;
  await sendOps([
    {
      entity_id: 1000,
      creates_slot: false,
      ops: [
        {
          op: 'queryOpSet',
          query: 'postId eq 1000',
          ops: [
            { op: 'set', field: 'publishedAt', value: pastTs },
          ],
        },
      ],
    },
  ]);
  await sleep(500);

  const pubQ = await freshQuery([{ Eq: ['isPublished', { Bool: true }] }]);
  const t1 = Array.isArray(pubQ?.ids) && pubQ.ids.includes(6001);
  results.push({ name: 'past-scheduled image immediately isPublished=true', pass: t1 });
  log(`  [1] isPublished=true ids=${JSON.stringify(pubQ?.ids)}: ${t1 ? 'PASS' : 'FAIL'}`);

  recordTest(group, results);
}

async function testC() {
  const group = 'C';
  const results = [];
  log(`\n--- ${group}. Activation-time visibility (3s wait) ---`);

  await sendOps([
    {
      entity_id: 7001,
      creates_slot: true,
      ops: [
        { op: 'alive' },
        { op: 'set', field: 'postId', value: 2000 },
      ],
    },
  ]);
  await sleep(200);

  const activateAt = nowUnix() + 3;
  await sendOps([
    {
      entity_id: 2000,
      creates_slot: false,
      ops: [
        {
          op: 'queryOpSet',
          query: 'postId eq 2000',
          ops: [
            { op: 'set', field: 'publishedAt', value: activateAt },
          ],
        },
      ],
    },
  ]);
  await sleep(500);

  const before = await freshQuery([{ Eq: ['isPublished', { Bool: true }] }]);
  const t1 = Array.isArray(before?.ids) && !before.ids.includes(7001);
  results.push({ name: 'before activation: not isPublished', pass: t1 });
  log(`  [1] before ids=${JSON.stringify(before?.ids)}: ${t1 ? 'PASS' : 'FAIL'}`);

  log('  [2] waiting 4s for activation...');
  await sleep(4000);

  const after = await freshQuery([{ Eq: ['isPublished', { Bool: true }] }]);
  const t2 = Array.isArray(after?.ids) && after.ids.includes(7001);
  results.push({ name: 'after activation: isPublished=true', pass: t2 });
  log(`  [2] after ids=${JSON.stringify(after?.ids)}: ${t2 ? 'PASS' : 'FAIL'}`);

  recordTest(group, results);
}

async function main() {
  log('=== BitDex E2E: queryOpSet deferred fan-out ===');
  log(`Server: ${BASE_URL}`);

  try {
    await setup();
    await testA();
    await testB();
    await testC();
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
      resolve(RESULTS_DIR, 'e2e-fanout-deferred-alive.json'),
      JSON.stringify({ passed, failed, groups: groupResults }, null, 2)
    );
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
