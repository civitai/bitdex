#!/usr/bin/env node
// Data Silo Validation — E2E Test Script
//
// Validates V5a-V5d from docs/design/data-silo-implementation-plan.md:
//   V5a: Dump correctness — silo files created, correct sizes, document counts
//   V5b: Document field correctness — fields readable via GET /documents/{slot}
//   V5c: Query correctness — filter/sort queries return expected results
//   V5d: Performance — read latency within bounds
//
// Uses deploy/configs/civitai-index.json for full data_schema (NOT empty fields).
//
// Prerequisites:
//   cargo build --profile fast --features "server,pg-sync" --bin bitdex-server
//   Test CSVs in .test-data/gate5-csvs/ (same as gate5 integration test)
//
// Usage: node tools/e2e-data-silo-validation.mjs [--port 3006] [--binary target/fast/bitdex-server.exe]

import { spawn } from 'child_process';
import { mkdirSync, rmSync, existsSync, readFileSync, readdirSync, statSync } from 'fs';
import path from 'path';

const PORT = parseInt(process.argv.find((a,i) => process.argv[i-1] === '--port') || '3006');
const BINARY = process.argv.find((a,i) => process.argv[i-1] === '--binary') || 'target/fast/bitdex-server.exe';
const BASE = `http://127.0.0.1:${PORT}`;
const INDEX = 'civitai-silo';
const DATA_DIR = path.resolve('.test-data/silo-validation-data');
const CSV_DIR = path.resolve('.test-data/gate5-csvs');

// Load the FULL production index config (critical: includes data_schema with field types)
const CIVITAI_CONFIG = JSON.parse(readFileSync('deploy/configs/civitai-index.json', 'utf-8'));
CIVITAI_CONFIG.name = INDEX;

let passed = 0, failed = 0, skipped = 0;
let serverProc = null;

function check(name, condition, detail) {
  if (condition) { console.log(`  \u2705 ${name}`); passed++; }
  else { console.log(`  \u274c ${name} \u2014 ${detail || 'FAILED'}`); failed++; }
}
function skip(name, reason) { console.log(`  \u23ed ${name} \u2014 ${reason}`); skipped++; }

async function req(method, urlPath, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  try {
    const res = await fetch(`${BASE}${urlPath}`, opts);
    const text = await res.text();
    try { return { status: res.status, body: JSON.parse(text) }; } catch { return { status: res.status, body: text }; }
  } catch (e) {
    return { status: 0, body: e.message };
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForServer(maxMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    try { const res = await fetch(`${BASE}/api/indexes`); if (res.ok) return true; } catch {}
    await sleep(500);
  }
  return false;
}

function startServer() {
  console.log(`Starting server: ${BINARY} --port ${PORT} --data-dir ${DATA_DIR}`);
  serverProc = spawn(BINARY, [
    '--port', String(PORT), '--data-dir', DATA_DIR, '--enable-traces',
  ], { stdio: ['ignore', 'pipe', 'pipe'] });
  serverProc.stderr.on('data', d => {
    const line = d.toString().trim();
    if (line.includes('error') || line.includes('panic') || line.includes('silo')) {
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

async function waitForDump(taskId, label, maxAttempts = 60) {
  let attempts = 0;
  while (attempts < maxAttempts) {
    await sleep(500);
    const task = await req('GET', `/api/tasks/${taskId}`);
    if (task.body?.status === 'complete' || task.body?.status === 'completed') {
      check(`${label} completed`, true);
      return true;
    }
    if (task.body?.status === 'error' || task.body?.status === 'failed') {
      check(`${label} completed`, false, `task failed: ${JSON.stringify(task.body)}`);
      return false;
    }
    attempts++;
  }
  check(`${label} completed`, false, 'timed out');
  return false;
}

// ══════════════════════════════════════════════════════════════
// V5a: Dump Correctness
// ══════════════════════════════════════════════════════════════
async function v5a_dump_correctness() {
  console.log('\n\u2550\u2550\u2550 V5a: Dump Correctness \u2550\u2550\u2550');

  // Start server + create index with FULL civitai config
  if (existsSync(DATA_DIR)) rmSync(DATA_DIR, { recursive: true });
  mkdirSync(DATA_DIR, { recursive: true });

  startServer();
  const ready = await waitForServer();
  check('Server started', ready);
  if (!ready) { console.log('FATAL: Server did not start'); process.exit(1); }

  const cr = await req('POST', '/api/indexes', CIVITAI_CONFIG);
  check('Index created with full civitai config', cr.status === 200 || cr.status === 201,
    `status=${cr.status} body=${JSON.stringify(cr.body).slice(0, 200)}`);

  // Images dump
  const imagesPath = path.resolve(CSV_DIR, 'images.csv').replace(/\\/g, '/');
  if (!existsSync(imagesPath)) {
    skip('Images dump', `CSV not found: ${imagesPath}`);
    return false;
  }

  const imagesDump = {
    name: 'images-silo',
    csv_path: imagesPath,
    format: 'csv',
    slot_field: 'id',
    sets_alive: true,
    fields: [
      'nsfwLevel', { column: 'type', target: 'type' }, 'userId', 'postId',
      { column: 'url', target: 'url' }, { column: 'hash', target: 'hash' },
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
      key: 'id', join_on: 'postId',
      fields: [{ column: 'publishedAtSecs', target: 'publishedAt' }],
      computed_fields: [{ target: 'postedToId', expression: 'lookup_key' }],
      enrichment: [],
    }],
  };

  const r1 = await req('PUT', `/api/indexes/${INDEX}/dumps`, imagesDump);
  check('Images dump accepted', r1.status === 201 || r1.status === 200,
    `status=${r1.status} body=${JSON.stringify(r1.body).slice(0, 200)}`);
  if (r1.body?.task_id) await waitForDump(r1.body.task_id, 'Images dump');

  // Check alive count
  const stats = await req('GET', `/api/indexes/${INDEX}/stats`);
  const alive = stats.body?.alive_count || stats.body?.alive || 0;
  check(`Alive count > 0 (got ${alive})`, alive > 0);

  // V5a.5: Check silo files exist
  const siloDir = path.join(DATA_DIR, 'indexes', INDEX, 'silos', 'images-silo');
  if (existsSync(siloDir)) {
    const siloFiles = readdirSync(siloDir).filter(f => f.endsWith('.dat'));
    check(`Silo files created (${siloFiles.length} files)`, siloFiles.length > 0);

    // Check file sizes are non-zero
    let totalSize = 0;
    for (const f of siloFiles) {
      totalSize += statSync(path.join(siloDir, f)).size;
    }
    check(`Silo data non-empty (${(totalSize / 1024).toFixed(1)} KB total)`, totalSize > 0);

    // Check doc_index.bin
    const indexPath = path.join(siloDir, 'doc_index.bin');
    check('doc_index.bin exists', existsSync(indexPath));
    if (existsSync(indexPath)) {
      const indexSize = statSync(indexPath).size;
      check(`doc_index.bin non-empty (${indexSize} bytes)`, indexSize > 100);
    }
  } else {
    check('Silo directory exists', false, `not found: ${siloDir}`);
  }

  return true;
}

// ══════════════════════════════════════════════════════════════
// V5b: Document Field Correctness
// ══════════════════════════════════════════════════════════════
async function v5b_document_fields() {
  console.log('\n\u2550\u2550\u2550 V5b: Document Field Correctness \u2550\u2550\u2550');

  // Get the first few alive IDs via a simple query
  const q = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [], sort: { field: 'id', direction: 'Asc' }, limit: 5, skip_cache: true,
  });

  if (!q.body?.ids?.length) {
    skip('Document field checks', 'no query results');
    return;
  }

  const testIds = q.body.ids.slice(0, 3);
  console.log(`  Checking documents for IDs: ${testIds.join(', ')}`);

  for (const id of testIds) {
    const doc = await req('GET', `/api/indexes/${INDEX}/documents/${id}`);
    if (doc.status !== 200 || !doc.body) {
      check(`GET /documents/${id}`, false, `status=${doc.status}`);
      continue;
    }

    const fields = doc.body.fields || doc.body;

    // V5b.2: Core filter fields present
    check(`Doc ${id}: has nsfwLevel`, fields.nsfwLevel !== undefined);
    check(`Doc ${id}: has userId`, fields.userId !== undefined);

    // V5b.1: doc-only fields if silo read path is active
    if (fields.url !== undefined) {
      check(`Doc ${id}: url is string`, typeof fields.url === 'string' || typeof fields.url?.String === 'string');
    }
    if (fields.width !== undefined) {
      check(`Doc ${id}: width is number`, typeof fields.width === 'number' || fields.width?.Integer !== undefined);
    }
  }
}

// ══════════════════════════════════════════════════════════════
// V5c: Query Correctness
// ══════════════════════════════════════════════════════════════
async function v5c_query_correctness() {
  console.log('\n\u2550\u2550\u2550 V5c: Query Correctness \u2550\u2550\u2550');

  // nsfwLevel eq 1
  const q1 = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }], limit: 100, skip_cache: true,
  });
  check(`nsfwLevel=1 returns results (${q1.body?.ids?.length || 0})`, (q1.body?.ids?.length || 0) >= 1);

  // userId filter
  const q2 = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['userId', { Integer: 100 }] }], limit: 100, skip_cache: true,
  });
  check(`userId=100 returns results (${q2.body?.ids?.length || 0})`, (q2.body?.ids?.length || 0) >= 1);

  // Sort by id asc
  const q3 = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [], sort: { field: 'id', direction: 'Asc' }, limit: 5, skip_cache: true,
  });
  check('Sort by id asc returns ordered results', (q3.body?.ids?.length || 0) >= 1);
  if (q3.body?.ids?.length >= 2) {
    const sorted = q3.body.ids.every((id, i) => i === 0 || id >= q3.body.ids[i-1]);
    check('IDs are in ascending order', sorted, `ids=${JSON.stringify(q3.body.ids)}`);
  }
}

// ══════════════════════════════════════════════════════════════
// V5d: Performance
// ══════════════════════════════════════════════════════════════
async function v5d_performance() {
  console.log('\n\u2550\u2550\u2550 V5d: Performance \u2550\u2550\u2550');

  // Point read latency: GET /documents/{slot}
  const q = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [], limit: 1, skip_cache: true,
  });
  if (!q.body?.ids?.length) {
    skip('Read latency test', 'no documents');
    return;
  }
  const testId = q.body.ids[0];

  // Warm up
  await req('GET', `/api/indexes/${INDEX}/documents/${testId}`);

  // Measure 10 reads
  const times = [];
  for (let i = 0; i < 10; i++) {
    const start = performance.now();
    await req('GET', `/api/indexes/${INDEX}/documents/${testId}`);
    times.push(performance.now() - start);
  }
  const p50 = times.sort((a,b) => a-b)[4];
  const p99 = times.sort((a,b) => a-b)[9];
  console.log(`  Point read latency: p50=${p50.toFixed(2)}ms, p99=${p99.toFixed(2)}ms`);
  check(`Point read p50 < 10ms (got ${p50.toFixed(2)}ms)`, p50 < 10);

  // Query latency
  const qTimes = [];
  for (let i = 0; i < 10; i++) {
    const start = performance.now();
    await req('POST', `/api/indexes/${INDEX}/query`, {
      filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }], limit: 10, skip_cache: true,
    });
    qTimes.push(performance.now() - start);
  }
  const qP50 = qTimes.sort((a,b) => a-b)[4];
  console.log(`  Query latency: p50=${qP50.toFixed(2)}ms`);
  check(`Query p50 < 50ms (got ${qP50.toFixed(2)}ms)`, qP50 < 50);
}

// ══════════════════════════════════════════════════════════════
// Main
// ══════════════════════════════════════════════════════════════
async function main() {
  console.log('\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557');
  console.log('\u2551  Data Silo Validation (V5a-V5d)          \u2551');
  console.log('\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d');
  console.log(`Server: ${BINARY}`);
  console.log(`Port: ${PORT}, Data: ${DATA_DIR}, CSVs: ${CSV_DIR}`);
  console.log(`Config: deploy/configs/civitai-index.json (full data_schema)`);

  if (!existsSync(CSV_DIR)) {
    console.log(`\nFATAL: CSV directory not found: ${CSV_DIR}`);
    console.log('Generate test CSVs first (see tools/e2e-gate5-integration.mjs)');
    process.exit(1);
  }

  try {
    const dumpOk = await v5a_dump_correctness();
    if (dumpOk) {
      // Restart to pick up silo files
      killServer();
      await sleep(2000);
      startServer();
      const ready = await waitForServer();
      check('Server restarted for read tests', ready);
      if (ready) {
        await sleep(1000); // Let lazy loading settle
        await v5b_document_fields();
        await v5c_query_correctness();
        await v5d_performance();
      }
    }
  } finally {
    killServer();
  }

  console.log(`\n${'='.repeat(40)}`);
  console.log(`Results: ${passed} passed, ${failed} failed, ${skipped} skipped`);
  console.log('='.repeat(40));
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => { console.error(e); killServer(); process.exit(1); });
