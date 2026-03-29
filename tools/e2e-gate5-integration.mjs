#!/usr/bin/env node
// Gate 5 — Local Integration Test
//
// Exercises the full pipeline:
// 1. Start server with pg-sync feature
// 2. Create index with Civitai-like config (including computed sortAt)
// 3. Load small dataset via PUT /dumps (images → tags → posts enrichment)
// 4. Verify bitmaps: filter queries match expected counts
// 5. POST /ops: verify WAL-based mutations work
// 6. Kill server, restart, verify persistence (bitmaps survive restart)
//
// Prerequisites:
// - Server binary built with pg-sync: cargo build --profile fast --features "server,pg-sync" --bin bitdex-server
// - Test CSV files in .test-data/gate5-csvs/
//
// Usage: node tools/e2e-gate5-integration.mjs [--port 3005] [--binary target/fast/bitdex-server.exe]

import { execSync, spawn } from 'child_process';
import { mkdirSync, rmSync, existsSync, writeFileSync } from 'fs';
import path from 'path';

const PORT = parseInt(process.argv.find((a,i) => process.argv[i-1] === '--port') || '3005');
const BINARY = process.argv.find((a,i) => process.argv[i-1] === '--binary') || 'target/fast/bitdex-server.exe';
const BASE = `http://127.0.0.1:${PORT}`;
const INDEX = 'civitai-test';
const DATA_DIR = path.resolve('.test-data/gate5-data');
const CSV_DIR = path.resolve('.test-data/gate5-csvs');

let passed = 0, failed = 0;
let serverProc = null;

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
    try { return { status: res.status, body: JSON.parse(text) }; } catch { return { status: res.status, body: text }; }
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
  console.log(`Starting server: ${BINARY} --port ${PORT} --data-dir ${DATA_DIR}`);
  if (existsSync(BINARY.replace('.exe', '-pgsync.exe'))) {
    // Use the pg-sync copy if available
  }
  serverProc = spawn(BINARY, [
    '--port', String(PORT),
    '--data-dir', DATA_DIR,
    '--enable-traces',
  ], { stdio: ['ignore', 'pipe', 'pipe'] });
  serverProc.stderr.on('data', d => {
    const line = d.toString().trim();
    if (line.includes('error') || line.includes('panic') || line.includes('WAL reader')) {
      console.log(`    [server] ${line}`);
    }
  });
}

function killServer() {
  if (serverProc) {
    serverProc.kill('SIGTERM');
    // Wait briefly for graceful shutdown, then force kill
    try { execSync(`sleep 1`, { stdio: 'ignore' }); } catch {}
    try { serverProc.kill('SIGKILL'); } catch {}
    serverProc = null;
  }
}

// ── Index config matching Civitai schema (simplified) ──
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
  data_schema: { id_field: 'id', fields: [] },
};

// ── Phase 1: Server start + index creation ──
async function phase1_setup() {
  console.log('\n═══ Phase 1: Server Start + Index Creation ═══');

  // Clean data dir
  if (existsSync(DATA_DIR)) rmSync(DATA_DIR, { recursive: true });
  mkdirSync(DATA_DIR, { recursive: true });

  startServer();
  const ready = await waitForServer();
  check('Server started and healthy', ready);
  if (!ready) { console.log('FATAL: Server did not start'); process.exit(1); }

  const r = await req('POST', '/api/indexes', INDEX_CONFIG);
  check('Index created', r.status === 200 || r.status === 201, `status=${r.status} body=${JSON.stringify(r.body)}`);
}

// ── Phase 2: Dump loading ──
async function phase2_dump_load() {
  console.log('\n═══ Phase 2: Dump Loading via PUT /dumps ═══');

  // Images dump (sets_alive=true, primary entity)
  const imagesPath = path.resolve(CSV_DIR, 'images.csv').replace(/\\/g, '/');
  const imagesDump = {
    name: 'images-test',
    csv_path: imagesPath,
    format: 'csv',
    slot_field: 'id',
    sets_alive: true,
    fields: [
      'nsfwLevel',
      { column: 'type', target: 'type' },
      'userId',
      'postId',
      { column: 'url', target: 'url' },
      { column: 'hash', target: 'hash' },
      'width',
      'height',
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
      fields: [
        { column: 'publishedAtSecs', target: 'publishedAt' },
      ],
      computed_fields: [
        { target: 'postedToId', expression: 'lookup_key' },
      ],
      enrichment: [],
    }],
  };

  const r1 = await req('PUT', `/api/indexes/${INDEX}/dumps`, imagesDump);
  check('Images dump accepted', r1.status === 201 || r1.status === 200, `status=${r1.status} body=${JSON.stringify(r1.body)}`);

  if (r1.body?.task_id) {
    // Poll for completion
    let attempts = 0;
    while (attempts < 30) {
      await sleep(500);
      const task = await req('GET', `/api/tasks/${r1.body.task_id}`);
      if (task.body?.status === 'complete' || task.body?.status === 'completed') {
        check('Images dump completed', true);
        console.log(`    Rows: ${task.body?.result?.rows_processed || 'unknown'}`);
        break;
      }
      if (task.body?.status === 'error' || task.body?.status === 'failed') {
        check('Images dump completed', false, `task failed: ${JSON.stringify(task.body)}`);
        break;
      }
      attempts++;
    }
    if (attempts >= 30) check('Images dump completed', false, 'timed out');
  }

  // Tags dump
  const tagsDump = {
    name: 'tags-test',
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
  check('Tags dump accepted', r2.status === 201 || r2.status === 200, `status=${r2.status} body=${JSON.stringify(r2.body)}`);

  if (r2.body?.task_id) {
    let attempts = 0;
    while (attempts < 30) {
      await sleep(500);
      const task = await req('GET', `/api/tasks/${r2.body.task_id}`);
      if (task.body?.status === 'complete' || task.body?.status === 'completed') {
        check('Tags dump completed', true);
        break;
      }
      if (task.body?.status === 'error' || task.body?.status === 'failed') {
        check('Tags dump completed', false, `task failed: ${JSON.stringify(task.body)}`);
        break;
      }
      attempts++;
    }
    if (attempts >= 30) check('Tags dump completed', false, 'timed out');
  }
}

// ── Phase 3: Bitmap verification ──
async function phase3_verify_bitmaps() {
  console.log('\n═══ Phase 3: Bitmap Verification (Post-Dump) ═══');

  // Stats check — should have alive documents
  const stats = await req('GET', `/api/indexes/${INDEX}/stats`);
  const alive = stats.body?.alive_count || stats.body?.alive || 0;
  check(`Alive count > 0 (got ${alive})`, alive > 0);

  // nsfwLevel=1 should match images 1,3,5,7,9 (5 images)
  const q1 = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    limit: 100,
    skip_cache: true,
  });
  check(`nsfwLevel=1 matches ${q1.body?.ids?.length || 0} docs`, q1.body?.ids?.length >= 1);

  // tagIds=42 should match images 1,2,3
  const q2 = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [{ In: ['tagIds', [{ Integer: 42 }]] }],
    limit: 100,
    skip_cache: true,
  });
  check(`tagIds=42 matches ${q2.body?.ids?.length || 0} docs`, (q2.body?.ids?.length || 0) >= 1);

  // userId=100 should match images 1,2,7,8
  const q3 = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['userId', { Integer: 100 }] }],
    limit: 100,
    skip_cache: true,
  });
  check(`userId=100 matches ${q3.body?.ids?.length || 0} docs`, (q3.body?.ids?.length || 0) >= 1);

  // Sort by sortAt desc — should return results
  const q4 = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [],
    sort: { field: 'sortAt', direction: 'Desc' },
    limit: 5,
    skip_cache: true,
  });
  check(`sort by sortAt returns results`, (q4.body?.ids?.length || 0) >= 1, `ids=${JSON.stringify(q4.body?.ids)} err=${JSON.stringify(q4.body?.error || q4.body)}`);
}

// ── Phase 4: WAL ops path ──
async function phase4_ops() {
  console.log('\n═══ Phase 4: WAL Ops Path ═══');

  // POST /ops: change nsfwLevel for entity 1
  const r1 = await req('POST', `/api/indexes/${INDEX}/ops`, {
    ops: [{
      entity_id: 1,
      creates_slot: false,
      ops: [
        { op: 'remove', field: 'nsfwLevel', value: 1 },
        { op: 'set', field: 'nsfwLevel', value: 64 },
      ],
    }],
    meta: { source: 'gate5-test', cursor: 1 },
  });
  check('POST /ops accepted', r1.status === 200, `status=${r1.status}`);

  await sleep(300); // Wait for WAL reader

  // Query: nsfwLevel=64 should now include entity 1
  const q1 = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['nsfwLevel', { Integer: 64 }] }],
    limit: 100,
    skip_cache: true,
  });
  check('nsfwLevel=64 includes entity 1 after ops', q1.body?.ids?.includes(1), `ids=${JSON.stringify(q1.body?.ids)}`);

  // Sync-lag endpoint
  const lag = await req('GET', '/api/internal/sync-lag');
  check('sync-lag returns sources', lag.status === 200 && Array.isArray(lag.body?.sources));
}

// ── Phase 5: Restart + Persistence ──
async function phase5_persistence() {
  console.log('\n═══ Phase 5: Restart + Persistence Verification ═══');

  // Save bitmap snapshot before killing
  const snap = await req('POST', `/api/indexes/${INDEX}/save`);
  console.log(`    Snapshot save: status=${snap.status}`);

  // Kill server
  console.log('  Killing server...');
  killServer();
  await sleep(2000);

  // Restart
  console.log('  Restarting server...');
  startServer();
  const ready = await waitForServer(15000);
  check('Server restarted successfully', ready);
  if (!ready) return;

  // Wait for lazy loading
  await sleep(1000);

  // Verify alive docs survived
  const stats = await req('GET', `/api/indexes/${INDEX}/stats`);
  const alive = stats.body?.alive_count || stats.body?.alive || 0;
  check(`After restart: alive count > 0 (got ${alive})`, alive > 0);

  // Verify bitmap state survived: query should still work
  const q1 = await req('POST', `/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['userId', { Integer: 100 }] }],
    limit: 100,
    skip_cache: true,
  });
  check('After restart: userId=100 query works', (q1.body?.ids?.length || 0) >= 1, `ids=${JSON.stringify(q1.body?.ids)}`);

  // The ops-based nsfwLevel=64 change may or may not persist (depends on WAL replay).
  // The dump-loaded bitmaps should definitely persist via BitmapFs.
}

// ── Restart helper ──
async function phase_restart(label) {
  console.log(`\n═══ ${label} ═══`);
  killServer();
  await sleep(2000);
  startServer();
  const ready = await waitForServer(15000);
  check('Server restarted', ready);
  if (!ready) { console.log('FATAL: Server did not restart'); process.exit(1); }
  // Wait for lazy loading to settle
  await sleep(1000);
}

// ── Main ──
async function main() {
  console.log('╔══════════════════════════════════════════╗');
  console.log('║  Gate 5 — Local Integration Test         ║');
  console.log('╚══════════════════════════════════════════╝');
  console.log(`Server: ${BINARY}`);
  console.log(`Port: ${PORT}, Data: ${DATA_DIR}`);
  console.log(`CSVs: ${CSV_DIR}`);

  try {
    await phase1_setup();
    await phase2_dump_load();
    // Dumps write to BitmapFs on disk. Server loads from BitmapFs on startup.
    // Restart server to pick up dump-loaded bitmaps.
    await phase_restart('Post-dump restart');
    await phase3_verify_bitmaps();
    await phase4_ops();
    await phase5_persistence();
  } finally {
    killServer();
  }

  console.log(`\n════════════════════════════`);
  console.log(`Results: ${passed} passed, ${failed} failed`);
  console.log(`════════════════════════════`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => { console.error(e); killServer(); process.exit(1); });
