#!/usr/bin/env node
// Phase 2 Validation Suite — V2.1 through V2.11
// Run against a local BitDex server with pg-sync feature enabled.

const BASE = process.env.BITDEX_URL || 'http://127.0.0.1:3005';
const INDEX = 'test';

let passed = 0, failed = 0, skipped = 0;

async function req(method, path, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE}${path}`, opts);
  const text = await res.text();
  try { return { status: res.status, body: JSON.parse(text) }; } catch { return { status: res.status, body: text }; }
}

async function postOps(ops, meta) {
  return req('POST', `/api/indexes/${INDEX}/ops`, { ops, meta });
}

async function query(filters, sort, limit = 100) {
  return req('POST', `/api/indexes/${INDEX}/query`, {
    filters: filters || [],
    sort: sort || null,
    limit,
    skip_cache: true,
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function check(name, condition, detail) {
  if (condition) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} — ${detail || 'FAILED'}`); failed++; }
}

// ── V2.1: Single op roundtrip ──
async function v2_1() {
  console.log('\n── V2.1: Single op roundtrip ──');

  // Insert entity 100 with nsfwLevel=16 via ops
  const r = await postOps([
    { entity_id: 100, creates_slot: true, ops: [
      { op: 'set', field: 'nsfwLevel', value: 16 },
      { op: 'set', field: 'reactionCount', value: 42 },
    ]},
  ], { source: 'test', cursor: 1 });
  check('POST /ops accepted', r.status === 200, `status=${r.status} body=${JSON.stringify(r.body)}`);

  // Wait for WAL reader to process
  await sleep(200);

  // Query for nsfwLevel=16
  const q = await query([{ Eq: ['nsfwLevel', { Integer: 16 }] }]);
  check('query finds entity 100', q.body?.ids?.includes(100), `ids=${JSON.stringify(q.body?.ids)}`);
}

// ── V2.2: Multi-value add/remove ──
async function v2_2() {
  console.log('\n── V2.2: Multi-value add/remove ──');

  // Add tagIds to entity 100
  await postOps([
    { entity_id: 100, creates_slot: false, ops: [
      { op: 'add', field: 'tagIds', value: 42 },
      { op: 'add', field: 'tagIds', value: 99 },
    ]},
  ]);
  await sleep(200);

  const q1 = await query([{ In: ['tagIds', [{ Integer: 42 }]] }]);
  check('tagIds=42 matches entity 100', q1.body?.ids?.includes(100), `ids=${JSON.stringify(q1.body?.ids)}`);

  // Remove tagId 42
  await postOps([
    { entity_id: 100, creates_slot: false, ops: [
      { op: 'remove', field: 'tagIds', value: 42 },
    ]},
  ]);
  await sleep(200);

  const q2 = await query([{ In: ['tagIds', [{ Integer: 42 }]] }]);
  check('tagIds=42 no longer matches after remove', !q2.body?.ids?.includes(100), `ids=${JSON.stringify(q2.body?.ids)}`);

  const q3 = await query([{ In: ['tagIds', [{ Integer: 99 }]] }]);
  check('tagIds=99 still matches', q3.body?.ids?.includes(100), `ids=${JSON.stringify(q3.body?.ids)}`);
}

// ── V2.3: Delete (clean delete) ──
async function v2_3() {
  console.log('\n── V2.3: Delete (clean delete) ──');

  // Create entity 200
  await postOps([
    { entity_id: 200, creates_slot: true, ops: [
      { op: 'set', field: 'nsfwLevel', value: 8 },
      { op: 'set', field: 'userId', value: 555 },
    ]},
  ]);
  await sleep(200);

  const q1 = await query([{ Eq: ['nsfwLevel', { Integer: 8 }] }]);
  check('entity 200 queryable before delete', q1.body?.ids?.includes(200), `ids=${JSON.stringify(q1.body?.ids)}`);

  // Delete entity 200
  await postOps([
    { entity_id: 200, creates_slot: false, ops: [{ op: 'delete' }] },
  ]);
  await sleep(200);

  const q2 = await query([{ Eq: ['nsfwLevel', { Integer: 8 }] }]);
  check('entity 200 gone after delete', !q2.body?.ids?.includes(200), `ids=${JSON.stringify(q2.body?.ids)}`);
}

// ── V2.4: queryOpSet fan-out ──
async function v2_4() {
  console.log('\n── V2.4: queryOpSet fan-out ──');

  // Create 5 entities with userId=777
  for (let i = 301; i <= 305; i++) {
    await postOps([
      { entity_id: i, creates_slot: true, ops: [
        { op: 'set', field: 'userId', value: 777 },
        { op: 'set', field: 'nsfwLevel', value: 1 },
      ]},
    ]);
  }
  await sleep(300);

  // queryOpSet: update all userId=777 slots
  await postOps([
    { entity_id: 0, creates_slot: false, ops: [
      { op: 'queryOpSet', query: 'userId eq 777', ops: [
        { op: 'remove', field: 'nsfwLevel', value: 1 },
        { op: 'set', field: 'nsfwLevel', value: 32 },
      ]},
    ]},
  ]);
  await sleep(300);

  const q = await query([{ Eq: ['nsfwLevel', { Integer: 32 }] }]);
  const matchCount = q.body?.ids?.filter(id => id >= 301 && id <= 305).length || 0;
  check(`queryOpSet updated ${matchCount}/5 slots`, matchCount === 5, `matched=${matchCount}`);
}

// ── V2.5: Deferred alive via ops ──
async function v2_5() {
  console.log('\n── V2.5: Deferred alive via ops ──');

  // Create entity with future publishedAt (year 2050)
  const futureTs = 2524608000;
  await postOps([
    { entity_id: 400, creates_slot: true, ops: [
      { op: 'set', field: 'nsfwLevel', value: 1 },
      { op: 'set', field: 'publishedAt', value: futureTs },
    ]},
  ]);
  await sleep(200);

  // Should NOT be queryable (deferred)
  const q = await query([{ Eq: ['nsfwLevel', { Integer: 1 }] }]);
  check('future publishedAt entity NOT queryable', !q.body?.ids?.includes(400), `ids=${JSON.stringify(q.body?.ids?.filter(id => id === 400))}`);
}

// ── V2.6: WAL cursor restart — tested via unit tests (cursor persistence) ──
async function v2_6() {
  console.log('\n── V2.6: WAL cursor restart ──');
  console.log('  ⏭️  Verified via unit test (test_cursor_persistence) — server restart test would require process kill/restart');
  skipped++;
}

// ── V2.7: PUT → WAL — SKIP (wal_writer not set on server startup yet) ──
async function v2_7() {
  console.log('\n── V2.7: PUT → WAL ──');
  console.log('  ⏭️  WAL writer not wired into server startup yet — set_wal_writer() needs to be called. Verified via unit tests (test_document_to_ops_*).');
  skipped++;
}

// ── V2.8: Op dedup (LIFO) ──
async function v2_8() {
  console.log('\n── V2.8: Op dedup (LIFO) ──');

  // Send duplicate entity ops in same batch — last should win
  await postOps([
    { entity_id: 500, creates_slot: true, ops: [
      { op: 'set', field: 'nsfwLevel', value: 1 },
    ]},
    { entity_id: 500, creates_slot: true, ops: [
      { op: 'set', field: 'nsfwLevel', value: 32 },
    ]},
  ]);
  await sleep(200);

  const q1 = await query([{ Eq: ['nsfwLevel', { Integer: 32 }] }]);
  const q2 = await query([{ Eq: ['nsfwLevel', { Integer: 1 }] }]);
  check('LIFO dedup: last op wins (nsfwLevel=32)', q1.body?.ids?.includes(500), `ids32=${JSON.stringify(q1.body?.ids)}`);
  check('LIFO dedup: first op overwritten (nsfwLevel≠1)', !q2.body?.ids?.includes(500), `ids1=${JSON.stringify(q2.body?.ids)}`);
}

// ── V2.9: Non-alive slot ops dropped ──
async function v2_9() {
  console.log('\n── V2.9: Non-alive slot ops dropped ──');

  // Send ops for a non-existent slot (never created)
  await postOps([
    { entity_id: 999999, creates_slot: false, ops: [
      { op: 'set', field: 'nsfwLevel', value: 4 },
    ]},
  ]);
  await sleep(200);

  const q = await query([{ Eq: ['nsfwLevel', { Integer: 4 }] }]);
  check('non-alive slot 999999 not in results', !q.body?.ids?.includes(999999), `ids=${JSON.stringify(q.body?.ids)}`);
}

// ── V2.10: Delete reads stored doc ──
async function v2_10() {
  console.log('\n── V2.10: Delete reads stored doc ──');

  // Create entity 600 with specific filter values
  await postOps([
    { entity_id: 600, creates_slot: true, ops: [
      { op: 'set', field: 'nsfwLevel', value: 4 },
      { op: 'set', field: 'userId', value: 888 },
    ]},
  ]);
  await sleep(200);

  // Verify it's queryable
  const q1 = await query([{ Eq: ['userId', { Integer: 888 }] }]);
  check('entity 600 queryable by userId', q1.body?.ids?.includes(600), `ids=${JSON.stringify(q1.body?.ids)}`);

  // Delete it
  await postOps([
    { entity_id: 600, creates_slot: false, ops: [{ op: 'delete' }] },
  ]);
  await sleep(200);

  // Both filter bitmaps should be cleared (clean delete)
  const q2 = await query([{ Eq: ['userId', { Integer: 888 }] }]);
  const q3 = await query([{ Eq: ['nsfwLevel', { Integer: 4 }] }]);
  check('userId=888 cleared after delete', !q2.body?.ids?.includes(600), `ids=${JSON.stringify(q2.body?.ids)}`);
  check('nsfwLevel=4 cleared after delete', !q3.body?.ids?.includes(600), `ids=${JSON.stringify(q3.body?.ids)}`);
}

// ── V2.11: Prometheus sync-lag endpoint ──
async function v2_11() {
  console.log('\n── V2.11: Prometheus sync-lag endpoint ──');

  const r = await req('GET', '/api/internal/sync-lag');
  check('sync-lag endpoint returns 200', r.status === 200, `status=${r.status}`);
  check('sync-lag has sources array', Array.isArray(r.body?.sources), `body=${JSON.stringify(r.body)}`);
}

// ── Run all ──
async function main() {
  console.log(`Phase 2 Validation — ${BASE}/api/indexes/${INDEX}`);

  await v2_1();
  await v2_2();
  await v2_3();
  await v2_4();
  await v2_5();
  await v2_6();
  await v2_7();
  await v2_8();
  await v2_9();
  await v2_10();
  await v2_11();

  console.log(`\n════════════════════════════`);
  console.log(`Results: ${passed} passed, ${failed} failed, ${skipped} skipped`);
  console.log(`════════════════════════════`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => { console.error(e); process.exit(1); });
