#!/usr/bin/env node
/**
 * E2E: scheduled-publish + ingested sortAt (W3 gate, scheduled-publish-design §2.D).
 *
 * Pins the critical cases the sortAt-ingestion + per-image-fan-out change must get right,
 * driving the steady-state /ops path directly (exactly the payloads the #326 PG triggers emit,
 * captured from a live local PG then re-templated with test-controlled timestamps):
 *
 *   A. Published image: ingested sortAtUnix (ms) -> sortAt (seconds) exactly; isPublished true;
 *      model3dId filter populated; visible.
 *   B. Scheduled image (future publishedAt): deferred — NOT visible, isPublished != true.
 *   C. CRITICAL no-op: cross Tf with NO further op — the overdue sweep alone flips isPublished
 *      AND the ingested future sortAt is preserved (sortAt == publishedAt == Tf). This is the
 *      case the engine-computed GREATEST path got wrong (W1-4: 7-28% misordered).
 *   D. Unpublish (publishedAt remove) -> invisible; republish -> visible, sortAt==publishedAt.
 *
 * Self-contained: creates its own index (civitai config + short overdue sweep), no PG required.
 * Server must be built with `--features server,pg-sync`.
 *
 * Usage: node tests/e2e/e2e-scheduled-publish-sortat.mjs [--url http://localhost:3001] [--verbose]
 */
const BASE_URL = process.argv.includes('--url') ? process.argv[process.argv.indexOf('--url') + 1] : 'http://localhost:3001';
const VERBOSE = process.argv.includes('--verbose');
const TOKEN = process.env.BITDEX_ADMIN_TOKEN || 'test123';
const INDEX = 'civitai-w3-sptest';
const H = { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKEN}` };

let passed = 0, failed = 0;
const log = (...a) => console.log(...a);
const vlog = (...a) => { if (VERBOSE) console.log('  [v]', ...a); };
const ok = (cond, msg) => { if (cond) passed++; else failed++; log(`  ${cond ? 'PASS' : 'FAIL'}  ${msg}`); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function post(path, body) { const r = await fetch(`${BASE_URL}${path}`, { method: 'POST', headers: H, body: JSON.stringify(body) }); let d = null; try { d = await r.json(); } catch {} return { status: r.status, data: d }; }
async function del(path) { try { await fetch(`${BASE_URL}${path}`, { method: 'DELETE', headers: H }); } catch {} }
const sendOps = (ops) => post(`/api/indexes/${INDEX}/ops`, { ops });
const query = (filters, extra = {}) => post(`/api/indexes/${INDEX}/query`, { filters, limit: 50, ...extra }).then((r) => r.data);
const doc = (slot, fields) => post(`/api/indexes/${INDEX}/document`, { slot_id: slot, fields }).then((r) => r.data);
const PUB = [{ Eq: ['isPublished', { Bool: true }] }];
const nowS = () => Math.floor(Date.now() / 1000);

// The full per-image op the #326 Image trigger emits (bitdex_image_ops || bitdex_image_sortat_ops),
// parameterized by publish time. publishedAt = SECONDS; sortAtUnix = MILLISECONDS (ms_to_seconds).
function imageOps(id, postId, publishedAtSecs, { model3dId = null, existedAtSecs = nowS() - 100000 } = {}) {
  return {
    entity_id: id, creates_slot: true,
    ops: [
      { op: 'alive' },
      { op: 'set', field: 'nsfwLevel', value: 8 },
      { op: 'set', field: 'type', value: 'image' },
      { op: 'set', field: 'userId', value: 7 },
      { op: 'set', field: 'postId', value: postId },
      { op: 'set', field: 'url', value: 'u' },
      { op: 'set', field: 'hash', value: 'h' },
      { op: 'set', field: 'width', value: 512 },
      { op: 'set', field: 'height', value: 768 },
      { op: 'set', field: 'existedAt', value: existedAtSecs },
      { op: 'set', field: 'publishedAt', value: publishedAtSecs },
      { op: 'set', field: 'availability', value: 'Public' },
      { op: 'set', field: 'postedToId', value: 321 },
      { op: 'set', field: 'model3dId', value: model3dId },
      { op: 'set', field: 'sortAtUnix', value: publishedAtSecs * 1000 },
    ],
  };
}

const CONFIG = {
  name: INDEX,
  config: {
    filter_fields: [
      { name: 'nsfwLevel', field_type: 'single_value', eager_load: true },
      { name: 'type', field_type: 'single_value', eager_load: true },
      { name: 'userId', field_type: 'single_value' },
      { name: 'availability', field_type: 'single_value' },
      { name: 'postId', field_type: 'single_value', per_value_lazy: true },
      { name: 'postedToId', field_type: 'single_value', per_value_lazy: true },
      { name: 'model3dId', field_type: 'single_value', per_value_lazy: true },
      { name: 'isPublished', field_type: 'boolean', eager_load: true },
      { name: 'hasMeta', field_type: 'boolean' },
      { name: 'onSite', field_type: 'boolean' },
      { name: 'poi', field_type: 'boolean' },
      { name: 'minor', field_type: 'boolean' },
      { name: 'blockedFor', field_type: 'single_value' },
    ],
    sort_fields: [
      { name: 'sortAt', bits: 32, eager_load: true },
      { name: 'publishedAt', bits: 32 },
      { name: 'existedAt', bits: 32 },
    ],
    max_page_size: 200,
    deferred_alive: { source_field: 'publishedAt', sweep_interval_secs: 2 },
    flush_interval_us: 200,
  },
  data_schema: {
    id_field: 'id', schema_version: 1,
    fields: [
      { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
      { source: 'userId', target: 'userId', value_type: 'integer' },
      { source: 'type', target: 'type', value_type: 'low_cardinality_string' },
      { source: 'availability', target: 'availability', value_type: 'low_cardinality_string', nullable: true },
      { source: 'postId', target: 'postId', value_type: 'integer', nullable: true },
      { source: 'postedToId', target: 'postedToId', value_type: 'integer', nullable: true },
      { source: 'model3dId', target: 'model3dId', value_type: 'integer', nullable: true },
      { source: 'publishedAtUnix', target: 'isPublished', value_type: 'exists_boolean' },
      { source: 'blockedFor', target: 'blockedFor', value_type: 'low_cardinality_string', nullable: true },
      { source: 'hasMeta', target: 'hasMeta', value_type: 'boolean', default: false },
      { source: 'onSite', target: 'onSite', value_type: 'boolean', default: false },
      { source: 'poi', target: 'poi', value_type: 'boolean', default: false },
      { source: 'minor', target: 'minor', value_type: 'boolean', default: false },
      { source: 'sortAtUnix', target: 'sortAt', value_type: 'integer', fallback: 'sortAt', ms_to_seconds: true },
      { source: 'publishedAtUnix', target: 'publishedAt', value_type: 'integer', ms_to_seconds: true },
      { source: 'existedAt', target: 'existedAt', value_type: 'integer' },
      { source: 'url', target: 'url', value_type: 'string', doc_only: true },
      { source: 'hash', target: 'hash', value_type: 'string', doc_only: true },
      { source: 'width', target: 'width', value_type: 'integer', doc_only: true },
      { source: 'height', target: 'height', value_type: 'integer', doc_only: true },
    ],
  },
};

async function main() {
  log('=== BitDex E2E: scheduled-publish + ingested sortAt ===');
  log(`Server: ${BASE_URL}`);
  await del(`/api/indexes/${INDEX}`); await sleep(300);
  const created = await post('/api/indexes', CONFIG);
  if (created.status !== 200 && created.status !== 201) { console.error('FATAL create index', created.status, JSON.stringify(created.data)); process.exit(1); }

  // ---- A. published image ----
  log('\n[A] published image (publishedAt in the past)');
  const pubAt = nowS() - 3600;
  await sendOps([imageOps(950001, 2001, pubAt, { model3dId: 42, existedAtSecs: pubAt - 100 })]);
  await sleep(600);
  {
    const d = await doc(950001, ['isPublished', 'sortAt', 'publishedAt', 'model3dId']);
    ok(d && d.isPublished === true, `isPublished=true (got ${d?.isPublished})`);
    ok(d && Number(d.sortAt) === pubAt, `ingested sortAt(${d?.sortAt}) == publishedAt seconds(${pubAt})`);
    ok(d && Number(d.publishedAt) === pubAt, `publishedAt stored as seconds (${d?.publishedAt})`);
    ok(d && Number(d.model3dId) === 42, `model3dId filter populated (${d?.model3dId})`);
    const vis = (await query([{ Eq: ['postId', { Integer: 2001 }] }, ...PUB])).ids || [];
    ok(vis.includes(950001), `visible in isPublished feed`);
  }

  // ---- B. scheduled image (future publishedAt) -> deferred ----
  log('\n[B] scheduled image (publishedAt = now + 6s) -> deferred/invisible');
  const tf = nowS() + 6;
  await sendOps([imageOps(950002, 2002, tf, { existedAtSecs: nowS() - 100000 })]);
  await sleep(800);
  {
    const d = await doc(950002, ['isPublished', 'sortAt']);
    const vis = (await query([{ Eq: ['postId', { Integer: 2002 }] }, ...PUB])).ids || [];
    ok(!vis.includes(950002) && (!d || d.isPublished !== true), `pre-Tf: not in feed AND isPublished!=true (deferred)`);
  }

  // ---- C. CRITICAL: no-op sweep activation at Tf ----
  log('\n[C] CRITICAL no-op: cross Tf with NO further op — sweep alone must flip + preserve sortAt');
  await sleep((tf - nowS() + 4) * 1000);
  {
    let d = null;
    for (let i = 0; i < 15; i++) { d = await doc(950002, ['isPublished', 'sortAt', 'publishedAt']); if (d && d.isPublished === true) break; await sleep(1000); }
    ok(d && d.isPublished === true, `isPublished flipped true via sweep (no op sent)`);
    ok(d && Number(d.sortAt) === tf, `ingested future sortAt preserved through activation (sortAt=${d?.sortAt} == Tf=${tf})`);
    ok(d && Number(d.sortAt) === Number(d.publishedAt), `sortAt == publishedAt`);
    const vis = (await query([{ Eq: ['postId', { Integer: 2002 }] }, ...PUB])).ids || [];
    ok(vis.includes(950002), `now visible in isPublished feed`);
  }

  // ---- D. unpublish + republish ----
  log('\n[D] unpublish (remove publishedAt) -> invisible; republish -> visible');
  await sendOps([{ entity_id: 950001, creates_slot: false, ops: [{ op: 'remove', field: 'publishedAt', value: pubAt }] }]);
  await sleep(700);
  {
    const d = await doc(950001, ['isPublished']);
    const vis = (await query([{ Eq: ['postId', { Integer: 2001 }] }, ...PUB])).ids || [];
    ok(d && d.isPublished === false && !vis.includes(950001), `unpublished: isPublished=false and invisible`);
  }
  const rp = nowS() - 60;
  await sendOps([{ entity_id: 950001, creates_slot: false, ops: [{ op: 'set', field: 'publishedAt', value: rp }, { op: 'set', field: 'sortAtUnix', value: rp * 1000 }] }]);
  await sleep(700);
  {
    const d = await doc(950001, ['isPublished', 'sortAt', 'publishedAt']);
    ok(d && d.isPublished === true, `republished: isPublished=true`);
    ok(d && Number(d.sortAt) === rp, `republished sortAt==publishedAt (${d?.sortAt})`);
    const vis = (await query([{ Eq: ['postId', { Integer: 2001 }] }, ...PUB])).ids || [];
    ok(vis.includes(950001), `visible again`);
  }

  await del(`/api/indexes/${INDEX}`);
  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}
main().catch((e) => { console.error('FATAL', e); process.exit(2); });
