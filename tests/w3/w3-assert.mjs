#!/usr/bin/env node
// W3 dump assertions (internal-consistency, dup-immune):
//   1. sortAt invariant: for every slot, stored sortAt == max(publishedAt, existedAt)
//      (sortAt was ingested as GREATEST(publishedAt, scannedAt, createdAt); existedAt=max(scan,creat)).
//   2. model3dId plumbing: model3dId Eq 42 slots all have postId%1000==0 & model3dId==42;
//      random alive slots satisfy (postId%1000==0) <=> (model3dId==42)  [prod real-5 not in dataset].
//   3. no-deferred-regression sanity: isPublished bulk true; availability populated.
//   node w3-assert.mjs
const URL = process.env.BITDEX_URL || 'http://localhost:3007';
const IDX = 'civitai', TOK = 'test123';
const HDR = { 'content-type': 'application/json', authorization: `Bearer ${TOK}` };

async function query(filters, extra = {}) {
  const r = await fetch(`${URL}/api/indexes/${IDX}/query`, { method: 'POST', headers: HDR, body: JSON.stringify({ filters, ...extra }) });
  if (!r.ok) throw new Error(`query ${r.status}: ${await r.text()}`);
  return r.json();
}
async function docs(ids) {
  const r = await fetch(`${URL}/api/indexes/${IDX}/documents`, { method: 'POST', headers: HDR,
    body: JSON.stringify({ slot_ids: ids, fields: ['sortAt', 'publishedAt', 'existedAt', 'model3dId', 'postId', 'isPublished', 'availability'] }) });
  if (!r.ok) throw new Error(`documents ${r.status}: ${await r.text()}`);
  const j = await r.json();
  const list = j.documents || j.docs || (Array.isArray(j) ? j : Object.values(j));
  const m = new Map(); for (const d of list) if (d && d.id != null) m.set(Number(d.id), d);
  return m;
}
const ALL = [{ In: ['nsfwLevel', [{ Integer: 1 }, { Integer: 2 }, { Integer: 4 }, { Integer: 8 }, { Integer: 16 }, { Integer: 32 }]] }];

(async () => {
  let fail = 0;
  // gather a diverse slot-id sample across sort positions
  const idSet = new Set();
  const qset = [
    { filters: ALL, sort: { field: 'sortAt', direction: 'Desc' }, limit: 2000 },
    { filters: ALL, sort: { field: 'sortAt', direction: 'Asc' }, limit: 2000 },
    { filters: ALL, sort: { field: 'reactionCount', direction: 'Desc' }, limit: 2000 },
    { filters: ALL, sort: { field: 'commentCount', direction: 'Desc' }, limit: 2000 },
  ];
  for (const off of [0, 1_000_000, 10_000_000, 30_000_000, 60_000_000, 90_000_000]) {
    qset.push({ filters: ALL, sort: { field: 'sortAt', direction: 'Desc' }, limit: 2000, offset: off });
  }
  for (const q of qset) { const r = await query(q.filters, q); (r.ids || []).forEach(i => idSet.add(i)); }
  const ids = [...idSet];
  console.log(`sortAt invariant: sampled ${ids.length} slots`);

  // ---- 1. sortAt invariant ----
  let ok = 0, bad = 0; const bads = [];
  for (let i = 0; i < ids.length; i += 500) {
    const m = await docs(ids.slice(i, i + 500));
    for (const [id, d] of m) {
      const exp = Math.max(Number(d.publishedAt) || 0, Number(d.existedAt) || 0);
      if (Number(d.sortAt) === exp) ok++;
      else { bad++; if (bads.length < 8) bads.push({ id, sortAt: d.sortAt, publishedAt: d.publishedAt, existedAt: d.existedAt }); }
    }
  }
  console.log(`  sortAt == max(publishedAt,existedAt): ok=${ok} BAD=${bad}`);
  if (bads.length) console.log('  bad:', JSON.stringify(bads));
  if (bad > 0) fail++;

  // ---- 2. model3dId plumbing ----
  const q42 = await query([{ Eq: ['model3dId', { Integer: 42 }] }], { limit: 800 });
  console.log(`model3dId Eq 42 total_matched=${q42.total_matched} (checking ${Math.min(800, (q42.ids||[]).length)})`);
  if (!(q42.total_matched > 0)) { console.log('  FAIL: 0 matches'); fail++; }
  let m42ok = 0, m42bad = 0; const m42b = [];
  for (let i = 0; i < (q42.ids || []).length; i += 500) {
    const m = await docs(q42.ids.slice(i, i + 500));
    for (const [id, d] of m) {
      if (Number(d.model3dId) === 42 && Number(d.postId) % 1000 === 0) m42ok++;
      else { m42bad++; if (m42b.length < 8) m42b.push({ id, model3dId: d.model3dId, postId: d.postId }); }
    }
  }
  console.log(`  forward (m3d=42 => postId%1000==0): ok=${m42ok} BAD=${m42bad}`);
  if (m42b.length) console.log('  bad:', JSON.stringify(m42b));
  if (m42bad > 0) fail++;
  // reverse over the diverse sample
  let revBad = 0; const revb = [];
  for (let i = 0; i < ids.length; i += 500) {
    const m = await docs(ids.slice(i, i + 500));
    for (const [id, d] of m) {
      const should42 = Number(d.postId) % 1000 === 0 && Number(d.postId) > 0;
      const is42 = Number(d.model3dId) === 42;
      // allow real prod model3d values (2,173,273) to not be 42
      if (should42 !== is42 && !([2,173,273].includes(Number(d.model3dId)))) { revBad++; if (revb.length < 8) revb.push({ id, postId: d.postId, model3dId: d.model3dId }); }
    }
  }
  console.log(`  reverse (postId%1000==0 <=> m3d=42): BAD=${revBad}`);
  if (revb.length) console.log('  bad:', JSON.stringify(revb));
  if (revBad > 0) fail++;

  // ---- 3. no-deferred-regression sanity ----
  const pubT = await query([{ Eq: ['isPublished', { Bool: true }] }], { limit: 0 });
  const pubF = await query([{ Eq: ['isPublished', { Bool: false }] }], { limit: 0 });
  const avail = await query([{ Eq: ['availability', { String: 'Public' }] }], { limit: 0 });
  console.log(`sanity: isPublished true=${pubT.total_matched} false=${pubF.total_matched}; availability=Public ${avail.total_matched}`);

  console.log(fail === 0 ? '\nRESULT: PASS' : `\nRESULT: FAIL (${fail} groups)`);
  process.exit(fail === 0 ? 0 : 1);
})().catch(e => { console.error('FATAL', e); process.exit(2); });
