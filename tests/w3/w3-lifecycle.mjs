#!/usr/bin/env node
// W3 gates 2 & 3 — scheduled-post lifecycle E2E against the local PG + triggers + poller + BitDex.
// Requires: docker w3-pg, poller running (BITDEX_URL=3010, START_CURSOR=0), steady server 3010.
import { execFileSync } from 'node:child_process';
const BITDEX = process.env.BITDEX_URL || 'http://localhost:3010';
const IDX = 'civitai', TOK = 'test123';
const psql = (sql) => execFileSync('docker', ['exec', 'w3-pg', 'psql', '-U', 'bitdex', '-d', 'civitai', '-t', '-A', '-c', sql], { encoding: 'utf8' }).trim();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowEpoch = () => parseInt(psql(`SELECT extract(epoch from now())::bigint`), 10);
async function doc(id, fields) {
  const r = await fetch(`${BITDEX}/api/indexes/${IDX}/document`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${TOK}` }, body: JSON.stringify({ slot_id: id, fields }) });
  if (!r.ok) return null; return r.json();
}
async function q(filters, extra = {}) {
  const r = await fetch(`${BITDEX}/api/indexes/${IDX}/query`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${TOK}` }, body: JSON.stringify({ filters, limit: 50, ...extra }) });
  if (!r.ok) throw new Error(`query ${r.status}: ${await r.text()}`); return r.json();
}
const PUB = [{ Eq: ['isPublished', { Bool: true }] }];
let fails = 0; const log = (ok, msg) => { if (!ok) fails++; console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${msg}`); };
async function pollUntil(fn, timeoutMs = 20000, every = 1000) {
  const t0 = Date.now(); let last;
  while (Date.now() - t0 < timeoutMs) { last = await fn(); if (last) return last; await sleep(every); }
  return last;
}
const visible = async (id) => (await q([...PUB], { limit: 0, /* narrow */ })) && (await q([{ Eq: ['postId', { Integer: (await doc(id, ['postId']))?.postId || -1 }] }, ...PUB])).ids?.includes(id);

(async () => {
  console.log('\n=== W3 Lifecycle E2E ===');

  // ---------- Scenario A: published image (sanity) ----------
  console.log('\n[A] published image already present (900001)');
  {
    const d = await pollUntil(async () => { const x = await doc(900001, ['isPublished', 'sortAt', 'publishedAt']); return x && x.isPublished === true ? x : null; });
    log(!!d && d.isPublished === true, `900001 isPublished=true`);
    log(!!d && Number(d.sortAt) === Number(d.publishedAt), `sortAt(${d?.sortAt}) == publishedAt(${d?.publishedAt})`);
  }

  // ---------- Scenario B: schedule -> invisible/deferred ----------
  console.log('\n[B] scheduled post 1002 (publishedAt = now+25s), image 900002');
  const tf = nowEpoch() + 25;
  psql(`INSERT INTO "Post"(id,"publishedAt",availability,"modelVersionId") VALUES (1002, to_timestamp(${tf}),'Public',777)`);
  psql(`INSERT INTO "Image"(id,url,"nsfwLevel",hash,flags,"type","userId","scannedAt","createdAt","postId",width,height) VALUES (900002,'u2',1,'h2',0,'image',42, now()-interval '1 day', now()-interval '1 day', 1002, 512,768)`);
  {
    // wait for poller to apply, then assert deferred (isPublished false, not visible)
    const gone = await pollUntil(async () => {
      const inPub = (await q([{ Eq: ['postId', { Integer: 1002 }] }, ...PUB])).ids || [];
      const d = await doc(900002, ['isPublished', 'sortAt']);
      // deferred: either not-yet-alive (doc null) or isPublished false, and NOT in published feed
      return (!inPub.includes(900002)) && (!d || d.isPublished === false) ? { d } : null;
    }, 12000);
    log(!!gone, `900002 pre-Tf: NOT in isPublished feed AND isPublished!=true (deferred/quarantined)`);
  }

  // ---------- Scenario C (CRITICAL): no-op sweep activation at Tf ----------
  console.log('\n[C] CRITICAL no-op: cross Tf with NO further PG op — sweep alone must flip + land sortAt');
  {
    const waitMs = (tf - nowEpoch() + 6) * 1000; // until a few s past Tf (sweep=3s)
    if (waitMs > 0) { console.log(`    waiting ${Math.round(waitMs/1000)}s past Tf (no PG writes)...`); await sleep(waitMs); }
    const d = await pollUntil(async () => { const x = await doc(900002, ['isPublished', 'sortAt', 'publishedAt']); return x && x.isPublished === true ? x : null; }, 20000);
    log(!!d && d.isPublished === true, `900002 isPublished flipped true via sweep (no op sent)`);
    log(!!d && Number(d.sortAt) === Number(d.publishedAt), `sortAt(${d?.sortAt}) == publishedAt(${d?.publishedAt}) == Tf(${tf})`);
    log(!!d && Number(d.sortAt) === tf, `sortAt equals scheduled Tf exactly`);
    const inPub = (await q([{ Eq: ['postId', { Integer: 1002 }] }, ...PUB])).ids || [];
    log(inPub.includes(900002), `900002 now visible in isPublished feed`);
  }

  // ---------- Scenario E: unpublish ----------
  console.log('\n[E] unpublish post 1001 (publishedAt -> NULL) -> image invisible');
  psql(`UPDATE "Post" SET "publishedAt" = NULL WHERE id = 1001`);
  {
    const ok = await pollUntil(async () => {
      const d = await doc(900001, ['isPublished']);
      const inPub = (await q([{ Eq: ['postId', { Integer: 1001 }] }, ...PUB])).ids || [];
      return (d && d.isPublished === false && !inPub.includes(900001)) ? true : null;
    }, 12000);
    log(!!ok, `900001 isPublished=false and not in published feed after unpublish`);
  }

  // ---------- Scenario F: republish ----------
  console.log('\n[F] republish post 1001 (publishedAt -> past) -> visible, sortAt==publishedAt');
  const rp = nowEpoch() - 30;
  psql(`UPDATE "Post" SET "publishedAt" = to_timestamp(${rp}) WHERE id = 1001`);
  {
    const d = await pollUntil(async () => { const x = await doc(900001, ['isPublished', 'sortAt', 'publishedAt']); return x && x.isPublished === true ? x : null; }, 12000);
    log(!!d && d.isPublished === true, `900001 republished isPublished=true`);
    log(!!d && Number(d.sortAt) === Number(d.publishedAt), `sortAt(${d?.sortAt}) == publishedAt(${d?.publishedAt})`);
    const inPub = (await q([{ Eq: ['postId', { Integer: 1001 }] }, ...PUB])).ids || [];
    log(inPub.includes(900001), `900001 visible again`);
  }

  // ---------- Scenario G [PR-m1]: postId change ----------
  console.log('\n[G] [PR-m1] move image between posts -> sortAt recomputes, postId bit moves');
  const p4 = nowEpoch() - 1000, p5 = nowEpoch() - 500;
  psql(`INSERT INTO "Post"(id,"publishedAt",availability,"modelVersionId") VALUES (1004, to_timestamp(${p4}),'Public',444),(1005, to_timestamp(${p5}),'Public',445)`);
  psql(`INSERT INTO "Image"(id,url,"nsfwLevel",hash,flags,"type","userId","scannedAt","createdAt","postId",width,height) VALUES (900005,'u5',1,'h5',0,'image',42, now()-interval '2000 sec', now()-interval '2000 sec', 1004, 512,768)`);
  {
    const before = await pollUntil(async () => { const inP = (await q([{ Eq: ['postId', { Integer: 1004 }] }, ...PUB])).ids || []; return inP.includes(900005) ? true : null; }, 12000);
    const d0 = await doc(900005, ['sortAt', 'postId']);
    log(!!before && d0?.postId === 1004, `900005 initially in post 1004, sortAt=${d0?.sortAt}`);
    log(Number(d0?.sortAt) === p4, `initial sortAt == post1004 publishedAt (${p4})`);
    // move to post 1005
    psql(`UPDATE "Image" SET "postId" = 1005 WHERE id = 900005`);
    const after = await pollUntil(async () => {
      const inNew = (await q([{ Eq: ['postId', { Integer: 1005 }] }, ...PUB])).ids || [];
      const inOld = (await q([{ Eq: ['postId', { Integer: 1004 }] }, ...PUB])).ids || [];
      const d = await doc(900005, ['sortAt', 'postId']);
      return (inNew.includes(900005) && !inOld.includes(900005) && d?.postId === 1005) ? d : null;
    }, 12000);
    log(!!after, `900005 moved: postId bit now 1005, not 1004`);
    log(!!after && Number(after.sortAt) === p5, `sortAt recomputed to post1005 publishedAt (${p5}), got ${after?.sortAt}`);
  }

  console.log(`\n=== RESULT: ${fails === 0 ? 'PASS' : `FAIL (${fails})`} ===`);
  process.exit(fails === 0 ? 0 : 1);
})().catch((e) => { console.error('FATAL', e); process.exit(2); });
