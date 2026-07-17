#!/usr/bin/env node
// Minimal W3 ops poller: tails "BitdexOps" (via docker psql) -> POST /ops to BitDex 3008.
// Starts at current head so it only forwards lifecycle-driven ops. No boot-dump.
import { execFileSync } from 'node:child_process';

const BITDEX = process.env.BITDEX_URL || 'http://localhost:3008';
const IDX = 'civitai', TOK = 'test123';
const PGC = ['exec', 'w3-pg', 'psql', '-U', 'bitdex', '-d', 'civitai', '-t', '-A', '-c'];
const psql = (sql) => execFileSync('docker', [...PGC, sql], { encoding: 'utf8', maxBuffer: 1 << 28 }).trim();

let cursor = process.env.START_CURSOR != null ? parseInt(process.env.START_CURSOR, 10)
  : (parseInt(psql(`SELECT COALESCE(MAX(id),0) FROM "BitdexOps"`), 10) || 0);
console.error(`[w3-poller] start cursor=${cursor}`);

async function postOps(batch) {
  const r = await fetch(`${BITDEX}/api/indexes/${IDX}/ops`, {
    method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${TOK}` },
    body: JSON.stringify({ ops: batch, meta: { source: 'w3-steady', cursor } }),
  });
  if (!r.ok) throw new Error(`POST /ops ${r.status}: ${await r.text()}`);
}

let applied = 0;
async function tick() {
  const raw = psql(`SELECT COALESCE(json_agg(json_build_object('id',id,'entity_id',entity_id,'ops',ops) ORDER BY id),'[]') FROM "BitdexOps" WHERE id > ${cursor}`);
  let rows; try { rows = JSON.parse(raw); } catch { rows = []; }
  if (!rows.length) return;
  const batch = rows.map((row) => {
    const hasAlive = row.ops.some((o) => o.op === 'alive');
    const ops = hasAlive ? row.ops.filter((o) => o.op !== 'alive') : row.ops;
    return { entity_id: Number(row.entity_id), ops, creates_slot: hasAlive };
  });
  await postOps(batch);
  cursor = Math.max(...rows.map((r) => r.id));
  applied += rows.length;
  console.error(`[w3-poller] applied ${rows.length} rows (cursor=${cursor}, total=${applied})`);
}

setInterval(() => { tick().catch((e) => console.error('[w3-poller] err', e.message)); }, 1000);
process.on('SIGTERM', () => process.exit(0));
