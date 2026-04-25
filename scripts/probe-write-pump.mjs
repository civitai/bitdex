#!/usr/bin/env node
// Write-pump probe to drive flush thread + cache worker.
//
// Fires upserts via `/api/indexes/civitai/documents/upsert` at high rate
// against existing slots, mutating reactionCount + sortAt. Each upsert
// produces a CacheWorkItem on the next flush cycle. With high volume the
// async cache worker channel can saturate, triggering the backpressure
// fallback path PR-B fixes.
//
// Usage:
//   node probe-write-pump.mjs [duration_seconds] [batch_size] [concurrency]

import http from 'node:http';

const DURATION_S = parseInt(process.argv[2] || '60');
const BATCH = parseInt(process.argv[3] || '200');
const CONC = parseInt(process.argv[4] || '8');

const TOKEN = 'test123';
const HOST = '127.0.0.1';
const PORT = 3002;

// Mutate slots scattered across the alive space → maximizes filter/sort
// bucket diversity → maximizes coalescer output per flush cycle.
const MAX_SLOT = 110_000_000;

function randomBatch(size) {
  const docs = [];
  const seen = new Set();
  while (docs.length < size) {
    const id = Math.floor(Math.random() * MAX_SLOT) + 1;
    if (seen.has(id)) continue;
    seen.add(id);
    docs.push({
      id,
      reactionCount: Math.floor(Math.random() * 100),
      sortAt: 1_700_000_000 + Math.floor(Math.random() * 86400_000),
    });
  }
  return docs;
}

function fireBatch() {
  return new Promise((resolve) => {
    const body = JSON.stringify({ documents: randomBatch(BATCH) });
    const req = http.request(
      {
        hostname: HOST,
        port: PORT,
        path: '/api/indexes/civitai/documents/upsert',
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'authorization': `Bearer ${TOKEN}`,
          'content-length': Buffer.byteLength(body),
        },
        agent: false,
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString() }));
      }
    );
    req.on('error', (e) => resolve({ status: 0, err: e.message }));
    req.write(body);
    req.end();
  });
}

let done = 0;
let errs = 0;
let upserted = 0;

async function worker() {
  while (Date.now() - t0 < DURATION_S * 1000) {
    const r = await fireBatch();
    if (r.status === 200) {
      done++;
      try {
        upserted += JSON.parse(r.body).upserted || 0;
      } catch (_) {}
    } else {
      errs++;
    }
  }
}

const t0 = Date.now();
console.log(`pumping upserts: batch=${BATCH} concurrency=${CONC} for ${DURATION_S}s`);

const stats_iv = setInterval(() => {
  const elapsed = (Date.now() - t0) / 1000;
  const rate = (upserted / elapsed).toFixed(0);
  console.log(`[${elapsed.toFixed(0)}s] batches=${done} upserted=${upserted} (${rate}/s) errs=${errs}`);
}, 5000);

const workers = Array.from({ length: CONC }, () => worker());
await Promise.all(workers);
clearInterval(stats_iv);

const elapsed = (Date.now() - t0) / 1000;
console.log(`\n=== final ===`);
console.log(`elapsed=${elapsed.toFixed(1)}s batches=${done} upserted=${upserted} (${(upserted / elapsed).toFixed(0)}/s) errs=${errs}`);
