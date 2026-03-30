#!/usr/bin/env node
/**
 * Gate 5 Task #27: Ops Velocity + Read Contention Stress Test
 *
 * Measures:
 * - Max sustainable ops/sec at different batch sizes
 * - Query latency (p50/p95/p99) under write load
 * - Dropped/failed ops count
 *
 * Usage:
 *   node tests/gate5-ops-velocity.mjs [--url http://localhost:3001] [--duration 30]
 */

const URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3001';
const INDEX = 'civitai';
const BASE = `${URL}/api/indexes/${INDEX}`;
const DURATION_SECS = process.argv.includes('--duration')
  ? parseInt(process.argv[process.argv.indexOf('--duration') + 1])
  : 30;

// --- Helpers ---

async function postOps(ops) {
  const start = performance.now();
  const resp = await fetch(`${BASE}/ops`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      ops,
      meta: { source: 'stress-test' },
    }),
  });
  const elapsed = performance.now() - start;
  const body = await resp.json();
  return { ok: resp.ok, elapsed, accepted: body.accepted || 0, bytes: body.bytes_written || 0 };
}

async function runQuery(filter, sort) {
  const start = performance.now();
  const resp = await fetch(`${BASE}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      filters: filter,
      sort: { field: sort, direction: 'Desc' },
      limit: 20,
    }),
  });
  const elapsed = performance.now() - start;
  const ok = resp.ok;
  try { await resp.json(); } catch {}
  return { ok, elapsed };
}

function generateOpsBatch(batchSize, baseEntity) {
  const ops = [];
  for (let i = 0; i < batchSize; i++) {
    const entityId = baseEntity + i;
    const opType = Math.random() < 0.7 ? 'set' : 'add';
    if (opType === 'set') {
      ops.push({
        entity_id: entityId,
        creates_slot: false,
        ops: [{ op: 'set', field: 'nsfwLevel', value: Math.floor(Math.random() * 32) + 1 }],
      });
    } else {
      ops.push({
        entity_id: entityId,
        creates_slot: false,
        ops: [{ op: 'add', field: 'tagIds', value: Math.floor(Math.random() * 200000) }],
      });
    }
  }
  return ops;
}

function percentile(arr, p) {
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil(sorted.length * p / 100) - 1;
  return sorted[Math.max(0, idx)];
}

// --- Query patterns ---
const QUERIES = [
  { name: 'nsfwLevel=1', filter: [{ Eq: ['nsfwLevel', { Integer: 1 }] }], sort: 'reactionCount' },
  { name: 'type=image', filter: [{ Eq: ['type', { String: 'image' }] }], sort: 'sortAt' },
  { name: 'hasMeta=true', filter: [{ Eq: ['hasMeta', { Bool: true }] }], sort: 'reactionCount' },
  { name: 'tagIds∋2013', filter: [{ In: ['tagIds', [{ Integer: 2013 }]] }], sort: 'reactionCount' },
];

// --- Test Runner ---

async function runVelocityTest(batchSize, durationSecs) {
  const startTime = Date.now();
  const endTime = startTime + durationSecs * 1000;

  let totalOps = 0;
  let totalBatches = 0;
  let failedBatches = 0;
  const opsLatencies = [];
  const queryLatencies = [];
  let queryCount = 0;
  let queryFails = 0;
  let baseEntity = 1000 + Math.floor(Math.random() * 100000000);

  // Start concurrent query loop
  let queryRunning = true;
  const queryLoop = (async () => {
    while (queryRunning) {
      const q = QUERIES[queryCount % QUERIES.length];
      const result = await runQuery(q.filter, q.sort);
      queryLatencies.push(result.elapsed);
      if (!result.ok) queryFails++;
      queryCount++;
      // Small delay between queries to not starve ops
      await new Promise(r => setTimeout(r, 50));
    }
  })();

  // Ops fire loop
  while (Date.now() < endTime) {
    const batch = generateOpsBatch(batchSize, baseEntity);
    baseEntity += batchSize;
    const result = await postOps(batch);
    opsLatencies.push(result.elapsed);
    if (result.ok) {
      totalOps += result.accepted;
      totalBatches++;
    } else {
      failedBatches++;
    }
  }

  queryRunning = false;
  await queryLoop;

  const elapsedSecs = (Date.now() - startTime) / 1000;
  const opsPerSec = totalOps / elapsedSecs;

  return {
    batchSize,
    durationSecs: elapsedSecs.toFixed(1),
    totalOps,
    totalBatches,
    failedBatches,
    opsPerSec: Math.round(opsPerSec),
    opsLatency: {
      p50: percentile(opsLatencies, 50).toFixed(1),
      p95: percentile(opsLatencies, 95).toFixed(1),
      p99: percentile(opsLatencies, 99).toFixed(1),
    },
    queryCount,
    queryFails,
    queryLatency: queryLatencies.length > 0 ? {
      p50: percentile(queryLatencies, 50).toFixed(1),
      p95: percentile(queryLatencies, 95).toFixed(1),
      p99: percentile(queryLatencies, 99).toFixed(1),
    } : null,
  };
}

// --- Main ---

async function main() {
  console.log(`=== Gate 5: Ops Velocity + Read Contention Stress Test ===`);
  console.log(`Server: ${URL}`);
  console.log(`Duration per batch size: ${DURATION_SECS}s`);
  console.log();

  // Health check
  const health = await fetch(`${URL}/api/health`).then(r => r.text()).catch(() => null);
  if (health !== 'ok') {
    console.error('ERROR: Server not healthy at', URL);
    process.exit(1);
  }
  console.log('Server healthy.\n');

  const batchSizes = [1, 10, 50, 100, 500];
  const results = [];

  for (const bs of batchSizes) {
    console.log(`--- Batch size: ${bs} (${DURATION_SECS}s) ---`);
    const result = await runVelocityTest(bs, DURATION_SECS);
    results.push(result);

    console.log(`  Ops: ${result.totalOps} total, ${result.opsPerSec} ops/sec`);
    console.log(`  Ops latency: p50=${result.opsLatency.p50}ms p95=${result.opsLatency.p95}ms p99=${result.opsLatency.p99}ms`);
    console.log(`  Batches: ${result.totalBatches} ok, ${result.failedBatches} failed`);
    console.log(`  Queries: ${result.queryCount} total, ${result.queryFails} failed`);
    if (result.queryLatency) {
      console.log(`  Query latency: p50=${result.queryLatency.p50}ms p95=${result.queryLatency.p95}ms p99=${result.queryLatency.p99}ms`);
    }
    console.log();

    // Small cooldown between batch sizes
    await new Promise(r => setTimeout(r, 2000));
  }

  // Summary table
  console.log('=== SUMMARY ===');
  console.log('Batch | Ops/sec | Ops p50 | Ops p95 | Ops p99 | Qry p50 | Qry p95 | Qry p99 | Failed');
  console.log('------|---------|---------|---------|---------|---------|---------|---------|-------');
  for (const r of results) {
    const qp50 = r.queryLatency?.p50 || '-';
    const qp95 = r.queryLatency?.p95 || '-';
    const qp99 = r.queryLatency?.p99 || '-';
    console.log(
      `${String(r.batchSize).padStart(5)} | ${String(r.opsPerSec).padStart(7)} | ${r.opsLatency.p50.padStart(7)}ms | ${r.opsLatency.p95.padStart(7)}ms | ${r.opsLatency.p99.padStart(7)}ms | ${String(qp50).padStart(7)}ms | ${String(qp95).padStart(7)}ms | ${String(qp99).padStart(7)}ms | ${r.failedBatches}`
    );
  }
}

main().catch(e => { console.error(e); process.exit(1); });
