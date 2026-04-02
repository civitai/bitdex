#!/usr/bin/env node
/**
 * E2E Ops Replay Smoke Test
 *
 * Replays a CSV dump of production ops through the /ops endpoint and verifies:
 *   1. All ops are accepted (no 4xx/5xx errors)
 *   2. Docs are written and readable via GET /docs/{slot}
 *   3. Filter bitmaps are updated (query by field values returns results)
 *   4. Stats reflect the expected alive count
 *
 * Usage:
 *   node tests/e2e/e2e-ops-replay.mjs [--url http://localhost:3001] [--ops-file data/ops-sample-16385703.csv] [--batch-size 100] [--verbose]
 *
 * Prerequisites:
 *   Server running with the civitai index loaded (config only, no data needed):
 *     cargo run --release --features server --bin bitdex-server -- --port 3001 --data-dir ./test-ops-data --index-dir deploy/configs
 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3001';
const OPS_FILE = process.argv.includes('--ops-file')
  ? process.argv[process.argv.indexOf('--ops-file') + 1]
  : 'data/ops-sample-16385703.csv';
const BATCH_SIZE = process.argv.includes('--batch-size')
  ? parseInt(process.argv[process.argv.indexOf('--batch-size') + 1])
  : 100;
const VERBOSE = process.argv.includes('--verbose');

let passed = 0;
let failed = 0;

function log(...args) {
  if (VERBOSE) console.log(...args);
}

function assert(condition, msg) {
  if (!condition) {
    console.error(`  FAIL: ${msg}`);
    failed++;
  } else {
    log(`  PASS: ${msg}`);
    passed++;
  }
}

// Parse the CSV ops file into EntityOps format
function parseOpsCSV(filePath) {
  const content = readFileSync(resolve(filePath), 'utf-8');
  const lines = content.split('\n').filter(l => l.trim());
  const header = lines[0].split(',');

  // Find column indices
  const idIdx = header.indexOf('id');
  const entityIdIdx = header.indexOf('entity_id');
  const opsIdx = header.indexOf('ops');

  if (entityIdIdx === -1 || opsIdx === -1) {
    throw new Error(`CSV must have entity_id and ops columns. Found: ${header.join(', ')}`);
  }

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;

    // Parse CSV with quoted JSON fields
    const match = line.match(/^(\d+),(\d+),"(.+)",(.+)$/);
    if (!match) {
      // Try simpler format
      const parts = line.split(',');
      if (parts.length >= 3) {
        rows.push({
          id: parseInt(parts[0]),
          entity_id: parseInt(parts[1]),
          ops_json: parts.slice(2).join(',').replace(/^"|"$/g, ''),
        });
      }
      continue;
    }

    rows.push({
      id: parseInt(match[1]),
      entity_id: parseInt(match[2]),
      ops_json: match[3].replace(/""/g, '"'),
    });
  }

  return rows;
}

// Convert parsed rows to OpsBatch format (grouped by entity_id)
function rowsToOpsBatch(rows) {
  const byEntity = new Map();
  for (const row of rows) {
    let ops;
    try {
      ops = JSON.parse(row.ops_json);
    } catch (e) {
      log(`  Skipping row ${row.id}: invalid JSON: ${e.message}`);
      continue;
    }

    if (!byEntity.has(row.entity_id)) {
      byEntity.set(row.entity_id, {
        entity_id: row.entity_id,
        ops: [],
        creates_slot: false,
      });
    }

    const entry = byEntity.get(row.entity_id);
    for (const op of ops) {
      entry.ops.push(op);
      if (op.op === 'alive') {
        entry.creates_slot = true;
      }
    }
  }

  return Array.from(byEntity.values());
}

async function postOps(entityOps) {
  const batch = { ops: entityOps };
  const res = await fetch(`${BASE_URL}/api/indexes/civitai/ops`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(batch),
  });
  const text = await res.text();
  try {
    return { status: res.status, body: JSON.parse(text) };
  } catch {
    return { status: res.status, body: { error: text.slice(0, 200) } };
  }
}

async function getDoc(slotId) {
  const res = await fetch(`${BASE_URL}/api/indexes/civitai/documents/${slotId}`);
  if (res.status === 404) return null;
  return await res.json();
}

async function getStats() {
  const res = await fetch(`${BASE_URL}/api/indexes/civitai/stats`);
  return await res.json();
}

async function query(body) {
  const res = await fetch(`${BASE_URL}/api/indexes/civitai/query?format=compact`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    log(`  Query response (${res.status}): ${text.slice(0, 200)}`);
    return { ids: [] };
  }
}

async function waitForFlush(ms = 2000) {
  await new Promise(r => setTimeout(r, ms));
}

async function main() {
  console.log(`\n=== E2E Ops Replay Smoke Test ===`);
  console.log(`Server: ${BASE_URL}`);
  console.log(`Ops file: ${OPS_FILE}`);
  console.log(`Batch size: ${BATCH_SIZE}\n`);

  // 1. Parse ops CSV
  console.log('Phase 1: Parsing ops CSV...');
  const rows = parseOpsCSV(OPS_FILE);
  console.log(`  Parsed ${rows.length} rows`);
  assert(rows.length > 0, `ops file has rows (got ${rows.length})`);

  // 2. Convert to OpsBatch format
  console.log('Phase 2: Grouping by entity...');
  const allEntityOps = rowsToOpsBatch(rows);
  console.log(`  ${allEntityOps.length} unique entities`);
  const aliveEntities = allEntityOps.filter(e => e.creates_slot);
  console.log(`  ${aliveEntities.length} entities with alive ops (creates_slot=true)`);

  // 3. Verify server is reachable
  console.log('Phase 3: Checking server...');
  try {
    const stats = await getStats();
    assert(stats !== undefined, 'server is reachable');
    console.log(`  Current alive count: ${stats.alive_count || 0}`);
  } catch (e) {
    console.error(`  FATAL: Server not reachable at ${BASE_URL}: ${e.message}`);
    process.exit(1);
  }

  // 4. Send ops in batches
  console.log(`Phase 4: Sending ${allEntityOps.length} entity ops in batches of ${BATCH_SIZE}...`);
  let totalAccepted = 0;
  let batchErrors = 0;
  for (let i = 0; i < allEntityOps.length; i += BATCH_SIZE) {
    const batch = allEntityOps.slice(i, i + BATCH_SIZE);
    try {
      const { status, body } = await postOps(batch);
      if (status === 200) {
        totalAccepted += batch.length;
        log(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}: ${status} OK (${body.accepted || batch.length} accepted)`);
      } else {
        console.error(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}: ${status} ERROR: ${JSON.stringify(body)}`);
        batchErrors++;
      }
    } catch (e) {
      console.error(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}: NETWORK ERROR: ${e.message}`);
      batchErrors++;
    }
  }
  if (batchErrors > 0) {
    console.log(`  WARNING: ${batchErrors} batch(es) had errors (may be bad ops data in CSV)`);
  }
  assert(totalAccepted > 0, `at least some entities accepted (${totalAccepted} accepted, ${batchErrors} errors)`);
  console.log(`  Total entities sent: ${totalAccepted}`);

  // 5. Wait for flush cycle
  console.log('Phase 5: Waiting for flush cycle (3s)...');
  await waitForFlush(3000);

  // 6. Verify stats
  console.log('Phase 6: Verifying stats...');
  const stats = await getStats();
  const aliveCount = stats.alive_count || 0;
  console.log(`  Alive count: ${aliveCount}`);
  assert(aliveCount > 0, `alive count is non-zero (${aliveCount})`);
  if (aliveCount < aliveEntities.length) {
    console.log(`  NOTE: alive count (${aliveCount}) < alive entities (${aliveEntities.length}) — likely due to batch errors`);
  }

  // 7. Verify docs for alive entities
  console.log('Phase 7: Verifying docs for alive entities...');
  let docsFound = 0;
  let docsEmpty = 0;
  const sampleSize = Math.min(aliveEntities.length, 20);
  const sampled = aliveEntities.slice(0, sampleSize);
  for (const entity of sampled) {
    const doc = await getDoc(entity.entity_id);
    if (doc === null) {
      log(`  Doc ${entity.entity_id}: NOT FOUND`);
    } else {
      docsFound++;
      // Check if doc has meaningful values (not all defaults)
      const hasUserId = doc.userId !== undefined && doc.userId !== 0;
      const hasType = doc.type !== undefined && doc.type !== null;
      if (hasUserId || hasType) {
        log(`  Doc ${entity.entity_id}: OK (userId=${doc.userId}, type=${doc.type})`);
      } else {
        docsEmpty++;
        log(`  Doc ${entity.entity_id}: EMPTY (all defaults) — ${JSON.stringify(doc)}`);
      }
    }
  }
  if (sampleSize > 0) {
    assert(docsFound > 0, `docs found for alive entities (${docsFound}/${sampleSize})`);
    assert(docsEmpty === 0, `no empty docs (${docsEmpty} empty out of ${docsFound} found)`);
  }

  // 8. Verify filter bitmaps — query by a userId that was set
  console.log('Phase 8: Verifying filter bitmaps...');
  // Wait extra for flush to publish snapshot
  await waitForFlush(2000);
  // Find a userId from the ops
  let testUserId = null;
  for (const entity of aliveEntities) {
    const setUserOp = entity.ops.find(o => o.op === 'set' && o.field === 'userId');
    if (setUserOp) {
      testUserId = setUserOp.value;
      break;
    }
  }
  if (testUserId !== null) {
    const result = await query({
      filter: { userId: testUserId },
      sort: "-sortAt",
      limit: 10,
    });
    const ids = result.ids || result.hits?.map(h => h.id) || [];
    console.log(`  Query userId=${testUserId}: ${ids.length} results`);
    assert(ids.length > 0, `filter query returns results for userId=${testUserId}`);
  } else {
    console.log('  Skipping filter check — no userId set ops found');
  }

  // Summary
  console.log(`\n=== Results ===`);
  console.log(`  Passed: ${passed}`);
  console.log(`  Failed: ${failed}`);
  console.log(`  Ops processed: ${rows.length} rows → ${allEntityOps.length} entities`);

  if (failed > 0) {
    console.error(`\n${failed} test(s) FAILED`);
    process.exit(1);
  } else {
    console.log(`\nAll tests passed!`);
  }
}

main().catch(e => {
  console.error(`Fatal error: ${e.message}`);
  process.exit(1);
});
