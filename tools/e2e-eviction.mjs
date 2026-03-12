#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Idle Eviction
 *
 * Tests the full eviction lifecycle:
 *   1. Create a test index with eviction enabled (low idle_seconds)
 *   2. Insert docs with known multi_value tag IDs
 *   3. Query to trigger lazy-load of specific tags
 *   4. Verify tags are resident via stats endpoint
 *   5. Pump mutations to drive flush cycles past the idle threshold
 *   6. Verify eviction occurred (resident count drops, evicted_total increases)
 *   7. Re-query evicted tag to trigger reload
 *   8. Verify resident count increases again
 *   9. Clean up (delete test index)
 *
 * Usage:
 *   node tools/e2e-eviction.mjs [--url http://localhost:3000] [--verbose] [--keep]
 *
 * Prerequisites:
 *   Server running with NO existing index (or use --keep to skip cleanup):
 *     cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-eviction-data
 */

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const VERBOSE = process.argv.includes('--verbose');
const KEEP = process.argv.includes('--keep'); // don't delete index after test

const INDEX = 'eviction-test';

let passed = 0;
let failed = 0;

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function apiPost(path, body) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  const data = await res.json();
  vlog(`GET ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiDelete(path) {
  const res = await fetch(`${BASE_URL}${path}`, { method: 'DELETE' });
  const data = await res.json();
  vlog(`DELETE ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

// Convenience wrappers for the test index
const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const query = (filters, sort, limit = 20) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, sort, limit }).then(r => r.data);
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const saveSnapshot = () =>
  apiPost(`/api/indexes/${INDEX}/snapshot`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create test index with eviction ---');

  // Delete existing test index if present
  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    log('  Deleting existing test index...');
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(500);
  }

  // Create index with eviction on tagIds (idle_seconds very low for testing)
  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'nsfwLevel', field_type: 'single_value' },
        {
          name: 'tagIds',
          field_type: 'multi_value',
          eviction: { idle_seconds: 0.5 },  // 500ms idle → evict
        },
      ],
      sort_fields: [
        { name: 'reactionCount', bits: 32 },
      ],
      max_page_size: 100,
      flush_interval_us: 50, // Fast flush for quick cycle accumulation
      eviction_sweep_interval: 5, // Check every 5 flush cycles (default 1000)
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'tagIds', target: 'tagIds', value_type: 'integer_array' },
        { source: 'reactionCount', target: 'reactionCount', value_type: 'integer' },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created (tagIds eviction: idle_seconds=0.5)`);

  // Insert 100 docs with various tagIds
  // Tags 1-5: common (appear in many docs), Tags 100-105: rare (appear in few docs)
  const docs = [];
  for (let i = 1; i <= 100; i++) {
    const tags = [1]; // all docs have tag 1
    if (i <= 50) tags.push(2);      // 50 docs have tag 2
    if (i <= 10) tags.push(100);    // 10 docs have tag 100 (rare)
    if (i <= 5)  tags.push(101);    // 5 docs have tag 101 (rare)
    if (i <= 2)  tags.push(102);    // 2 docs have tag 102 (very rare)
    docs.push({
      id: i,
      nsfwLevel: 1,
      tagIds: tags,
      reactionCount: 100 - i, // descending by id
    });
  }

  const res = await upsert(docs);
  assert(res.upserted === 100, `Expected 100 upserted, got ${res.upserted}`);
  log(`  Inserted 100 documents with tags [1, 2, 100, 101, 102]`);

  // Wait for flush to process all mutations
  await sleep(500);

  // Save bitmap snapshot to disk so evicted values can be reloaded later
  const snap = await saveSnapshot();
  log(`  Bitmap snapshot saved in ${snap.elapsed_secs?.toFixed(3)}s`);

  const s = await stats();
  log(`  Stats: alive=${s.alive_count}, slot_count=${s.slot_count}, flush_cycle=${s.flush_cycle}`);
  assert(s.alive_count === 100, `Expected 100 alive, got ${s.alive_count}`);
}

// ----- Test A: Query triggers loading, values become resident -----

async function testA_QueryTriggersLoading() {
  log('\n--- A. Query Triggers Value Loading ---');

  // Query for tag 100 — should trigger lazy load
  const r = await query(
    [{ In: ['tagIds', [{ Integer: 100 }]] }],
    { field: 'reactionCount', direction: 'Desc' },
    20,
  );
  assert(r.ids && r.ids.length === 10, `Expected 10 results for tag 100, got ${r.ids?.length}`);
  log(`  [1] Query for tagIds IN [100]: ${r.ids.length} results`);

  // Also query tag 101
  const r2 = await query(
    [{ In: ['tagIds', [{ Integer: 101 }]] }],
    { field: 'reactionCount', direction: 'Desc' },
    20,
  );
  assert(r2.ids && r2.ids.length === 5, `Expected 5 results for tag 101, got ${r2.ids?.length}`);
  log(`  [2] Query for tagIds IN [101]: ${r2.ids.length} results`);

  // Check stats — should show resident values for tagIds
  await sleep(300);
  const s = await stats();
  const evictionEntry = s.eviction?.find(e => e.field === 'tagIds');
  log(`  [3] Eviction stats: ${JSON.stringify(evictionEntry)}`);
  // At this point, tags 100 and 101 should be resident (loaded by queries above)
  // Plus tag 1 and tag 2 which were loaded when the docs were inserted
  if (evictionEntry) {
    assert(evictionEntry.resident_values >= 2,
      `Expected >= 2 resident values for tagIds, got ${evictionEntry.resident_values}`);
    log(`  [4] Resident values: ${evictionEntry.resident_values}, evicted_total: ${evictionEntry.evicted_total}`);
  } else {
    log(`  [4] No eviction stats yet (field may not be tracked until first sweep)`);
  }
}

// ----- Test B: Idle values get evicted -----

async function testB_IdleEviction() {
  log('\n--- B. Idle Values Get Evicted ---');

  // Record baseline
  let s = await stats();
  const baselineCycle = s.flush_cycle;
  const baselineEvictionEntry = s.eviction?.find(e => e.field === 'tagIds');
  const baselineEvicted = baselineEvictionEntry?.evicted_total || 0;
  const baselineResident = baselineEvictionEntry?.resident_values || 0;
  log(`  [1] Baseline: cycle=${baselineCycle}, resident=${baselineResident}, evicted=${baselineEvicted}`);

  // Keep querying tag 1 (to keep it alive) while letting tags 100/101/102 go idle.
  // Pump mutations via small upserts to drive flush cycles.
  // With flush_interval_us=50 and idle_seconds=0.5, we need ~10K+ cycles.
  // Each mutation pumps 1 cycle, so we need to be smart about this.
  // Strategy: rapid-fire upserts of a single doc to tick the flush cycle counter.
  log(`  [2] Pumping flush cycles for ~3 seconds (keeping tag 1 alive, letting 100/101 go idle)...`);

  const pumpStart = Date.now();
  let pumpCount = 0;
  let queryCount = 0;
  while (Date.now() - pumpStart < 3000) {
    // Upsert doc 1 to tick flush cycle
    await upsert([{ id: 1, nsfwLevel: 1, tagIds: [1, 2], reactionCount: 99 }]);
    pumpCount++;
    // Query tag 1 every 5 upserts to keep it alive (refresh eviction stamp)
    if (pumpCount % 5 === 0) {
      await query(
        [{ In: ['tagIds', [{ Integer: 1 }]] }],
        { field: 'reactionCount', direction: 'Desc' },
        5,
      );
      queryCount++;
    }
    // Small delay to not overwhelm the server
    await sleep(5);
  }

  s = await stats();
  const afterCycle = s.flush_cycle;
  const afterEvictionEntry = s.eviction?.find(e => e.field === 'tagIds');
  const afterEvicted = afterEvictionEntry?.evicted_total || 0;
  const afterResident = afterEvictionEntry?.resident_values || 0;

  log(`  [3] After pumping: ${pumpCount} upserts, cycle ${baselineCycle}→${afterCycle} (+${afterCycle - baselineCycle})`);
  log(`  [4] Eviction: resident ${baselineResident}→${afterResident}, evicted_total ${baselineEvicted}→${afterEvicted}`);

  // Verify eviction occurred
  assert(afterEvicted > baselineEvicted,
    `Expected evicted_total to increase: ${baselineEvicted} → ${afterEvicted}`);
  log(`  [5] CONFIRMED: ${afterEvicted - baselineEvicted} values evicted`);

  // Verify tags 100/101 are no longer resident (they had no queries during pump)
  // Tag 1 should still be resident (we queried it)
  assert(afterResident < baselineResident,
    `Expected resident to decrease: ${baselineResident} → ${afterResident}`);
  log(`  [6] CONFIRMED: resident values decreased (${baselineResident} → ${afterResident})`);
}

// ----- Test C: Re-query triggers reload -----

async function testC_ReloadAfterEviction() {
  log('\n--- C. Re-query Reloads Evicted Values ---');

  // Clear unified cache so the query must go through the filter path
  // (otherwise cache hit would bypass the evicted filter bitmap)
  await apiDelete(`/api/indexes/${INDEX}/cache`);
  log(`  [0] Cleared unified cache`);

  let s = await stats();
  const beforeResident = s.eviction?.find(e => e.field === 'tagIds')?.resident_values || 0;
  log(`  [1] Before reload: resident=${beforeResident}`);

  // Query tag 100 again — should trigger lazy reload from disk
  const r = await query(
    [{ In: ['tagIds', [{ Integer: 100 }]] }],
    { field: 'reactionCount', direction: 'Desc' },
    20,
  );
  assert(r.ids && r.ids.length === 10,
    `Expected 10 results for tag 100 after reload, got ${r.ids?.length}`);
  log(`  [2] Query for tag 100 after eviction: ${r.ids.length} results (correct!)`);

  // Wait for flush to process the lazy load
  await sleep(500);

  s = await stats();
  const afterResident = s.eviction?.find(e => e.field === 'tagIds')?.resident_values || 0;
  log(`  [3] After reload: resident=${afterResident}`);

  assert(afterResident > beforeResident,
    `Expected resident to increase after reload: ${beforeResident} → ${afterResident}`);
  log(`  [4] CONFIRMED: value reloaded (resident ${beforeResident} → ${afterResident})`);
}

// ----- Test D: Nonexistent tags don't load (existence set) -----

async function testD_NonexistentTagSkipped() {
  log('\n--- D. Nonexistent Tag Query (Existence Set) ---');

  // Query for tag 999999 which doesn't exist in any document
  const r = await query(
    [{ In: ['tagIds', [{ Integer: 999999 }]] }],
    { field: 'reactionCount', direction: 'Desc' },
    20,
  );
  assert(r.ids && r.ids.length === 0, `Expected 0 results for nonexistent tag, got ${r.ids?.length}`);
  log(`  [1] Query for tag 999999: ${r.ids.length} results (correct — doesn't exist)`);

  // The query should be fast because the existence set filters it before disk
  if (r.elapsed_us !== undefined) {
    log(`  [2] Latency: ${r.elapsed_us}us (should be very fast — no disk lookup)`);
    assert(r.elapsed_us < 5000, `Expected < 5ms for nonexistent tag, got ${r.elapsed_us}us`);
  }
}

// ----- Cleanup -----

async function cleanup() {
  if (KEEP) {
    log(`\n  --keep specified, leaving index '${INDEX}' in place`);
    return;
  }
  log(`\n--- Cleanup: Deleting test index ---`);
  await apiDelete(`/api/indexes/${INDEX}`);
  log(`  Index '${INDEX}' deleted`);
}

// ----- Runner -----

const groups = [
  ['Setup', 'Create test index + insert data', setup],
  ['A', 'Query triggers value loading', testA_QueryTriggersLoading],
  ['B', 'Idle values get evicted', testB_IdleEviction],
  ['C', 'Re-query reloads evicted values', testC_ReloadAfterEviction],
  ['D', 'Nonexistent tag skipped (existence set)', testD_NonexistentTagSkipped],
];

async function main() {
  log('BitDex Idle Eviction E2E Tests');
  log(`Server: ${BASE_URL}`);
  log(`Index:  ${INDEX}`);

  // Verify server is reachable
  try {
    const res = await fetch(`${BASE_URL}/api/indexes`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    log('Server is reachable');
  } catch (e) {
    log(`\nERROR: Cannot reach server at ${BASE_URL}`);
    log(`  ${e.message}`);
    log(`\nStart the server first (fresh, no existing index):`);
    log(`  cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-eviction-data`);
    process.exit(1);
  }

  for (const [id, name, fn] of groups) {
    try {
      await fn();
      passed++;
      log(`  PASS: ${id}. ${name}`);
    } catch (e) {
      failed++;
      log(`  FAIL: ${id}. ${name}`);
      log(`    ${e.message}`);
      if (VERBOSE) console.error(e.stack);
    }
  }

  await cleanup();

  log(`\n${'='.repeat(50)}`);
  log(`Results: ${passed} passed, ${failed} failed out of ${groups.length}`);
  log(`${'='.repeat(50)}`);

  process.exit(failed > 0 ? 1 : 0);
}

main();
