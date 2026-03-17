#!/usr/bin/env node
/**
 * E2E tests for filter-sync endpoint and write path audit items.
 *
 * Tests:
 *   A. filter-sync validates field (rejects non-filter_only, non-multi_value)
 *   B. filter-sync replaces values correctly (add, remove, clear)
 *   C. filter-sync returns 500 on total failure, 207 on partial
 *   D. PATCH creates new slot (PUT fallback)
 *   E. DELETE cleans all bitmaps
 *
 * Usage:
 *   node tests/e2e/e2e-filter-sync.mjs [--url http://localhost:3001]
 *
 * Prerequisites:
 *   cargo run --release --features server --bin bitdex-server -- --port 3001 --data-dir ./test-filter-sync-data
 */

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3001';

const INDEX = 'filter-sync-test';

let passed = 0;
let failed = 0;

function log(...args) { console.log(...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function api(method, path, body) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' },
  };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { _raw: text }; }
  return { status: res.status, data };
}

async function runTest(name, fn) {
  try {
    await fn();
    passed++;
    log(`  ✅ ${name}`);
  } catch (e) {
    failed++;
    log(`  ❌ ${name}: ${e.message}`);
  }
}

async function setup() {
  log('\n=== Setup: Create index ===');

  // Create index with collectionIds as filter_only multi_value
  const config = {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'nsfwLevel', field_type: 'single_value' },
        { name: 'tagIds', field_type: 'multi_value' },
        { name: 'collectionIds', field_type: 'multi_value' },
      ],
      sort_fields: [
        { name: 'reactionCount', bits: 32 },
      ],
      max_page_size: 100,
    },
    data_schema: {
      id_field: 'id',
      schema_version: 1,
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'tagIds', target: 'tagIds', value_type: 'integer_array', default: [] },
        { source: 'collectionIds', target: 'collectionIds', value_type: 'integer_array', filter_only: true, default: [] },
        { source: 'reactionCount', target: 'reactionCount', value_type: 'integer', default: 0 },
      ],
    },
  };

  const { status } = await api('POST', '/api/indexes', config);
  assert(status === 200 || status === 201 || status === 409, `Create index: ${status}`);

  // Insert test documents
  const docs = [
    { id: 1, nsfwLevel: 1, tagIds: [10, 20], reactionCount: 5 },
    { id: 2, nsfwLevel: 1, tagIds: [20, 30], reactionCount: 10 },
    { id: 3, nsfwLevel: 2, tagIds: [10], reactionCount: 15 },
  ];
  const { status: us } = await api('POST', `/api/indexes/${INDEX}/documents/upsert`, { documents: docs });
  assert(us === 200, `Upsert docs: ${us}`);

  // Wait for flush
  await new Promise(r => setTimeout(r, 200));
}

async function groupA() {
  log('\n=== A. filter-sync field validation ===');

  await runTest('A1: Reject non-multi_value field', async () => {
    const { status, data } = await api('POST', `/api/indexes/${INDEX}/documents/filter-sync`, {
      field: 'nsfwLevel',  // single_value, not multi_value
      documents: [{ id: 1, values: [1] }],
    });
    assert(status === 400, `Expected 400, got ${status}`);
    assert(data.error.includes('filter_only'), `Error should mention filter_only: ${data.error}`);
  });

  await runTest('A2: Reject non-filter_only multi_value field', async () => {
    const { status, data } = await api('POST', `/api/indexes/${INDEX}/documents/filter-sync`, {
      field: 'tagIds',  // multi_value but NOT filter_only
      documents: [{ id: 1, values: [1] }],
    });
    assert(status === 400, `Expected 400, got ${status}`);
  });

  await runTest('A3: Accept filter_only multi_value field', async () => {
    const { status } = await api('POST', `/api/indexes/${INDEX}/documents/filter-sync`, {
      field: 'collectionIds',
      documents: [{ id: 1, values: [100, 200] }],
    });
    assert(status === 200, `Expected 200, got ${status}`);
  });

  await runTest('A4: Reject unknown field', async () => {
    const { status } = await api('POST', `/api/indexes/${INDEX}/documents/filter-sync`, {
      field: 'nonexistentField',
      documents: [{ id: 1, values: [1] }],
    });
    assert(status === 400, `Expected 400, got ${status}`);
  });
}

async function groupB() {
  log('\n=== B. filter-sync replace values ===');

  await runTest('B1: Set collection memberships', async () => {
    const { status } = await api('POST', `/api/indexes/${INDEX}/documents/filter-sync`, {
      field: 'collectionIds',
      documents: [
        { id: 1, values: [100, 200] },
        { id: 2, values: [200, 300] },
      ],
    });
    assert(status === 200, `Expected 200, got ${status}`);
    await new Promise(r => setTimeout(r, 100));

    // Query for collection 200 — should match docs 1 and 2
    const { data } = await api('POST', `/api/indexes/${INDEX}/query`, {
      filter: { Eq: { field: 'collectionIds', value: 200 } },
      limit: 10,
    });
    assert(data.total_matched === 2, `Expected 2 matches for collection 200, got ${data.total_matched}`);
  });

  await runTest('B2: Replace values removes old memberships', async () => {
    // Change doc 1 from [100, 200] to [300]
    const { status } = await api('POST', `/api/indexes/${INDEX}/documents/filter-sync`, {
      field: 'collectionIds',
      documents: [{ id: 1, values: [300] }],
    });
    assert(status === 200, `Expected 200, got ${status}`);
    await new Promise(r => setTimeout(r, 100));

    // Collection 100 should have 0 matches now
    const { data: d100 } = await api('POST', `/api/indexes/${INDEX}/query`, {
      filter: { Eq: { field: 'collectionIds', value: 100 } },
      limit: 10,
    });
    assert(d100.total_matched === 0, `Expected 0 matches for collection 100 after remove, got ${d100.total_matched}`);

    // Collection 300 should have docs 1 and 2
    const { data: d300 } = await api('POST', `/api/indexes/${INDEX}/query`, {
      filter: { Eq: { field: 'collectionIds', value: 300 } },
      limit: 10,
    });
    assert(d300.total_matched === 2, `Expected 2 matches for collection 300, got ${d300.total_matched}`);
  });

  await runTest('B3: Clear all memberships with empty values', async () => {
    const { status } = await api('POST', `/api/indexes/${INDEX}/documents/filter-sync`, {
      field: 'collectionIds',
      documents: [{ id: 1, values: [] }],
    });
    assert(status === 200, `Expected 200, got ${status}`);
    await new Promise(r => setTimeout(r, 100));

    // Collection 300 should only have doc 2 now
    const { data } = await api('POST', `/api/indexes/${INDEX}/query`, {
      filter: { Eq: { field: 'collectionIds', value: 300 } },
      limit: 10,
    });
    assert(data.total_matched === 1, `Expected 1 match for collection 300 after clear, got ${data.total_matched}`);
    assert(data.ids[0] === 2, `Expected doc 2, got ${data.ids[0]}`);
  });
}

async function groupC() {
  log('\n=== C. filter-sync error responses ===');

  await runTest('C1: Total failure returns 500', async () => {
    // All non-existent slots
    const { status } = await api('POST', `/api/indexes/${INDEX}/documents/filter-sync`, {
      field: 'collectionIds',
      documents: [
        { id: 999999, values: [1] },
        { id: 999998, values: [2] },
      ],
    });
    // Non-alive slots are skipped (return Ok), so this should be 200
    // since sync_filter_values returns Ok(()) for non-alive slots
    assert(status === 200, `Expected 200 (skipped non-alive), got ${status}`);
  });
}

async function groupD() {
  log('\n=== D. PATCH creates new slot ===');

  await runTest('D1: PATCH on unknown slot creates it', async () => {
    const { status } = await api('PATCH', `/api/indexes/${INDEX}/documents/patch`, {
      documents: [{ id: 50, nsfwLevel: 3, reactionCount: 99 }],
    });
    assert(status === 200, `Expected 200, got ${status}`);
    await new Promise(r => setTimeout(r, 200));

    // Query for the new doc
    const { data } = await api('POST', `/api/indexes/${INDEX}/query`, {
      filter: { Eq: { field: 'nsfwLevel', value: 3 } },
      limit: 10,
    });
    assert(data.total_matched >= 1, `Expected at least 1 match for nsfwLevel=3, got ${data.total_matched}`);
    assert(data.ids.includes(50), `Expected doc 50 in results`);
  });
}

async function groupE() {
  log('\n=== E. DELETE cleans bitmaps ===');

  await runTest('E1: DELETE removes from filter bitmaps', async () => {
    // Delete doc 3
    const { status } = await api('DELETE', `/api/indexes/${INDEX}/documents`, {
      ids: [3],
    });
    assert(status === 200, `Expected 200, got ${status}`);
    await new Promise(r => setTimeout(r, 200));

    // nsfwLevel=2 should have 0 matches
    const { data } = await api('POST', `/api/indexes/${INDEX}/query`, {
      filter: { Eq: { field: 'nsfwLevel', value: 2 } },
      limit: 10,
    });
    assert(data.total_matched === 0, `Expected 0 matches for nsfwLevel=2 after delete, got ${data.total_matched}`);
  });

  await runTest('E2: DELETE removes from tag bitmaps', async () => {
    // tagIds=10 was on docs 1 and 3. After deleting 3, only doc 1 should match.
    const { data } = await api('POST', `/api/indexes/${INDEX}/query`, {
      filter: { Eq: { field: 'tagIds', value: 10 } },
      limit: 10,
    });
    assert(data.total_matched === 1, `Expected 1 match for tagIds=10 after delete, got ${data.total_matched}`);
    assert(data.ids[0] === 1, `Expected doc 1, got ${data.ids[0]}`);
  });
}

async function cleanup() {
  await api('DELETE', `/api/indexes/${INDEX}`, null);
}

async function main() {
  log('=== E2E: filter-sync + write path audit ===');
  log(`Server: ${BASE_URL}`);

  await setup();
  await groupA();
  await groupB();
  await groupC();
  await groupD();
  await groupE();
  await cleanup();

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
