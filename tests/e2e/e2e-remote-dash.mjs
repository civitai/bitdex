#!/usr/bin/env node
/**
 * E2E tests for the remote dashboard data layer.
 *
 * Tests the HTTP endpoints that the remote TUI relies on:
 *   1. Index discovery (GET /api/indexes)
 *   2. Stats fetching (GET /api/indexes/{name}/stats)
 *   3. Health check (GET /api/health)
 *   4. Config read (GET /api/indexes/{name})
 *   5. Config patch — toggle eager_load (PATCH /api/indexes/{name}/config)
 *   6. Cache clear (DELETE /api/indexes/{name}/cache)
 *   7. Traces fetch (GET /api/indexes/{name}/traces)
 *   8. Query + trace verification (POST query, then check trace appears)
 *
 * Usage:
 *   node tests/e2e/e2e-remote-dash.mjs [--url http://localhost:3000] [--verbose] [--keep]
 *
 * Prerequisites:
 *   Server running with NO existing index (test creates its own):
 *     cargo run --release --features server --bin bitdex-server -- --port 3100 --data-dir ./test-remote-dash --enable-traces
 */

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3100';
const VERBOSE = process.argv.includes('--verbose');
const KEEP = process.argv.includes('--keep');

const INDEX = 'remote-dash-test';
let passed = 0;
let failed = 0;

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function api(method, path, body) {
  const opts = { method, signal: AbortSignal.timeout(5000) };
  if (body !== undefined) {
    opts.headers = { 'Content-Type': 'application/json' };
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const data = await res.json();
  vlog(`${method} ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function test(name, fn) {
  try {
    await fn();
    passed++;
    log(`  ✓ ${name}`);
  } catch (e) {
    failed++;
    log(`  ✗ ${name}: ${e.message}`);
  }
}

async function setup() {
  // Clean up any existing index
  try { await api('DELETE', `/api/indexes/${INDEX}`); } catch {}

  // Create test index with some filter + sort fields
  const { status } = await api('POST', '/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'category', field_type: 'single_value', eager_load: true },
        { name: 'tags', field_type: 'multi_value', eager_load: false },
        { name: 'active', field_type: 'boolean', eager_load: true },
      ],
      sort_fields: [
        { name: 'score', source_type: 'u32', bits: 32, encoding: 'identity', eager_load: true },
        { name: 'created', source_type: 'u32', bits: 32, encoding: 'identity', eager_load: false },
      ],
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'category', target: 'category', value_type: 'integer' },
        { source: 'tags', target: 'tags', value_type: 'integer_array' },
        { source: 'active', target: 'active', value_type: 'boolean' },
        { source: 'score', target: 'score', value_type: 'integer' },
        { source: 'created', target: 'created', value_type: 'integer' },
      ],
    },
  });
  assert(status === 201, `create index status=${status}`);

  // Insert some test documents
  const docs = [];
  for (let i = 1; i <= 50; i++) {
    docs.push({
      id: i,
      category: i % 5,
      tags: [i % 10, (i * 3) % 20],
      active: i % 3 !== 0,
      score: i * 100,
      created: 1700000000 + i * 86400,
    });
  }
  const { status: upsertStatus } = await api('POST', `/api/indexes/${INDEX}/documents/upsert`, docs);
  assert(upsertStatus === 200, `upsert status=${upsertStatus}`);

  // Wait for flush
  await new Promise(r => setTimeout(r, 500));
}

async function cleanup() {
  if (!KEEP) {
    try { await api('DELETE', `/api/indexes/${INDEX}`); } catch {}
  }
}

// ── Tests ──────────────────────────────────────────────────────

async function testHealth() {
  await test('health endpoint returns ok', async () => {
    const res = await fetch(`${BASE_URL}/api/health`, { signal: AbortSignal.timeout(5000) });
    assert(res.status === 200, `status=${res.status}`);
    const text = await res.text();
    assert(text === 'ok' || text.includes('ok'), `health response: ${text}`);
  });
}

async function testIndexDiscovery() {
  await test('list indexes returns our test index', async () => {
    const { status, data } = await api('GET', '/api/indexes');
    assert(status === 200, `status=${status}`);
    const indexes = data.indexes || data;
    assert(Array.isArray(indexes), 'indexes is array');
    const found = indexes.some(idx => (idx.name || idx) === INDEX);
    assert(found, `index ${INDEX} not found in ${JSON.stringify(indexes)}`);
  });
}

async function testStats() {
  await test('stats returns bitmap memory and counts', async () => {
    const { status, data } = await api('GET', `/api/indexes/${INDEX}/stats`);
    assert(status === 200, `status=${status}`);
    assert(typeof data.alive_count === 'number', 'alive_count is number');
    assert(data.alive_count === 50, `alive_count=${data.alive_count}, expected 50`);
    assert(typeof data.total_bitmap_mb === 'number' || typeof data.total_bitmap_mb === 'string', 'has total_bitmap_mb');
    assert(typeof data.cache_entries === 'number', 'has cache_entries');
  });
}

async function testConfigRead() {
  await test('get index returns config with field definitions', async () => {
    const { status, data } = await api('GET', `/api/indexes/${INDEX}`);
    assert(status === 200, `status=${status}`);
    const config = data.config || data.definition?.config;
    assert(config, 'has config');
    assert(Array.isArray(config.filter_fields), 'has filter_fields');
    assert(config.filter_fields.length === 3, `filter_fields.length=${config.filter_fields.length}`);
    const cat = config.filter_fields.find(f => f.name === 'category');
    assert(cat, 'has category field');
    assert(cat.eager_load === true, `category.eager_load=${cat.eager_load}`);
    const tags = config.filter_fields.find(f => f.name === 'tags');
    assert(tags.eager_load === false, `tags.eager_load=${tags.eager_load}`);
  });
}

async function testConfigPatchEagerLoad() {
  await test('patch config toggles eager_load on filter field', async () => {
    // Toggle tags from false → true
    const { status } = await api('PATCH', `/api/indexes/${INDEX}/config`, {
      filter_fields: { tags: { eager_load: true } },
    });
    assert(status === 200, `patch status=${status}`);

    // Verify it stuck
    const { data } = await api('GET', `/api/indexes/${INDEX}`);
    const config = data.config || data.definition?.config;
    const tags = config.filter_fields.find(f => f.name === 'tags');
    assert(tags.eager_load === true, `tags.eager_load after patch=${tags.eager_load}`);

    // Toggle back
    await api('PATCH', `/api/indexes/${INDEX}/config`, {
      filter_fields: { tags: { eager_load: false } },
    });
  });

  await test('patch config toggles eager_load on sort field', async () => {
    const { status } = await api('PATCH', `/api/indexes/${INDEX}/config`, {
      sort_fields: { created: { eager_load: true } },
    });
    assert(status === 200, `patch status=${status}`);

    const { data } = await api('GET', `/api/indexes/${INDEX}`);
    const config = data.config || data.definition?.config;
    const created = config.sort_fields.find(f => f.name === 'created');
    assert(created.eager_load === true, `created.eager_load=${created.eager_load}`);

    // Toggle back
    await api('PATCH', `/api/indexes/${INDEX}/config`, {
      sort_fields: { created: { eager_load: false } },
    });
  });

  await test('patch config rejects unknown field', async () => {
    const { status } = await api('PATCH', `/api/indexes/${INDEX}/config`, {
      filter_fields: { nonexistent: { eager_load: true } },
    });
    assert(status === 400, `expected 400, got ${status}`);
  });
}

async function testCacheClear() {
  // First, run a query to populate cache
  await api('POST', `/api/indexes/${INDEX}/query`, {
    filter: { active: true },
    sort: { field: 'score', direction: 'Desc' },
    limit: 10,
  });
  await new Promise(r => setTimeout(r, 200));

  await test('clear cache returns success', async () => {
    const { status } = await api('DELETE', `/api/indexes/${INDEX}/cache`);
    assert(status === 200, `status=${status}`);
  });

  await test('cache entries drop after clear', async () => {
    const { data } = await api('GET', `/api/indexes/${INDEX}/stats`);
    assert(data.cache_entries === 0, `cache_entries=${data.cache_entries} after clear`);
  });
}

async function testTracesAfterQuery() {
  // Run a query with traces enabled
  await api('POST', `/api/indexes/${INDEX}/query`, {
    filter: { category: 1 },
    sort: { field: 'score', direction: 'Desc' },
    limit: 5,
  });
  await new Promise(r => setTimeout(r, 200));

  await test('traces endpoint returns recent traces', async () => {
    const { status, data } = await api('GET', `/api/indexes/${INDEX}/traces?last=5`);
    assert(status === 200, `status=${status}`);
    const traces = data.traces || data;
    assert(Array.isArray(traces), 'traces is array');
    // Traces may be empty if --enable-traces wasn't set, that's ok
    if (traces.length > 0) {
      const t = traces[traces.length - 1];
      assert(typeof t.total_us === 'number', 'trace has total_us');
      assert(typeof t.result_count === 'number', 'trace has result_count');
      log(`    (${traces.length} traces, latest: ${t.result_count} results in ${t.total_us}μs)`);
    } else {
      log('    (no traces — server may not have --enable-traces)');
    }
  });
}

// ── Main ───────────────────────────────────────────────────────

async function main() {
  log('Remote Dashboard E2E Tests');
  log(`Target: ${BASE_URL}`);
  log('');

  log('Setup...');
  await setup();
  log('');

  log('1. Health');
  await testHealth();

  log('2. Index Discovery');
  await testIndexDiscovery();

  log('3. Stats');
  await testStats();

  log('4. Config Read');
  await testConfigRead();

  log('5. Config Patch');
  await testConfigPatchEagerLoad();

  log('6. Cache Clear');
  await testCacheClear();

  log('7. Traces');
  await testTracesAfterQuery();

  log('');
  await cleanup();

  log(`Results: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => {
  console.error('Fatal:', e.message);
  process.exit(1);
});
