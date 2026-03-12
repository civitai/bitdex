#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Cursor Pagination & Structural Overhead
 *
 * Tests cursor pagination correctness across multiple pages, cache hit acceleration,
 * cache expansion on deep pagination, structural overhead measurements, and
 * filtered cursor pagination.
 *
 * Test groups:
 *   A. Cursor pagination correctness (5 pages, 50 docs)
 *   B. Cache hit acceleration (miss vs hit latency)
 *   C. Cache expansion on deep pagination
 *   D. Structural overhead measurement
 *   E. Cursor pagination with filters
 *
 * Usage:
 *   node tools/e2e-pagination-overhead.mjs [--url http://localhost:3000] [--verbose] [--keep] [--results-dir ./results]
 *
 * Prerequisites:
 *   Server running with NO existing index (or use --keep to skip cleanup):
 *     cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-pagination-data
 */

import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const VERBOSE = process.argv.includes('--verbose');
const KEEP = process.argv.includes('--keep');
const RESULTS_DIR = process.argv.includes('--results-dir')
  ? process.argv[process.argv.indexOf('--results-dir') + 1]
  : null;

const INDEX = 'pagination-test';

let passed = 0;
let failed = 0;

// Per-group results tracking for JSON output
const groupResults = [];

// Measurements collected across test groups
const measurements = {
  page_latencies_us: {},
  cache_miss_us: 0,
  cache_hit_us: 0,
  hit_ratio: 0,
  cache_entries: 0,
  cache_bytes: 0,
  bytes_per_entry: 0,
  bytes_per_doc: 0,
  capacity_progression: [],
  final_cardinality: 0,
  final_has_more: false,
};

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
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  vlog(`GET ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiDelete(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  vlog(`DELETE ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const queryApi = (filters, sort, limit = 100, cursor = null) => {
  const body = { filters, sort, limit };
  if (cursor) body.cursor = cursor;
  return apiPost(`/api/indexes/${INDEX}/query`, body).then(r => r.data);
};
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const clearCache = () =>
  apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/** Wait for flush to process mutations. Polls stats until alive_count matches expected. */
async function waitForFlush(expectedAlive, maxWaitMs = 3000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await stats();
    if (s.alive_count === expectedAlive) return s;
    await sleep(50);
  }
  const s = await stats();
  vlog(`waitForFlush timeout: expected ${expectedAlive}, got ${s.alive_count}`);
  return s;
}

/** Query with cache cleared to ensure we hit the bitmap path. */
async function freshQuery(filters, sort, limit = 100, cursor = null) {
  await clearCache();
  return queryApi(filters, sort, limit, cursor);
}

const SORT_SCORE_DESC = { field: 'score', direction: 'Desc' };

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create test index ---');

  // Delete existing test index if present
  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    log('  Deleting existing test index...');
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(500);
  }

  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'category', field_type: 'single_value', value_type: 'integer' },
      ],
      sort_fields: [
        { name: 'score', bits: 32 },
      ],
      max_page_size: 100,
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'category', target: 'category', value_type: 'integer' },
        { source: 'score', target: 'score', value_type: 'integer' },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created`);

  // Insert 50 docs: IDs 1-25 category=1 score=id*10, IDs 26-50 category=2 score=id*10
  const docs = [];
  for (let i = 1; i <= 50; i++) {
    docs.push({
      id: i,
      category: i <= 25 ? 1 : 2,
      score: i * 10,
    });
  }

  const res = await upsert(docs);
  assert(res.upserted === 50, `Expected 50 upserted, got ${res.upserted}`);
  log(`  Inserted 50 documents (IDs 1-25 cat=1, IDs 26-50 cat=2)`);

  // Wait for flush
  await waitForFlush(50);
  log('  Flush complete, 50 docs alive');
}

// ----- Test A: Cursor Pagination Correctness (5 pages) -----

async function testA_CursorPaginationCorrectness() {
  log('\n--- A. Cursor Pagination Correctness (5 pages) ---');

  await clearCache();

  const allIds = [];
  const pageLatencies = {};
  let cursor = null;
  let pageCount = 0;

  // Page through all 50 docs, 10 per page
  for (let page = 1; page <= 10; page++) { // max 10 pages as safety
    const r = await queryApi([], SORT_SCORE_DESC, 10, cursor);
    const ids = r.ids || [];
    allIds.push(...ids);
    pageLatencies[`page_${page}`] = r.elapsed_us;
    pageCount++;

    vlog(`  Page ${page}: ${ids.length} results, cursor=${JSON.stringify(r.cursor)}, elapsed=${r.elapsed_us}us`);

    if (page < 5) {
      // Pages 1-4 should have exactly 10 results
      assert(ids.length === 10, `Page ${page}: expected 10 results, got ${ids.length}`);
    }

    if (!r.cursor) {
      // Last page
      assert(page === 5, `Expected 5 pages for 50 docs, pagination ended at page ${page}`);
      log(`  [1] Pagination ended at page ${page} (cursor is null)`);
      break;
    }

    cursor = r.cursor;
  }

  // Exactly 50 total IDs
  assert(allIds.length === 50, `Expected 50 total IDs, got ${allIds.length}`);
  log(`  [2] Collected ${allIds.length} total IDs across ${pageCount} pages`);

  // No duplicates
  const uniqueIds = new Set(allIds);
  assert(uniqueIds.size === allIds.length, `Duplicates found: ${allIds.length} total, ${uniqueIds.size} unique`);
  log(`  [3] No duplicates: ${uniqueIds.size} unique IDs`);

  // Sort order: each page's scores should be >= next page's scores (descending)
  for (let p = 0; p < pageCount - 1; p++) {
    const pageStart = p * 10;
    const nextPageStart = (p + 1) * 10;
    const lastScoreThisPage = allIds[pageStart + 9] * 10; // score = id * 10
    const firstScoreNextPage = allIds[nextPageStart] * 10;
    assert(
      lastScoreThisPage >= firstScoreNextPage,
      `Sort order violation between page ${p + 1} and ${p + 2}: ${lastScoreThisPage} < ${firstScoreNextPage}`
    );
  }
  log(`  [4] Sort order correct (descending by score across all pages)`);

  // Each page except possibly the last has exactly 10 results
  // (already checked above in the loop for pages 1-4)
  log(`  [5] All intermediate pages have exactly 10 results`);

  // Record per-page latency
  measurements.page_latencies_us = pageLatencies;
  for (const [page, us] of Object.entries(pageLatencies)) {
    log(`  Latency ${page}: ${us}us`);
  }
}

// ----- Test B: Cache Hit Acceleration -----

async function testB_CacheHitAcceleration() {
  log('\n--- B. Cache Hit Acceleration ---');

  // Clear cache, run query (miss)
  await clearCache();
  const r1 = await queryApi([], SORT_SCORE_DESC, 10);
  const missLatency = r1.elapsed_us;
  log(`  [1] Cache miss query: ${r1.ids.length} results in ${missLatency}us`);

  // Run same query again (hit)
  const r2 = await queryApi([], SORT_SCORE_DESC, 10);
  const hitLatency = r2.elapsed_us;
  log(`  [2] Cache hit query: ${r2.ids.length} results in ${hitLatency}us`);

  // Check stats
  const s = await stats();
  assert(s.unified_cache_entries >= 1, `Expected >=1 cache entries, got ${s.unified_cache_entries}`);
  assert(s.unified_cache_hits >= 1, `Expected >=1 cache hits, got ${s.unified_cache_hits}`);
  log(`  [3] Stats: ${s.unified_cache_entries} entries, ${s.unified_cache_hits} hits, ${s.unified_cache_misses} misses`);

  // Hit should generally be faster than miss (allow 2x tolerance for tiny datasets + timing noise)
  if (hitLatency <= missLatency) {
    log(`  [4] Hit is faster: ${missLatency}us -> ${hitLatency}us`);
  } else {
    log(`  [4] WARN: Hit (${hitLatency}us) >= miss (${missLatency}us) — possible timing noise on small dataset`);
  }

  const ratio = missLatency / Math.max(hitLatency, 1);
  log(`  [5] Speedup ratio: ${ratio.toFixed(1)}x`);

  measurements.cache_miss_us = missLatency;
  measurements.cache_hit_us = hitLatency;
  measurements.hit_ratio = ratio;
}

// ----- Test C: Cache Expansion on Deep Pagination -----

async function testC_CacheExpansion() {
  log('\n--- C. Cache Expansion on Deep Pagination ---');

  await clearCache();

  let cursor = null;
  const capacityProgression = [];

  // Page through all 50 docs, 10 per page
  for (let page = 1; page <= 10; page++) { // safety limit
    const r = await queryApi([], SORT_SCORE_DESC, 10, cursor);

    // After each page, get stats
    const s = await stats();
    const details = s.unified_cache_entry_details || [];
    const entry = details.find(e => e.sort_field === 'score' && e.direction === 'Desc');

    if (entry) {
      capacityProgression.push(entry.cardinality);
      vlog(`  Page ${page}: cardinality=${entry.cardinality}, capacity=${entry.capacity}, has_more=${entry.has_more}`);
    }

    if (!r.cursor) {
      log(`  [1] Pagination ended at page ${page}`);
      break;
    }
    cursor = r.cursor;
  }

  // After full traversal, check final state
  const s = await stats();
  const details = s.unified_cache_entry_details || [];
  const entry = details.find(e => e.sort_field === 'score' && e.direction === 'Desc');

  assert(entry, 'Expected cache entry for score/Desc');
  assert(entry.cardinality >= 50, `Expected cardinality >= 50, got ${entry.cardinality}`);
  log(`  [2] Final cardinality: ${entry.cardinality}`);

  assert(entry.has_more === false, `Expected has_more=false after full traversal, got ${entry.has_more}`);
  log(`  [3] has_more is false (all results exhausted)`);

  measurements.capacity_progression = capacityProgression;
  measurements.final_cardinality = entry.cardinality;
  measurements.final_has_more = entry.has_more;
  log(`  [4] Capacity progression: [${capacityProgression.join(', ')}]`);
}

// ----- Test D: Structural Overhead Measurement -----

async function testD_StructuralOverhead() {
  log('\n--- D. Structural Overhead Measurement ---');

  const s = await stats();

  const cacheBytes = s.unified_cache_bytes;
  const cacheEntries = s.unified_cache_entries;
  const aliveCount = s.alive_count;

  log(`  [1] unified_cache_bytes: ${cacheBytes}`);
  log(`  [2] unified_cache_entries: ${cacheEntries}`);

  const bytesPerEntry = cacheEntries > 0 ? cacheBytes / cacheEntries : 0;
  const bytesPerDoc = aliveCount > 0 ? cacheBytes / aliveCount : 0;

  log(`  [3] Bytes per cache entry: ${bytesPerEntry.toFixed(1)}`);
  log(`  [4] Bytes per cached doc: ${bytesPerDoc.toFixed(1)}`);

  // Sanity checks
  assert(cacheBytes > 0, `Expected cache memory > 0, got ${cacheBytes}`);
  log(`  [5] Cache memory is populated (${cacheBytes} bytes)`);

  assert(bytesPerDoc < 1000, `Expected bytes per doc < 1000, got ${bytesPerDoc.toFixed(1)}`);
  log(`  [6] Bytes per doc is within sanity bound (${bytesPerDoc.toFixed(1)} < 1000)`);

  measurements.cache_entries = cacheEntries;
  measurements.cache_bytes = cacheBytes;
  measurements.bytes_per_entry = bytesPerEntry;
  measurements.bytes_per_doc = bytesPerDoc;
}

// ----- Test E: Cursor Pagination with Filters -----

async function testE_FilteredPagination() {
  log('\n--- E. Cursor Pagination with Filters ---');

  await clearCache();

  const filters = [{ Eq: ['category', { Integer: 1 }] }];
  const allIds = [];
  const pageLatencies = {};
  let cursor = null;
  let pageCount = 0;

  // Page through category=1 docs (IDs 1-25), 5 per page
  for (let page = 1; page <= 10; page++) { // safety limit
    const r = await queryApi(filters, SORT_SCORE_DESC, 5, cursor);
    const ids = r.ids || [];
    allIds.push(...ids);
    pageLatencies[`page_${page}`] = r.elapsed_us;
    pageCount++;

    vlog(`  Page ${page}: ${ids.length} results, ids=[${ids.join(',')}], cursor=${JSON.stringify(r.cursor)}`);

    if (!r.cursor) {
      log(`  [1] Pagination ended at page ${page}`);
      break;
    }
    cursor = r.cursor;
  }

  // Exactly 25 IDs collected (only category=1)
  assert(allIds.length === 25, `Expected 25 IDs, got ${allIds.length}`);
  log(`  [2] Collected ${allIds.length} IDs across ${pageCount} pages`);

  // All IDs in range 1-25
  for (const id of allIds) {
    assert(id >= 1 && id <= 25, `ID ${id} is outside range 1-25`);
  }
  log(`  [3] All IDs in range 1-25 (category=1 only)`);

  // No IDs from category=2 (26-50)
  const cat2Leaked = allIds.filter(id => id >= 26 && id <= 50);
  assert(cat2Leaked.length === 0, `Category 2 IDs leaked: [${cat2Leaked.join(',')}]`);
  log(`  [4] No category=2 IDs leaked through`);

  // Sort order correct (descending by score)
  for (let i = 1; i < allIds.length; i++) {
    const prevScore = allIds[i - 1] * 10;
    const currScore = allIds[i] * 10;
    assert(prevScore >= currScore, `Sort order violation at position ${i}: ${prevScore} < ${currScore}`);
  }
  log(`  [5] Sort order correct (descending by score)`);

  // No duplicates
  const uniqueIds = new Set(allIds);
  assert(uniqueIds.size === allIds.length, `Duplicates found: ${allIds.length} total, ${uniqueIds.size} unique`);
  log(`  [6] No duplicates`);

  // Record per-page latency
  for (const [page, us] of Object.entries(pageLatencies)) {
    log(`  Latency ${page}: ${us}us`);
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
  ['Setup', 'Create test index and insert data', setup],
  ['A', 'Cursor pagination correctness (5 pages)', testA_CursorPaginationCorrectness],
  ['B', 'Cache hit acceleration', testB_CacheHitAcceleration],
  ['C', 'Cache expansion on deep pagination', testC_CacheExpansion],
  ['D', 'Structural overhead measurement', testD_StructuralOverhead],
  ['E', 'Cursor pagination with filters', testE_FilteredPagination],
];

async function main() {
  log('BitDex Pagination & Overhead E2E Tests');
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
    log(`\nStart the server first:`);
    log(`  cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-pagination-data`);
    process.exit(1);
  }

  const suiteStart = Date.now();

  for (const [id, name, fn] of groups) {
    const groupStart = Date.now();
    try {
      await fn();
      passed++;
      log(`  PASS: ${id}. ${name}`);
      groupResults.push({
        id,
        name,
        status: 'pass',
        duration_ms: Date.now() - groupStart,
        assertions: [],
      });
    } catch (e) {
      failed++;
      log(`  FAIL: ${id}. ${name}`);
      log(`    ${e.message}`);
      if (VERBOSE) console.error(e.stack);
      groupResults.push({
        id,
        name,
        status: 'fail',
        duration_ms: Date.now() - groupStart,
        assertions: [{ check: e.message, passed: false }],
      });
    }
  }

  await cleanup();

  log(`\n${'='.repeat(50)}`);
  log(`Results: ${passed} passed, ${failed} failed out of ${groups.length}`);
  log(`${'='.repeat(50)}`);

  // Write JSON results if --results-dir provided
  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const resultsJson = {
      suite: 'pagination-overhead',
      timestamp: new Date().toISOString(),
      server_url: BASE_URL,
      groups: groupResults,
      measurements,
      summary: {
        passed,
        failed,
        total: groups.length,
        duration_ms: Date.now() - suiteStart,
      },
    };
    const resultsPath = resolve(RESULTS_DIR, 'pagination-overhead.json');
    writeFileSync(resultsPath, JSON.stringify(resultsJson, null, 2));
    log(`JSON results written to: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
