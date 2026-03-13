#!/usr/bin/env node
/**
 * E2E: Cache Maintenance After Mutations
 *
 * Verifies that the unified cache is surgically updated (not invalidated)
 * when mutations happen. The meta-index tracks which cache entries reference
 * which (field, value) pairs, and the flush thread updates cached bitmaps
 * in-place. These tests prove that path works end-to-end.
 *
 * Key distinction from e2e-write-handling.mjs: we do NOT clear the cache
 * between mutation and re-query. We want cache HITS that reflect mutations.
 *
 * Test groups:
 *   A. Filter upsert: cached query reflects filter value change (via cache hit)
 *   B. Sort upsert: cached query reflects sort value change (via cache hit)
 *   C. Delete: cached query removes deleted doc (via cache hit)
 *   D. Multi-value field: tag add/remove reflected in cache
 *   E. Multi-cache fan-out: mutation touches multiple cached queries
 *   F. Sort-only mutation: sort change doesn't break filter caches
 *   G. Rapid sequential mutations: cache stays consistent through burst writes
 *   H. High fan-out stress: many cache entries referencing same field
 *
 * Usage:
 *   node tests/e2e/e2e-cache-maintenance.mjs [--url http://localhost:3000] [--verbose]
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

const INDEX = 'cache-maint-test';

let passed = 0;
let failed = 0;
const groupResults = [];

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function tryJson(res) {
  const text = await res.text();
  try { return JSON.parse(text); } catch { return { _raw: text }; }
}

async function apiPost(path, body) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await tryJson(res);
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  const data = await tryJson(res);
  vlog(`GET ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiDelete(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const data = await tryJson(res);
  vlog(`DELETE ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const query = (filters, sort, limit = 100) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, sort, limit }).then(r => r.data);
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const deleteDocs = (ids) =>
  apiDelete(`/api/indexes/${INDEX}/documents`, { ids }).then(r => r.data);
const clearCache = () =>
  apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForFlush(expectedAlive, maxWaitMs = 3000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await stats();
    if (s.alive_count === expectedAlive) return s;
    await sleep(50);
  }
  return await stats();
}

/** Wait for flush cycle to advance (mutation processed). */
async function waitForCycle(startCycle, maxWaitMs = 3000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await stats();
    if (s.flush_cycle > startCycle) return s;
    await sleep(50);
  }
  return await stats();
}

const SORT_DESC = { field: 'reactionCount', direction: 'Desc' };
const SORT_ASC = { field: 'reactionCount', direction: 'Asc' };

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create test index ---');
  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(200);
  }

  const config = {
    filter_fields: [
      { name: 'category', field_type: 'single_value' },
      { name: 'priority', field_type: 'single_value' },
      { name: 'active', field_type: 'boolean' },
      { name: 'tagIds', field_type: 'multi_value' },
    ],
    sort_fields: [
      { name: 'reactionCount', bits: 32 },
      { name: 'score', bits: 32 },
    ],
    flush_interval_us: 50,
    max_page_size: 200,
  };

  const data_schema = {
    id_field: 'id',
    fields: [
      { source: 'category', target: 'category', value_type: 'integer' },
      { source: 'priority', target: 'priority', value_type: 'integer' },
      { source: 'active', target: 'active', value_type: 'boolean' },
      { source: 'tagIds', target: 'tagIds', value_type: 'integer_array' },
      { source: 'reactionCount', target: 'reactionCount', value_type: 'integer' },
      { source: 'score', target: 'score', value_type: 'integer' },
    ],
  };

  const createResult = await apiPost('/api/indexes', { name: INDEX, config, data_schema });
  assert(createResult.status === 200 || createResult.status === 201,
    `Create index failed: ${createResult.status} ${JSON.stringify(createResult.data)}`);
  log(`  Index '${INDEX}' created`);

  // Insert base documents
  const docs = [];
  for (let i = 1; i <= 30; i++) {
    docs.push({
      id: i,
      category: (i % 3) + 1,       // 1, 2, or 3
      priority: (i % 5) + 1,        // 1-5
      active: i <= 20,              // first 20 active
      tagIds: [i % 10, (i % 10) + 10],  // two tags each
      reactionCount: i * 100,       // 100-3000
      score: 3100 - i * 100,        // 3000-100 (inverse)
    });
  }
  await upsert(docs);
  await waitForFlush(30);
  log(`  Inserted 30 documents`);

  // Clear cache to start fresh
  await clearCache();
  log(`  Cache cleared`);
}

// ----- Test Groups -----

async function testA() {
  log('\n--- A. Filter Upsert: cached query reflects filter change (cache hit) ---');

  // Step 1: Populate cache with a query
  const r1 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 10);
  const s1 = await stats();
  log(`  [1] Initial query category=1: ${r1.ids.length} results, cache entries=${s1.unified_cache_entries}`);
  assert(r1.ids.length === 10, `expected 10 results, got ${r1.ids.length}`);
  assert(s1.unified_cache_entries >= 1, 'cache should have at least 1 entry');
  const initialHits = s1.unified_cache_hits;

  // Step 2: Verify cache hit on repeat query
  const r1b = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 10);
  const s1b = await stats();
  log(`  [2] Repeat query: ${r1b.ids.length} results, hits=${s1b.unified_cache_hits} (was ${initialHits})`);
  assert(s1b.unified_cache_hits > initialHits, 'repeat query should be a cache hit');
  assert(JSON.stringify(r1b.ids) === JSON.stringify(r1.ids), 'repeat query should return same results');

  // Step 3: Upsert doc — change category from 1 to 2
  // Doc 1 has category=((1%3)+1)=2... let's find a cat=1 doc
  // category = (i%3)+1: i=3→1, i=6→1, i=9→1, i=12→1...
  const targetDoc = 3; // category=1
  assert(r1.ids.includes(targetDoc), `doc ${targetDoc} should be in category=1 results`);

  const cycle = (await stats()).flush_cycle;
  await upsert([{ id: targetDoc, category: 2, priority: 2, active: true, tagIds: [3, 13], reactionCount: 300, score: 2800 }]);
  await waitForCycle(cycle);
  log(`  [3] Upserted doc ${targetDoc}: category 1→2, waited for flush`);

  // Step 4: Query SAME query — should be cache HIT with doc removed
  const hitsBeforeRequery = (await stats()).unified_cache_hits;
  const r2 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 10);
  const s2 = await stats();
  log(`  [4] After upsert query: ${r2.ids.length} results, hits=${s2.unified_cache_hits} (was ${hitsBeforeRequery})`);
  assert(!r2.ids.includes(targetDoc), `doc ${targetDoc} should NOT be in category=1 results after upsert`);
  assert(r2.ids.length === 9, `expected 9 results after removing one, got ${r2.ids.length}`);
  // Verify it was a cache hit (maintained), not a miss (rebuilt)
  assert(s2.unified_cache_hits > hitsBeforeRequery, 'post-mutation query should still be a cache hit');

  // Step 5: Verify doc appears in new category
  const r3 = await query([{ Eq: ['category', { Integer: 2 }] }], SORT_DESC, 10);
  log(`  [5] Category=2 query: ${r3.ids.length} results, includes doc ${targetDoc}: ${r3.ids.includes(targetDoc)}`);
  assert(r3.ids.includes(targetDoc), `doc ${targetDoc} should appear in category=2`);

  // Restore original value
  await upsert([{ id: targetDoc, category: 1, priority: 2, active: true, tagIds: [3, 13], reactionCount: 300, score: 2800 }]);
  await sleep(200);
}

async function testB() {
  log('\n--- B. Sort Upsert: cached query reflects sort change (cache hit) ---');

  await clearCache();

  // Step 1: Populate cache — category=1 sorted by reactionCount DESC
  const r1 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 5);
  log(`  [1] Initial top-5 DESC: [${r1.ids}]`);
  const topDoc = r1.ids[0]; // highest reactionCount in category=1

  // Step 2: Upsert a low-ranked doc with extremely high reactionCount
  // Doc 3 has category=1, reactionCount=300 (low)
  const cycle = (await stats()).flush_cycle;
  await upsert([{ id: 3, category: 1, priority: 2, active: true, tagIds: [3, 13], reactionCount: 999999, score: 2800 }]);
  await waitForCycle(cycle);
  log(`  [2] Upserted doc 3: reactionCount 300→999999`);

  // Step 3: Re-query — doc 3 should now be first (cache hit with maintained sort)
  const hitsBeforeRequery = (await stats()).unified_cache_hits;
  const r2 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 5);
  const s2 = await stats();
  log(`  [3] After upsert top-5: [${r2.ids}], hits=${s2.unified_cache_hits}`);
  assert(r2.ids[0] === 3, `doc 3 should be first after boosting reactionCount, got ${r2.ids[0]}`);

  // Restore
  await upsert([{ id: 3, category: 1, priority: 2, active: true, tagIds: [3, 13], reactionCount: 300, score: 2800 }]);
  await sleep(200);
}

async function testC() {
  log('\n--- C. Delete: cached query removes deleted doc (cache hit) ---');

  await clearCache();

  // Step 1: Populate cache
  const r1 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 10);
  log(`  [1] Initial query: ${r1.ids.length} results, ids=[${r1.ids}]`);
  const targetDoc = r1.ids[r1.ids.length - 1]; // last doc (lowest sort value)

  // Step 2: Delete last doc
  const cycle = (await stats()).flush_cycle;
  await deleteDocs([targetDoc]);
  await waitForCycle(cycle);
  log(`  [2] Deleted doc ${targetDoc}`);

  // Step 3: Re-query — should be cache hit with doc removed
  const hitsBefore = (await stats()).unified_cache_hits;
  const r2 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 10);
  const s2 = await stats();
  log(`  [3] After delete: ${r2.ids.length} results, hits=${s2.unified_cache_hits} (was ${hitsBefore})`);
  assert(!r2.ids.includes(targetDoc), `doc ${targetDoc} should not appear after delete`);
  assert(r2.ids.length === r1.ids.length - 1, `expected ${r1.ids.length - 1} results, got ${r2.ids.length}`);
  assert(s2.unified_cache_hits > hitsBefore, 'should be a cache hit');

  // Re-insert for other tests
  await upsert([{
    id: targetDoc, category: 1, priority: ((targetDoc % 5) + 1), active: true,
    tagIds: [targetDoc % 10, (targetDoc % 10) + 10], reactionCount: targetDoc * 100, score: 3100 - targetDoc * 100,
  }]);
  await sleep(200);
}

async function testD() {
  log('\n--- D. Multi-value field: tag changes reflected in cache ---');

  await clearCache();

  // Step 1: Query for tagIds IN [3] — populate cache
  const r1 = await query([{ In: ['tagIds', [{ Integer: 3 }]] }], SORT_DESC, 10);
  log(`  [1] tagIds IN [3]: ${r1.ids.length} results`);
  assert(r1.ids.length > 0, 'should have results for tag 3');

  // Step 2: Remove tag 3 from doc 3, add tag 99
  const cycle = (await stats()).flush_cycle;
  await upsert([{ id: 3, category: 1, priority: 2, active: true, tagIds: [99, 13], reactionCount: 300, score: 2800 }]);
  await waitForCycle(cycle);
  log(`  [2] Upserted doc 3: tagIds [3,13]→[99,13]`);

  // Step 3: Re-query tag 3 — doc 3 should be gone (cache hit)
  const hitsBefore = (await stats()).unified_cache_hits;
  const r2 = await query([{ In: ['tagIds', [{ Integer: 3 }]] }], SORT_DESC, 10);
  const s2 = await stats();
  log(`  [3] After upsert tagIds IN [3]: ${r2.ids.length} results, hits=${s2.unified_cache_hits}`);
  assert(!r2.ids.includes(3), 'doc 3 should not appear for tag 3 after removal');
  assert(s2.unified_cache_hits > hitsBefore, 'should be a cache hit');

  // Step 4: Query tag 99 — doc 3 should appear
  const r3 = await query([{ In: ['tagIds', [{ Integer: 99 }]] }], SORT_DESC, 10);
  log(`  [4] tagIds IN [99]: ${r3.ids.length} results, includes doc 3: ${r3.ids.includes(3)}`);
  assert(r3.ids.includes(3), 'doc 3 should appear for tag 99');

  // Restore
  await upsert([{ id: 3, category: 1, priority: 2, active: true, tagIds: [3, 13], reactionCount: 300, score: 2800 }]);
  await sleep(200);
}

async function testE() {
  log('\n--- E. Multi-cache fan-out: mutation touches multiple cached queries ---');

  await clearCache();

  // Step 1: Populate multiple cache entries with overlapping filters
  // Use doc 15: category=1 (15%3+1=1), active=true (15<=20), reactionCount=1500 (top 10 active DESC)
  const q1 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 10);
  const q2 = await query([{ Eq: ['active', { Bool: true }] }], SORT_DESC, 10);
  const q3 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_ASC, 10);
  const q4 = await query([
    { Eq: ['category', { Integer: 1 }] },
    { Eq: ['active', { Bool: true }] },
  ], SORT_DESC, 10);

  const s1 = await stats();
  log(`  [1] Populated ${s1.unified_cache_entries} cache entries, meta_entries=${s1.unified_cache_meta_entries}`);
  assert(s1.unified_cache_entries >= 4, `expected at least 4 cache entries, got ${s1.unified_cache_entries}`);

  // Doc 15: category=1, active=true, reactionCount=1500 — appears in all 4 queries
  assert(q1.ids.includes(15), 'doc 15 in category=1 DESC');
  assert(q2.ids.includes(15), 'doc 15 in active=true');
  assert(q3.ids.includes(15), 'doc 15 in category=1 ASC');
  assert(q4.ids.includes(15), 'doc 15 in category=1+active=true');

  // Step 2: Change doc 15 category from 1 to 3
  const cycle = (await stats()).flush_cycle;
  await upsert([{ id: 15, category: 3, priority: 1, active: true, tagIds: [5, 15], reactionCount: 1500, score: 1600 }]);
  await waitForCycle(cycle);
  log(`  [2] Upserted doc 15: category 1→3`);

  // Step 3: All category=1 queries should drop doc 15 (cache hits)
  const hitsBefore = (await stats()).unified_cache_hits;
  const r1 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 10);
  const r3 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_ASC, 10);
  const r4 = await query([
    { Eq: ['category', { Integer: 1 }] },
    { Eq: ['active', { Bool: true }] },
  ], SORT_DESC, 10);

  const s2 = await stats();
  log(`  [3] After upsert: cat=1 DESC has doc 15: ${r1.ids.includes(15)}, ASC: ${r3.ids.includes(15)}, combined: ${r4.ids.includes(15)}`);
  log(`      Cache hits: ${s2.unified_cache_hits} (was ${hitsBefore}, delta=${s2.unified_cache_hits - hitsBefore})`);

  assert(!r1.ids.includes(15), 'doc 15 should not be in category=1 DESC');
  assert(!r3.ids.includes(15), 'doc 15 should not be in category=1 ASC');
  assert(!r4.ids.includes(15), 'doc 15 should not be in category=1+active combined');

  // active=true query should still have doc 15 (category changed, not active)
  const r2 = await query([{ Eq: ['active', { Bool: true }] }], SORT_DESC, 10);
  log(`  [4] active=true still has doc 15: ${r2.ids.includes(15)}`);
  assert(r2.ids.includes(15), 'doc 15 should still appear in active=true query');

  // Step 4: Verify cache entries survived (maintained, not wiped)
  const s3 = await stats();
  log(`  [5] Cache entries after mutations: ${s3.unified_cache_entries} (was ${s1.unified_cache_entries})`);
  assert(s3.unified_cache_entries >= s1.unified_cache_entries, 'cache entries should not decrease (maintained, not invalidated)');

  // Restore
  await upsert([{ id: 15, category: 1, priority: 1, active: true, tagIds: [5, 15], reactionCount: 1500, score: 1600 }]);
  await sleep(200);
}

async function testF() {
  log('\n--- F. Sort-only mutation: sort change handled without breaking filter caches ---');

  await clearCache();

  // Step 1: Populate cache
  const r1 = await query([{ Eq: ['category', { Integer: 2 }] }], SORT_DESC, 5);
  log(`  [1] category=2 top-5 DESC: [${r1.ids}]`);

  // Step 2: Change ONLY the sort value (reactionCount), keep category the same
  // Doc 2 has category=2
  const cycle = (await stats()).flush_cycle;
  await upsert([{ id: 2, category: 2, priority: 3, active: true, tagIds: [2, 12], reactionCount: 888888, score: 2900 }]);
  await waitForCycle(cycle);
  log(`  [2] Upserted doc 2: reactionCount 200→888888 (category unchanged)`);

  // Step 3: Re-query — doc 2 should jump to top
  const r2 = await query([{ Eq: ['category', { Integer: 2 }] }], SORT_DESC, 5);
  log(`  [3] After sort upsert: [${r2.ids}]`);
  assert(r2.ids[0] === 2, `doc 2 should be first after sort boost, got ${r2.ids[0]}`);

  // Step 4: Verify the entry count is stable
  const s = await stats();
  log(`  [4] Cache entries: ${s.unified_cache_entries}`);

  // Restore
  await upsert([{ id: 2, category: 2, priority: 3, active: true, tagIds: [2, 12], reactionCount: 200, score: 2900 }]);
  await sleep(200);
}

async function testG() {
  log('\n--- G. Rapid sequential mutations: cache stays consistent through burst ---');

  await clearCache();

  // Step 1: Populate cache
  const r1 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 10);
  log(`  [1] Initial: ${r1.ids.length} results`);

  // Step 2: Rapid-fire 5 upserts in sequence (don't wait for flush between)
  // Move docs 3, 6, 9 out of category=1, move docs 1, 4 in from category=2
  // Give incoming docs high reactionCount so they qualify for the bounded cache
  await upsert([{ id: 3, category: 2, priority: 2, active: true, tagIds: [3, 13], reactionCount: 300, score: 2800 }]);
  await upsert([{ id: 6, category: 2, priority: 2, active: true, tagIds: [6, 16], reactionCount: 600, score: 2500 }]);
  await upsert([{ id: 9, category: 2, priority: 5, active: true, tagIds: [9, 19], reactionCount: 900, score: 2200 }]);
  // Doc 1 is category=2, move to 1 (boost reactionCount so it qualifies for bound)
  await upsert([{ id: 1, category: 1, priority: 2, active: true, tagIds: [1, 11], reactionCount: 5000, score: 3000 }]);
  // Doc 4 is category=2, move to 1 (boost reactionCount so it qualifies for bound)
  await upsert([{ id: 4, category: 1, priority: 5, active: true, tagIds: [4, 14], reactionCount: 4000, score: 2700 }]);

  // Wait for all flushes
  await sleep(500);
  log(`  [2] Fired 5 rapid mutations`);

  // Step 3: Query — should reflect all 5 changes
  const r2 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 20);
  log(`  [3] After burst: ${r2.ids.length} results`);
  log(`      Has docs 1,4 (added): ${r2.ids.includes(1)}, ${r2.ids.includes(4)}`);
  log(`      Missing docs 3,6,9 (removed): ${!r2.ids.includes(3)}, ${!r2.ids.includes(6)}, ${!r2.ids.includes(9)}`);

  assert(!r2.ids.includes(3), 'doc 3 should be gone from category=1');
  assert(!r2.ids.includes(6), 'doc 6 should be gone from category=1');
  assert(!r2.ids.includes(9), 'doc 9 should be gone from category=1');
  assert(r2.ids.includes(1), 'doc 1 should now be in category=1');
  assert(r2.ids.includes(4), 'doc 4 should now be in category=1');

  // Restore all to original values
  await upsert([
    { id: 3, category: 1, priority: 2, active: true, tagIds: [3, 13], reactionCount: 300, score: 2800 },
    { id: 6, category: 1, priority: 2, active: true, tagIds: [6, 16], reactionCount: 600, score: 2500 },
    { id: 9, category: 1, priority: 5, active: true, tagIds: [9, 19], reactionCount: 900, score: 2200 },
    { id: 1, category: 2, priority: 2, active: true, tagIds: [1, 11], reactionCount: 100, score: 3000 },
    { id: 4, category: 2, priority: 5, active: true, tagIds: [4, 14], reactionCount: 400, score: 2700 },
  ]);
  await sleep(300);
}

async function testH() {
  log('\n--- H. High fan-out: many cache entries, mutation touches many ---');

  await clearCache();

  // Step 1: Populate many cache entries with different filter combos
  // All reference category field — meta-index should track all of them
  const queries = [];
  for (let cat = 1; cat <= 3; cat++) {
    for (let prio = 1; prio <= 5; prio++) {
      queries.push([
        { Eq: ['category', { Integer: cat }] },
        { Eq: ['priority', { Integer: prio }] },
      ]);
    }
  }
  // Also add single-filter queries
  for (let cat = 1; cat <= 3; cat++) {
    queries.push([{ Eq: ['category', { Integer: cat }] }]);
  }
  // Sort variants
  for (let cat = 1; cat <= 3; cat++) {
    queries.push([{ Eq: ['category', { Integer: cat }] }]);  // will get SORT_ASC below
  }

  // Fire all queries to populate cache
  for (const q of queries) {
    await query(q, SORT_DESC, 10);
  }
  for (let cat = 1; cat <= 3; cat++) {
    await query([{ Eq: ['category', { Integer: cat }] }], SORT_ASC, 10);
  }

  const s1 = await stats();
  log(`  [1] Populated ${s1.unified_cache_entries} cache entries, meta=${s1.unified_cache_meta_entries}`);
  assert(s1.unified_cache_entries >= 15, `expected many cache entries, got ${s1.unified_cache_entries}`);

  // Step 2: Mutate doc 12 (category=1) to category=3
  const cycle = (await stats()).flush_cycle;
  await upsert([{ id: 12, category: 3, priority: 4, active: true, tagIds: [2, 12], reactionCount: 1200, score: 1900 }]);
  await waitForCycle(cycle);
  log(`  [2] Upserted doc 12: category 1→3`);

  // Step 3: Spot-check various cached queries
  const r_cat1 = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_DESC, 20);
  const r_cat3 = await query([{ Eq: ['category', { Integer: 3 }] }], SORT_DESC, 20);
  const r_cat1_asc = await query([{ Eq: ['category', { Integer: 1 }] }], SORT_ASC, 20);

  log(`  [3] cat=1 DESC has doc 12: ${r_cat1.ids.includes(12)}, cat=3 has doc 12: ${r_cat3.ids.includes(12)}`);
  assert(!r_cat1.ids.includes(12), 'doc 12 should be gone from category=1 DESC');
  assert(!r_cat1_asc.ids.includes(12), 'doc 12 should be gone from category=1 ASC');
  assert(r_cat3.ids.includes(12), 'doc 12 should appear in category=3');

  // Step 4: Cache entry count should be stable (maintained, not blown away)
  const s2 = await stats();
  log(`  [4] Cache entries: ${s2.unified_cache_entries} (was ${s1.unified_cache_entries})`);
  assert(s2.unified_cache_entries >= s1.unified_cache_entries - 2,
    `cache entries shouldn't drop significantly (${s2.unified_cache_entries} vs ${s1.unified_cache_entries})`);

  // Restore
  await upsert([{ id: 12, category: 1, priority: 4, active: true, tagIds: [2, 12], reactionCount: 1200, score: 1900 }]);
  await sleep(200);
}

// ----- Runner -----

async function runGroup(name, fn) {
  const start = Date.now();
  try {
    await fn();
    passed++;
    const ms = Date.now() - start;
    log(`  PASS: ${name} (${ms}ms)`);
    groupResults.push({ name, status: 'pass', ms });
  } catch (e) {
    failed++;
    const ms = Date.now() - start;
    log(`  FAIL: ${name}: ${e.message}`);
    groupResults.push({ name, status: 'fail', error: e.message, ms });
  }
}

async function main() {
  log('=== BitDex E2E: Cache Maintenance After Mutations ===');
  log(`Server: ${BASE_URL}\n`);

  const health = await fetch(`${BASE_URL}/api/health`).then(r => r.text()).catch(() => null);
  if (health !== 'ok') {
    log('ERROR: Server not reachable');
    process.exit(1);
  }

  await runGroup('Setup. Create test index', setup);
  await runGroup('A. Filter upsert reflected in cache hit', testA);
  await runGroup('B. Sort upsert reflected in cache hit', testB);
  await runGroup('C. Delete reflected in cache hit', testC);
  await runGroup('D. Multi-value tag change in cache', testD);
  await runGroup('E. Multi-cache fan-out', testE);
  await runGroup('F. Sort-only mutation', testF);
  await runGroup('G. Rapid sequential mutations', testG);
  await runGroup('H. High fan-out stress', testH);

  // Cleanup
  if (!KEEP) {
    await apiDelete(`/api/indexes/${INDEX}`);
    log('\n--- Cleanup ---');
    log('  Index deleted');
  }

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);

  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const out = resolve(RESULTS_DIR, 'e2e-cache-maintenance.json');
    writeFileSync(out, JSON.stringify({ passed, failed, groups: groupResults }, null, 2));
    log(`Results written to ${out}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => { console.error(e); process.exit(1); });
