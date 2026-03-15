#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Arena-Free Bulk Loader
 *
 * Tests the arena-free bulk load pipeline end-to-end through the HTTP API.
 * Simulates the bulk load path by inserting documents via upsert and verifying
 * that the key behaviors of the arena-free loader are correct:
 *
 *   1. Multi-value fields (tagIds, toolIds, etc.) are stored and queryable
 *   2. Documents retrieved via include_docs have correctly reconstructed arrays
 *   3. Sort fields work correctly after bulk-style insertion
 *   4. Cursor pagination works across the dataset
 *   5. Doc-only scalar fields (url, hash) are preserved
 *   6. Large multi-value arrays (overflow case) are handled correctly
 *   7. Combined filter + multi-value + sort queries work
 *   8. Sync sidecar cursor compatibility (cursor set/get)
 *
 * Usage:
 *   node tests/e2e/e2e-bulk-load-arena-free.mjs [--url http://localhost:3000] [--verbose] [--keep]
 *
 * Prerequisites:
 *   Server running with NO existing index:
 *     cargo run --release --features server --bin bitdex-server -- --port 3100 --data-dir .test-data/e2e
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

const INDEX = 'bulk-arena-free-test';

let passed = 0;
let failed = 0;
const groupResults = [];

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
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 800));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  const data = await res.json();
  vlog(`GET ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiDelete(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const data = await res.json();
  vlog(`DELETE ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const query = (filters, sort, limit = 100, opts = {}) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters, sort, limit, ...opts }).then(r => r.data);
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const clearCache = () =>
  apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForFlush(expectedAlive, maxWaitMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await stats();
    if (s.alive_count === expectedAlive) return s;
    await sleep(50);
  }
  return await stats();
}

async function freshQuery(filters, sort, limit = 100, opts = {}) {
  await clearCache();
  return query(filters, sort, limit, opts);
}

const SORT_BY_ID_ASC = { field: 'sortAt', direction: 'Asc' };
const SORT_BY_ID_DESC = { field: 'sortAt', direction: 'Desc' };
const SORT_BY_REACTIONS_DESC = { field: 'reactionCount', direction: 'Desc' };

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create test index ---');

  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    log('  Deleting existing test index...');
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(500);
  }

  // Index config matches the Civitai schema shape used by the arena-free bulk loader
  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'nsfwLevel', field_type: 'single_value' },
        { name: 'userId', field_type: 'single_value' },
        { name: 'tagIds', field_type: 'multi_value' },
        { name: 'toolIds', field_type: 'multi_value' },
        { name: 'techniqueIds', field_type: 'multi_value' },
        { name: 'modelVersionIds', field_type: 'multi_value' },
      ],
      sort_fields: [
        { name: 'sortAt', bits: 32 },
        { name: 'reactionCount', bits: 32 },
      ],
      max_page_size: 200,
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'userId', target: 'userId', value_type: 'integer' },
        { source: 'tagIds', target: 'tagIds', value_type: 'integer_array' },
        { source: 'toolIds', target: 'toolIds', value_type: 'integer_array' },
        { source: 'techniqueIds', target: 'techniqueIds', value_type: 'integer_array' },
        { source: 'modelVersionIds', target: 'modelVersionIds', value_type: 'integer_array' },
        { source: 'sortAt', target: 'sortAt', value_type: 'integer' },
        { source: 'reactionCount', target: 'reactionCount', value_type: 'integer' },
        // Doc-only fields (not indexed, stored for serving)
        { source: 'url', target: 'url', value_type: 'string', doc_only: true },
        { source: 'hash', target: 'hash', value_type: 'string', doc_only: true },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created`);

  // Insert 20 documents simulating the arena-free bulk load output.
  // Each doc has multi-value fields, sort fields, and doc-only scalars.
  const docs = [];
  for (let i = 1; i <= 20; i++) {
    const doc = {
      id: i,
      nsfwLevel: i <= 15 ? 1 : 2,
      userId: Math.floor((i - 1) / 5) + 1, // users 1-4
      tagIds: [100, 200 + i],               // shared tag 100, unique tag per doc
      toolIds: i <= 5 ? [10] : [],          // first 5 docs have tool 10
      techniqueIds: i === 3 ? [30, 31] : [],
      modelVersionIds: i <= 3 ? [999] : [],
      sortAt: 1700000000 + i * 100,         // increasing sort values
      reactionCount: i * 10,
      url: `https://example.com/img-${i}.jpg`,
      hash: `hash_${i}`,
    };
    docs.push(doc);
  }

  const res = await upsert(docs);
  assert(res.upserted === 20, `Expected 20 upserted, got ${res.upserted}`);
  log(`  [1] Inserted 20 documents (simulating bulk load)`);

  await waitForFlush(20);
  log('  [2] Flush complete, 20 documents alive');
}

// ----- Test A: Document count matches expected -----

async function testA_DocumentCount() {
  log('\n--- A. Document Count After Bulk Load ---');

  const s = await stats();
  assert(s.alive_count === 20, `Expected 20 alive, got ${s.alive_count}`);
  log(`  [1] alive_count = ${s.alive_count} (correct)`);

  const r = await freshQuery([], SORT_BY_ID_ASC);
  assert(r.total_matched === 20, `Expected total_matched=20, got ${r.total_matched}`);
  assert(r.ids.length === 20, `Expected 20 IDs, got ${r.ids.length}`);
  log(`  [2] Query with no filters: total_matched=${r.total_matched}, ids=${r.ids.length} (correct)`);
}

// ----- Test B: Tag filtering works -----

async function testB_TagFiltering() {
  log('\n--- B. Tag Filtering on Bulk-Loaded Data ---');

  // All 20 docs have tag 100
  const r1 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 100 }]] }],
    SORT_BY_ID_ASC,
  );
  assert(r1.total_matched === 20, `Expected 20 results for tag 100, got ${r1.total_matched}`);
  log(`  [1] tagIds IN [100]: ${r1.total_matched} results (all docs, correct)`);

  // Only doc 5 has tag 205 (200 + 5)
  const r2 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 205 }]] }],
    SORT_BY_ID_ASC,
  );
  assert(r2.total_matched === 1, `Expected 1 result for tag 205, got ${r2.total_matched}`);
  assert(r2.ids[0] === 5, `Expected doc 5, got ${r2.ids[0]}`);
  log(`  [2] tagIds IN [205]: ${r2.total_matched} result, doc ${r2.ids[0]} (correct)`);

  // Multi-tag filter: docs with BOTH tag 100 AND tag 201 (only doc 1)
  const r3 = await freshQuery(
    [{ And: [
      { In: ['tagIds', [{ Integer: 100 }]] },
      { In: ['tagIds', [{ Integer: 201 }]] },
    ]}],
    SORT_BY_ID_ASC,
  );
  assert(r3.total_matched === 1, `Expected 1 result for tags 100+201, got ${r3.total_matched}`);
  assert(r3.ids[0] === 1, `Expected doc 1, got ${r3.ids[0]}`);
  log(`  [3] tagIds AND [100, 201]: ${r3.total_matched} result (correct)`);

  // Tag that does not exist
  const r4 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 99999 }]] }],
    SORT_BY_ID_ASC,
  );
  assert(r4.total_matched === 0, `Expected 0 results for nonexistent tag, got ${r4.total_matched}`);
  log(`  [4] tagIds IN [99999]: ${r4.total_matched} results (correct, nonexistent tag)`);
}

// ----- Test C: Multi-value field reconstruction in documents -----

async function testC_MultiValueReconstruction() {
  log('\n--- C. Multi-Value Field Reconstruction (include_docs) ---');

  // Get doc 1 with include_docs
  const r = await freshQuery(
    [{ Eq: ['userId', { Integer: 1 }] }],
    SORT_BY_ID_ASC,
    10,
    { include_docs: true },
  );
  assert(r.documents && r.documents.length >= 1, `Expected >= 1 document, got ${r.documents?.length}`);

  // Find doc 1
  const doc1 = r.documents.find(d => d.id === 1);
  assert(doc1, 'Doc 1 not found in results');

  // tagIds should contain [100, 201] (the shared tag + unique tag)
  assert(Array.isArray(doc1.tagIds), `Expected tagIds array, got ${typeof doc1.tagIds}`);
  assert(doc1.tagIds.length === 2, `Expected 2 tags on doc 1, got ${doc1.tagIds.length}`);
  assert(doc1.tagIds.includes(100), `Doc 1 missing tag 100`);
  assert(doc1.tagIds.includes(201), `Doc 1 missing tag 201`);
  log(`  [1] Doc 1 tagIds: ${JSON.stringify(doc1.tagIds)} (correct)`);

  // toolIds for doc 1 should be [10]
  assert(Array.isArray(doc1.toolIds), `Expected toolIds array, got ${typeof doc1.toolIds}`);
  assert(doc1.toolIds.length === 1, `Expected 1 tool on doc 1, got ${doc1.toolIds.length}`);
  assert(doc1.toolIds[0] === 10, `Expected toolId 10, got ${doc1.toolIds[0]}`);
  log(`  [2] Doc 1 toolIds: ${JSON.stringify(doc1.toolIds)} (correct)`);

  // modelVersionIds for doc 1 should be [999]
  assert(Array.isArray(doc1.modelVersionIds), `Expected modelVersionIds array, got ${typeof doc1.modelVersionIds}`);
  assert(doc1.modelVersionIds.length === 1, `Expected 1 MV on doc 1, got ${doc1.modelVersionIds.length}`);
  assert(doc1.modelVersionIds[0] === 999, `Expected MV 999, got ${doc1.modelVersionIds[0]}`);
  log(`  [3] Doc 1 modelVersionIds: ${JSON.stringify(doc1.modelVersionIds)} (correct)`);

  // Doc 3 should have techniqueIds [30, 31]
  const doc3 = r.documents.find(d => d.id === 3);
  assert(doc3, 'Doc 3 not found in results');
  assert(Array.isArray(doc3.techniqueIds), `Expected techniqueIds array`);
  assert(doc3.techniqueIds.length === 2, `Expected 2 techniques on doc 3, got ${doc3.techniqueIds.length}`);
  assert(doc3.techniqueIds.includes(30), `Doc 3 missing technique 30`);
  assert(doc3.techniqueIds.includes(31), `Doc 3 missing technique 31`);
  log(`  [4] Doc 3 techniqueIds: ${JSON.stringify(doc3.techniqueIds)} (correct)`);

  // Doc 10 should have empty toolIds (only docs 1-5 have tools)
  const r2 = await freshQuery(
    [{ Eq: ['userId', { Integer: 2 }] }],
    SORT_BY_ID_ASC,
    10,
    { include_docs: true },
  );
  const doc10 = r2.documents.find(d => d.id === 10);
  if (doc10) {
    assert(Array.isArray(doc10.toolIds), `Expected toolIds array on doc 10`);
    assert(doc10.toolIds.length === 0, `Expected 0 tools on doc 10, got ${doc10.toolIds.length}`);
    log(`  [5] Doc 10 toolIds: ${JSON.stringify(doc10.toolIds)} (empty, correct)`);
  } else {
    log(`  [5] Doc 10 not in userId=2 results (may be userId=3), skipping check`);
  }
}

// ----- Test D: Sort fields work correctly -----

async function testD_SortFields() {
  log('\n--- D. Sort Fields After Bulk Load ---');

  // Sort by sortAt ascending
  const r_asc = await freshQuery([], SORT_BY_ID_ASC, 5);
  assert(r_asc.ids.length === 5, `Expected 5 results, got ${r_asc.ids.length}`);
  // Doc with lowest sortAt should be doc 1 (1700000100)
  assert(r_asc.ids[0] === 1, `Expected doc 1 first in Asc sort, got ${r_asc.ids[0]}`);
  log(`  [1] Sort Asc: first 5 = [${r_asc.ids.join(', ')}] (correct, doc 1 first)`);

  // Sort by sortAt descending
  const r_desc = await freshQuery([], SORT_BY_ID_DESC, 5);
  assert(r_desc.ids[0] === 20, `Expected doc 20 first in Desc sort, got ${r_desc.ids[0]}`);
  log(`  [2] Sort Desc: first 5 = [${r_desc.ids.join(', ')}] (correct, doc 20 first)`);

  // Sort by reactionCount descending
  const r_react = await freshQuery([], SORT_BY_REACTIONS_DESC, 5);
  assert(r_react.ids[0] === 20, `Expected doc 20 first by reactions (200), got ${r_react.ids[0]}`);
  assert(r_react.ids[4] === 16, `Expected doc 16 fifth by reactions (160), got ${r_react.ids[4]}`);
  log(`  [3] Sort by reactionCount Desc: [${r_react.ids.join(', ')}] (correct)`);

  // Verify sort stability: ascending order means IDs should be monotonically increasing
  const r_full = await freshQuery([], SORT_BY_ID_ASC, 20);
  for (let i = 1; i < r_full.ids.length; i++) {
    assert(
      r_full.ids[i] > r_full.ids[i - 1],
      `Sort order violated at position ${i}: ${r_full.ids[i - 1]} >= ${r_full.ids[i]}`,
    );
  }
  log(`  [4] Full ascending sort verified: all 20 IDs monotonically increasing`);
}

// ----- Test E: Cursor pagination -----

async function testE_CursorPagination() {
  log('\n--- E. Cursor Pagination Across Bulk-Loaded Data ---');

  const pageSize = 5;
  let allIds = [];
  let cursor = undefined;
  let pageNum = 0;

  // Paginate through all 20 docs
  while (true) {
    pageNum++;
    const queryBody = { filters: [], sort: SORT_BY_ID_ASC, limit: pageSize };
    if (cursor) queryBody.cursor = cursor;

    await clearCache();
    const r = await query([], SORT_BY_ID_ASC, pageSize, cursor ? { cursor } : {});

    assert(r.ids && Array.isArray(r.ids), `Page ${pageNum}: invalid ids`);
    allIds.push(...r.ids);

    vlog(`Page ${pageNum}: ids=[${r.ids.join(',')}], cursor=${JSON.stringify(r.cursor)}`);

    if (!r.cursor || r.ids.length < pageSize) break;
    cursor = r.cursor;

    // Safety: no more than 10 pages for 20 docs
    if (pageNum > 10) {
      assert(false, 'Too many pages — infinite loop?');
    }
  }

  assert(allIds.length === 20, `Expected 20 total IDs, got ${allIds.length}`);
  log(`  [1] Paginated through ${pageNum} pages, collected ${allIds.length} IDs`);

  // Verify no duplicates
  const uniqueIds = new Set(allIds);
  assert(uniqueIds.size === 20, `Expected 20 unique IDs, got ${uniqueIds.size} (duplicates in pagination)`);
  log(`  [2] No duplicate IDs across pages (${uniqueIds.size} unique)`);

  // Verify all IDs are present (1-20)
  for (let i = 1; i <= 20; i++) {
    assert(uniqueIds.has(i), `Missing ID ${i} from pagination`);
  }
  log(`  [3] All IDs 1-20 present in paginated results`);

  // Verify ordering within and across pages
  for (let i = 1; i < allIds.length; i++) {
    assert(
      allIds[i] > allIds[i - 1],
      `Sort order broken across pages at position ${i}: ${allIds[i - 1]} >= ${allIds[i]}`,
    );
  }
  log(`  [4] Sort order preserved across all pages`);
}

// ----- Test F: Doc-only scalar fields -----

async function testF_DocOnlyScalars() {
  log('\n--- F. Doc-Only Scalar Fields (url, hash) ---');

  const r = await freshQuery(
    [{ Eq: ['userId', { Integer: 1 }] }],
    SORT_BY_ID_ASC,
    5,
    { include_docs: true },
  );
  assert(r.documents && r.documents.length >= 1, 'No documents returned');

  const doc1 = r.documents.find(d => d.id === 1);
  assert(doc1, 'Doc 1 not found');

  // url and hash should be preserved (doc-only, not in any bitmap)
  assert(doc1.url === 'https://example.com/img-1.jpg',
    `Expected url 'https://example.com/img-1.jpg', got '${doc1.url}'`);
  log(`  [1] Doc 1 url: ${doc1.url} (correct)`);

  assert(doc1.hash === 'hash_1',
    `Expected hash 'hash_1', got '${doc1.hash}'`);
  log(`  [2] Doc 1 hash: ${doc1.hash} (correct)`);

  // Check a different doc
  const doc5 = r.documents.find(d => d.id === 5);
  if (doc5) {
    assert(doc5.url === 'https://example.com/img-5.jpg',
      `Expected url for doc 5, got '${doc5.url}'`);
    log(`  [3] Doc 5 url: ${doc5.url} (correct)`);
  }
}

// ----- Test G: Large multi-value arrays (overflow case) -----

async function testG_LargeMultiValueArrays() {
  log('\n--- G. Large Multi-Value Arrays (Overflow Case) ---');

  // Insert a document with 60 tags (exceeds typical inline thresholds)
  const manyTags = [];
  for (let t = 5000; t < 5060; t++) {
    manyTags.push(t);
  }

  const res = await upsert([{
    id: 1000,
    nsfwLevel: 1,
    userId: 999,
    tagIds: manyTags,
    toolIds: [],
    techniqueIds: [],
    modelVersionIds: [],
    sortAt: 1700099999,
    reactionCount: 9999,
    url: 'https://example.com/overflow.jpg',
    hash: 'overflow_hash',
  }]);
  assert(res.upserted === 1, `Expected 1 upserted, got ${res.upserted}`);

  await waitForFlush(21);
  log(`  [1] Inserted doc 1000 with ${manyTags.length} tags`);

  // Verify the document has all 60 tags via include_docs
  const r = await freshQuery(
    [{ Eq: ['userId', { Integer: 999 }] }],
    SORT_BY_ID_ASC,
    1,
    { include_docs: true },
  );
  assert(r.documents && r.documents.length === 1, `Expected 1 document, got ${r.documents?.length}`);

  const doc = r.documents[0];
  assert(doc.id === 1000, `Expected doc 1000, got ${doc.id}`);
  assert(Array.isArray(doc.tagIds), `Expected tagIds array`);
  assert(doc.tagIds.length === 60, `Expected 60 tags, got ${doc.tagIds.length}`);
  log(`  [2] Doc 1000 has ${doc.tagIds.length} tags (correct)`);

  // Verify a specific tag from the middle of the range
  assert(doc.tagIds.includes(5030), `Doc 1000 missing tag 5030`);
  log(`  [3] Tag 5030 present in doc 1000 (correct)`);

  // Verify the bitmap for one of the overflow tags works
  const r2 = await freshQuery(
    [{ In: ['tagIds', [{ Integer: 5045 }]] }],
    SORT_BY_ID_ASC,
  );
  assert(r2.total_matched === 1, `Expected 1 result for tag 5045, got ${r2.total_matched}`);
  assert(r2.ids[0] === 1000, `Expected doc 1000, got ${r2.ids[0]}`);
  log(`  [4] tagIds IN [5045] returns doc 1000 (bitmap correct)`);
}

// ----- Test H: Combined filter + multi-value + sort -----

async function testH_CombinedQueries() {
  log('\n--- H. Combined Filter + Multi-Value + Sort ---');

  // nsfwLevel=1 AND toolIds IN [10], sorted by reactionCount DESC
  // Docs 1-5 have nsfwLevel=1 and toolIds=[10]
  const r1 = await freshQuery(
    [{ And: [
      { Eq: ['nsfwLevel', { Integer: 1 }] },
      { In: ['toolIds', [{ Integer: 10 }]] },
    ]}],
    SORT_BY_REACTIONS_DESC,
  );
  assert(r1.total_matched === 5, `Expected 5 results, got ${r1.total_matched}`);
  assert(r1.ids[0] === 5, `Expected doc 5 first (highest reactions among tool-10 docs), got ${r1.ids[0]}`);
  assert(r1.ids[4] === 1, `Expected doc 1 last (lowest reactions), got ${r1.ids[4]}`);
  log(`  [1] nsfwLevel=1 AND toolIds IN [10], sort reactionCount Desc: [${r1.ids.join(', ')}] (correct)`);

  // nsfwLevel=2 AND tagIds IN [100]
  // Docs 16-20 have nsfwLevel=2
  const r2 = await freshQuery(
    [{ And: [
      { Eq: ['nsfwLevel', { Integer: 2 }] },
      { In: ['tagIds', [{ Integer: 100 }]] },
    ]}],
    SORT_BY_ID_ASC,
  );
  assert(r2.total_matched === 5, `Expected 5 results for nsfwLevel=2 + tag 100, got ${r2.total_matched}`);
  log(`  [2] nsfwLevel=2 AND tagIds IN [100]: ${r2.total_matched} results (correct)`);

  // modelVersionIds IN [999] — only docs 1-3
  const r3 = await freshQuery(
    [{ In: ['modelVersionIds', [{ Integer: 999 }]] }],
    SORT_BY_ID_ASC,
  );
  assert(r3.total_matched === 3, `Expected 3 results for MV 999, got ${r3.total_matched}`);
  assert(r3.ids[0] === 1 && r3.ids[1] === 2 && r3.ids[2] === 3,
    `Expected [1, 2, 3], got [${r3.ids.join(', ')}]`);
  log(`  [3] modelVersionIds IN [999]: [${r3.ids.join(', ')}] (correct)`);

  // techniqueIds IN [30] — only doc 3
  const r4 = await freshQuery(
    [{ In: ['techniqueIds', [{ Integer: 30 }]] }],
    SORT_BY_ID_ASC,
  );
  assert(r4.total_matched === 1, `Expected 1 result for technique 30, got ${r4.total_matched}`);
  assert(r4.ids[0] === 3, `Expected doc 3, got ${r4.ids[0]}`);
  log(`  [4] techniqueIds IN [30]: doc ${r4.ids[0]} (correct)`);
}

// ----- Test I: Cursor seeding (sync sidecar compatibility) -----

async function testI_CursorSeeding() {
  log('\n--- I. Cursor Seeding (Sync Sidecar Compatibility) ---');

  // Set a cursor (simulates what the bulk loader does after load)
  const cursorName = 'pg-sync-test';
  const cursorValue = 12345;

  const setRes = await apiPost(`/api/indexes/${INDEX}/cursors/${cursorName}`, { value: cursorValue });
  assert(setRes.status === 200, `Set cursor failed: ${setRes.status}`);
  log(`  [1] Set cursor '${cursorName}' = ${cursorValue}`);

  // Read it back
  const getRes = await apiGet(`/api/indexes/${INDEX}/cursors/${cursorName}`);
  assert(getRes.status === 200, `Get cursor failed: ${getRes.status}`);
  assert(getRes.data.value === cursorValue, `Expected cursor ${cursorValue}, got ${getRes.data.value}`);
  log(`  [2] Get cursor '${cursorName}' = ${getRes.data.value} (correct)`);

  // Update cursor (simulates sidecar advancing after processing changes)
  const newValue = 67890;
  const updateRes = await apiPost(`/api/indexes/${INDEX}/cursors/${cursorName}`, { value: newValue });
  assert(updateRes.status === 200, `Update cursor failed: ${updateRes.status}`);

  const getRes2 = await apiGet(`/api/indexes/${INDEX}/cursors/${cursorName}`);
  assert(getRes2.data.value === newValue, `Expected cursor ${newValue}, got ${getRes2.data.value}`);
  log(`  [3] Cursor advanced: ${cursorValue} -> ${newValue} (correct)`);
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
  ['Setup', 'Create index and insert bulk data', setup],
  ['A', 'Document count after bulk load', testA_DocumentCount],
  ['B', 'Tag filtering on bulk-loaded data', testB_TagFiltering],
  ['C', 'Multi-value field reconstruction (include_docs)', testC_MultiValueReconstruction],
  ['D', 'Sort fields after bulk load', testD_SortFields],
  ['E', 'Cursor pagination across bulk-loaded data', testE_CursorPagination],
  ['F', 'Doc-only scalar fields (url, hash)', testF_DocOnlyScalars],
  ['G', 'Large multi-value arrays (overflow case)', testG_LargeMultiValueArrays],
  ['H', 'Combined filter + multi-value + sort', testH_CombinedQueries],
  ['I', 'Cursor seeding (sync sidecar compatibility)', testI_CursorSeeding],
];

async function main() {
  log('BitDex Arena-Free Bulk Loader E2E Tests');
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
    log(`  cargo run --release --features server --bin bitdex-server -- --port 3100 --data-dir .test-data/e2e`);
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
      suite: 'bulk-load-arena-free',
      timestamp: new Date().toISOString(),
      server_url: BASE_URL,
      groups: groupResults,
      summary: {
        passed,
        failed,
        total: groups.length,
        duration_ms: Date.now() - suiteStart,
      },
    };
    const resultsPath = resolve(RESULTS_DIR, 'bulk-load-arena-free.json');
    writeFileSync(resultsPath, JSON.stringify(resultsJson, null, 2));
    log(`JSON results written to: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
