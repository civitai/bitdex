#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Pluggable Query Formats
 *
 * Tests that all three query formats (bitdex, compact, meilisearch) produce
 * identical results when querying the same data through the HTTP API.
 * Also tests the format selection mechanism (?format= query param),
 * the /api/formats endpoint, and error handling per format.
 *
 * Test groups:
 *   A. /api/formats endpoint
 *   B. Query equivalence across formats (same data, same results)
 *   C. Format-specific features (compact cursor, meili page/hitsPerPage, meili array filter)
 *   D. Error handling per format (invalid syntax, unknown format, malformed bodies)
 *   E. Default format override (bitdex format still works when default is "compact")
 *
 * Usage:
 *   node tests/e2e/e2e-query-formats.mjs [--url http://localhost:3000] [--verbose] [--keep] [--results-dir ./results]
 *
 * Prerequisites:
 *   Server running:
 *     cargo run --release --features server --bin bitdex-server -- --port 3000 --data-dir .test-data/manual
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

const INDEX = 'query-formats-test';

let passed = 0;
let failed = 0;

const groupResults = [];

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function apiPost(path, body, { raw = false } = {}) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: raw ? body : JSON.stringify(body),
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { _raw: text }; }
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { _raw: text }; }
  vlog(`GET ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiDelete(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { _raw: text }; }
  vlog(`DELETE ${path}:`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const clearCache = () => apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

/** Query using the default (bitdex) format. filters should be an array of FilterClause. */
const queryBitdex = (filters, sort, limit = 100) =>
  apiPost(`/api/indexes/${INDEX}/query`, { filters: filters || [], sort, limit }).then(r => r.data);

/** Query using compact format via ?format=compact. */
const queryCompact = (body) =>
  apiPost(`/api/indexes/${INDEX}/query?format=compact`, body).then(r => r.data);

/** Query using meilisearch format via ?format=meilisearch. */
const queryMeili = (body) =>
  apiPost(`/api/indexes/${INDEX}/query?format=meilisearch`, body).then(r => r.data);

const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForFlush(expectedAlive, maxWaitMs = 3000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const s = await stats();
    if (s.alive_count === expectedAlive) return s;
    await sleep(50);
  }
  return stats();
}

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create test index with sample data ---');

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
        { name: 'nsfwLevel', field_type: 'single_value' },
        { name: 'type', field_type: 'single_value' },
        { name: 'tagIds', field_type: 'multi_value' },
        { name: 'onSite', field_type: 'boolean' },
      ],
      sort_fields: [
        { name: 'score', bits: 32 },
      ],
      max_page_size: 1000,
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'type', target: 'type', value_type: 'integer' },
        { source: 'tagIds', target: 'tagIds', value_type: 'integer_array' },
        { source: 'onSite', target: 'onSite', value_type: 'boolean' },
        { source: 'score', target: 'score', value_type: 'integer' },
      ],
    },
  });
  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created`);

  // Insert test documents
  // type: 1 = image, 2 = video
  const docs = [
    { id: 100, nsfwLevel: 1, type: 1, tagIds: [10, 20], onSite: true,  score: 500 },
    { id: 101, nsfwLevel: 1, type: 1, tagIds: [10, 30], onSite: true,  score: 300 },
    { id: 102, nsfwLevel: 2, type: 1, tagIds: [20, 30], onSite: false, score: 800 },
    { id: 103, nsfwLevel: 1, type: 2, tagIds: [10],     onSite: true,  score: 200 },
    { id: 104, nsfwLevel: 3, type: 2, tagIds: [40],     onSite: false, score: 100 },
    { id: 105, nsfwLevel: 1, type: 1, tagIds: [10, 40], onSite: true,  score: 900 },
  ];
  await upsert(docs);
  const s = await waitForFlush(6);
  assert(s.alive_count === 6, `Expected 6 alive, got ${s.alive_count}`);
  log(`  Inserted 6 documents, alive_count = ${s.alive_count}`);
}

// ----- Group A: /api/formats endpoint -----

async function testA_FormatsEndpoint() {
  log('\n--- A. /api/formats endpoint ---');

  // A1: GET /api/formats returns all registered formats
  const { status, data } = await apiGet('/api/formats');
  assert(status === 200, `A1: Expected 200, got ${status}`);
  assert(Array.isArray(data.formats), 'A1: formats should be an array');
  assert(data.formats.includes('bitdex'), 'A1: should include bitdex');
  assert(data.formats.includes('compact'), 'A1: should include compact');
  assert(data.formats.includes('meilisearch'), 'A1: should include meilisearch');
  assert(typeof data.default === 'string', 'A1: default should be a string');
  log(`  [1] Formats: ${data.formats.join(', ')} (default: ${data.default})`);
}

// ----- Group B: Query equivalence across formats -----

async function testB_QueryEquivalence() {
  log('\n--- B. Query equivalence across all three formats ---');
  await clearCache();

  // B1: Simple equality filter + sort desc
  const b1_bitdex = await queryBitdex(
    [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    { field: 'score', direction: 'Desc' },
    100,
  );
  await clearCache();
  const b1_compact = await queryCompact({
    filter: { nsfwLevel: 1 },
    sort: '-score',
    limit: 100,
  });
  await clearCache();
  const b1_meili = await queryMeili({
    filter: 'nsfwLevel = 1',
    sort: ['score:desc'],
    limit: 100,
  });

  assert(
    JSON.stringify(b1_bitdex.ids) === JSON.stringify(b1_compact.ids),
    `B1: bitdex vs compact ids mismatch: ${JSON.stringify(b1_bitdex.ids)} vs ${JSON.stringify(b1_compact.ids)}`,
  );
  assert(
    JSON.stringify(b1_bitdex.ids) === JSON.stringify(b1_meili.ids),
    `B1: bitdex vs meili ids mismatch: ${JSON.stringify(b1_bitdex.ids)} vs ${JSON.stringify(b1_meili.ids)}`,
  );
  assert(b1_bitdex.total_matched === b1_compact.total_matched, 'B1: total_matched mismatch');
  assert(b1_bitdex.total_matched === b1_meili.total_matched, 'B1: total_matched mismatch');
  log(`  [1] Eq nsfwLevel=1 sort -score: ids=${JSON.stringify(b1_bitdex.ids)} (all 3 match)`);

  // B2: In operator on multi-value field
  await clearCache();
  const b2_bitdex = await queryBitdex(
    [{ In: ['tagIds', [{ Integer: 10 }, { Integer: 40 }]] }],
    { field: 'score', direction: 'Desc' },
    100,
  );
  await clearCache();
  const b2_compact = await queryCompact({
    filter: { tagIds: [10, 40] },
    sort: '-score',
    limit: 100,
  });
  await clearCache();
  const b2_meili = await queryMeili({
    filter: 'tagIds IN [10, 40]',
    sort: ['score:desc'],
    limit: 100,
  });

  assert(
    JSON.stringify(b2_bitdex.ids) === JSON.stringify(b2_compact.ids),
    `B2: bitdex vs compact ids mismatch`,
  );
  assert(
    JSON.stringify(b2_bitdex.ids) === JSON.stringify(b2_meili.ids),
    `B2: bitdex vs meili ids mismatch`,
  );
  log(`  [2] In tagIds=[10,40] sort -score: ids=${JSON.stringify(b2_bitdex.ids)} (all 3 match)`);

  // B3: AND compound — nsfwLevel=1 AND type=1, sorted by score desc
  await clearCache();
  const b3_bitdex = await queryBitdex(
    [{ And: [
      { Eq: ['nsfwLevel', { Integer: 1 }] },
      { Eq: ['type', { Integer: 1 }] },
    ]}],
    { field: 'score', direction: 'Desc' },
    100,
  );
  await clearCache();
  const b3_compact = await queryCompact({
    filter: { nsfwLevel: 1, type: 1 },
    sort: '-score',
    limit: 100,
  });
  await clearCache();
  const b3_meili = await queryMeili({
    filter: 'nsfwLevel = 1 AND type = 1',
    sort: ['score:desc'],
    limit: 100,
  });

  assert(
    JSON.stringify(b3_bitdex.ids) === JSON.stringify(b3_compact.ids),
    `B3: bitdex vs compact ids mismatch`,
  );
  assert(
    JSON.stringify(b3_bitdex.ids) === JSON.stringify(b3_meili.ids),
    `B3: bitdex vs meili ids mismatch`,
  );
  log(`  [3] AND nsfwLevel=1,type=1 sort -score: ids=${JSON.stringify(b3_bitdex.ids)} (all 3 match)`);

  // B4: OR compound — nsfwLevel=1 OR nsfwLevel=3, sort asc
  await clearCache();
  const b4_bitdex = await queryBitdex(
    [{ Or: [
      { Eq: ['nsfwLevel', { Integer: 1 }] },
      { Eq: ['nsfwLevel', { Integer: 3 }] },
    ]}],
    { field: 'score', direction: 'Asc' },
    100,
  );
  await clearCache();
  const b4_compact = await queryCompact({
    filter: { '$or': [{ nsfwLevel: 1 }, { nsfwLevel: 3 }] },
    sort: 'score',
    limit: 100,
  });
  await clearCache();
  const b4_meili = await queryMeili({
    filter: 'nsfwLevel = 1 OR nsfwLevel = 3',
    sort: ['score:asc'],
    limit: 100,
  });

  assert(
    JSON.stringify(b4_bitdex.ids) === JSON.stringify(b4_compact.ids),
    `B4: bitdex vs compact ids mismatch: ${JSON.stringify(b4_bitdex.ids)} vs ${JSON.stringify(b4_compact.ids)}`,
  );
  assert(
    JSON.stringify(b4_bitdex.ids) === JSON.stringify(b4_meili.ids),
    `B4: bitdex vs meili ids mismatch: ${JSON.stringify(b4_bitdex.ids)} vs ${JSON.stringify(b4_meili.ids)}`,
  );
  log(`  [4] OR nsfwLevel=1|3 sort +score: ids=${JSON.stringify(b4_bitdex.ids)} (all 3 match)`);

  // B5: Boolean filter — onSite=true
  await clearCache();
  const b5_bitdex = await queryBitdex(
    [{ Eq: ['onSite', { Bool: true }] }],
    { field: 'score', direction: 'Desc' },
    100,
  );
  await clearCache();
  const b5_compact = await queryCompact({
    filter: { onSite: true },
    sort: '-score',
    limit: 100,
  });
  await clearCache();
  const b5_meili = await queryMeili({
    filter: 'onSite = true',
    sort: ['score:desc'],
    limit: 100,
  });

  assert(
    JSON.stringify(b5_bitdex.ids) === JSON.stringify(b5_compact.ids),
    `B5: bitdex vs compact ids mismatch`,
  );
  assert(
    JSON.stringify(b5_bitdex.ids) === JSON.stringify(b5_meili.ids),
    `B5: bitdex vs meili ids mismatch`,
  );
  log(`  [5] Bool onSite=true sort -score: ids=${JSON.stringify(b5_bitdex.ids)} (all 3 match)`);

  // B6: No filters (all docs), sort desc — verify total_matched across all formats
  await clearCache();
  const b6_bitdex = await queryBitdex(
    [],
    { field: 'score', direction: 'Desc' },
    100,
  );
  await clearCache();
  const b6_compact = await queryCompact({
    sort: '-score',
    limit: 100,
  });
  await clearCache();
  const b6_meili = await queryMeili({
    sort: ['score:desc'],
    limit: 100,
  });

  assert(b6_bitdex.ids.length === 6, `B6: expected 6 results, got ${b6_bitdex.ids.length}`);
  assert(
    JSON.stringify(b6_bitdex.ids) === JSON.stringify(b6_compact.ids),
    `B6: bitdex vs compact ids mismatch`,
  );
  assert(
    JSON.stringify(b6_bitdex.ids) === JSON.stringify(b6_meili.ids),
    `B6: bitdex vs meili ids mismatch`,
  );
  log(`  [6] No filters, sort -score: all 6 docs, same order across all formats`);
}

// ----- Group C: Format-specific features -----

async function testC_FormatSpecificFeatures() {
  log('\n--- C. Format-specific features ---');
  await clearCache();

  // C1: Compact cursor pagination — string "sort_value:slot_id" format
  const c1_first = await queryCompact({
    filter: { nsfwLevel: 1 },
    sort: '-score',
    limit: 2,
  });
  assert(c1_first.ids.length === 2, `C1: expected 2 results, got ${c1_first.ids.length}`);
  assert(c1_first.cursor != null, 'C1: expected cursor in first page');
  log(`  [1] Compact cursor: first page ids=${JSON.stringify(c1_first.ids)}, cursor=${JSON.stringify(c1_first.cursor)}`);

  // C2: Meilisearch page/hitsPerPage pagination
  const c2_page1 = await queryMeili({
    filter: 'nsfwLevel = 1',
    sort: ['score:desc'],
    hitsPerPage: 2,
    page: 1,
  });
  assert(c2_page1.ids.length === 2, `C2: expected 2 results for page 1, got ${c2_page1.ids.length}`);

  await clearCache();
  const c2_page2 = await queryMeili({
    filter: 'nsfwLevel = 1',
    sort: ['score:desc'],
    hitsPerPage: 2,
    page: 2,
  });
  // Page 2 should have different IDs than page 1
  const page1Set = new Set(c2_page1.ids);
  const overlap = c2_page2.ids.filter(id => page1Set.has(id));
  assert(overlap.length === 0, `C2: page 1 and 2 should not overlap, overlap: ${JSON.stringify(overlap)}`);
  log(`  [2] Meili page/hitsPerPage: page1=${JSON.stringify(c2_page1.ids)}, page2=${JSON.stringify(c2_page2.ids)}`);

  // C3: Meilisearch array filter format (outer=AND, inner=OR)
  await clearCache();
  const c3 = await queryMeili({
    filter: [['nsfwLevel = 1', 'nsfwLevel = 3'], 'onSite = true'],
    sort: ['score:desc'],
    limit: 100,
  });
  // nsfwLevel=1 OR 3, AND onSite=true
  // Doc 104 has nsfwLevel=3 but onSite=false, so excluded
  // SFW + onSite: 100, 101, 103, 105
  assert(c3.ids.length === 4, `C3: expected 4 results, got ${c3.ids.length}: ${JSON.stringify(c3.ids)}`);
  log(`  [3] Meili array filter [OR, AND]: ids=${JSON.stringify(c3.ids)}`);

  // C4: Compact $not operator
  await clearCache();
  const c4 = await queryCompact({
    filter: { '$not': { nsfwLevel: 1 } },
    sort: '-score',
    limit: 100,
  });
  // NOT nsfwLevel=1: docs 102 (nsfw=2), 104 (nsfw=3)
  assert(c4.ids.length === 2, `C4: expected 2 results, got ${c4.ids.length}: ${JSON.stringify(c4.ids)}`);
  log(`  [4] Compact $not nsfwLevel=1: ids=${JSON.stringify(c4.ids)}`);

  // C5: Meilisearch NOT operator
  await clearCache();
  const c5 = await queryMeili({
    filter: 'NOT nsfwLevel = 1',
    sort: ['score:desc'],
    limit: 100,
  });
  assert(
    JSON.stringify(c4.ids) === JSON.stringify(c5.ids),
    `C5: compact NOT vs meili NOT mismatch: ${JSON.stringify(c4.ids)} vs ${JSON.stringify(c5.ids)}`,
  );
  log(`  [5] Meili NOT nsfwLevel=1: ids match compact NOT`);
}

// ----- Group D: Error handling per format -----

async function testD_ErrorHandling() {
  log('\n--- D. Error handling per format ---');

  // D1: Unknown format returns error
  const d1 = await apiPost(`/api/indexes/${INDEX}/query?format=algolia`, {});
  assert(d1.status === 400, `D1: expected 400 for unknown format, got ${d1.status}`);
  assert(d1.data.error && d1.data.error.includes('unknown query format'), `D1: error should mention unknown format`);
  log(`  [1] Unknown format 'algolia': ${d1.status} — ${d1.data.error}`);

  // D2: Compact format with invalid body
  const d2 = await apiPost(`/api/indexes/${INDEX}/query?format=compact`, 'not json', { raw: true });
  assert(d2.status === 400, `D2: expected 400, got ${d2.status}`);
  log(`  [2] Compact with garbage body: ${d2.status}`);

  // D3: Meilisearch format with invalid filter syntax
  const d3 = await apiPost(`/api/indexes/${INDEX}/query?format=meilisearch`, {
    filter: '= = invalid syntax',
  });
  assert(d3.status === 400, `D3: expected 400, got ${d3.status}`);
  log(`  [3] Meili with invalid filter: ${d3.status} — ${d3.data.error}`);

  // D4: Compact format with unknown operator
  const d4 = await apiPost(`/api/indexes/${INDEX}/query?format=compact`, {
    filter: { score: { '$regex': '.*' } },
  });
  assert(d4.status === 400, `D4: expected 400, got ${d4.status}`);
  assert(d4.data.error && d4.data.error.includes('unsupported operator'), `D4: should mention unsupported operator`);
  log(`  [4] Compact with $regex: ${d4.status} — ${d4.data.error}`);

  // D5: Meilisearch with unterminated string
  const d5 = await apiPost(`/api/indexes/${INDEX}/query?format=meilisearch`, {
    filter: "type = 'unterminated",
  });
  assert(d5.status === 400, `D5: expected 400, got ${d5.status}`);
  log(`  [5] Meili with unterminated string: ${d5.status} — ${d5.data.error}`);

  // D6: Default format still works fine (no ?format= param)
  await clearCache();
  const d6 = await apiPost(`/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'score', direction: 'Desc' },
    limit: 10,
  });
  assert(d6.status === 200, `D6: default format should work, got ${d6.status}`);
  assert(d6.data.ids && d6.data.ids.length > 0, `D6: should return results`);
  log(`  [6] Default format (no ?format=): ${d6.status}, ${d6.data.ids.length} results`);

  // D7: Explicit ?format=bitdex works
  const d7 = await apiPost(`/api/indexes/${INDEX}/query?format=bitdex`, {
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'score', direction: 'Desc' },
    limit: 10,
  });
  assert(d7.status === 200, `D7: explicit bitdex format should work, got ${d7.status}`);
  assert(
    JSON.stringify(d6.data.ids) === JSON.stringify(d7.data.ids),
    `D7: explicit bitdex should match default`,
  );
  log(`  [7] Explicit ?format=bitdex matches default`);
}

// ----- Group E: Offset pagination equivalence -----

async function testE_OffsetPagination() {
  log('\n--- E. Offset pagination equivalence ---');
  await clearCache();

  // E1: Same offset/limit across all formats produces same results
  const e1_bitdex = await queryBitdex(
    [],
    { field: 'score', direction: 'Desc' },
    2,
  );
  // Get page 2 (offset 2)
  const e1b_bitdex = (await apiPost(`/api/indexes/${INDEX}/query`, {
    filters: [],
    sort: { field: 'score', direction: 'Desc' },
    limit: 2,
    offset: 2,
  })).data;

  await clearCache();
  const e1b_compact = await queryCompact({
    sort: '-score',
    limit: 2,
    offset: 2,
  });

  await clearCache();
  const e1b_meili = await queryMeili({
    sort: ['score:desc'],
    hitsPerPage: 2,
    page: 2,
  });

  assert(
    JSON.stringify(e1b_bitdex.ids) === JSON.stringify(e1b_compact.ids),
    `E1: bitdex offset=2 vs compact offset=2: ${JSON.stringify(e1b_bitdex.ids)} vs ${JSON.stringify(e1b_compact.ids)}`,
  );
  assert(
    JSON.stringify(e1b_bitdex.ids) === JSON.stringify(e1b_meili.ids),
    `E1: bitdex offset=2 vs meili page=2: ${JSON.stringify(e1b_bitdex.ids)} vs ${JSON.stringify(e1b_meili.ids)}`,
  );

  // Verify page 2 doesn't overlap with page 1
  const page1Set = new Set(e1_bitdex.ids);
  const overlap = e1b_bitdex.ids.filter(id => page1Set.has(id));
  assert(overlap.length === 0, `E1: page 1 and 2 should not overlap`);

  log(`  [1] Offset page 2 (limit=2, offset=2): ${JSON.stringify(e1b_bitdex.ids)} (all 3 match)`);
}

// ----- Cleanup -----

async function cleanup() {
  if (KEEP) {
    log(`\n--- Keeping index '${INDEX}' (--keep) ---`);
    return;
  }
  log(`\n--- Cleanup: Deleting index '${INDEX}' ---`);
  await apiDelete(`/api/indexes/${INDEX}`);
}

// ----- Main -----

const groups = [
  ['A', '/api/formats endpoint', testA_FormatsEndpoint],
  ['B', 'Query equivalence across formats', testB_QueryEquivalence],
  ['C', 'Format-specific features', testC_FormatSpecificFeatures],
  ['D', 'Error handling per format', testD_ErrorHandling],
  ['E', 'Offset pagination equivalence', testE_OffsetPagination],
];

async function main() {
  log('='.repeat(60));
  log('BitDex E2E: Pluggable Query Formats');
  log('='.repeat(60));
  log(`Server: ${BASE_URL}`);

  // Verify server is reachable
  try {
    const res = await fetch(`${BASE_URL}/api/health`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    log('Server is reachable');
  } catch (e) {
    log(`\nERROR: Cannot reach server at ${BASE_URL}`);
    log(`  ${e.message}`);
    log(`\nStart the server first:`);
    log(`  cargo run --release --features server --bin bitdex-server -- --port 3000 --data-dir .test-data/manual`);
    process.exit(1);
  }

  await setup();

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

  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const resultsJson = {
      suite: 'query-formats',
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
    const resultsPath = resolve(RESULTS_DIR, 'query-formats.json');
    writeFileSync(resultsPath, JSON.stringify(resultsJson, null, 2));
    log(`JSON results written to: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
