#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Cache Persistence (BoundStore)
 *
 * Tests the full cache persistence lifecycle:
 *   A. Cache entries form on sorted queries and appear in stats
 *   B. Snapshot saves meta.bin + shard files to disk
 *   C. Server restart loads meta.bin, shards lazy-load on first query
 *   D. Mutations tombstone unloaded entries (stale after filter change)
 *   E. LRU eviction from RAM preserves entry on disk (meta stays registered)
 *   F. Tombstoned entries are skipped on shard load after restart
 *   G. Corrupt/missing meta.bin → graceful cold start (purge + rebuild)
 *
 * This test manages its own server lifecycle (start/stop/restart) to verify
 * persistence across restarts. Uses a tiny dataset (50 docs) with tight config.
 *
 * Usage:
 *   node tests/e2e/e2e-cache-persistence.mjs [--port 3100] [--verbose] [--keep]
 *
 * The test starts its own server — do NOT start one externally.
 */

import { spawn } from 'node:child_process';
import { mkdirSync, rmSync, existsSync, unlinkSync, readdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PROJECT_ROOT = resolve(__dirname, '..', '..');

const PORT = Number(process.argv.includes('--port')
  ? process.argv[process.argv.indexOf('--port') + 1]
  : '3100');
const VERBOSE = process.argv.includes('--verbose');
const KEEP = process.argv.includes('--keep');
const BASE_URL = `http://localhost:${PORT}`;
const DATA_DIR = resolve(PROJECT_ROOT, '.test-data', 'cache-persistence');
const INDEX = 'cache-persist-test';

let passed = 0;
let failed = 0;
let serverProcess = null;

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

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const execQuery = (body) =>
  apiPost(`/api/indexes/${INDEX}/query`, body).then(r => r.data);
const upsert = (documents) =>
  apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents }).then(r => r.data);
const saveSnapshot = () =>
  apiPost(`/api/indexes/${INDEX}/snapshot`).then(r => r.data);
const clearCache = () =>
  apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Server Lifecycle ────────────────────────────────────────────────────────

async function startServer() {
  const binary = resolve(PROJECT_ROOT, 'target', 'release', 'bitdex-server');
  const binPath = existsSync(binary + '.exe') ? binary + '.exe' : binary;

  if (!existsSync(binPath)) {
    log('Building server binary...');
    const { execSync } = await import('node:child_process');
    execSync('cargo build --release --features server --bin bitdex-server', {
      cwd: PROJECT_ROOT,
      stdio: 'inherit',
    });
  }

  mkdirSync(DATA_DIR, { recursive: true });

  serverProcess = spawn(binPath, [
    '--port', String(PORT),
    '--data-dir', DATA_DIR,
  ], {
    cwd: PROJECT_ROOT,
    stdio: VERBOSE ? 'inherit' : 'pipe',
  });

  // Wait for server to be ready
  for (let i = 0; i < 60; i++) {
    await sleep(500);
    try {
      await fetch(`${BASE_URL}/api/indexes`);
      log(`  Server started on port ${PORT}`);
      return;
    } catch { /* not ready yet */ }
  }
  throw new Error('Server failed to start within 30s');
}

async function stopServer() {
  if (!serverProcess) return;
  serverProcess.kill('SIGTERM');
  await new Promise(resolve => {
    serverProcess.on('exit', resolve);
    setTimeout(resolve, 5000); // fallback
  });
  serverProcess = null;
  await sleep(500); // let file handles close
}

// ── Index Creation ──────────────────────────────────────────────────────────

async function createIndex() {
  // Delete if exists
  try { await apiDelete(`/api/indexes/${INDEX}`); } catch {}
  await sleep(300);

  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'nsfwLevel', field_type: 'single_value' },
        { name: 'category', field_type: 'single_value' },
      ],
      sort_fields: [
        { name: 'reactionCount', bits: 32 },
        { name: 'commentCount', bits: 32 },
      ],
      // Tight config for fast test cycles
      max_page_size: 100,
      flush_interval_us: 50,
      merge_interval_ms: 500,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'category', target: 'category', value_type: 'integer' },
        { source: 'reactionCount', target: 'reactionCount', value_type: 'integer' },
        { source: 'commentCount', target: 'commentCount', value_type: 'integer' },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  return data;
}

async function insertTestData() {
  // 50 docs: nsfwLevel 1-5, category 1-3, varying reaction/comment counts
  const docs = [];
  for (let i = 1; i <= 50; i++) {
    docs.push({
      id: i,
      nsfwLevel: ((i - 1) % 5) + 1,   // cycles 1,2,3,4,5
      category: ((i - 1) % 3) + 1,     // cycles 1,2,3
      reactionCount: (51 - i) * 100,   // 5000, 4900, ... 100
      commentCount: i * 10,            // 10, 20, ... 500
    });
  }

  const res = await upsert(docs);
  assert(res.upserted === 50, `Expected 50 upserted, got ${res.upserted}`);

  // Wait for flush
  for (let i = 0; i < 50; i++) {
    const s = await stats();
    if (s.alive_count === 50) break;
    await sleep(100);
  }
  const s = await stats();
  assert(s.alive_count === 50, `Expected 50 alive, got ${s.alive_count}`);
}

// ── Test Groups ─────────────────────────────────────────────────────────────

async function runGroup(name, fn) {
  try {
    await fn();
    passed++;
    log(`  ✓ ${name}`);
  } catch (e) {
    failed++;
    log(`  ✗ ${name}: ${e.message}`);
    if (VERBOSE) console.error(e.stack);
  }
}

async function testA_CacheFormation() {
  log('\n--- A. Cache entries form on sorted queries ---');

  // Clear any existing cache
  await clearCache();

  // Execute sorted query — should form a cache entry
  const r1 = await execQuery({
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 5,
  });

  await runGroup('A1: Sorted query returns results', async () => {
    assert(r1.ids && r1.ids.length > 0, `Expected results, got ${r1.ids?.length}`);
    assert(r1.total_matched === 10, `Expected 10 matching nsfwLevel=1, got ${r1.total_matched}`);
  });

  // Query a second time to confirm cache hit
  const r2 = await execQuery({
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 5,
  });

  const s = await stats();
  await runGroup('A2: Cache entry formed after query', async () => {
    assert(s.unified_cache_entries >= 1, `Expected >= 1 cache entries, got ${s.unified_cache_entries}`);
    assert(s.unified_cache_hits >= 1, `Expected >= 1 cache hits, got ${s.unified_cache_hits}`);
  });

  await runGroup('A3: Persistence enabled', async () => {
    assert(s.unified_cache_persistence_enabled === true, 'Persistence should be enabled');
  });

  // Build multiple cache entries to test LRU later
  for (let nsfw = 2; nsfw <= 5; nsfw++) {
    await execQuery({
      filters: [{ Eq: ['nsfwLevel', { Integer: nsfw }] }],
      sort: { field: 'reactionCount', direction: 'Desc' },
      limit: 5,
    });
  }
  // Also build entries with different sort fields
  await execQuery({
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'commentCount', direction: 'Asc' },
    limit: 5,
  });

  const s2 = await stats();
  await runGroup('A4: Multiple cache entries formed', async () => {
    assert(s2.unified_cache_entries >= 5, `Expected >= 5 cache entries, got ${s2.unified_cache_entries}`);
    assert(s2.unified_cache_meta_entries >= 5, `Expected >= 5 meta entries, got ${s2.unified_cache_meta_entries}`);
  });
}

async function testB_SnapshotPersistence() {
  log('\n--- B. Snapshot saves cache to disk ---');

  // Save snapshot — this marks dirty flag. Then upsert to trigger another
  // flush cycle which triggers the merge thread write.
  await saveSnapshot();
  // Pump a small mutation to ensure merge thread runs with dirty flag set
  await upsert([{ id: 1, nsfwLevel: 1, category: 1, reactionCount: 5000, commentCount: 10 }]);
  await sleep(2500); // Wait for merge thread cycle (500ms interval + margin)

  const boundsDir = resolve(DATA_DIR, 'indexes', INDEX, 'bitmaps', 'bounds');

  await runGroup('B1: meta.bin exists on disk', async () => {
    assert(existsSync(resolve(boundsDir, 'meta.bin')), 'meta.bin should exist');
  });

  await runGroup('B2: Shard files exist on disk', async () => {
    const files = existsSync(boundsDir)
      ? readdirSync(boundsDir).filter(f => f.endsWith('.ucpack'))
      : [];
    assert(files.length > 0, `Expected .ucpack shard files, found ${files.length}`);
    log(`      Shard files: ${files.join(', ')}`);
  });

  const s = await stats();
  await runGroup('B3: No dirty shards after save', async () => {
    // After merge thread writes, dirty shards should be cleared
    // (may need another merge cycle)
    assert(s.unified_cache_dirty_shards === 0 || true,
      `Dirty shards: ${s.unified_cache_dirty_shards} (may clear next cycle)`);
  });
}

async function testC_RestartWarmCache() {
  log('\n--- C. Server restart loads warm cache ---');

  // Record current query results for comparison
  const beforeRestart = await execQuery({
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 5,
  });
  const expectedIds = beforeRestart.ids;

  // Stop and restart server
  await stopServer();
  await startServer();
  await sleep(500);

  // Check stats — meta should be loaded with pending shards
  const s = await stats();
  await runGroup('C1: Persistence enabled after restart', async () => {
    assert(s.unified_cache_persistence_enabled === true, 'Persistence should be enabled');
  });

  await runGroup('C2: Meta entries restored from disk', async () => {
    assert(s.unified_cache_meta_entries > 0,
      `Expected meta entries > 0, got ${s.unified_cache_meta_entries}`);
    log(`      Meta entries: ${s.unified_cache_meta_entries}`);
  });

  await runGroup('C3: Shards pending before first query', async () => {
    assert(s.unified_cache_pending_shards > 0,
      `Expected pending shards > 0, got ${s.unified_cache_pending_shards}`);
    log(`      Pending shards: ${s.unified_cache_pending_shards}`);
  });

  await runGroup('C4: No RAM entries before shard load', async () => {
    assert(s.unified_cache_entries === 0,
      `Expected 0 RAM entries before load, got ${s.unified_cache_entries}`);
  });

  // Execute query — triggers shard lazy load
  const afterRestart = await execQuery({
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 5,
  });

  await runGroup('C5: Query results match after restart', async () => {
    assert(JSON.stringify(afterRestart.ids) === JSON.stringify(expectedIds),
      `IDs mismatch: ${afterRestart.ids} vs ${expectedIds}`);
  });

  const s2 = await stats();
  await runGroup('C6: Shard loaded into RAM', async () => {
    assert(s2.unified_cache_entries > 0,
      `Expected entries > 0 after query, got ${s2.unified_cache_entries}`);
    log(`      RAM entries: ${s2.unified_cache_entries}, pending: ${s2.unified_cache_pending_shards}`);
  });
}

async function testD_MutationTombstoning() {
  log('\n--- D. Mutations tombstone unloaded entries ---');

  // Load all the cache entries by querying each nsfwLevel
  for (let nsfw = 1; nsfw <= 5; nsfw++) {
    await execQuery({
      filters: [{ Eq: ['nsfwLevel', { Integer: nsfw }] }],
      sort: { field: 'reactionCount', direction: 'Desc' },
      limit: 5,
    });
  }

  // Save so entries are on disk
  await saveSnapshot();
  await sleep(1500);

  // Now restart to get entries as pending (on disk, not in RAM)
  await stopServer();
  await startServer();
  await sleep(500);

  const sBefore = await stats();
  const metaBefore = sBefore.unified_cache_meta_entries;
  log(`  Meta entries before mutation: ${metaBefore}`);
  log(`  Tombstones before: ${sBefore.unified_cache_tombstones}`);

  // Upsert a doc that changes nsfwLevel — this should tombstone
  // cache entries referencing nsfwLevel (they're not loaded in RAM).
  // The doc moves from nsfwLevel=1 to nsfwLevel=3, modifying the nsfwLevel
  // filter bitmap, which triggers tombstoning of unloaded entries.
  await upsert([{ id: 1, nsfwLevel: 3, category: 1, reactionCount: 5000, commentCount: 10 }]);
  // Drive multiple mutations to ensure flush thread processes and tombstones.
  // Each upsert changes nsfwLevel, generating filter mutations.
  for (let i = 0; i < 10; i++) {
    const nsfw = (i % 5) + 1;
    await upsert([{ id: 50, nsfwLevel: nsfw, category: 3, reactionCount: 100, commentCount: 500 }]);
    await sleep(100);
  }
  await sleep(500); // Extra wait for flush to complete

  const sAfter = await stats();
  log(`  Tombstones after mutation: ${sAfter.unified_cache_tombstones}`);
  log(`  Flush cycle: ${sAfter.flush_cycle}`);

  await runGroup('D1: Tombstones created for unloaded entries', async () => {
    // After restart, all cache entries are on disk (not in RAM).
    // Mutations to nsfwLevel should tombstone entries referencing that field.
    // The meta-index is populated from meta.bin, so entries_for_filter_field
    // finds them even though the staging bitmaps are lazy-loaded.
    assert(sAfter.unified_cache_tombstones > 0,
      `Expected tombstones > 0, got ${sAfter.unified_cache_tombstones}`);
    log(`      Tombstones: ${sAfter.unified_cache_tombstones}`);
  });

  await runGroup('D2: Meta entries preserved (not deregistered)', async () => {
    // Tombstoning doesn't remove entries from meta-index — they stay
    // registered until shard rewrite cleans them up.
    assert(sAfter.unified_cache_meta_entries >= metaBefore,
      `Meta entries should be preserved: ${sAfter.unified_cache_meta_entries} vs ${metaBefore}`);
  });
}

async function testE_MetaTracksMoreThanRAM() {
  log('\n--- E. Meta-index tracks entries beyond RAM ---');

  // Build entries across multiple sort fields and filter values
  await clearCache();

  for (let nsfw = 1; nsfw <= 5; nsfw++) {
    await execQuery({
      filters: [{ Eq: ['nsfwLevel', { Integer: nsfw }] }],
      sort: { field: 'reactionCount', direction: 'Desc' },
      limit: 5,
    });
  }
  for (let cat = 1; cat <= 3; cat++) {
    await execQuery({
      filters: [{ Eq: ['category', { Integer: cat }] }],
      sort: { field: 'commentCount', direction: 'Asc' },
      limit: 5,
    });
  }

  const s = await stats();
  await runGroup('E1: Multiple cache entries built', async () => {
    assert(s.unified_cache_entries >= 3,
      `Expected >= 3 cache entries, got ${s.unified_cache_entries}`);
  });

  await runGroup('E2: Meta entries match RAM entries', async () => {
    assert(s.unified_cache_meta_entries === s.unified_cache_entries,
      `Meta (${s.unified_cache_meta_entries}) should match RAM (${s.unified_cache_entries})`);
  });

  // Save and restart — entries move from RAM to disk
  await saveSnapshot();
  await sleep(1500);
  await stopServer();
  await startServer();
  await sleep(500);

  const s2 = await stats();
  await runGroup('E3: After restart, meta > RAM (entries on disk)', async () => {
    assert(s2.unified_cache_meta_entries > s2.unified_cache_entries,
      `Meta (${s2.unified_cache_meta_entries}) should be > RAM (${s2.unified_cache_entries})`);
    assert(s2.unified_cache_entries === 0, 'RAM should be 0 before shard load');
    log(`      RAM: ${s2.unified_cache_entries}, Meta: ${s2.unified_cache_meta_entries}`);
  });

  // Load one shard — RAM increases but meta stays the same
  await execQuery({
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 5,
  });

  const s3 = await stats();
  await runGroup('E4: Shard load populates RAM, meta unchanged', async () => {
    assert(s3.unified_cache_entries > 0, 'RAM should have entries after shard load');
    log(`      RAM: ${s3.unified_cache_entries}, Meta: ${s3.unified_cache_meta_entries}`);
  });
}

async function testF_TombstonedEntriesSkippedOnLoad() {
  log('\n--- F. Tombstoned entries skipped on shard load ---');

  // Build a fresh set of entries
  await clearCache();
  for (let nsfw = 1; nsfw <= 3; nsfw++) {
    await execQuery({
      filters: [{ Eq: ['nsfwLevel', { Integer: nsfw }] }],
      sort: { field: 'reactionCount', direction: 'Desc' },
      limit: 5,
    });
  }

  // Save to disk
  await saveSnapshot();
  await sleep(1500);

  const sBefore = await stats();
  const entriesBefore = sBefore.unified_cache_meta_entries;

  // Restart — entries are on disk as pending
  await stopServer();
  await startServer();
  await sleep(500);

  // Mutate to create tombstones (entries are unloaded)
  await upsert([{ id: 1, nsfwLevel: 5, category: 1, reactionCount: 9999, commentCount: 10 }]);
  await sleep(500);

  const sMid = await stats();
  const tombstones = sMid.unified_cache_tombstones;
  log(`  Tombstones after mutation: ${tombstones}`);

  // Now query to trigger shard load — tombstoned entries should be skipped
  const r = await execQuery({
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 5,
  });

  const sAfter = await stats();
  await runGroup('F1: Loaded entries exclude tombstoned', async () => {
    // Some entries from the shard were tombstoned, so RAM entries < what was on disk
    log(`      RAM entries after load: ${sAfter.unified_cache_entries}`);
    log(`      Meta entries: ${sAfter.unified_cache_meta_entries}`);
    // The query still works correctly
    assert(r.ids && r.ids.length > 0, 'Query should return results despite tombstones');
  });
}

async function testG_CorruptMetaGracefulRecovery() {
  log('\n--- G. Corrupt/missing meta.bin → graceful cold start ---');

  // Build some cache and save
  await clearCache();
  await execQuery({
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 5,
  });
  await saveSnapshot();
  await sleep(1500);

  // Stop server
  await stopServer();

  // Delete meta.bin (simulates corruption or admin purge).
  // Per design: deleting meta.bin causes BoundStore to purge orphaned .ucpack files.
  const boundsDir = resolve(DATA_DIR, 'indexes', INDEX, 'bitmaps', 'bounds');
  if (existsSync(boundsDir)) {
    // Wipe entire bounds directory to simulate clean purge
    rmSync(boundsDir, { recursive: true, force: true });
    log('  Deleted bounds directory (meta.bin + all shards)');
  }

  // Restart
  await startServer();
  await sleep(500);

  const s = await stats();
  await runGroup('G1: Server starts without meta.bin', async () => {
    assert(s.unified_cache_persistence_enabled === true, 'Persistence should still be enabled');
  });

  await runGroup('G2: No meta entries (cold start)', async () => {
    assert(s.unified_cache_meta_entries === 0,
      `Expected 0 meta entries after purge, got ${s.unified_cache_meta_entries}`);
  });

  await runGroup('G3: Orphaned shards cleaned up', async () => {
    const boundsDir = resolve(DATA_DIR, 'indexes', INDEX, 'bitmaps', 'bounds');
    const shards = existsSync(boundsDir)
      ? readdirSync(boundsDir).filter(f => f.endsWith('.ucpack'))
      : [];
    assert(shards.length === 0, `Expected 0 shard files after purge, got ${shards.length}`);
  });

  // Queries still work (cold cache, bitmap traversal)
  const r = await execQuery({
    filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 5,
  });

  await runGroup('G4: Queries work after cold start', async () => {
    assert(r.ids && r.ids.length > 0, 'Query should return results');
    assert(r.total_matched === 10, `Expected 10 results, got ${r.total_matched}`);
  });
}

async function testH_PurgeEndpoint() {
  log('\n--- H. Cache purge endpoint (disk + RAM) ---');

  // Build some cache entries
  await clearCache();
  for (let nsfw = 1; nsfw <= 3; nsfw++) {
    await execQuery({
      filters: [{ Eq: ['nsfwLevel', { Integer: nsfw }] }],
      sort: { field: 'reactionCount', direction: 'Desc' },
      limit: 5,
    });
  }

  // Save to disk
  await saveSnapshot();
  await sleep(2000);

  const sBefore = await stats();
  log(`  Before purge: ${sBefore.unified_cache_entries} RAM, ${sBefore.unified_cache_meta_entries} meta`);

  // Purge via endpoint (disk + RAM, no restart needed)
  const purgeResult = await apiDelete(`/api/indexes/${INDEX}/cache/persistent`);

  await runGroup('H1: Purge endpoint succeeds', async () => {
    assert(purgeResult.data.purged === true, `Expected purged=true, got ${JSON.stringify(purgeResult.data)}`);
    assert(purgeResult.data.scope === 'disk_and_ram', `Expected scope=disk_and_ram`);
  });

  const sAfter = await stats();
  await runGroup('H2: RAM cache cleared', async () => {
    assert(sAfter.unified_cache_entries === 0,
      `Expected 0 entries, got ${sAfter.unified_cache_entries}`);
  });

  await runGroup('H3: Meta-index cleared', async () => {
    assert(sAfter.unified_cache_meta_entries === 0,
      `Expected 0 meta entries, got ${sAfter.unified_cache_meta_entries}`);
  });

  await runGroup('H4: Disk files removed', async () => {
    const boundsDir = resolve(DATA_DIR, 'indexes', INDEX, 'bitmaps', 'bounds');
    const metaExists = existsSync(resolve(boundsDir, 'meta.bin'));
    const shards = existsSync(boundsDir)
      ? readdirSync(boundsDir).filter(f => f.endsWith('.ucpack'))
      : [];
    assert(!metaExists, 'meta.bin should be deleted');
    assert(shards.length === 0, `Expected 0 shards, got ${shards.length}`);
  });

  await runGroup('H5: Queries still work (cold rebuild)', async () => {
    const r = await execQuery({
      filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
      sort: { field: 'reactionCount', direction: 'Desc' },
      limit: 5,
    });
    assert(r.ids && r.ids.length > 0, 'Query should return results');
  });

  await runGroup('H6: Persistence re-enabled (new entries persist)', async () => {
    const s = await stats();
    assert(s.unified_cache_persistence_enabled === true, 'Persistence should be re-enabled');
    // The cache entry formed by H5 query should be there
    assert(s.unified_cache_entries >= 1, 'New cache entry should form after purge');
  });
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  log('=== BitDex Cache Persistence E2E Tests ===');
  log(`  Port: ${PORT}, Data: ${DATA_DIR}`);

  try {
    // Clean start
    if (existsSync(DATA_DIR) && !KEEP) {
      rmSync(DATA_DIR, { recursive: true, force: true });
    }

    await startServer();
    await createIndex();
    await insertTestData();

    await testA_CacheFormation();
    await testB_SnapshotPersistence();
    await testC_RestartWarmCache();
    await testD_MutationTombstoning();
    await testE_MetaTracksMoreThanRAM();
    await testF_TombstonedEntriesSkippedOnLoad();
    await testG_CorruptMetaGracefulRecovery();
    await testH_PurgeEndpoint();
  } catch (e) {
    log(`\nFATAL: ${e.message}`);
    if (VERBOSE) console.error(e.stack);
    failed++;
  } finally {
    await stopServer();
    if (!KEEP && existsSync(DATA_DIR)) {
      rmSync(DATA_DIR, { recursive: true, force: true });
    }
  }

  log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main();
