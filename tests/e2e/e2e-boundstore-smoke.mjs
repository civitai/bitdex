#!/usr/bin/env node
/**
 * BoundStore Smoke Benchmark — Fast Local Regression Detector
 *
 * Measures BoundStore operational characteristics with a tiny synthetic dataset.
 * Target runtime: <30 seconds. Deterministic for reproducible comparisons.
 *
 * Phases:
 *   1. Build cache (1K docs, 20 sorted queries → ~20 cache entries)
 *   2. Persist (meta.bin + shards — measure time, size, bytes/entry)
 *   3. Drop + Restore (restart server, measure meta load, shard load)
 *   4. Warm query replay (shard lazy load + cache hit latency)
 *   5. Cold comparison (clear cache, replay cold)
 *   6. Tombstone churn (mutate docs, measure tombstone creation/cleanup)
 *   7. Report with PASS/WARN/FAIL thresholds
 *
 * Usage:
 *   node tests/e2e/e2e-boundstore-smoke.mjs [--port 3100] [--verbose]
 *
 * The test starts its own server — do NOT start one externally.
 */

import { spawn } from 'node:child_process';
import { mkdirSync, rmSync, existsSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PROJECT_ROOT = resolve(__dirname, '..', '..');

const PORT = Number(process.argv.includes('--port')
  ? process.argv[process.argv.indexOf('--port') + 1]
  : '3100');
const VERBOSE = process.argv.includes('--verbose');
const RESULTS_DIR = process.argv.includes('--results-dir')
  ? process.argv[process.argv.indexOf('--results-dir') + 1]
  : null;
const BASE_URL = `http://localhost:${PORT}`;
const DATA_DIR = resolve(PROJECT_ROOT, '.test-data', 'boundstore-smoke');
const INDEX = 'bs-smoke';

let serverProcess = null;

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [v]', ...args); }

async function apiPost(path, body) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: res.status, data: await res.json() };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  return { status: res.status, data: await res.json() };
}

async function apiDelete(path) {
  const res = await fetch(`${BASE_URL}${path}`, { method: 'DELETE' });
  return { status: res.status, data: await res.json() };
}

const stats = () => apiGet(`/api/indexes/${INDEX}/stats`).then(r => r.data);
const execQuery = (body) => apiPost(`/api/indexes/${INDEX}/query`, body).then(r => r.data);
const upsert = (docs) => apiPost(`/api/indexes/${INDEX}/documents/upsert`, { documents: docs }).then(r => r.data);
const saveSnapshot = () => apiPost(`/api/indexes/${INDEX}/snapshot`).then(r => r.data);
const clearCache = () => apiDelete(`/api/indexes/${INDEX}/cache`).then(r => r.data);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function dirSize(dir) {
  if (!existsSync(dir)) return 0;
  return readdirSync(dir).reduce((sum, f) => {
    const p = resolve(dir, f);
    const s = statSync(p);
    return sum + (s.isDirectory() ? dirSize(p) : s.size);
  }, 0);
}

// ── Server Lifecycle ────────────────────────────────────────────────────────

async function startServer() {
  const binary = resolve(PROJECT_ROOT, 'target', 'release', 'bitdex-server');
  const binPath = existsSync(binary + '.exe') ? binary + '.exe' : binary;
  if (!existsSync(binPath)) {
    log('Building server...');
    const { execSync } = await import('node:child_process');
    execSync('cargo build --release --features server --bin bitdex-server', {
      cwd: PROJECT_ROOT, stdio: 'inherit',
    });
  }
  mkdirSync(DATA_DIR, { recursive: true });
  serverProcess = spawn(binPath, ['--port', String(PORT), '--data-dir', DATA_DIR], {
    cwd: PROJECT_ROOT,
    stdio: VERBOSE ? 'inherit' : 'pipe',
  });
  for (let i = 0; i < 60; i++) {
    await sleep(500);
    try { await fetch(`${BASE_URL}/api/indexes`); return; } catch {}
  }
  throw new Error('Server start timeout');
}

async function stopServer() {
  if (!serverProcess) return;
  serverProcess.kill('SIGTERM');
  await new Promise(r => { serverProcess.on('exit', r); setTimeout(r, 5000); });
  serverProcess = null;
  await sleep(500);
}

// ── Benchmark Queries ───────────────────────────────────────────────────────

const QUERIES = [];
// 5 nsfwLevel values × 2 sort fields × 2 directions = 20 queries
for (let nsfw = 1; nsfw <= 5; nsfw++) {
  for (const sf of ['reactionCount', 'commentCount']) {
    for (const dir of ['Desc', 'Asc']) {
      QUERIES.push({
        filters: [{ Eq: ['nsfwLevel', { Integer: nsfw }] }],
        sort: { field: sf, direction: dir },
        limit: 10,
      });
    }
  }
}

async function replayQueries() {
  const latencies = [];
  for (const q of QUERIES) {
    const t0 = performance.now();
    await execQuery(q);
    latencies.push(performance.now() - t0);
  }
  latencies.sort((a, b) => a - b);
  return {
    count: latencies.length,
    p50: latencies[Math.floor(latencies.length * 0.5)],
    p95: latencies[Math.floor(latencies.length * 0.95)],
    mean: latencies.reduce((s, v) => s + v, 0) / latencies.length,
  };
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  log('=== BoundStore Smoke Benchmark ===');
  const results = {};

  try {
    if (existsSync(DATA_DIR)) rmSync(DATA_DIR, { recursive: true, force: true });

    // ── Phase 1: Build Cache ──────────────────────────────────────────
    await startServer();

    // Create index
    try { await apiDelete(`/api/indexes/${INDEX}`); } catch {}
    await sleep(200);
    await apiPost('/api/indexes', {
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

    // Insert 1K docs
    const docs = [];
    for (let i = 1; i <= 1000; i++) {
      docs.push({
        id: i,
        nsfwLevel: ((i - 1) % 5) + 1,
        category: ((i - 1) % 3) + 1,
        reactionCount: (1001 - i) * 10,
        commentCount: i * 5,
      });
    }
    await upsert(docs);
    // Wait for flush
    for (let i = 0; i < 50; i++) {
      const s = await stats();
      if (s.alive_count === 1000) break;
      await sleep(100);
    }

    // Build cache by running all queries
    await replayQueries();
    // Run again to ensure cache hits
    await replayQueries();

    const s1 = await stats();
    results.cache_entries_before = s1.unified_cache_entries;
    results.cache_meta_entries_before = s1.unified_cache_meta_entries;
    results.cache_hits_before = s1.unified_cache_hits;
    results.cache_misses_before = s1.unified_cache_misses;
    log(`\n  Phase 1: ${results.cache_entries_before} cache entries, ${results.cache_hits_before} hits, ${results.cache_misses_before} misses`);

    // ── Phase 2: Persist ──────────────────────────────────────────────
    const t_persist_start = performance.now();
    await saveSnapshot();
    await upsert([{ id: 1, nsfwLevel: 1, category: 1, reactionCount: 10000, commentCount: 5 }]);
    await sleep(2000); // Wait for merge thread
    const t_persist = performance.now() - t_persist_start;

    const boundsDir = resolve(DATA_DIR, 'indexes', INDEX, 'bitmaps', 'bounds');
    const diskBytes = dirSize(boundsDir);
    const shardFiles = existsSync(boundsDir)
      ? readdirSync(boundsDir).filter(f => f.endsWith('.ucpack'))
      : [];
    const metaSize = existsSync(resolve(boundsDir, 'meta.bin'))
      ? statSync(resolve(boundsDir, 'meta.bin')).size
      : 0;

    results.persist_ms = t_persist;
    results.disk_bytes = diskBytes;
    results.meta_bytes = metaSize;
    results.shard_count = shardFiles.length;
    results.bytes_per_entry = results.cache_entries_before > 0
      ? Math.round(diskBytes / results.cache_entries_before)
      : 0;
    results.meta_bytes_per_entry = results.cache_entries_before > 0
      ? Math.round(metaSize / results.cache_entries_before)
      : 0;

    log(`  Phase 2: persist ${t_persist.toFixed(0)}ms, disk ${diskBytes} bytes (${results.bytes_per_entry} bytes/entry)`);
    log(`           meta.bin ${metaSize} bytes (${results.meta_bytes_per_entry}/entry), ${shardFiles.length} shards`);

    // ── Phase 3: Drop + Restore ───────────────────────────────────────
    await stopServer();
    const t_restore_start = performance.now();
    await startServer();
    const t_restore = performance.now() - t_restore_start;

    const s2 = await stats();
    results.restore_ms = t_restore;
    results.meta_entries_restored = s2.unified_cache_meta_entries;
    results.pending_shards_after = s2.unified_cache_pending_shards;
    results.ram_entries_after = s2.unified_cache_entries;

    log(`  Phase 3: restore ${t_restore.toFixed(0)}ms, meta=${s2.unified_cache_meta_entries} entries, pending=${s2.unified_cache_pending_shards} shards`);

    // ── Phase 4: Warm query replay ────────────────────────────────────
    const warm = await replayQueries();
    const s3 = await stats();
    results.warm_p50 = warm.p50;
    results.warm_p95 = warm.p95;
    results.warm_mean = warm.mean;
    results.shard_loads = s3.unified_cache_shard_load_count ?? 0;
    results.entries_restored_count = s3.unified_cache_entries_restored ?? 0;

    log(`  Phase 4: warm p50=${warm.p50.toFixed(2)}ms p95=${warm.p95.toFixed(2)}ms mean=${warm.mean.toFixed(2)}ms`);
    log(`           ${results.shard_loads} shard loads, ${results.entries_restored_count} entries restored`);

    // Hit rate after restore
    const s4 = await stats();
    const hits_after = s4.unified_cache_hits;
    const misses_after = s4.unified_cache_misses;
    const total_after = hits_after + misses_after;
    results.hit_rate_after = total_after > 0 ? (hits_after / total_after * 100) : 0;
    const total_before = results.cache_hits_before + results.cache_misses_before;
    results.hit_rate_before = total_before > 0 ? (results.cache_hits_before / total_before * 100) : 0;
    results.hit_rate_delta = results.hit_rate_after - results.hit_rate_before;

    // ── Phase 5: Cold comparison ──────────────────────────────────────
    await clearCache();
    const cold = await replayQueries();
    results.cold_p50 = cold.p50;
    results.cold_p95 = cold.p95;
    results.cold_mean = cold.mean;
    results.speedup = cold.p50 > 0 ? (cold.p50 / warm.p50) : 0;

    log(`  Phase 5: cold p50=${cold.p50.toFixed(2)}ms, warm p50=${warm.p50.toFixed(2)}ms, speedup=${results.speedup.toFixed(1)}x`);

    // ── Phase 6: Tombstone churn ──────────────────────────────────────
    // Restart to put entries on disk, then mutate
    await saveSnapshot();
    await sleep(1500);
    await stopServer();
    await startServer();
    await sleep(500);

    const s5 = await stats();
    const tombstones_before = s5.unified_cache_tombstones;

    // Mutate 200 docs (change nsfwLevel)
    const mutations = [];
    for (let i = 1; i <= 200; i++) {
      mutations.push({
        id: i,
        nsfwLevel: ((i - 1) % 5) + 2, // shift by 1 — changes filter membership
        category: ((i - 1) % 3) + 1,
        reactionCount: (1001 - i) * 10,
        commentCount: i * 5,
      });
    }
    await upsert(mutations);
    await sleep(1000);

    const s6 = await stats();
    results.tombstones_after_churn = s6.unified_cache_tombstones ?? 0;
    results.tombstones_created = s6.unified_cache_tombstones_created ?? 0;
    results.tombstones_cleaned = s6.unified_cache_tombstones_cleaned ?? 0;
    const cleanup_ratio = results.tombstones_created > 0
      ? (results.tombstones_cleaned / results.tombstones_created * 100)
      : 100;
    results.tombstone_cleanup_ratio = cleanup_ratio;

    log(`  Phase 6: tombstones created=${results.tombstones_created} cleaned=${results.tombstones_cleaned} ratio=${cleanup_ratio.toFixed(0)}%`);

    // ── Phase 7: Report ───────────────────────────────────────────────
    log('\n=== BoundStore Smoke Report ===\n');

    const rows = [
      ['meta bytes/entry',     results.meta_bytes_per_entry,  results.meta_bytes_per_entry > 250 ? 'WARN' : 'ok'],
      ['disk bytes/entry',     results.bytes_per_entry,       results.bytes_per_entry > 8192 ? 'WARN' : 'ok'],
      ['shard count',          results.shard_count,           'info'],
      ['persist (ms)',         results.persist_ms.toFixed(0), 'info'],
      ['restore (ms)',         results.restore_ms.toFixed(0), 'info'],
      ['meta entries restored',results.meta_entries_restored, results.meta_entries_restored > 0 ? 'ok' : 'WARN'],
      ['warm p50 (ms)',        results.warm_p50.toFixed(2),   'info'],
      ['cold p50 (ms)',        results.cold_p50.toFixed(2),   'info'],
      ['speedup',              results.speedup.toFixed(1)+'x',results.speedup < 1.0 ? 'FAIL' : 'ok'],
      ['hit rate before (%)',  results.hit_rate_before.toFixed(1), 'info'],
      ['hit rate after (%)',   results.hit_rate_after.toFixed(1),  'info'],
      ['tombstones created',   results.tombstones_created,    results.tombstones_created > 0 ? 'ok' : 'WARN'],
    ];

    const maxLabel = Math.max(...rows.map(r => r[0].length));
    for (const [label, value, status] of rows) {
      const flag = status === 'FAIL' ? ' ← FAIL' : status === 'WARN' ? ' ← WARN' : '';
      log(`  ${label.padEnd(maxLabel)}  ${String(value).padStart(10)}  ${flag}`);
    }

    const hasFail = rows.some(r => r[2] === 'FAIL');
    const hasWarn = rows.some(r => r[2] === 'WARN');

    // One-line summary
    log(`\nBoundStore smoke: meta=${results.meta_bytes_per_entry}B/entry disk=${results.bytes_per_entry}B/entry` +
        ` persist=${results.persist_ms.toFixed(0)}ms warm_p50=${results.warm_p50.toFixed(1)}ms` +
        ` cold_p50=${results.cold_p50.toFixed(1)}ms speedup=${results.speedup.toFixed(1)}x` +
        ` tombstones=${results.tombstones_created}` +
        ` ${hasFail ? 'FAIL' : hasWarn ? 'WARN' : 'PASS'}`);

    if (hasFail) process.exitCode = 1;

    // Write JSON results if --results-dir provided
    if (RESULTS_DIR) {
      mkdirSync(RESULTS_DIR, { recursive: true });
      const resultsJson = {
        suite: 'boundstore-smoke',
        timestamp: new Date().toISOString(),
        results,
        thresholds: {
          meta_bytes_per_entry: { warn: 250 },
          bytes_per_entry: { warn: 8192 },
          speedup: { fail_below: 1.0 },
        },
        status: hasFail ? 'FAIL' : hasWarn ? 'WARN' : 'PASS',
      };
      const resultsPath = resolve(RESULTS_DIR, 'boundstore-smoke.json');
      writeFileSync(resultsPath, JSON.stringify(resultsJson, null, 2));
      log(`\nJSON results written to: ${resultsPath}`);
    }

  } catch (e) {
    log(`\nFATAL: ${e.message}`);
    if (VERBOSE) console.error(e.stack);
    process.exitCode = 1;
  } finally {
    await stopServer();
    if (existsSync(DATA_DIR)) rmSync(DATA_DIR, { recursive: true, force: true });
  }
}

main();
