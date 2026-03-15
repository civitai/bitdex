#!/usr/bin/env node
/**
 * E2E test: verify unified cache persists across server restarts.
 *
 * 1. Ensure server is running
 * 2. Clear cache
 * 3. Run a query (cold miss — should be slow)
 * 4. Run again (cache hit — should be fast)
 * 5. Check stats: cache entries > 0
 * 6. Wait for merge thread to persist (poll stats for a few seconds)
 * 7. Restart server via dev-server daemon
 * 8. Wait for server ready
 * 9. Run same query (should restore from persisted cache — fast, not cold miss)
 * 10. Compare latencies
 */

const BASE = 'http://localhost:3001';
const SKILL_DIR = process.env.HOME + '/.claude/skills/bitdex';
const DEV_SERVER = '.claude/skills/dev-server/cli.mjs';

const QUERY = {
  filters: [
    { In: ['nsfwLevel', [{ Integer: 1 }, { Integer: 2 }]] },
    { Eq: ['isPublished', { Bool: true }] },
    { In: ['type', [{ String: 'image' }]] },
  ],
  sort: { field: 'reactionCount', direction: 'Desc' },
  limit: 50,
};

async function req(method, path, body = null) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE}${path}`, opts);
  return res.json();
}

async function waitForServer(maxWait = 30000) {
  const start = Date.now();
  while (Date.now() - start < maxWait) {
    try {
      const res = await fetch(`${BASE}/api/health`);
      if (res.ok) return true;
    } catch {}
    await new Promise(r => setTimeout(r, 1000));
  }
  throw new Error('Server did not come up in time');
}

async function runDevServer(cmd) {
  const { execSync } = await import('child_process');
  const result = execSync(`node ${DEV_SERVER} ${cmd}`, {
    cwd: process.cwd(),
    encoding: 'utf8',
    timeout: 30000,
  });
  return result;
}

function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}

async function main() {
  console.log('=== Cache Persistence E2E Test ===\n');

  // 1. Ensure server running
  log('Checking server...');
  await waitForServer();
  const stats0 = await req('GET', '/api/indexes/civitai/stats');
  log(`Server up: ${stats0.alive_count?.toLocaleString()} records`);

  // 2. Purge cache (RAM + disk)
  log('Purging cache (RAM + disk)...');
  await req('DELETE', '/api/indexes/civitai/cache/persistent');

  // 3. Cold miss query
  log('Running query (cold miss)...');
  const cold = await req('POST', '/api/indexes/civitai/query', QUERY);
  log(`Cold miss: ${cold.elapsed_us}μs, matched: ${cold.total_matched?.toLocaleString()}`);

  // 4. Cache hit query
  log('Running query (cache hit)...');
  const warm = await req('POST', '/api/indexes/civitai/query', QUERY);
  log(`Cache hit: ${warm.elapsed_us}μs`);

  // 5. Verify cache populated
  const stats1 = await req('GET', '/api/indexes/civitai/stats');
  log(`Cache entries: ${stats1.unified_cache_entries}, hits: ${stats1.unified_cache_hits}`);
  if (stats1.unified_cache_entries === 0) {
    console.error('FAIL: No cache entries after query');
    process.exit(1);
  }

  // 6. Wait for merge thread to persist (check shard file appears)
  log('Waiting for cache persistence (merge thread writes every ~5s)...');
  const { existsSync } = await import('fs');
  const shardPath = 'data/indexes/civitai/bitmaps/bounds/reactionCount_Desc.ucpack';
  for (let i = 0; i < 6; i++) {
    await new Promise(r => setTimeout(r, 3000));
    if (existsSync(shardPath)) {
      const { statSync } = await import('fs');
      const stat = statSync(shardPath);
      log(`Shard file exists: ${shardPath} (${stat.size} bytes, modified ${stat.mtime.toISOString().slice(11,19)})`);
      break;
    }
    log(`Waiting... (${(i+1)*3}s)`);
  }

  const stats2 = await req('GET', '/api/indexes/civitai/stats');
  log(`Cache entries: ${stats2.unified_cache_entries}, hits: ${stats2.unified_cache_hits}`);

  // 7. Restart server
  log('Restarting server via daemon...');
  try {
    runDevServer('stop');
  } catch {}
  await new Promise(r => setTimeout(r, 3000));
  try {
    runDevServer('start');
  } catch {}

  // 8. Wait for server ready
  log('Waiting for server to come back...');
  await new Promise(r => setTimeout(r, 5000));
  await waitForServer();
  log('Server is back');

  // Give it a moment to finish eager loading
  await new Promise(r => setTimeout(r, 3000));

  // 9. Run same query — should hit persisted cache
  log('Running query after restart (should use persisted cache)...');
  const restored = await req('POST', '/api/indexes/civitai/query', QUERY);
  log(`After restart: ${restored.elapsed_us}μs, matched: ${restored.total_matched?.toLocaleString()}`);

  // 10. Run again for good measure
  const restored2 = await req('POST', '/api/indexes/civitai/query', QUERY);
  log(`Second query: ${restored2.elapsed_us}μs`);

  // Check cache state
  const stats3 = await req('GET', '/api/indexes/civitai/stats');
  log(`Cache entries: ${stats3.unified_cache_entries}, hits: ${stats3.unified_cache_hits}`);

  // Summary
  console.log('\n=== Results ===');
  console.log(`Cold miss:         ${cold.elapsed_us}μs`);
  console.log(`Cache hit:         ${warm.elapsed_us}μs`);
  console.log(`After restart #1:  ${restored.elapsed_us}μs`);
  console.log(`After restart #2:  ${restored2.elapsed_us}μs`);

  const coldUs = cold.elapsed_us;
  const restoredUs = restored.elapsed_us;
  const hitUs = warm.elapsed_us;

  if (restoredUs < coldUs * 0.5) {
    console.log(`\nPASS: Restored query (${restoredUs}μs) is >2x faster than cold miss (${coldUs}μs)`);
    console.log('Cache persistence is working!');
  } else if (restoredUs < coldUs) {
    console.log(`\nWARN: Restored (${restoredUs}μs) faster than cold (${coldUs}μs) but not by 2x`);
    console.log('Cache may have partially restored, or shard load added overhead');
  } else {
    console.log(`\nFAIL: Restored (${restoredUs}μs) not faster than cold (${coldUs}μs)`);
    console.log('Cache persistence may not be working');
    process.exit(1);
  }
}

main().catch(e => {
  console.error('Error:', e.message);
  process.exit(1);
});
