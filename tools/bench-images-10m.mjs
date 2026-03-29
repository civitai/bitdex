#!/usr/bin/env node
/**
 * Quick images-only benchmark: sends JUST the images dump with 10M row limit.
 * Captures per-component timing breakdown from the server log.
 *
 * Usage:
 *   1. Start server (with 28 threads)
 *   2. Run: node tools/bench-images-10m.mjs
 */

const BASE = 'http://localhost:3001';
const STAGE_DIR = 'C:/Dev/Repos/open-source/bitdex-v2/data/load_stage';

// Images dump request — same as validate-dump-processor but standalone
const IMAGES_REQUEST = {
  name: 'images-bench-10m',
  csv_path: `${STAGE_DIR}/images.csv`,
  format: 'csv',
  slot_field: 'id',
  columns: ['id', 'url', 'nsfwLevel', 'hash', 'flags', 'type', 'userId', 'blockedFor', 'scannedAtSecs', 'createdAtSecs', 'postId'],
  sets_alive: true,
  fields: [
    'nsfwLevel',
    { column: 'type', target: 'type' },
    'userId',
    'postId',
    'blockedFor',
    { column: 'url', target: 'url' },
    { column: 'hash', target: 'hash' },
  ],
  computed_fields: [
    { target: 'hasMeta', expression: '(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0' },
    { target: 'onSite', expression: '(flags >> 14) & 1 == 1' },
    { target: 'minor', expression: '(flags >> 3) & 1 == 1' },
    { target: 'poi', expression: '(flags >> 4) & 1 == 1' },
    { target: 'existedAt', expression: 'max(scannedAtSecs, createdAtSecs)' },
    { target: 'id', expression: 'id' },
  ],
  enrichment: [
    {
      csv_path: `${STAGE_DIR}/posts.csv`,
      columns: ['id', 'publishedAtSecs', 'availability', 'modelVersionId'],
      key: 'id',
      join_on: 'postId',
      fields: [
        { column: 'publishedAtSecs', target: 'publishedAt' },
        { column: 'availability', target: 'availability' },
      ],
      computed_fields: [
        { target: 'postedToId', expression: 'lookup_key' },
        { target: 'isPublished', expression: 'publishedAtSecs != null' },
      ],
    },
  ],
};

// We use a truncated CSV — create it before running if it doesn't exist.
const TRUNCATED_PATH = `${STAGE_DIR}/images-10m.csv`;
const IMAGES_10M_REQUEST = { ...IMAGES_REQUEST, csv_path: TRUNCATED_PATH };

async function sendDump(request) {
  const res = await fetch(`${BASE}/api/indexes/civitai/dumps`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });
  return res.json();
}

async function pollTask(taskId) {
  const startTime = Date.now();
  while (true) {
    if (Date.now() - startTime > 10 * 60 * 1000) {
      throw new Error('TIMEOUT: exceeded 10 minutes');
    }
    const res = await fetch(`${BASE}/api/tasks/${taskId}`);
    const task = await res.json();
    const progress = task.progress?.records_processed || 0;
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = progress > 0 ? Math.round(progress / elapsed) : 0;

    process.stdout.write(`\r  [${Math.round(elapsed)}s] ${(progress / 1e6).toFixed(1)}M rows (${(rate / 1000).toFixed(0)}K/s)   `);

    if (task.status === 'complete') {
      console.log('');
      return { elapsed_s: elapsed, rows: progress, rate };
    }
    if (task.status === 'error') throw new Error(`Task failed: ${task.error}`);

    await new Promise(r => setTimeout(r, 1000)); // poll every 1s for tighter timing
  }
}

async function main() {
  console.log('=== Images 10M Benchmark ===\n');

  // Check server
  try {
    await fetch(`${BASE}/api/health`);
  } catch {
    console.error('ERROR: Server not running on', BASE);
    process.exit(1);
  }

  // Check truncated CSV exists
  const fs = await import('fs');
  if (!fs.existsSync(TRUNCATED_PATH)) {
    console.error(`  ERROR: ${TRUNCATED_PATH} not found.`);
    console.error(`  Create it first: head -10000000 ${STAGE_DIR}/images.csv > ${TRUNCATED_PATH}`);
    process.exit(1);
  }
  const size = fs.statSync(TRUNCATED_PATH).size;
  console.log(`  CSV: ${TRUNCATED_PATH} (${(size / 1e6).toFixed(0)} MB)`);

  console.log('  Submitting images dump (10M rows)...');
  const result = await sendDump(IMAGES_10M_REQUEST);

  if (result.error) {
    console.error('  ERROR:', result.error);
    process.exit(1);
  }

  if (result.task_id) {
    const completed = await pollTask(result.task_id);
    console.log(`\n  Done: ${(completed.rows / 1e6).toFixed(1)}M rows in ${completed.elapsed_s.toFixed(1)}s (${(completed.rate / 1000).toFixed(0)}K/s)`);
    console.log('\n  Check server.log for component_timing and save_timing JSON lines.');
  }
}

main().catch(e => { console.error(e); process.exit(1); });
