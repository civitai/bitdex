#!/usr/bin/env node
/**
 * Tags-only benchmark. Submits tags dump, polls progress, reports rate.
 * Usage: node bench-tags-only.mjs [port]
 */
const PORT = process.argv[2] || '3001';
const BASE = `http://localhost:${PORT}`;
const STAGE_DIR = 'C:/Dev/Repos/open-source/bitdex-v2/data/load_stage';

const TAGS_REQUEST = {
  name: 'tags-bench',
  csv_path: `${STAGE_DIR}/tags.csv`,
  format: 'csv',
  slot_field: 'imageId',
  columns: ['tagId', 'imageId'],
  sets_alive: false,
  fields: [{ column: 'tagId', target: 'tagIds' }],
};

async function main() {
  console.log(`=== Tags Benchmark (${BASE}) ===\n`);

  try { await fetch(`${BASE}/api/health`); }
  catch { console.error('Server not running'); process.exit(1); }

  const result = await (await fetch(`${BASE}/api/indexes/civitai/dumps`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(TAGS_REQUEST),
  })).json();

  if (result.error) { console.error('ERROR:', result.error); process.exit(1); }

  const taskId = result.task_id;
  console.log(`  Task: ${taskId}`);
  const start = Date.now();
  let lastRows = 0, lastTime = start;

  while (true) {
    await new Promise(r => setTimeout(r, 3000));
    const elapsed = (Date.now() - start) / 1000;
    const task = await (await fetch(`${BASE}/api/tasks/${taskId}`)).json();
    const rows = task.progress?.records_processed || 0;

    // Instantaneous rate (last 3s window)
    const dt = (Date.now() - lastTime) / 1000;
    const instRate = dt > 0 ? (rows - lastRows) / dt : 0;
    lastRows = rows; lastTime = Date.now();

    console.log(`  [${Math.round(elapsed)}s] ${(rows/1e6).toFixed(1)}M rows  avg=${(rows/elapsed/1e6).toFixed(1)}M/s  inst=${(instRate/1e6).toFixed(1)}M/s`);

    if (task.status === 'complete') {
      console.log(`\n  DONE: ${(rows/1e6).toFixed(1)}M rows in ${elapsed.toFixed(1)}s (${(rows/elapsed/1e6).toFixed(1)}M/s)`);
      break;
    }
    if (task.status === 'error') { console.error('FAILED:', task.error); break; }
    if (elapsed > 600) { console.log('TIMEOUT 10min'); break; }
  }
}

main().catch(e => { console.error(e); process.exit(1); });
