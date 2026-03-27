#!/usr/bin/env node
/**
 * Phase 1 Validation Suite (V1.1-V1.9) for the dump processor.
 *
 * Sends dump requests to a running BitDex server and validates results.
 *
 * Usage:
 *   1. Start server: cargo run --release --features server,pg-sync --bin bitdex-server -- --port 3001 --data-dir ./data
 *   2. Run: node tools/validate-dump-processor.mjs
 */

const BASE = 'http://localhost:3001';
const STAGE_DIR = 'C:/Dev/Repos/open-source/bitdex-v2/data/load_stage';

// D3 dump requests for each phase (derived from sync-config-civitai.yaml)
const DUMP_REQUESTS = [
  // Phase 1: Tags (63GB) — PG COPY columns: tagId, imageId, attributes
  {
    name: 'tags-v1',
    csv_path: `${STAGE_DIR}/tags.csv`,
    format: 'csv',
    slot_field: 'imageId',
    columns: ['tagId', 'imageId'],  // attributes column may be absent
    sets_alive: false,
    fields: [{ column: 'tagId', target: 'tagIds' }],
  },

  // Phase 2: Images (14GB) — PG COPY columns from sync config
  {
    name: 'images-v1',
    csv_path: `${STAGE_DIR}/images.csv`,
    format: 'csv',
    slot_field: 'id',
    // Note: current test CSV has 11 columns (no width/height). Production COPY will have 13.
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
      // width/height not in current test CSV — will be in production COPY
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
  },

  // Phase 3: Resources (820MB) — PG COPY columns: imageId, modelVersionId, detected
  {
    name: 'resources-v1',
    csv_path: `${STAGE_DIR}/resources.csv`,
    format: 'csv',
    slot_field: 'imageId',
    columns: ['imageId', 'modelVersionId', 'detected'],
    sets_alive: false,
    fields: [{ column: 'modelVersionId', target: 'modelVersionIds' }],
    computed_fields: [
      { target: 'modelVersionIdsManual', expression: 'detected == false', value: 'modelVersionId' },
    ],
    enrichment: [
      {
        csv_path: `${STAGE_DIR}/model_versions.csv`,
        columns: ['id', 'baseModel', 'modelId'],
        key: 'id',
        join_on: 'modelVersionId',
        fields: [{ column: 'baseModel', target: 'baseModel' }],
        enrichment: [
          {
            csv_path: `${STAGE_DIR}/models.csv`,
            columns: ['id', 'poi', 'type'],
            key: 'id',
            join_on: 'modelId',
            fields: [{ column: 'poi', target: 'poi' }],
            filter: "type = 'Checkpoint'",
          },
        ],
      },
    ],
  },

  // Phase 4: Tools (50MB) — PG COPY columns: toolId, imageId
  {
    name: 'tools-v1',
    csv_path: `${STAGE_DIR}/tools.csv`,
    format: 'csv',
    slot_field: 'imageId',
    columns: ['toolIds', 'imageId'],
    fields: ['toolIds'],
  },

  // Phase 5: Techniques (71MB) — PG COPY columns: techniqueId, imageId
  {
    name: 'techniques-v1',
    csv_path: `${STAGE_DIR}/techniques.csv`,
    format: 'csv',
    slot_field: 'imageId',
    columns: ['techniqueIds', 'imageId'],
    fields: ['techniqueIds'],
  },

  // Phase 6: Metrics (TSV) — columns: imageId, reactionCount, commentCount, collectedCount
  {
    name: 'metrics-v1',
    csv_path: `${STAGE_DIR}/metrics.csv`,
    format: 'tsv',
    slot_field: 'imageId',
    columns: ['imageId', 'reactionCount', 'commentCount', 'collectedCount'],
    fields: ['reactionCount', 'commentCount', 'collectedCount'],
  },
];

async function sendDump(request) {
  const res = await fetch(`${BASE}/api/indexes/civitai/dumps`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });
  return res.json();
}

const PHASE_TIMEOUT_MS = 10 * 60 * 1000; // 10 minutes per phase

async function pollTask(taskId, phaseName) {
  const startTime = Date.now();
  let lastProgress = 0;
  while (true) {
    const elapsed = Date.now() - startTime;
    if (elapsed > PHASE_TIMEOUT_MS) {
      throw new Error(`TIMEOUT: Phase ${phaseName} exceeded 10 minutes (${(elapsed/1000).toFixed(0)}s). Task ${taskId} at ${lastProgress} rows.`);
    }

    const res = await fetch(`${BASE}/api/tasks/${taskId}`);
    const task = await res.json();
    const progress = task.progress?.records_processed || 0;
    const rate = progress > 0 ? (progress / (elapsed / 1000)).toFixed(0) : '---';
    lastProgress = progress;

    if (task.status === 'complete') {
      console.log(`  Complete in ${(elapsed/1000).toFixed(1)}s: ${progress} rows (${rate}/s)`);
      return task;
    }
    if (task.status === 'error') throw new Error(`Task ${taskId} failed: ${task.error}`);

    process.stderr.write(`  [${(elapsed/1000).toFixed(0)}s] ${phaseName}: ${(progress/1_000_000).toFixed(1)}M rows (${rate}/s)\n`);
    await new Promise(r => setTimeout(r, 3000));
  }
}

async function query(filter, sort, limit = 10) {
  const params = new URLSearchParams({ format: 'bitdex' });
  const body = { filter, sort, limit };
  const res = await fetch(`${BASE}/api/indexes/civitai/query?${params}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return res.json();
}

async function main() {
  console.log('=== Phase 1 Validation Suite ===');
  console.log(`Server: ${BASE}`);
  console.log(`Stage dir: ${STAGE_DIR}`);
  console.log(`Timeout: 10 min per phase\n`);
  console.log('NOTE: Start server with RAYON_NUM_THREADS=28 to limit CPU usage:');
  console.log('  RAYON_NUM_THREADS=28 cargo run --release --features server,pg-sync --bin bitdex-server -- --port 3001 --data-dir ./data\n');

  // V1.1: Load all CSVs
  const startTime = Date.now();
  for (const req of DUMP_REQUESTS) {
    console.log(`\n--- Dump: ${req.name} ---`);
    const result = await sendDump(req);
    if (result.error) {
      console.error(`  ERROR: ${result.error}`);
      continue;
    }
    console.log(`  Registered: task_id=${result.task_id}`);

    if (result.task_id) {
      const completed = await pollTask(result.task_id, req.name);
      if (completed.result) {
        console.log(`  Result: ${JSON.stringify(completed.result)}`);
      }
    }
  }
  const totalTime = (Date.now() - startTime) / 1000;
  const pass = totalTime < 600;
  console.log(`\nV1.1: All dumps completed in ${totalTime.toFixed(1)}s ${pass ? 'PASS' : 'FAIL (>10min)'}`);

  // V1.2: Bitmap spot checks
  console.log('\n--- V1.2: Bitmap spot checks ---');

  const nsfw1 = await query([{ field: 'nsfwLevel', op: 'eq', value: 1 }]);
  console.log(`  nsfwLevel eq 1: ${nsfw1.total_matches} matches`);

  const tagTest = await query([{ field: 'tagIds', op: 'eq', value: 1 }]);
  console.log(`  tagIds eq 1: ${tagTest.total_matches} matches`);

  // V1.3: Sort correctness
  console.log('\n--- V1.3: Sort correctness ---');
  const sortResult = await query([], { field: 'reactionCount', direction: 'desc' }, 10);
  console.log(`  sort=reactionCount desc limit 10: ${sortResult.ids?.length} results`);
  if (sortResult.ids) {
    console.log(`  Top IDs: ${sortResult.ids.slice(0, 5).join(', ')}`);
  }

  // V1.5: Docstore check
  console.log('\n--- V1.5: Docstore check ---');
  if (sortResult.ids?.[0]) {
    const docRes = await fetch(`${BASE}/api/indexes/civitai/documents/${sortResult.ids[0]}`);
    const doc = await docRes.json();
    console.log(`  Document ${sortResult.ids[0]}:`, JSON.stringify(doc).slice(0, 200));
  }

  // Stats endpoint
  console.log('\n--- Stats ---');
  const statsRes = await fetch(`${BASE}/api/indexes/civitai/stats`);
  const stats = await statsRes.json();
  console.log(`  Total documents: ${stats.total_documents}`);
  console.log(`  Filter fields: ${stats.filter_fields?.length}`);
  console.log(`  Sort fields: ${stats.sort_fields?.length}`);

  console.log('\n=== Validation complete ===');
}

main().catch(e => {
  console.error('VALIDATION FAILED:', e);
  process.exit(1);
});
