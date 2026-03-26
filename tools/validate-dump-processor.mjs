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
  // Phase 1: Tags (63GB)
  {
    name: 'tags-v1',
    csv_path: `${STAGE_DIR}/tags.csv`,
    format: 'csv',
    slot_field: 'imageId',
    sets_alive: false,
    fields: [{ column: 'tagId', target: 'tagIds' }],
    // Note: attributes column may not exist in test CSVs — filter is optional
  },

  // Phase 2: Images (14GB, primary entity)
  {
    name: 'images-v1',
    csv_path: `${STAGE_DIR}/images.csv`,
    format: 'csv',
    slot_field: 'id',
    sets_alive: true,
    fields: [
      'nsfwLevel',
      { column: 'type', target: 'type' },
      'userId',
      'postId',
      'blockedFor',
      { column: 'url', target: 'url' },
      { column: 'hash', target: 'hash' },
      'width',
      'height',
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

  // Phase 3: Resources (820MB)
  {
    name: 'resources-v1',
    csv_path: `${STAGE_DIR}/resources.csv`,
    format: 'csv',
    slot_field: 'imageId',
    sets_alive: false,
    fields: [{ column: 'modelVersionId', target: 'modelVersionIds' }],
    computed_fields: [
      { target: 'modelVersionIdsManual', expression: 'detected == false', value: 'modelVersionId' },
    ],
    enrichment: [
      {
        csv_path: `${STAGE_DIR}/model_versions.csv`,
        key: 'id',
        join_on: 'modelVersionId',
        fields: [{ column: 'baseModel', target: 'baseModel' }],
        enrichment: [
          {
            csv_path: `${STAGE_DIR}/models.csv`,
            key: 'id',
            join_on: 'modelId',
            fields: [{ column: 'poi', target: 'poi' }],
            filter: "type = 'Checkpoint'",
          },
        ],
      },
    ],
  },

  // Phase 4: Tools (50MB)
  {
    name: 'tools-v1',
    csv_path: `${STAGE_DIR}/tools.csv`,
    format: 'csv',
    slot_field: 'imageId',
    fields: ['toolIds'],
  },

  // Phase 5: Techniques (71MB)
  {
    name: 'techniques-v1',
    csv_path: `${STAGE_DIR}/techniques.csv`,
    format: 'csv',
    slot_field: 'imageId',
    fields: ['techniqueIds'],
  },

  // Phase 6: Metrics (TSV from ClickHouse)
  {
    name: 'metrics-v1',
    csv_path: `${STAGE_DIR}/metrics.csv`,
    format: 'tsv',
    slot_field: 'imageId',
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

async function pollTask(taskId) {
  while (true) {
    const res = await fetch(`${BASE}/api/tasks/${taskId}`);
    const task = await res.json();
    if (task.status === 'complete') return task;
    if (task.status === 'error') throw new Error(`Task ${taskId} failed: ${task.error}`);
    process.stderr.write(`  Task ${taskId}: ${task.status} (${task.progress?.records_processed || 0} rows)\n`);
    await new Promise(r => setTimeout(r, 5000));
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
  console.log('=== Phase 1 Validation Suite ===\n');

  // V1.1: Load all CSVs
  const startTime = Date.now();
  for (const req of DUMP_REQUESTS) {
    console.log(`\n--- Dump: ${req.name} ---`);
    const result = await sendDump(req);
    console.log(`  Registered: task_id=${result.task_id}`);

    if (result.task_id) {
      const completed = await pollTask(result.task_id);
      console.log(`  Complete: ${JSON.stringify(completed.result)}`);
    }
  }
  const totalTime = (Date.now() - startTime) / 1000;
  console.log(`\nV1.1: All dumps completed in ${totalTime.toFixed(1)}s`);

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
