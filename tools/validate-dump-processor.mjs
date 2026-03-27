#!/usr/bin/env node
/**
 * Phase 1 Validation Suite (V1.1-V1.9) for the dump processor.
 *
 * Sends dump requests to a running BitDex server and validates results.
 *
 * Usage:
 *   1. Start server: RAYON_NUM_THREADS=28 ./target/release/bitdex-server --port 3001 --data-dir ./data
 *   2. Run: node tools/validate-dump-processor.mjs
 */

const BASE = 'http://localhost:3001';
const STAGE_DIR = 'C:/Dev/Repos/open-source/bitdex-v2/data/load_stage';

const DUMP_REQUESTS = [
  {
    name: 'tags-v1',
    csv_path: `${STAGE_DIR}/tags.csv`,
    format: 'csv',
    slot_field: 'imageId',
    columns: ['tagId', 'imageId'],
    sets_alive: false,
    fields: [{ column: 'tagId', target: 'tagIds' }],
  },
  {
    name: 'images-v1',
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
  },
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
  {
    name: 'tools-v1',
    csv_path: `${STAGE_DIR}/tools.csv`,
    format: 'csv',
    slot_field: 'imageId',
    columns: ['toolIds', 'imageId'],
    fields: ['toolIds'],
  },
  {
    name: 'techniques-v1',
    csv_path: `${STAGE_DIR}/techniques.csv`,
    format: 'csv',
    slot_field: 'imageId',
    columns: ['techniqueIds', 'imageId'],
    fields: ['techniqueIds'],
  },
  {
    name: 'metrics-v1',
    csv_path: `${STAGE_DIR}/metrics.csv`,
    format: 'tsv',
    slot_field: 'imageId',
    columns: ['imageId', 'reactionCount', 'commentCount', 'collectedCount'],
    fields: ['reactionCount', 'commentCount', 'collectedCount'],
  },
];

const PHASE_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes per phase

// ── Helpers ──────────────────────────────────────────────────────────────────

async function sendDump(request) {
  const res = await fetch(`${BASE}/api/indexes/civitai/dumps`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });
  return res.json();
}

async function pollTask(taskId, phaseName) {
  const startTime = Date.now();
  let lastProgress = 0;
  while (true) {
    const elapsed = Date.now() - startTime;
    if (elapsed > PHASE_TIMEOUT_MS) {
      throw new Error(`TIMEOUT: ${phaseName} exceeded 15 minutes. Task ${taskId} at ${lastProgress} rows.`);
    }

    const res = await fetch(`${BASE}/api/tasks/${taskId}`);
    const task = await res.json();
    const progress = task.progress?.records_processed || 0;
    const rate = progress > 0 ? Math.round(progress / (elapsed / 1000)) : 0;
    lastProgress = progress;

    if (task.status === 'complete') {
      return { ...task, elapsed_s: elapsed / 1000, rows: progress, rate };
    }
    if (task.status === 'error') throw new Error(`Task ${taskId} failed: ${task.error}`);

    process.stderr.write(`  [${(elapsed/1000).toFixed(0)}s] ${phaseName}: ${(progress/1e6).toFixed(1)}M rows (${rate ? (rate/1000).toFixed(0)+'K' : '---'}/s)\n`);
    await new Promise(r => setTimeout(r, 3000));
  }
}

async function query(filter, sort, limit = 10) {
  const body = { filter, sort, limit };
  const res = await fetch(`${BASE}/api/indexes/civitai/query?format=compact`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return res.json();
}

async function getIndex() {
  const res = await fetch(`${BASE}/api/indexes/civitai`);
  return res.json();
}

function fmt(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(n);
}

function fmtDur(s) {
  if (s >= 60) return `${Math.floor(s/60)}m${(s%60).toFixed(0)}s`;
  return `${s.toFixed(1)}s`;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║        Dump Processor Validation Suite           ║');
  console.log('╚══════════════════════════════════════════════════╝');
  console.log(`  Server:    ${BASE}`);
  console.log(`  Stage dir: ${STAGE_DIR}`);
  console.log(`  Timeout:   15 min per phase\n`);

  // Pre-flight: check server is up
  try {
    const idx = await getIndex();
    console.log(`  Index:     civitai (${fmt(idx.stats?.alive_count || 0)} alive)\n`);
  } catch {
    console.error('ERROR: Cannot reach server. Start with:');
    console.error('  RAYON_NUM_THREADS=28 ./target/release/bitdex-server --port 3001 --data-dir ./data');
    process.exit(1);
  }

  // ── V1.1: Run all dump phases ──────────────────────────────────────────
  console.log('── V1.1: Dump Phases ──────────────────────────────\n');

  const pipelineStart = Date.now();
  const phaseResults = [];

  // Submit ALL dump phases at once (server queues them as async tasks)
  const submitted = [];
  for (const req of DUMP_REQUESTS) {
    console.log(`  ▸ ${req.name} (submitting)`);
    const result = await sendDump(req);
    if (result.error) {
      console.error(`    ERROR: ${result.error}`);
      phaseResults.push({ name: req.name, error: result.error });
    } else if (result.task_id) {
      submitted.push({ req, task_id: result.task_id, start: Date.now() });
    }
  }
  console.log(`\n  Submitted ${submitted.length} phases, polling...\n`);

  // Poll all tasks — they execute sequentially on the server but we
  // submit them all upfront so there's no client-side gap between phases.
  for (const { req, task_id, start } of submitted) {
    const completed = await pollTask(task_id, req.name);
    const wallClock = (Date.now() - start) / 1000;
    phaseResults.push({
      name: req.name,
      rows: completed.rows,
      rate: completed.rows > 0 ? Math.round(completed.rows / wallClock) : 0,
      wall_s: wallClock,
      task_s: completed.elapsed_s,
    });
    console.log(`    ✓ ${fmt(completed.rows)} rows in ${fmtDur(wallClock)} (${fmt(phaseResults.at(-1).rate)}/s wall)`);
  }

  const pipelineTotal = (Date.now() - pipelineStart) / 1000;

  // ── Summary table ──────────────────────────────────────────────────────
  console.log('\n── Pipeline Summary ───────────────────────────────\n');
  console.log('  Phase          │    Rows  │   Wall  │  Rate/s  │ Task Poll');
  console.log('  ───────────────┼──────────┼─────────┼──────────┼──────────');
  for (const p of phaseResults) {
    if (p.error) {
      console.log(`  ${p.name.padEnd(15)}│ ERROR    │         │          │ ${p.error}`);
      continue;
    }
    console.log(
      `  ${p.name.padEnd(15)}│ ${fmt(p.rows).padStart(8)} │ ${fmtDur(p.wall_s).padStart(7)} │ ${fmt(p.rate).padStart(7)}/s │ ${fmtDur(p.task_s)}`
    );
  }
  const totalRows = phaseResults.reduce((s, p) => s + (p.rows || 0), 0);
  console.log('  ───────────────┼──────────┼─────────┼──────────┼──────────');
  console.log(`  ${'TOTAL'.padEnd(15)}│ ${fmt(totalRows).padStart(8)} │ ${fmtDur(pipelineTotal).padStart(7)} │ ${fmt(Math.round(totalRows/pipelineTotal)).padStart(7)}/s │`);

  const pass = pipelineTotal < 900; // 15 min target
  console.log(`\n  Pipeline: ${fmtDur(pipelineTotal)} ${pass ? '✓ PASS' : '✗ FAIL (>15min)'}`);

  // ── V1.2: Query verification ───────────────────────────────────────────
  console.log('\n── V1.2: Query Spot Checks ────────────────────────\n');

  const idx = await getIndex();
  console.log(`  Alive:  ${fmt(idx.stats?.alive_count || 0)}`);
  console.log(`  Slots:  ${fmt(idx.stats?.slot_count || 0)}`);

  // Filter checks (compact format)
  const checks = [
    { label: 'hasMeta=true', filter: { hasMeta: true } },
    { label: 'onSite=true', filter: { onSite: true } },
    { label: 'nsfwLevel=2', filter: { nsfwLevel: 2 } },
    { label: 'tagIds $in [1,2,3]', filter: { tagIds: { '$in': [1, 2, 3] } } },
  ];
  for (const c of checks) {
    try {
      const r = await query(c.filter, '-reactionCount', 3);
      console.log(`  ${c.label.padEnd(25)} → ${fmt(r.total_matched || 0)} matched`);
    } catch (e) {
      console.log(`  ${c.label.padEnd(25)} → ERROR: ${e.message}`);
    }
  }

  // Sort check
  console.log('');
  try {
    const sortR = await query({}, '-reactionCount', 5);
    console.log(`  sort=reactionCount desc  → ${fmt(sortR.total_matched || 0)} matched, top IDs: [${(sortR.ids || []).join(', ')}]`);
    if (sortR.cursor?.sort_value > 0) {
      console.log(`  Top sort value: ${sortR.cursor.sort_value}`);
    }
  } catch (e) {
    console.log(`  sort=reactionCount desc  → ERROR: ${e.message}`);
  }

  // ── V1.3: Docstore check ──────────────────────────────────────────────
  console.log('\n── V1.3: Docstore Check ───────────────────────────\n');
  try {
    // Pick a known-alive slot by querying
    const probe = await query({ hasMeta: true }, '-reactionCount', 1);
    if (probe.ids?.[0]) {
      const docRes = await fetch(`${BASE}/api/indexes/civitai/documents/${probe.ids[0]}`);
      if (docRes.ok) {
        const doc = await docRes.json();
        const fields = Object.keys(doc).sort().join(', ');
        console.log(`  Doc ${probe.ids[0]}: ${fields}`);
        console.log(`  Preview: ${JSON.stringify(doc).slice(0, 200)}...`);
      } else {
        console.log(`  Doc ${probe.ids[0]}: HTTP ${docRes.status}`);
      }
    } else {
      console.log('  No documents to check (query returned 0 results)');
    }
  } catch (e) {
    console.log(`  ERROR: ${e.message}`);
  }

  console.log('\n══════════════════════════════════════════════════');
  console.log(`  Validation complete — ${fmtDur(pipelineTotal)} total`);
  console.log('══════════════════════════════════════════════════\n');
}

main().catch(e => {
  console.error('VALIDATION FAILED:', e);
  process.exit(1);
});
