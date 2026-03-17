#!/usr/bin/env node
/**
 * BitDex Full Reload Script
 *
 * Usage: node .claude/skills/deploy/reload.mjs <step> [args]
 *
 * Steps (run in order):
 *   1. suspend       — Suspend Flux, scale to 0, verify pods gone
 *   2. wipe          — Wipe bitmaps/docs/cursors/CSVs on both PVCs
 *   3. dump          — Dump fresh CSVs from PG directly to disk
 *   4. transfer      — Copy CSVs from PG pod to both PVCs
 *   5. load          — Run bulk load jobs on both PVCs
 *   6. cursor-reset  — Stop pods, write cursor files, update PG
 *   7. start         — Scale to 2, verify cursors + sync
 *   8. resume        — Unsuspend Flux, remove safety cursor
 *   9. cleanup       — Delete dump files from PG pod, old jobs
 *
 * Each step verifies preconditions before running and prints a
 * checklist-style report when done.
 */
import { execSync } from 'child_process';

const NS = 'bitdex';
const STS = 'bitdex';
const NODE = 'talos-fq9-f3k';
const PG_POD = 'cnpg-cluster-nvme0-1';
const PG_NS = 'cnpg-database';
const PG_DUMP_DIR = '/var/lib/postgresql/data/bitdex_dump';
const INDEX_PATH = '/data/indexes/civitai';
const LOAD_STAGE = `${INDEX_PATH}/load_stage`;

// ClickHouse (for metrics — handled by the bulk loader, not this script)
// The loader reads CH credentials from K8s secrets.

function run(cmd, opts = {}) {
  try {
    return execSync(cmd, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: opts.timeout || 120000,
      ...opts,
    }).trim();
  } catch (e) {
    if (opts.ignoreError) return e.stderr?.trim() || e.stdout?.trim() || '';
    throw e;
  }
}

function kubectl(args, opts) {
  return run(`kubectl ${args} -n ${NS} 2>/dev/null`, opts);
}

function pgExec(sql, opts) {
  // Use execFileSync to avoid shell quoting issues entirely
  try {
    const result = require('child_process').execFileSync(
      'kubectl', ['exec', '-n', PG_NS, PG_POD, '--', 'psql', '-U', 'postgres', '-d', 'civitai', '-t', '-c', sql],
      { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'], timeout: opts?.timeout || 120000 },
    );
    return result.trim();
  } catch (e) {
    if (opts?.ignoreError) return '';
    throw e;
  }
}

function pgCopy(query, outFile) {
  const sql = `SET statement_timeout = 0; COPY (${query}) TO '${PG_DUMP_DIR}/${outFile}' CSV;`;
  return run(
    `MSYS_NO_PATHCONV=1 kubectl exec -n ${PG_NS} ${PG_POD} -- psql -U postgres -d civitai -c "${sql}" 2>/dev/null`,
    { timeout: 600000 },
  );
}

function mountPvc(name, pvcIndex) {
  kubectl(
    `run ${name} --image=busybox ` +
    `--overrides='${JSON.stringify({
      spec: {
        containers: [{ name: 'm', image: 'busybox', command: ['sleep', '3600'],
          volumeMounts: [{ name: 'd', mountPath: '/data' }] }],
        volumes: [{ name: 'd', persistentVolumeClaim: { claimName: `data-bitdex-${pvcIndex}` } }],
        nodeSelector: { 'kubernetes.io/hostname': NODE },
      },
    }).replace(/'/g, "\\'")}' --restart=Never`,
    { ignoreError: true },
  );
}

function execOnMount(name, cmd) {
  return run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${NS} ${name} -- sh -c '${cmd}' 2>/dev/null`);
}

function deleteMount(name) {
  kubectl(`delete pod ${name} --force --grace-period=0`, { ignoreError: true });
}

function log(msg) { console.log(`  ✓ ${msg}`); }
function err(msg) { console.error(`  ✗ ${msg}`); }
function heading(msg) { console.log(`\n=== ${msg} ===`); }

// --- The actual COPY queries from copy_queries.rs ---
const TABLES = {
  'images.csv': `SELECT id, url, "nsfwLevel", hash, flags, type::text, "userId", "blockedFor", extract(epoch from "scannedAt")::bigint, extract(epoch from "createdAt")::bigint, "postId" FROM "Image"`,
  'posts.csv': `SELECT id, extract(epoch from "publishedAt")::bigint, availability::text, "modelVersionId" FROM "Post"`,
  'tags.csv': `SELECT "tagId", "imageId" FROM "TagsOnImageDetails" WHERE disabled = false`,
  'tools.csv': `SELECT "toolId", "imageId" FROM "ImageTool"`,
  'techniques.csv': `SELECT "techniqueId", "imageId" FROM "ImageTechnique"`,
  'resources.csv': `SELECT "imageId", "modelVersionId", detected FROM "ImageResourceNew"`,
  'model_versions.csv': `SELECT id, "baseModel", "modelId" FROM "ModelVersion"`,
  'models.csv': `SELECT id, poi, type::text FROM "Model"`,
  'collection_items.csv': `SELECT "collectionId", "imageId" FROM "CollectionItem" WHERE "imageId" IS NOT NULL AND status = 'ACCEPTED'`,
};

// =========================================================================
// STEPS
// =========================================================================

function step1_suspend() {
  heading('Step 1: Suspend Flux + Scale to 0');

  // Check Flux suspend status
  const suspended = run(
    `kubectl get kustomization bitdex -n flux-system -o jsonpath='{.spec.suspend}' 2>/dev/null`,
    { ignoreError: true },
  );
  if (suspended === 'true') {
    log('Flux already suspended');
  } else {
    console.log('  Waiting for Flux to reconcile suspend from git...');
    console.log('  (Arabella must push suspend:true to talos-infra first)');
    // Try patching anyway in case it sticks
    run(`kubectl patch kustomization bitdex -n flux-system --type=json -p='[{"op":"replace","path":"/spec/suspend","value":true}]' 2>/dev/null`, { ignoreError: true });
  }

  // Scale to 0
  kubectl(`scale statefulset/${STS} --replicas=0`);
  run('sleep 10');
  kubectl(`delete pod bitdex-0 bitdex-1 --force --grace-period=0`, { ignoreError: true });
  run('sleep 5');

  // Verify
  const replicas = kubectl(`get statefulset ${STS} -o jsonpath='{.spec.replicas}'`);
  const pods = kubectl(`get pods --no-headers`, { ignoreError: true })
    .split('\n').filter(l => l.match(/^bitdex-[01]\s/) && !l.includes('Completed')).length;

  if (replicas.includes('0') && pods === 0) {
    log(`Replicas: 0, Pods: 0`);
  } else {
    err(`Replicas: ${replicas}, Pods: ${pods} — expected 0/0`);
    process.exit(1);
  }

  // Set safety cursor in PG
  pgExec(`INSERT INTO bitdex_cursors (replica_id, last_outbox_id, updated_at) VALUES ('safety-hold', 0, now()) ON CONFLICT (replica_id) DO UPDATE SET last_outbox_id = 0, updated_at = now();`);
  log('Safety cursor set in PG (prevents outbox cleanup)');

  // Record outbox head
  const head = pgExec(`SELECT MAX(id) FROM "BitdexOutbox";`).trim();
  log(`Current outbox head: ${head}`);

  // Wait and verify Flux isn't bringing pods back
  console.log('  Waiting 30s to verify Flux stays suspended...');
  run('sleep 30');
  const podsAfter = kubectl(`get pods --no-headers`, { ignoreError: true })
    .split('\n').filter(l => l.match(/^bitdex-[01]\s/) && !l.includes('Completed')).length;
  if (podsAfter > 0) {
    err(`Flux brought pods back! (${podsAfter} pods). Flux must be suspended via git push.`);
    process.exit(1);
  }
  log('Flux confirmed suspended — no pods came back after 30s');
}

function step2_wipe() {
  heading('Step 2: Wipe both PVCs');

  // Verify pods are down
  const pods = kubectl(`get pods --no-headers`, { ignoreError: true })
    .split('\n').filter(l => l.match(/^bitdex-[01]\s/)).length;
  if (pods > 0) {
    err('Pods still running — run step 1 first');
    process.exit(1);
  }

  for (const i of [0, 1]) {
    mountPvc(`wipe-${i}`, i);
  }
  run('sleep 10');

  for (const i of [0, 1]) {
    execOnMount(`wipe-${i}`,
      `rm -rf ${INDEX_PATH}/bitmaps ${INDEX_PATH}/docs ${INDEX_PATH}/bounds ` +
      `${INDEX_PATH}/slot_arena.bin ${INDEX_PATH}/snapshot.meta && ` +
      `rm -rf ${LOAD_STAGE}/* && echo wiped-${i}`);
    const contents = execOnMount(`wipe-${i}`, `ls ${INDEX_PATH}/`);
    log(`PVC-${i} wiped: ${contents.replace(/\n/g, ', ')}`);
    deleteMount(`wipe-${i}`);
  }
}

function step3_dump() {
  heading('Step 3: Dump fresh CSVs from PG');

  // Create/clear dump dir
  run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${PG_NS} ${PG_POD} -- sh -c 'mkdir -p ${PG_DUMP_DIR} && rm -f ${PG_DUMP_DIR}/*.csv ${PG_DUMP_DIR}/cursor.txt' 2>/dev/null`);
  log('Dump directory cleared');

  // Record outbox head BEFORE dump
  const headBefore = pgExec(`SELECT MAX(id) FROM "BitdexOutbox";`).trim();
  log(`Outbox head before dump: ${headBefore}`);

  // Dump each table
  for (const [file, query] of Object.entries(TABLES)) {
    console.log(`  Dumping ${file}...`);
    try {
      const result = pgCopy(query, file);
      const countMatch = result.match(/COPY (\d+)/);
      log(`${file}: ${countMatch ? countMatch[1] + ' rows' : 'done'}`);
    } catch (e) {
      err(`${file} FAILED: ${e.message?.slice(0, 100)}`);
      process.exit(1);
    }
  }

  // Record outbox head AFTER dump
  const headAfter = pgExec(`SELECT MAX(id) FROM "BitdexOutbox";`).trim();
  log(`Outbox head after dump: ${headAfter}`);

  // Save cursor (use the BEFORE value — CSVs reflect state at that point)
  run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${PG_NS} ${PG_POD} -- sh -c 'echo -n ${headBefore} > ${PG_DUMP_DIR}/cursor.txt' 2>/dev/null`);
  log(`Cursor saved: ${headBefore}`);

  // Verify all files exist
  const files = run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${PG_NS} ${PG_POD} -- ls -lh ${PG_DUMP_DIR}/ 2>/dev/null`);
  console.log(files);
}

function step4_transfer() {
  heading('Step 4: Transfer CSVs to both PVCs');

  // Read cursor value
  const cursor = run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${PG_NS} ${PG_POD} -- cat ${PG_DUMP_DIR}/cursor.txt 2>/dev/null`);
  log(`Cursor from dump: ${cursor}`);

  // Mount both PVCs
  for (const i of [0, 1]) {
    mountPvc(`xfer-${i}`, i);
  }
  run('sleep 10');

  // Transfer each file + create .done markers
  const allFiles = [...Object.keys(TABLES), 'cursor.txt'];
  // Also need metrics.csv if it exists
  const hasMetrics = run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${PG_NS} ${PG_POD} -- test -f ${PG_DUMP_DIR}/metrics.csv && echo yes || echo no 2>/dev/null`);

  for (const file of allFiles) {
    console.log(`  Transferring ${file}...`);
    for (const i of [0, 1]) {
      // Use kubectl cp from PG pod to a temp location, then to PVC
      // Actually: both pods are on the same node, so we can use a direct copy via a helper pod
      // Simplest: kubectl cp from PG pod to local, then kubectl cp to PVC mount
      // But that's slow for big files. Better: create a pod that mounts BOTH the PG hostpath and the PVC.
      // Simplest reliable approach: kubectl cp
      run(
        `kubectl cp ${PG_NS}/${PG_POD}:${PG_DUMP_DIR}/${file} ${NS}/xfer-${i}:${LOAD_STAGE}/${file} 2>/dev/null`,
        { timeout: 600000 },
      );
    }
    // Create .done markers (skip for cursor.txt)
    if (file !== 'cursor.txt') {
      for (const i of [0, 1]) {
        execOnMount(`xfer-${i}`, `echo ok > ${LOAD_STAGE}/${file}.done`);
      }
    }
    log(`${file} transferred to both PVCs`);
  }

  // Verify
  for (const i of [0, 1]) {
    const listing = execOnMount(`xfer-${i}`, `ls ${LOAD_STAGE}/`);
    log(`PVC-${i} load_stage: ${listing.split('\n').length} files`);
    deleteMount(`xfer-${i}`);
  }
}

function step5_load() {
  heading('Step 5: Run bulk load on both PVCs');

  // Verify pods are down
  const pods = kubectl(`get pods --no-headers`, { ignoreError: true })
    .split('\n').filter(l => l.match(/^bitdex-[01]\s/)).length;
  if (pods > 0) {
    err('Server pods still running — scale to 0 first');
    process.exit(1);
  }

  // Delete any old jobs
  kubectl('delete job fresh-load-0 fresh-load-1', { ignoreError: true });
  run('sleep 5');

  // Create load jobs
  kubectl(`create job fresh-load-0 --from=cronjob/bitdex-bulk-load`);
  log('Job fresh-load-0 created');

  // Create job for PVC-1
  const cjJson = run(`kubectl get cronjob -n ${NS} bitdex-bulk-load -o json 2>/dev/null`);
  const cj = JSON.parse(cjJson);
  const spec = cj.spec.jobTemplate.spec;
  for (const v of spec.template.spec.volumes) {
    if (v.persistentVolumeClaim?.claimName === 'data-bitdex-0')
      v.persistentVolumeClaim.claimName = 'data-bitdex-1';
  }
  for (const c of spec.template.spec.containers) {
    for (const e of (c.env || [])) {
      if (e.name === 'BITDEX_REPLICA_ID') e.value = 'bitdex-1';
    }
  }
  const job1 = JSON.stringify({
    apiVersion: 'batch/v1', kind: 'Job',
    metadata: { name: 'fresh-load-1', namespace: NS },
    spec,
  });
  run(`echo '${job1.replace(/'/g, "\\'")}' | kubectl apply -f - 2>/dev/null`);
  log('Job fresh-load-1 created');

  console.log('  Waiting for both jobs to complete (~10 min)...');
  while (true) {
    const s0 = kubectl(`get job fresh-load-0 -o jsonpath='{.status.conditions[0].type}'`, { ignoreError: true });
    const s1 = kubectl(`get job fresh-load-1 -o jsonpath='{.status.conditions[0].type}'`, { ignoreError: true });
    if (s0.includes('Complete') && s1.includes('Complete')) break;
    const tail = run(`kubectl logs -n ${NS} job/fresh-load-0 --tail=1 2>/dev/null`, { ignoreError: true });
    console.log(`  ... ${tail}`);
    run('sleep 30');
  }
  log('Both load jobs complete');

  // Verify collectionIds
  for (const i of [0, 1]) {
    mountPvc(`verify-${i}`, i);
  }
  run('sleep 8');
  for (const i of [0, 1]) {
    const fpacks = execOnMount(`verify-${i}`, `ls ${INDEX_PATH}/bitmaps/filters/collectionIds/ 2>/dev/null | wc -l`);
    if (parseInt(fpacks) > 0) {
      log(`PVC-${i}: collectionIds has ${fpacks.trim()} fpack files`);
    } else {
      err(`PVC-${i}: collectionIds has 0 fpack files!`);
    }
    deleteMount(`verify-${i}`);
  }

  // Clean up jobs
  kubectl('delete job fresh-load-0 fresh-load-1', { ignoreError: true });
}

function step6_cursorReset() {
  heading('Step 6: Reset cursors on both PVCs');

  // Verify pods are down
  const pods = kubectl(`get pods --no-headers`, { ignoreError: true })
    .split('\n').filter(l => l.match(/^bitdex-[01]\s/) && !l.includes('Completed')).length;
  if (pods > 0) {
    err(`${pods} server pods still running — scale to 0 first`);
    kubectl(`scale statefulset/${STS} --replicas=0`);
    run('sleep 10');
    kubectl(`delete pod bitdex-0 bitdex-1 --force --grace-period=0`, { ignoreError: true });
    run('sleep 5');
  }

  // Read cursor from PVC-0 load_stage
  for (const i of [0, 1]) {
    mountPvc(`cr-${i}`, i);
  }
  run('sleep 8');

  const cursor = execOnMount('cr-0', `cat ${LOAD_STAGE}/cursor.txt`).trim();
  if (!cursor || cursor.length < 5) {
    err(`Invalid cursor: "${cursor}" — expected numeric value from dump`);
    process.exit(1);
  }
  log(`Cursor from dump: ${cursor}`);

  // Write cursor files
  for (const i of [0, 1]) {
    execOnMount(`cr-${i}`, `echo -n ${cursor} > ${INDEX_PATH}/bitmaps/cursors/pg-sync-bitdex-${i}`);
    const verify = execOnMount(`cr-${i}`, `cat ${INDEX_PATH}/bitmaps/cursors/pg-sync-bitdex-${i}`);
    if (verify.trim() === cursor) {
      log(`PVC-${i} cursor: ${verify.trim()}`);
    } else {
      err(`PVC-${i} cursor mismatch: expected ${cursor}, got ${verify.trim()}`);
      process.exit(1);
    }
    deleteMount(`cr-${i}`);
  }

  // Update PG
  pgExec(`UPDATE bitdex_cursors SET last_outbox_id = ${cursor} WHERE replica_id IN ('pg-sync-bitdex-0', 'pg-sync-bitdex-1');`);
  log('PG cursors updated');
}

function step7_start() {
  heading('Step 7: Start pods + verify');

  kubectl(`scale statefulset/${STS} --replicas=2`);
  console.log('  Waiting for rollout...');
  run(`kubectl rollout status statefulset/${STS} -n ${NS} --timeout=300s 2>/dev/null`, { timeout: 310000 });
  log('Both pods running');

  // Wait for pg-sync to start
  console.log('  Waiting 20s for pg-sync to boot...');
  run('sleep 20');

  // Check cursors
  for (const i of [0, 1]) {
    const cursor = run(`kubectl logs -n ${NS} bitdex-${i} -c pg-sync 2>/dev/null | grep starting_cursor`, { ignoreError: true });
    log(`bitdex-${i}: ${cursor.trim()}`);
  }

  // Wait and check cursor advancement
  console.log('  Waiting 60s to verify sync is processing...');
  run('sleep 60');

  for (const i of [0, 1]) {
    const logs = run(`kubectl logs -n ${NS} bitdex-${i} -c pg-sync --tail=10 2>/dev/null`, { ignoreError: true });
    const cursorMatch = logs.match(/cursor=(\d+)/g);
    const lastCursor = cursorMatch ? cursorMatch[cursorMatch.length - 1] : 'unknown';
    const processed = (logs.match(/processed \d+ changes/g) || []).length;
    log(`bitdex-${i}: ${lastCursor}, ${processed} batches in last 10 lines`);
  }
}

function step8_resume() {
  heading('Step 8: Resume Flux + remove safety cursor');

  // Unsuspend Flux
  run(`kubectl patch kustomization bitdex -n flux-system --type=json -p='[{"op":"replace","path":"/spec/suspend","value":false}]' 2>/dev/null`, { ignoreError: true });
  log('Flux unsuspended (notify Arabella to remove suspend:true from git)');

  // Remove safety cursor
  pgExec(`DELETE FROM bitdex_cursors WHERE replica_id = 'safety-hold';`);
  log('Safety cursor removed from PG');
}

function step9_cleanup() {
  heading('Step 9: Cleanup');

  // Delete dump files from PG pod
  run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${PG_NS} ${PG_POD} -- rm -rf ${PG_DUMP_DIR} 2>/dev/null`, { ignoreError: true });
  log('Dump files deleted from PG pod');

  // Delete any lingering transfer/wipe/verify pods
  for (const prefix of ['wipe', 'xfer', 'verify', 'cr', 'clean', 'pvc-mount', 'cursor-setter', 'cs', 'bf', 'check', 'config-writer', 'config-fix', 'cursor-reset']) {
    for (const i of [0, 1, 2, 3]) {
      kubectl(`delete pod ${prefix}-${i} --force --grace-period=0`, { ignoreError: true });
    }
  }
  log('Lingering pods cleaned up');
}

// =========================================================================
// ROUTER
// =========================================================================

const step = process.argv[2];
const steps = {
  'suspend': step1_suspend,
  'wipe': step2_wipe,
  'dump': step3_dump,
  'transfer': step4_transfer,
  'load': step5_load,
  'cursor-reset': step6_cursorReset,
  'start': step7_start,
  'resume': step8_resume,
  'cleanup': step9_cleanup,
};

if (!step || !steps[step]) {
  console.log('BitDex Full Reload Script');
  console.log('');
  console.log('Usage: node .claude/skills/deploy/reload.mjs <step>');
  console.log('');
  console.log('Steps (run in order):');
  console.log('  1. suspend       — Suspend Flux, scale to 0, set safety cursor');
  console.log('  2. wipe          — Wipe bitmaps/docs/cursors/CSVs on both PVCs');
  console.log('  3. dump          — Dump fresh CSVs from PG directly to disk');
  console.log('  4. transfer      — Copy CSVs from PG pod to both PVCs');
  console.log('  5. load          — Run bulk load jobs on both PVCs');
  console.log('  6. cursor-reset  — Write correct cursor to both PVCs + PG');
  console.log('  7. start         — Scale to 2, verify cursors + sync');
  console.log('  8. resume        — Unsuspend Flux, remove safety cursor');
  console.log('  9. cleanup       — Delete dump files, lingering pods');
  process.exit(1);
}

steps[step]();
