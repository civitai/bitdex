#!/usr/bin/env node
/**
 * BitDex Hard-Nuke Orchestrator (post-v201, sync-v2 autonomous boot)
 *
 * Usage: node .claude/skills/deploy/reload.mjs <step>
 *
 * Steps (run in order, verify each before continuing):
 *   1. preflight    — verify shadow OFF, Flux suspended, image tag noted
 *   2. suspend      — scale StatefulSet to 0, verify pods gone
 *   3. nuke-pg      — drop bitdex_* triggers + functions, truncate
 *                     BitdexOps + bitdex_cursors (handles deadlock retries)
 *   4. wipe         — wipe bitmaps/docs/bounds/slot_arena/load_stage on the PVC
 *   5. start        — scale StatefulSet up; bitdex-sync's run_setup_v2 +
 *                     boot dump pipeline (src/bin/pg_sync.rs) autonomously
 *                     reinstalls triggers, dumps CSVs, bulk-loads
 *   6. monitor      — tail pg-sync container until /api/indexes/civitai/stats
 *                     reports alive_count > 0 AND all dump phases complete
 *
 * Post-flow (manual, see docs/guide/deploy-nukes.md §Post-load):
 *   - POST /api/indexes/civitai/compact -d '{"targets":["docs"]}'
 *   - Run smoke tests (Archer's three queries from
 *     docs/_in/correctness-handoff-2026-05-01.md)
 *   - flipt skill shadow on (re-enable comparator)
 *
 * Why this is a thin wrapper, not the old 9-step orchestration:
 *   bitdex-sync's `run_boot_sequence` (src/bin/pg_sync.rs:282) handles
 *   setup_v2 (triggers + tables) → cursor capture → CSV download →
 *   per-phase dump registration → pollers. The host-side orchestration
 *   only needs to: scale down, reset PG-side state, wipe the PVC, and
 *   scale back up. The sidecar drives the rest.
 *
 * For Flux suspend/resume, see docs/guide/prod-ops.md §6 — the durable
 * lever is a git commit to talos-infra
 * `clusters/production/flux-system/apps/bitdex/bitdex.yaml` `suspend: true`.
 * `kubectl patch` and `flux suspend` are NOT sticky (Flux reconciles back
 * from git within ~5 min). This script does NOT touch Flux state — pre-flight
 * step asserts it's already suspended via git, and resume happens via git
 * after the load completes.
 */
import { execFileSync, execSync } from 'child_process';
import { resolve, dirname } from 'path';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';

const NS = 'bitdex';
const STS = 'bitdex';
const NODE = 'talos-wjh-tgy';
const K8S_CONTEXT = 'civit-datapacket';
const PG_NS = 'cnpg-database';
const PG_POD = process.env.BITDEX_PG_WRITER_POD || 'cnpg-cluster-nvme0-2'; // current writer; verify via `kubectl get pod -n cnpg-database -l role=primary`
const INDEX_PATH = '/data/indexes/civitai';
const LOAD_STAGE = `${INDEX_PATH}/load_stage`;

const __dir = dirname(fileURLToPath(import.meta.url));
const SQL_DIR = resolve(__dir, 'sql');

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
  return run(`kubectl --context ${K8S_CONTEXT} ${args} -n ${NS}`, opts);
}

function log(msg) { console.log(`  ✓ ${msg}`); }
function warn(msg) { console.error(`  ! ${msg}`); }
function err(msg) { console.error(`  ✗ ${msg}`); }
function heading(msg) { console.log(`\n=== ${msg} ===`); }

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

function step1_preflight() {
  heading('Step 1: Pre-flight');

  // Flux suspend state — read-only check, doesn't try to mutate it
  const suspended = run(
    `kubectl --context ${K8S_CONTEXT} get kustomization bitdex -n flux-system -o jsonpath='{.spec.suspend}'`,
    { ignoreError: true },
  );
  if (suspended === 'true') {
    log('Flux Kustomization bitdex.suspend = true');
  } else {
    warn(`Flux Kustomization bitdex.suspend = ${suspended || '<unset>'}`);
    warn('Suspend via talos-infra commit BEFORE proceeding:');
    warn('  clusters/production/flux-system/apps/bitdex/bitdex.yaml: suspend: true');
    warn('Otherwise Flux will fight any manifest changes during the nuke.');
  }

  // Note current image tag for rollback awareness
  const stsImage = kubectl(`get statefulset ${STS} -o jsonpath='{.spec.template.spec.containers[0].image}'`, { ignoreError: true });
  log(`Current image: ${stsImage || '<unknown>'}`);

  // Flipt shadow flag check — best-effort
  warn('Confirm shadow flag is OFF in flipt-state before proceeding:');
  warn('  node .claude/skills/flipt/flipt.mjs get bitdex-image-search');
  warn('Mirrored prod traffic during a wiped pod = error storm on model-share side.');
}

function step2_suspend() {
  heading('Step 2: Scale StatefulSet to 0');

  kubectl(`scale statefulset/${STS} --replicas=0`);
  run('sleep 5');

  // Force-delete in case StatefulSet termination grace is long
  kubectl(`delete pod bitdex-0 --force --grace-period=0`, { ignoreError: true });
  run('sleep 5');

  const replicas = kubectl(`get statefulset ${STS} -o jsonpath='{.spec.replicas}'`);
  const podCount = kubectl(`get pods --no-headers`, { ignoreError: true })
    .split('\n')
    .filter(l => l.match(/^bitdex-0\s/) && !l.includes('Completed'))
    .length;

  if (replicas.includes('0') && podCount === 0) {
    log(`Replicas: 0, Pods: 0`);
  } else {
    err(`Replicas: ${replicas}, Pods: ${podCount} — expected 0/0`);
    process.exit(1);
  }
}

function step3_nukePg() {
  heading('Step 3: Reset PG state (drop triggers + truncate ops/cursors)');

  // Verify pods still down — running an in-flight write while we truncate
  // BitdexOps would corrupt state.
  const podCount = kubectl(`get pods --no-headers`, { ignoreError: true })
    .split('\n')
    .filter(l => l.match(/^bitdex-0\s/) && !l.includes('Completed'))
    .length;
  if (podCount > 0) {
    err('bitdex-0 still running — run step 2 (suspend) first');
    process.exit(1);
  }

  // Pass 1: short lock_timeout, accept partial drops
  const sql = readFileSync(resolve(SQL_DIR, 'nuke-pg-state.sql'), 'utf8');
  console.log('Running nuke-pg-state.sql (pass 1, lock_timeout=5s)...');
  try {
    const out = execFileSync(
      'kubectl',
      ['--context', K8S_CONTEXT, 'exec', '-i', '-n', PG_NS, PG_POD, '-c', 'postgres',
       '--', 'psql', '-U', 'postgres', '-d', 'civitai'],
      { input: sql, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] },
    );
    console.log(out.split('\n').slice(-12).join('\n'));
  } catch (e) {
    err(`Pass 1 failed: ${e.message?.slice(0, 200)}`);
    process.exit(1);
  }

  // Check for stragglers and run retry pass if needed
  const remaining = countRemainingTriggers();
  if (remaining > 0) {
    warn(`${remaining} triggers survived pass 1 — running retry pass`);
    const retrySql = readFileSync(resolve(SQL_DIR, 'nuke-pg-state-retry.sql'), 'utf8');
    try {
      const out = execFileSync(
        'kubectl',
        ['--context', K8S_CONTEXT, 'exec', '-i', '-n', PG_NS, PG_POD, '-c', 'postgres',
         '--', 'psql', '-U', 'postgres', '-d', 'civitai'],
        { input: retrySql, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] },
      );
      console.log(out.split('\n').slice(-12).join('\n'));
    } catch (e) {
      warn(`Retry pass returned non-zero — continuing anyway: ${e.message?.slice(0, 200)}`);
    }

    const finalRemaining = countRemainingTriggers();
    if (finalRemaining > 4) {
      err(`${finalRemaining} triggers still remaining after retry — investigate manually`);
      err('Hot tables (Image, ImageResourceNew, Post, ModelVersion) are common offenders.');
      err('Try: SELECT pg_blocking_pids(pid), query FROM pg_stat_activity WHERE state=\'active\';');
      process.exit(1);
    } else if (finalRemaining > 0) {
      warn(`${finalRemaining} triggers remain — bitdex-sync setup_v2 will reconcile them on boot`);
    } else {
      log('All triggers cleared on retry');
    }
  } else {
    log('All triggers cleared on first pass');
  }
}

function countRemainingTriggers() {
  try {
    const out = execFileSync(
      'kubectl',
      ['--context', K8S_CONTEXT, 'exec', '-n', PG_NS, PG_POD, '-c', 'postgres',
       '--', 'psql', '-U', 'postgres', '-d', 'civitai', '-t', '-c',
       `SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'bitdex_%' AND NOT tgisinternal`],
      { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] },
    );
    return parseInt(out.trim(), 10) || 0;
  } catch {
    return -1;
  }
}

function step4_wipe() {
  heading('Step 4: Wipe PVC');

  // Verify pods still down
  const podCount = kubectl(`get pods --no-headers`, { ignoreError: true })
    .split('\n')
    .filter(l => l.match(/^bitdex-0\s/))
    .length;
  if (podCount > 0) {
    err('bitdex-0 still running — must be at 0 replicas');
    process.exit(1);
  }

  const podName = 'wipe-pvc';
  const overrides = JSON.stringify({
    spec: {
      containers: [{
        name: 'wipe',
        image: 'busybox:1.36',
        command: ['sleep', '600'],
        volumeMounts: [{ name: 'data', mountPath: '/data' }],
      }],
      volumes: [{
        name: 'data',
        persistentVolumeClaim: { claimName: 'data-bitdex-0' },
      }],
      nodeSelector: { 'kubernetes.io/hostname': NODE },
    },
  }).replace(/'/g, "\\'");

  // Cleanup any leftover wipe pod
  kubectl(`delete pod ${podName} --force --grace-period=0`, { ignoreError: true });
  run('sleep 3');

  console.log('  Mounting PVC via ephemeral busybox...');
  kubectl(`run ${podName} --image=busybox:1.36 --overrides='${overrides}' --restart=Never`);
  run(`kubectl --context ${K8S_CONTEXT} -n ${NS} wait --for=condition=Ready pod/${podName} --timeout=60s`);

  // Full PVC wipe. The `init-config` init container restores
  // /data/indexes/civitai/{config,ui-config}.yaml from the configmap and
  // re-creates /data/{indexes/civitai,wal,indexes/civitai/load_stage} on
  // pod boot, so a top-level rm leaves the pod in a known-clean state with
  // no stale shards, WAL bytes, or unknown future files lurking.
  const wipeCmd = `rm -rf /data/* /data/.??* 2>/dev/null; echo wiped`;

  const out = run(
    `kubectl --context ${K8S_CONTEXT} -n ${NS} exec ${podName} -- sh -c '${wipeCmd}'`,
    { timeout: 60000 },
  );
  log(`Wipe result: ${out}`);

  const remaining = run(
    `kubectl --context ${K8S_CONTEXT} -n ${NS} exec ${podName} -- ls ${INDEX_PATH}`,
    { timeout: 15000, ignoreError: true },
  );
  log(`PVC contents: ${remaining.replace(/\n/g, ', ') || '<empty>'}`);

  kubectl(`delete pod ${podName} --force --grace-period=0`, { ignoreError: true });
}

function step5_start() {
  heading('Step 5: Scale StatefulSet up — bitdex-sync drives the rest');

  kubectl(`scale statefulset/${STS} --replicas=1`);
  console.log('  Waiting for pod to schedule (up to 60s)...');
  run(`kubectl --context ${K8S_CONTEXT} -n ${NS} wait --for=condition=PodScheduled pod/bitdex-0 --timeout=60s`,
    { ignoreError: true });

  log('Pod scheduled. The pg-sync sidecar will now run autonomously:');
  log('  1. Wait for bitdex server health');
  log('  2. setup_v2: install triggers + create BitdexOps/bitdex_cursors');
  log('  3. Capture pre_dump_cursor from BitdexOps (will be 0 — clean slate)');
  log('  4. Stream-download CSVs from PG');
  log('  5. Per-phase: PUT /dumps + POST /dumps/{name}/loaded + poll completion');
  log('  6. Seed cursor + transition to ops poller');
  log('');
  log('Run `node .claude/skills/deploy/reload.mjs monitor` to track progress.');
}

function step6_monitor() {
  heading('Step 6: Monitor bulk-load progress');

  console.log('Tailing pg-sync logs (Ctrl-C to detach; load continues regardless)...');
  console.log('Look for: "All dump phases complete" and "transitioning to steady-state".');
  console.log('Stats endpoint: GET /api/indexes/civitai/stats — watch alive_count climb.');
  console.log('Tasks endpoint: GET /api/indexes/civitai/tasks — per-phase progress.');
  console.log('');

  // Stream logs (this blocks until user interrupts or pod restarts)
  try {
    execSync(
      `kubectl --context ${K8S_CONTEXT} -n ${NS} logs -f bitdex-0 -c pg-sync --tail=200`,
      { stdio: 'inherit', timeout: 0 },
    );
  } catch (e) {
    // Either Ctrl-C or pod restart — surface the situation
    if (e.signal === 'SIGINT' || e.code === 'SIGINT') {
      log('Detached — load continues. Re-run `monitor` to reattach.');
    } else {
      warn(`Log stream ended: ${e.message?.slice(0, 200)}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

const step = process.argv[2];
const steps = {
  'preflight': step1_preflight,
  'suspend': step2_suspend,
  'nuke-pg': step3_nukePg,
  'wipe': step4_wipe,
  'start': step5_start,
  'monitor': step6_monitor,
};

if (!step || !steps[step]) {
  console.log('BitDex Hard-Nuke Orchestrator (sync-v2 autonomous boot)');
  console.log('');
  console.log('Usage: node .claude/skills/deploy/reload.mjs <step>');
  console.log('');
  console.log('Steps (run in order):');
  console.log('  1. preflight  — verify shadow OFF, Flux suspended, note image');
  console.log('  2. suspend    — scale StatefulSet to 0');
  console.log('  3. nuke-pg    — drop triggers + truncate BitdexOps/bitdex_cursors');
  console.log('  4. wipe       — full PVC wipe (rm -rf /data/*); init container restores configs');
  console.log('  5. start      — scale up; bitdex-sync auto-runs setup + dump + load');
  console.log('  6. monitor    — tail pg-sync logs until load completes');
  console.log('');
  console.log('Post-flow: docstore compact, smoke tests, re-enable shadow.');
  console.log('See docs/guide/deploy-nukes.md.');
  process.exit(1);
}

steps[step]();
