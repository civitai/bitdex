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
 * Extra step (NOT numbered — run it BEFORE wiping anything, see "Two
 * replicas" below):
 *      pin-cursor  — pin a replica's bitdex_cursors row to MAX(BitdexOps.id)
 *
 * Two replicas
 * ------------
 * Production runs TWO pods and TWO locally-pinned PVCs:
 *   bitdex-0 / data-bitdex-0 on talos-wjh-tgy   (HAProxy ACTIVE — serves all)
 *   bitdex-1 / data-bitdex-1 on talos-48r-b3a   (warm failover only)
 *
 * Steps that touch one pod's disk take `--replica=N` (or BITDEX_REPLICA=N).
 * `suspend` and `nuke-pg` are fleet-wide by definition and take no replica.
 *
 * REBUILD THE PODS SEQUENTIALLY, NEVER CONCURRENTLY. `cleanup_bitdex_ops`
 * deletes every row below MIN(last_outbox_id) across bitdex_cursors. A pod
 * that has been wiped has NO cursor row yet — it seeds one only AFTER its
 * dump finishes — so MIN is the *other* pod's advancing cursor, and the
 * healthy pod's progress trims away exactly the ops the rebuilding pod will
 * need. Observed 2026-08-18: bitdex-1 came up at cursor 5,502 against an ops
 * table starting at 171,697 ("ALERT — hole above id 5502 exceeds 100000 ids")
 * and had to be rebuilt a second time.
 *
 * So: `pin-cursor --replica=N` for every replica you have not rebuilt yet,
 * BEFORE you wipe anything. The pin holds the retention floor down until that
 * pod has caught up on its own.
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
const K8S_CONTEXT = 'civit-datapacket';
const PG_NS = 'cnpg-database';
const PG_POD = process.env.BITDEX_PG_WRITER_POD || 'cnpg-cluster-nvme0-5'; // current writer; verify via `kubectl get pod -n cnpg-database -l role=primary`

// PVCs are openebs-hostpath and hard-pinned by PV nodeAffinity — a pod cannot
// move off its node without a PVC migration. Verify with:
//   kubectl get pv $(kubectl -n bitdex get pvc data-bitdex-N -o jsonpath='{.spec.volumeName}') -o jsonpath='{.spec.nodeAffinity}'
const REPLICA_NODES = { 0: 'talos-wjh-tgy', 1: 'talos-48r-b3a' };
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

// --- replica selection ------------------------------------------------------
// Steps that touch one pod's disk or cursor MUST name the replica. There is no
// default: silently defaulting to 0 is how a two-pod fleet ends up with a
// one-pod runbook, which is the bug this argument exists to fix.
function requireReplica() {
  const flag = process.argv.find(a => a.startsWith('--replica='));
  const raw = flag ? flag.slice('--replica='.length) : process.env.BITDEX_REPLICA;
  if (raw === undefined || raw === '') {
    err('This step needs --replica=N (or BITDEX_REPLICA=N).');
    err(`Known replicas: ${Object.keys(REPLICA_NODES).join(', ')}`);
    process.exit(1);
  }
  const n = Number(raw);
  if (!Object.prototype.hasOwnProperty.call(REPLICA_NODES, n)) {
    err(`Unknown replica ${raw}. Known: ${Object.keys(REPLICA_NODES).join(', ')}`);
    process.exit(1);
  }
  return { n, pod: `bitdex-${n}`, pvc: `data-bitdex-${n}`, node: REPLICA_NODES[n], cursor: `pg-sync-bitdex-${n}` };
}

// Every non-Completed bitdex-N pod, whatever the ordinal. The old checks
// grepped `^bitdex-0` only, so `suspend` reported "Pods: 0" while bitdex-1 was
// still Terminating and the destructive steps would run against a live pod.
function livePods() {
  return kubectl('get pods --no-headers', { ignoreError: true })
    .split('\n')
    .map(l => l.trim())
    .filter(l => /^bitdex-\d+\s/.test(l) && !l.includes('Completed'))
    .map(l => l.split(/\s+/)[0]);
}

function psql(sql, opts = {}) {
  return execFileSync(
    'kubectl',
    ['--context', K8S_CONTEXT, 'exec', '-n', PG_NS, PG_POD, '-c', 'postgres',
     '--', 'psql', '-U', 'postgres', '-d', 'civitai', ...(opts.tuples ? ['-t'] : []), '-c', sql],
    { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] },
  ).trim();
}

// Pin a replica's retention floor to the current head of BitdexOps so
// cleanup_bitdex_ops cannot delete ops that replica will still need while it
// rebuilds. See the "Two replicas" note at the top of this file.
function stepPinCursor() {
  const r = requireReplica();
  heading(`Pin cursor for ${r.cursor} to MAX(BitdexOps.id)`);

  console.log(psql('SELECT replica_id, last_outbox_id FROM bitdex_cursors ORDER BY replica_id'));

  psql(`INSERT INTO bitdex_cursors (replica_id, last_outbox_id, updated_at)
        VALUES ('${r.cursor}', (SELECT max(id) FROM "BitdexOps"), now())
        ON CONFLICT (replica_id) DO UPDATE
          SET last_outbox_id = excluded.last_outbox_id, updated_at = now()`);

  console.log(psql(
    'SELECT c.replica_id, c.last_outbox_id, (SELECT max(id) FROM "BitdexOps") AS max_op'
    + ' FROM bitdex_cursors c ORDER BY c.replica_id'));
  log(`${r.cursor} pinned. Ops below MIN(last_outbox_id) stay retained until it catches up.`);
  warn('The ops table will grow while the pin holds — expected. It drains once the pod catches up.');
}

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

  const priorReplicas = kubectl(`get statefulset ${STS} -o jsonpath='{.spec.replicas}'`);
  log(`StatefulSet was at ${priorReplicas} replica(s) — pass the same to step 5`);

  kubectl(`scale statefulset/${STS} --replicas=0`);
  run('sleep 5');

  // Force-delete every ordinal, not just bitdex-0 — StatefulSet termination
  // grace is long and the old single-pod delete left bitdex-1 Terminating.
  for (const pod of livePods()) {
    kubectl(`delete pod ${pod} --force --grace-period=0`, { ignoreError: true });
  }
  run('sleep 5');

  const replicas = kubectl(`get statefulset ${STS} -o jsonpath='{.spec.replicas}'`);
  const pods = livePods();

  if (replicas.includes('0') && pods.length === 0) {
    log(`Replicas: 0, Pods: 0`);
  } else {
    err(`Replicas: ${replicas}, Pods: ${pods.length} (${pods.join(', ') || 'none'}) — expected 0/0`);
    err('Re-run once Terminating pods clear; do NOT proceed to nuke-pg or wipe.');
    process.exit(1);
  }
}

function step3_nukePg() {
  heading('Step 3: Reset PG state (drop triggers + truncate ops/cursors)');

  // Verify pods still down — running an in-flight write while we truncate
  // BitdexOps would corrupt state.
  const pods = livePods();
  if (pods.length > 0) {
    err(`Still running: ${pods.join(', ')} — run step 2 (suspend) first`);
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
  const r = requireReplica();
  heading(`Step 4: Wipe PVC ${r.pvc} (replica ${r.n}, node ${r.node})`);

  // Verify pods still down — any ordinal, not just bitdex-0
  const pods = livePods();
  if (pods.length > 0) {
    err(`Still running: ${pods.join(', ')} — must be at 0 replicas`);
    process.exit(1);
  }

  const podName = `wipe-pvc-${r.n}`;
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
        persistentVolumeClaim: { claimName: r.pvc },
      }],
      nodeSelector: { 'kubernetes.io/hostname': r.node },
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

  const flag = process.argv.find(a => a.startsWith('--replicas='));
  const want = Number(flag ? flag.slice('--replicas='.length) : process.env.BITDEX_REPLICAS || 2);
  kubectl(`scale statefulset/${STS} --replicas=${want}`);
  console.log(`  Waiting for ${want} pod(s) to schedule (up to 60s each)...`);
  for (let i = 0; i < want; i++) {
    run(`kubectl --context ${K8S_CONTEXT} -n ${NS} wait --for=condition=PodScheduled pod/bitdex-${i} --timeout=60s`,
      { ignoreError: true });
  }
  const pending = kubectl('get pods --no-headers', { ignoreError: true })
    .split('\n').filter(l => /^bitdex-\d+\s/.test(l.trim()) && l.includes('Pending'));
  if (pending.length) {
    warn('Still Pending — the PVC pins each pod to one node, so it waits for headroom there:');
    pending.forEach(l => warn(`  ${l.trim()}`));
    warn('Check with: kubectl describe node <node> | sed -n "/Allocated resources/,/Events/p"');
  }

  log('Pod(s) scheduled. The pg-sync sidecar will now run autonomously:');
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

  const rm = requireReplica();
  console.log(`Tailing ${rm.pod} pg-sync logs (Ctrl-C to detach; load continues regardless)...`);
  console.log('Look for: "All dump phases complete" and "transitioning to steady-state".');
  console.log('Stats endpoint: GET /api/indexes/civitai/stats — watch alive_count climb.');
  console.log('Tasks endpoint: GET /api/indexes/civitai/tasks — per-phase progress.');
  console.log('');

  // Stream logs (this blocks until user interrupts or pod restarts)
  try {
    execSync(
      `kubectl --context ${K8S_CONTEXT} -n ${NS} logs -f ${rm.pod} -c pg-sync --tail=200`,
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
  'pin-cursor': stepPinCursor,
  'suspend': step2_suspend,
  'nuke-pg': step3_nukePg,
  'wipe': step4_wipe,
  'start': step5_start,
  'monitor': step6_monitor,
};

if (!step || !steps[step]) {
  console.log('BitDex Hard-Nuke Orchestrator (sync-v2 autonomous boot)');
  console.log('');
  console.log('Usage: node .claude/skills/deploy/reload.mjs <step> [--replica=N]');
  console.log('');
  console.log('Steps (run in order):');
  console.log('  1. preflight   — verify shadow OFF, Flux suspended, note image');
  console.log('  2. suspend     — scale StatefulSet to 0 (fleet-wide)');
  console.log('  3. nuke-pg     — drop triggers + truncate BitdexOps/bitdex_cursors');
  console.log('  4. wipe        — full PVC wipe for ONE replica  (--replica=N required)');
  console.log('  5. start       — scale up (--replicas=N, default 2); sidecar does setup+dump+load');
  console.log("  6. monitor     — tail one pod's pg-sync logs   (--replica=N required)");
  console.log('');
  console.log("     pin-cursor  — pin a replica's cursor to MAX(BitdexOps.id) (--replica=N)");
  console.log('                   REQUIRED for every replica you are not rebuilding first —');
  console.log("                   otherwise the healthy pod's cursor lets cleanup delete the");
  console.log('                   ops the rebuilding pod needs. Rebuild SEQUENTIALLY.');
  console.log('');
  console.log('Prod is TWO pods / TWO node-pinned PVCs:');
  console.log('  bitdex-0 / data-bitdex-0 on talos-wjh-tgy  (HAProxy ACTIVE)');
  console.log('  bitdex-1 / data-bitdex-1 on talos-48r-b3a  (warm failover)');
  console.log('');
  console.log('Post-flow: docstore compact, smoke tests, re-enable shadow.');
  console.log('See docs/guide/deploy-nukes.md.');
  process.exit(1);
}

steps[step]();
