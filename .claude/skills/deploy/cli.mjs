#!/usr/bin/env node
/**
 * BitDex Production Deploy CLI
 *
 * Usage: node .claude/skills/deploy/cli.mjs <command> [args]
 *
 * Modules:
 *   lib/kubectl.mjs     — K8s helpers, transfer pods, port-forward
 *   lib/prometheus.mjs   — PromQL queries, metrics
 *   lib/sync-config.mjs  — V2 sync config YAML parser
 *   lib/csv-dump.mjs     — Config-driven CSV dump with progress tracking
 */
import { readFileSync, writeFileSync, existsSync, createWriteStream, statSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { spawnSync } from 'child_process';
import http from 'http';
import https from 'https';

import {
  run, kubectl, kubectlExec, kubectlExecPod,
  runEphemeralPod, createTransferPod, deletePod,
  portForward, killPortForward,
  K8S_CONTEXT, NS, STS, NODE, INDEX_PATH, PG_NS, PG_POD, PG_SVC,
} from './lib/kubectl.mjs';

import {
  ensurePromPortForward, promQuery, promQueryRange,
  extractScalar, parseDuration,
} from './lib/prometheus.mjs';

import { csvDump, csvDumpProgress, csvDumpCleanup, csvDumpTables, csvServe, csvServeStop, csvDownload, csvVerify, csvDownloadWatch, csvFullPipeline } from './lib/csv-dump.mjs';

// Load .env from dev-server skill dir (shared admin token)
const __d = dirname(fileURLToPath(import.meta.url));
try {
  const envFile = readFileSync(resolve(__d, '..', 'dev-server', '.env'), 'utf8');
  for (const line of envFile.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq > 0) {
      const key = trimmed.slice(0, eq).trim();
      const val = trimmed.slice(eq + 1).trim();
      if (!process.env[key]) process.env[key] = val;
    }
  }
} catch { /* no .env */ }

const ADMIN_TOKEN = process.env.BITDEX_ADMIN_TOKEN || null;
const BITDEX_URL = process.env.BITDEX_PROD_URL || 'https://bitdex.civitai.com';
const GHCR = 'ghcr.io/civitai/bitdex';
const DOCKER_WORKFLOW = 'docker.yml';

function json(data) { console.log(JSON.stringify(data, null, 2)); }
function err(msg) { console.error(msg); }

// --- Release & Deploy ---

function release() {
  const cargoPath = resolve('Cargo.toml');
  const cargo = readFileSync(cargoPath, 'utf8');
  const match = cargo.match(/version\s*=\s*"(\d+\.\d+\.)(\d+)"/);
  if (!match) { json({ error: 'Could not parse version from Cargo.toml' }); process.exit(1); }

  const prefix = match[1];
  const patch = parseInt(match[2]) + 1;
  const version = `${prefix}${patch}`;
  const tag = `v${version}`;

  err(`Bumping to ${version}...`);
  writeFileSync(cargoPath, cargo.replace(/version\s*=\s*"[^"]+"/, `version = "${version}"`));

  run(`git add Cargo.toml`);
  run(`git commit -m "release: ${tag}\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"`);
  run(`git tag ${tag}`);
  run(`git push origin main --tags`);
  run(`gh workflow run ${DOCKER_WORKFLOW} --ref ${tag}`);

  const runs = JSON.parse(run(`gh run list --workflow ${DOCKER_WORKFLOW} --limit 3 --json databaseId,headBranch,status`));
  const runId = runs.find(r => r.headBranch === tag)?.databaseId || runs[0]?.databaseId;

  json({ version, tag, runId, pushed: true, buildTriggered: true });
}

function watchBuild() {
  const runId = process.argv[3] || findLatestBuildRunId();
  if (!runId) { json({ error: 'No run ID found' }); process.exit(1); }

  err(`Watching build ${runId}...`);
  while (true) {
    const result = JSON.parse(run(`gh run view ${runId} --json status,conclusion`));
    if (result.status === 'completed') { json({ runId, ...result }); return; }
    spawnSync('sleep', ['15']);
  }
}

function watchBuildNotify() {
  const runId = process.argv[3] || findLatestBuildRunId();
  const notifyAgent = process.argv.includes('--notify') ? process.argv[process.argv.indexOf('--notify') + 1] : null;
  if (!runId) { json({ error: 'No run ID found' }); process.exit(1); }

  err(`Watching build ${runId} (non-blocking, will notify when done)...`);
  const poll = () => {
    try {
      const result = JSON.parse(run(`gh run view ${runId} --json status,conclusion`));
      if (result.status === 'completed') {
        const msg = `Docker build ${runId} ${result.conclusion}`;
        err(msg);
        json({ runId, ...result });

        if (notifyAgent) {
          run(`node ~/.claude/skills/mailbox/query.mjs send ${notifyAgent} "${msg}. Ready to deploy."`, { throws: false });
          err(`Notified ${notifyAgent}`);
        }
        return;
      }
      err(`  Build ${runId}: ${result.status}...`);
    } catch (e) {
      err(`  Poll error: ${e.message}`);
    }
    setTimeout(poll, 30000); // poll every 30s
  };
  poll();
}

function buildStatus() {
  const runs = JSON.parse(run(`gh run list --workflow ${DOCKER_WORKFLOW} --limit 3 --json databaseId,headBranch,status,conclusion,createdAt`));
  json({ builds: runs });
}

function findLatestBuildRunId() {
  const runs = JSON.parse(run(`gh run list --workflow ${DOCKER_WORKFLOW} --limit 1 --json databaseId`));
  return runs[0]?.databaseId;
}

function rollout(version) {
  if (!version) { json({ error: 'Usage: rollout <version>' }); process.exit(1); }
  const image = `${GHCR}:${version}`;
  err(`Rolling out ${image}...`);

  kubectl(`set image statefulset/${STS} bitdex=${image} pg-sync=${image}`);
  const result = run(`kubectl rollout status statefulset/${STS} -n ${NS} --timeout=300s 2>/dev/null`);
  err(result);

  json({ version, image, rolledOut: true });
}

function deploy(version) {
  if (!version) { json({ error: 'Usage: deploy <version>' }); process.exit(1); }

  // Store previous version for rollback
  const prevImage = run(`kubectl get statefulset -n ${NS} ${STS} -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null`, { throws: false });
  const prevVersion = prevImage?.split(':')[1] || null;

  rollout(version);

  // Pre-rollout health checks
  err('Running post-deploy health checks...');
  spawnSync('sleep', ['10']);

  const checks = { pgSync: false, memory: false, pods: false };

  // Check pg-sync health
  const health = pgSyncHealthRaw();
  const hasCursor = Object.values(health).some(h => h.lastCursor);
  const hasErrors = Object.values(health).some(h => h.errors > 5);
  checks.pgSync = hasCursor && !hasErrors;
  if (!checks.pgSync) err('  WARNING: pg-sync health check failed');
  else err('  pg-sync: OK');

  // Check pods are ready
  const podsJson = run(`kubectl get pods -n ${NS} -o json 2>/dev/null`, { throws: false });
  try {
    const pods = JSON.parse(podsJson).items;
    checks.pods = pods.every(p => p.status.containerStatuses?.every(c => c.ready));
  } catch {}
  if (!checks.pods) err('  WARNING: not all pods ready');
  else err('  pods: OK');

  // Check memory
  const top = run(`kubectl top pod -n ${NS} 2>/dev/null`, { throws: false });
  if (top) {
    const memLine = top.split('\n').find(l => l.includes('bitdex-0'));
    if (memLine) {
      const mem = memLine.trim().split(/\s+/)[2];
      err(`  memory: ${mem}`);
      checks.memory = true;
    }
  }

  json({
    version,
    previousVersion: prevVersion,
    image: `${GHCR}:${version}`,
    deployed: true,
    healthChecks: checks,
    rollbackCmd: prevVersion ? `deploy rollback ${prevVersion}` : null,
  });
}

function rollback(version) {
  version = version || process.argv[3];
  if (!version) { json({ error: 'Usage: rollback <version>' }); process.exit(1); }
  err(`Rolling back to ${version}...`);
  rollout(version);
  spawnSync('sleep', ['10']);
  pgSyncHealth();
}

// --- Status & Health ---

function status() {
  const pods = run(`kubectl get pods -n ${NS} -o json 2>/dev/null`, { throws: false });
  let podInfo = [];
  try {
    const parsed = JSON.parse(pods);
    podInfo = parsed.items.map(p => ({
      name: p.metadata.name,
      status: p.status.phase,
      ready: p.status.containerStatuses?.every(c => c.ready) || false,
      image: p.spec.containers[0]?.image,
      age: p.metadata.creationTimestamp,
      restarts: p.status.containerStatuses?.reduce((s, c) => s + c.restartCount, 0) || 0,
    }));
  } catch {}

  const stsImage = run(`kubectl get statefulset -n ${NS} ${STS} -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null`, { throws: false });
  json({ statefulsetImage: stsImage, pods: podInfo });
}

function scale(replicas) {
  replicas = parseInt(replicas ?? process.argv[3]);
  if (isNaN(replicas)) { json({ error: 'Usage: scale <replicas>' }); process.exit(1); }

  kubectl(`scale statefulset/${STS} --replicas=${replicas}`);
  if (replicas === 0) {
    err('Waiting for pods to terminate...');
    run(`kubectl wait --for=delete pod -l app=bitdex -n ${NS} --timeout=120s 2>/dev/null`, { throws: false });
  } else {
    err('Waiting for rollout...');
    run(`kubectl rollout status statefulset/${STS} -n ${NS} --timeout=300s 2>/dev/null`);
  }
  json({ replicas, scaled: true });
}

function health() {
  err('Checking pod health...');
  const podsJson = run(`kubectl get pods -n ${NS} -o json 2>/dev/null`, { throws: false });
  let pods = [];
  try {
    pods = JSON.parse(podsJson).items.map(p => {
      const containers = (p.status.containerStatuses || []).map(c => ({
        name: c.name, ready: c.ready, restarts: c.restartCount, state: Object.keys(c.state || {})[0] || 'unknown',
      }));
      return { name: p.metadata.name, status: p.status.phase, age: p.metadata.creationTimestamp, image: p.spec.containers[0]?.image, containers };
    });
  } catch {}
  const top = run(`kubectl top pod -n ${NS} 2>/dev/null`, { throws: false });
  if (top) for (const line of top.split('\n').filter(l => l.includes('bitdex'))) {
    const parts = line.trim().split(/\s+/);
    const pod = pods.find(p => p.name === parts[0]);
    if (pod) { pod.cpu = parts[1]; pod.memory = parts[2]; }
  }
  let pgSyncCursor = null;
  const logs = run(`kubectl logs -n ${NS} bitdex-0 -c pg-sync --tail=10 2>/dev/null`, { throws: false });
  if (logs) { const m = logs.match(/cursor=(\d+)/g); if (m) pgSyncCursor = m[m.length - 1].replace('cursor=', ''); }
  for (const p of pods) {
    const ready = p.containers.every(c => c.ready) ? 'READY' : 'NOT READY';
    const restarts = p.containers.reduce((s, c) => s + c.restarts, 0);
    err(`  ${p.name}: ${p.status} (${ready}), restarts=${restarts}, cpu=${p.cpu || '?'}, mem=${p.memory || '?'}`);
  }
  if (pgSyncCursor) err(`  pg-sync cursor: ${pgSyncCursor}`);
  json({ pods, pgSyncCursor });
}

// --- Cursors ---

function cursorReset(value) {
  value = value || process.argv[3];
  if (!value) { json({ error: 'Usage: cursor-reset <value>' }); process.exit(1); }

  const pods = run(`kubectl get pods -n ${NS} -l app=bitdex --no-headers 2>/dev/null`, { throws: false });
  if (pods && !pods.includes('No resources')) {
    json({ error: 'Pods must be scaled to 0 before resetting cursors. Run: deploy scale 0' });
    process.exit(1);
  }

  err(`Resetting cursors to ${value}...`);
  for (const i of [0, 1]) {
    const cmd = `echo -n ${value} > ${INDEX_PATH}/bitmaps/cursors/pg-sync-bitdex-${i} && cat ${INDEX_PATH}/bitmaps/cursors/pg-sync-bitdex-${i}`;
    const { logs, success } = runEphemeralPod(`cursor-reset-${i}`, { command: cmd, pvcIndex: i });
    err(`  PVC ${i}: ${logs}`);
  }

  const pgCmd = `UPDATE bitdex_cursors SET last_outbox_id = ${value} WHERE replica_id IN ('pg-sync-bitdex-0', 'pg-sync-bitdex-1');`;
  run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${PG_NS} ${PG_POD} --context ${K8S_CONTEXT} -- psql -U postgres -d civitai -c "${pgCmd}" 2>/dev/null`);
  err(`  PG: updated`);

  json({ cursor: value, pvc0: true, pvc1: true, pg: true });
}

function cursorRead() {
  const cursors = {};
  for (const i of [0, 1]) {
    try {
      const val = kubectlExec(`cat ${INDEX_PATH}/bitmaps/cursors/pg-sync-bitdex-${i}`, { pod: `bitdex-${i}` });
      cursors[`bitdex-${i}`] = val;
    } catch { cursors[`bitdex-${i}`] = null; }
  }
  json(cursors);
}

function cursorCsv() {
  try {
    const val = kubectlExec(`cat ${INDEX_PATH}/load_stage/cursor.txt`);
    json({ csvCursor: val });
  } catch { json({ csvCursor: null, error: 'Could not read cursor.txt' }); }
}

// --- PG-Sync ---

function pgSyncHealthRaw() {
  const health = {};
  for (const i of [0, 1]) {
    const logs = run(`kubectl logs -n ${NS} bitdex-${i} -c pg-sync --tail=50 2>/dev/null`, { throws: false });
    const cursorMatch = logs.match(/cursor=(\d+)/g);
    const lastCursor = cursorMatch ? cursorMatch[cursorMatch.length - 1].replace('cursor=', '') : null;
    const errors = (logs.match(/error|failed/gi) || []).length;
    const processed = (logs.match(/processed \d+ changes/g) || []);
    const lastProcessed = processed.length > 0 ? processed[processed.length - 1] : null;
    const metricsUpdated = (logs.match(/Metrics: updated \d+ documents/g) || []);
    const lastMetrics = metricsUpdated.length > 0 ? metricsUpdated[metricsUpdated.length - 1] : null;

    health[`bitdex-${i}`] = { lastCursor, errors, lastProcessed, lastMetrics };
  }
  return health;
}

function pgSyncHealth() {
  const health = pgSyncHealthRaw();
  json(health);
}

function pgSyncLogs() {
  const pod = process.argv[3] || '0';
  const lines = process.argv[4] || '30';
  const logs = run(`kubectl logs -n ${NS} bitdex-${pod} -c pg-sync --tail=${lines} 2>/dev/null`, { throws: false });
  const filtered = logs.split('\n').filter(l => !l.includes('slot not found') && !l.includes('slow statement')).join('\n');
  console.log(filtered);
}

function serverLogs() {
  const pod = process.argv[3] || '0';
  const lines = process.argv[4] || '20';
  const logs = run(`kubectl logs -n ${NS} bitdex-${pod} -c bitdex --tail=${lines} 2>/dev/null`, { throws: false });
  console.log(logs);
}

// --- Operations ---

function resources() {
  const top = run(`kubectl top pod -n ${NS} 2>/dev/null`, { throws: false });
  console.log(top);
}

function wipe() {
  err('Wiping bitmap/docstore data on both PVCs (keeping CSVs)...');
  for (const i of [0, 1]) {
    const cmd = `rm -rf ${INDEX_PATH}/bitmaps ${INDEX_PATH}/docs ${INDEX_PATH}/bounds ${INDEX_PATH}/slot_arena.bin ${INDEX_PATH}/snapshot.meta && echo wiped-${i}`;
    const { logs } = runEphemeralPod(`bitdex-wipe-${i}`, { command: cmd, pvcIndex: i, timeout: 60 });
    err(`  PVC ${i}: ${logs}`);
  }
  json({ wiped: true });
}

function configRead() {
  const config = kubectlExec(`cat ${INDEX_PATH}/config.json`);
  try { json(JSON.parse(config)); } catch { json({ error: 'Could not read config', raw: config }); }
}

function configPatch(jsonStr) {
  jsonStr = jsonStr || process.argv[3];
  if (!jsonStr) { json({ error: 'Usage: config-patch \'{"key": "value"}\'' }); process.exit(1); }
  if (!ADMIN_TOKEN) { json({ error: 'BITDEX_ADMIN_TOKEN not set' }); process.exit(1); }
  try { JSON.parse(jsonStr); } catch { json({ error: 'Invalid JSON', input: jsonStr }); process.exit(1); }
  const localPort = 3098;
  portForward({ target: 'pod/bitdex-0', localPort, remotePort: 3000 });
  run('sleep 2', { throws: false });
  const result = run(`curl -sf -X PATCH -H "Content-Type: application/json" -H "Authorization: Bearer ${ADMIN_TOKEN}" -d '${jsonStr}' http://localhost:${localPort}/api/indexes/civitai/config`, { throws: false });
  killPortForward(localPort, 3000);
  try { json(JSON.parse(result)); } catch { json({ raw: result }); }
}

function memory() {
  err('Checking pod memory...');
  const top = run(`kubectl top pod -n ${NS} 2>/dev/null`, { throws: false });
  let topParsed = null;
  if (top && !top.includes('error')) {
    topParsed = top.split('\n').filter(l => l.includes('bitdex')).map(l => {
      const parts = l.trim().split(/\s+/);
      return { pod: parts[0], cpu: parts[1], memory: parts[2] };
    });
  }

  let debugMemory = null;
  const localPort = 3098;
  portForward({ target: 'pod/bitdex-0', localPort, remotePort: 3000 });
  run('sleep 2', { throws: false });
  try {
    const headers = ADMIN_TOKEN ? `-H "Authorization: Bearer ${ADMIN_TOKEN}"` : '';
    const result = run(`curl -sf ${headers} http://localhost:${localPort}/debug/memory`, { throws: false });
    if (result && result.startsWith('{')) debugMemory = JSON.parse(result);
  } catch {}
  killPortForward(localPort, 3000);

  if (topParsed) for (const p of topParsed) err(`  ${p.pod}: CPU=${p.cpu}, Memory=${p.memory}`);
  if (debugMemory?.human) {
    err(`  RSS: ${debugMemory.human.rss}, Headroom: ${debugMemory.human.headroom}, Tracked: ${debugMemory.human.tracked}`);
  }
  json({ kubectl_top: topParsed, debug_memory: debugMemory });
}

function disk() {
  err('Checking PVC disk usage...');
  const dfOut = kubectlExec('df -h /data');
  const duOut = kubectlExec('du -h --max-depth=2 /data/indexes');
  let dfParsed = null;
  if (dfOut) {
    const line = dfOut.split('\n').find(l => l && !l.startsWith('Filesystem'));
    if (line) { const p = line.trim().split(/\s+/); dfParsed = { total: p[1], used: p[2], available: p[3], percent: p[4] }; }
  }
  const directories = (duOut || '').split('\n').filter(Boolean).map(l => {
    const p = l.trim().split(/\s+/); return { size: p[0], path: p.slice(1).join(' ') };
  });
  if (dfParsed) err(`  Disk: ${dfParsed.used} / ${dfParsed.total} (${dfParsed.percent} used, ${dfParsed.available} free)`);
  json({ df: dfParsed, directories });
}

function cleanup(target) {
  target = target || process.argv[3];
  const targets = {
    captures:   { paths: ['/data/captures/*'], label: 'capture snapshots' },
    load_stage: { paths: ['/data/indexes/civitai/load_stage', '/data/indexes/load_stage'], label: 'load stage CSVs' },
    legacy:     { paths: ['/data/indexes/civitai/bitmaps/_legacy_bitmapfs'], label: 'legacy BitmapFs' },
    bounds:     { paths: ['/data/indexes/civitai/bitmaps/shardstore/bounds/*'], label: 'bound cache shards' },
  };
  if (!target || !targets[target]) { json({ error: `Usage: cleanup <target>. Targets: ${Object.keys(targets).join(', ')}` }); process.exit(1); }
  const t = targets[target];
  err(`Cleaning ${t.label}...`);
  for (const p of t.paths) kubectlExec(`rm -rf ${p}`);
  const dfOut = kubectlExec('df -h /data');
  let used = null;
  if (dfOut) { const line = dfOut.split('\n').find(l => l && !l.startsWith('Filesystem')); if (line) used = line.trim().split(/\s+/)[2]; }
  err(`  Done. Disk now: ${used || '?'}`);
  json({ target, label: t.label, cleaned: true, disk_used: used });
}

// --- Tunnels ---

// bitdex user credentials — NOT cnpg-cluster-nvme0-app (that's the civitai user with 120s timeout)
const PG_SECRET_NAME = 'bitdex-credentials';
const PG_SECRET_NS = 'cnpg-database';
const ENV_FILE = resolve('data', '.env.bitdex');

function fetchPgPassword() {
  const b64 = run(
    `MSYS_NO_PATHCONV=1 kubectl get secret ${PG_SECRET_NAME} -n ${PG_SECRET_NS} --context ${K8S_CONTEXT} -o jsonpath='{.data.password}' 2>/dev/null`,
    { throws: false }
  );
  if (!b64) return null;
  return Buffer.from(b64, 'base64').toString('utf8').trim();
}

function writeEnvFile(pgUrl) {
  const dir = resolve('data');
  run(`mkdir -p "${dir}"`, { throws: false });
  const content = [
    `# Generated by deploy CLI tunnel — do not commit`,
    `# Password fetched from K8s secret ${PG_SECRET_NAME}`,
    `DATABASE_URL=${pgUrl}`,
    `BITDEX_URL=http://localhost:3000`,
    `BITDEX_PROD_URL=https://bitdex.civitai.com`,
    '',
  ].join('\n');
  writeFileSync(ENV_FILE, content);
  err(`  .env written: ${ENV_FILE}`);
}

function tunnel(target, action) {
  action = action || 'start';
  const targets = {
    pg: { ns: PG_NS, svc: PG_SVC, localPort: 5433, remotePort: 5432, healthCmd: `pg_isready -h localhost -p 5433 2>/dev/null`, label: 'PostgreSQL (CNPG, bitdex user)' },
    bitdex: { ns: NS, svc: 'pod/bitdex-0', localPort: 3000, remotePort: 3000, healthCmd: 'curl -sf http://localhost:3000/api/health 2>/dev/null', label: 'BitDex server (pod 0)' },
  };
  const t = targets[target];
  if (!t) { json({ error: `Unknown tunnel target: ${target}. Available: ${Object.keys(targets).join(', ')}` }); process.exit(1); }

  if (action === 'stop') {
    killPortForward(t.localPort, t.remotePort);
    json({ status: 'stopped', target, label: t.label });
    return;
  }

  if (action === 'status') {
    try {
      const check = run(t.healthCmd, { throws: false });
      const running = check && !check.includes('refused') && !check.includes('error');
      json({ status: running ? 'running' : 'not_running', target, label: t.label, port: t.localPort });
    } catch { json({ status: 'not_running', target, label: t.label }); }
    return;
  }

  const ready = portForward({
    namespace: t.ns, target: t.svc,
    localPort: t.localPort, remotePort: t.remotePort,
    healthCmd: t.healthCmd,
  });

  if (ready) {
    if (target === 'pg') {
      // Fetch password from K8s secret and build full DATABASE_URL
      const password = fetchPgPassword();
      const pgUrl = password
        ? `postgresql://bitdex:${password}@localhost:${t.localPort}/civitai`
        : `postgresql://bitdex@localhost:${t.localPort}/civitai`;
      if (!password) err('  WARNING: Could not fetch password from K8s secret');

      // Write .env file
      writeEnvFile(pgUrl);

      json({ status: 'started', target, label: t.label, port: t.localPort, connection: pgUrl, envFile: ENV_FILE });
    } else {
      const conn = `http://localhost:${t.localPort}`;
      json({ status: 'started', target, label: t.label, port: t.localPort, connection: conn });
    }
  } else {
    json({ status: 'failed', target, label: t.label, message: 'Could not establish port-forward after 10s' });
    process.exit(1);
  }
}

// --- Snapshot Download ---

function snapshotStatus(sessionId) {
  if (!sessionId) { json({ error: 'Usage: snapshot-status <session_id>' }); process.exit(1); }
  if (!ADMIN_TOKEN) { json({ error: 'BITDEX_ADMIN_TOKEN not set. Add it to .claude/skills/dev-server/.env' }); process.exit(1); }
  const result = run(`curl -sf -H "Authorization: Bearer ${ADMIN_TOKEN}" "${BITDEX_URL}/debug/snapshot/${sessionId}/status"`, { throws: false });
  try { json(JSON.parse(result)); } catch { json({ error: 'Failed to fetch status', raw: result }); }
}

async function snapshotDownload(sessionId) {
  if (!sessionId) { json({ error: 'Usage: snapshot-download <session_id> [--output <path>]' }); process.exit(1); }
  if (!ADMIN_TOKEN) { json({ error: 'BITDEX_ADMIN_TOKEN not set. Add it to .claude/skills/dev-server/.env' }); process.exit(1); }

  const outputIdx = process.argv.indexOf('--output');
  const outputPath = outputIdx !== -1 ? process.argv[outputIdx + 1] : `data/snapshots/snapshot-${sessionId}.tar.gz`;
  const outputDir = resolve(dirname(outputPath));
  run(`mkdir -p "${outputDir}"`, { throws: false });

  const url = `${BITDEX_URL}/debug/snapshot/${sessionId}/download`;
  const fullPath = resolve(outputPath);

  let startByte = 0;
  if (existsSync(fullPath)) {
    startByte = statSync(fullPath).size;
    err(`Resuming from byte ${startByte} (${(startByte / 1073741824).toFixed(2)} GB)`);
  }

  err(`Downloading snapshot ${sessionId} from ${BITDEX_URL}`);
  err(`Output: ${fullPath}`);

  const urlObj = new URL(url);
  const client = urlObj.protocol === 'https:' ? https : http;

  return new Promise((resolveP, rejectP) => {
    const headers = { 'Authorization': `Bearer ${ADMIN_TOKEN}` };
    if (startByte > 0) headers['Range'] = `bytes=${startByte}-`;

    const req = client.get(urlObj, { headers }, (res) => {
      if (res.statusCode === 401 || res.statusCode === 403) {
        err(`Auth error: ${res.statusCode}`);
        json({ error: `Auth failed (${res.statusCode})`, hint: 'Check BITDEX_ADMIN_TOKEN in .env' });
        process.exit(1);
      }
      if (res.statusCode >= 400) {
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => { json({ error: `HTTP ${res.statusCode}`, body }); process.exit(1); });
        return;
      }

      const totalHeader = res.headers['content-length'];
      const total = totalHeader ? parseInt(totalHeader, 10) + startByte : null;
      const flags = startByte > 0 ? { flags: 'a' } : {};
      const file = createWriteStream(fullPath, flags);
      let downloaded = startByte;
      let lastPrint = 0;

      res.on('data', (chunk) => {
        file.write(chunk);
        downloaded += chunk.length;
        const now = Date.now();
        if (now - lastPrint > 2000) {
          const gb = (downloaded / 1073741824).toFixed(2);
          const pct = total ? ` (${((downloaded / total) * 100).toFixed(1)}%)` : '';
          process.stderr.write(`\r  ${gb} GB downloaded${pct}`);
          lastPrint = now;
        }
      });

      res.on('end', () => {
        file.end();
        const gb = (downloaded / 1073741824).toFixed(2);
        err(`\nDownload complete: ${gb} GB → ${fullPath}`);
        json({ downloaded_bytes: downloaded, path: fullPath, session_id: sessionId });
        resolveP();
      });

      res.on('error', (e) => {
        file.end();
        err(`\nDownload error at ${(downloaded / 1073741824).toFixed(2)} GB: ${e.message}`);
        err('Re-run the same command to resume.');
        json({ error: e.message, downloaded_bytes: downloaded, path: fullPath, resumable: true });
        rejectP(e);
      });
    });

    req.on('error', (e) => { err(`Connection error: ${e.message}`); json({ error: e.message }); rejectP(e); });
    req.on('timeout', () => {
      req.destroy();
      err('Connection timed out. Re-run to resume.');
      json({ error: 'timeout', downloaded_bytes: startByte, resumable: true });
      rejectP(new Error('timeout'));
    });
  });
}

// --- Prometheus Metrics ---

function metricsNow() {
  ensurePromPortForward();

  const queries = {
    qps: 'sum(rate(bitdex_query_total{index="civitai"}[5m]))',
    query_p50_ms: 'histogram_quantile(0.5, sum by (le) (rate(bitdex_query_duration_seconds_bucket{index="civitai"}[5m]))) * 1000',
    query_p95_ms: 'histogram_quantile(0.95, sum by (le) (rate(bitdex_query_duration_seconds_bucket{index="civitai"}[5m]))) * 1000',
    query_p99_ms: 'histogram_quantile(0.99, sum by (le) (rate(bitdex_query_duration_seconds_bucket{index="civitai"}[5m]))) * 1000',
    docs_p95_ms: 'histogram_quantile(0.95, sum by (le) (rate(bitdex_query_docs_seconds_bucket{index="civitai"}[5m]))) * 1000',
    http_p95_ms: 'histogram_quantile(0.95, sum by (le) (rate(bitdex_http_response_seconds_bucket{method="POST",path=~".*civitai.*"}[5m]))) * 1000',
    cache_hits: 'sum(bitdex_cache_hits_total{index="civitai"})',
    cache_misses: 'sum(bitdex_cache_misses_total{index="civitai"})',
  };

  const results = {};
  for (const [name, q] of Object.entries(queries)) {
    const v = extractScalar(promQuery(q));
    results[name] = v != null ? (name.includes('_ms') ? Math.round(v * 100) / 100 : name === 'qps' ? Math.round(v * 10) / 10 : v) : null;
  }

  if (results.cache_hits != null && results.cache_misses != null) {
    const total = results.cache_hits + results.cache_misses;
    results.cache_hit_rate = total > 0 ? `${((results.cache_hits / total) * 100).toFixed(1)}%` : '—';
  }

  err(`BitDex Production Metrics (5m window)`);
  err(`  QPS:          ${results.qps ?? '—'}`);
  err(`  Query p50:    ${results.query_p50_ms ?? '—'} ms`);
  err(`  Query p95:    ${results.query_p95_ms ?? '—'} ms`);
  err(`  Query p99:    ${results.query_p99_ms ?? '—'} ms`);
  err(`  Docs p95:     ${results.docs_p95_ms ?? '—'} ms`);
  err(`  HTTP p95:     ${results.http_p95_ms ?? '—'} ms`);
  err(`  Cache:        ${results.cache_hit_rate ?? '—'} (${results.cache_hits ?? 0} hits, ${results.cache_misses ?? 0} misses)`);
  json(results);
}

function metricsTrend(window) {
  ensurePromPortForward();
  const win = window || '15m';
  const end = Math.floor(Date.now() / 1000);
  const durSec = parseDuration(win);
  const start = end - durSec;
  const step = Math.max(15, Math.floor(durSec / 60));

  const queries = {
    qps: `sum(rate(bitdex_query_total{index="civitai"}[1m]))`,
    query_p95_ms: `histogram_quantile(0.95, sum by (le) (rate(bitdex_query_duration_seconds_bucket{index="civitai"}[1m]))) * 1000`,
  };

  const results = {};
  for (const [name, q] of Object.entries(queries)) {
    const data = promQueryRange(q, start, end, step);
    if (data?.data?.result?.[0]?.values) {
      results[name] = data.data.result[0].values.map(([ts, val]) => ({
        time: new Date(ts * 1000).toISOString().slice(11, 19),
        value: Math.round(parseFloat(val) * 100) / 100,
      }));
    }
  }

  if (results.qps) {
    err(`QPS trend (${win}):`);
    for (const p of results.qps) err(`  ${p.time}  ${p.value}`);
  }
  if (results.query_p95_ms) {
    err(`p95 latency trend (${win}):`);
    for (const p of results.query_p95_ms) err(`  ${p.time}  ${p.value} ms`);
  }
  json(results);
}

function metricsQuery(query) {
  if (!query) { json({ error: 'Usage: metrics-query <promql>' }); process.exit(1); }
  ensurePromPortForward();
  const result = promQuery(query);
  if (result?.data?.result) {
    for (const r of result.data.result) {
      const labels = Object.entries(r.metric || {}).map(([k, v]) => `${k}="${v}"`).join(', ');
      const val = r.value?.[1] ?? '—';
      err(`  {${labels}} => ${val}`);
    }
  }
  json(result);
}

// --- Command Router ---

const command = process.argv[2];
switch (command) {
  // Release & Deploy
  case 'release': release(); break;
  case 'watch-build': watchBuild(); break;
  case 'watch-build-notify': watchBuildNotify(); break;
  case 'build-status': buildStatus(); break;
  case 'rollout': rollout(process.argv[3]); break;
  case 'deploy': deploy(process.argv[3]); break;
  case 'rollback': rollback(); break;
  case 'status': status(); break;
  case 'scale': scale(); break;

  // Cursors
  case 'cursor-reset': cursorReset(); break;
  case 'cursor-read': cursorRead(); break;
  case 'cursor-csv': cursorCsv(); break;

  // PG-Sync
  case 'pg-sync-health': pgSyncHealth(); break;
  case 'pg-sync-logs': pgSyncLogs(); break;
  case 'server-logs': serverLogs(); break;

  // Operations
  case 'resources': resources(); break;
  case 'wipe': wipe(); break;
  case 'config-read': configRead(); break;
  case 'config-patch': configPatch(); break;
  case 'memory': memory(); break;
  case 'disk': disk(); break;
  case 'cleanup': cleanup(); break;
  case 'health': health(); break;

  // Tunnels
  case 'tunnel': tunnel(process.argv[3], process.argv[4]); break;

  // Snapshots
  case 'snapshot-status': snapshotStatus(process.argv[3]); break;
  case 'snapshot-download': await snapshotDownload(process.argv[3]); break;

  // Prometheus Metrics
  case 'metrics-now': metricsNow(); break;
  case 'metrics-trend': metricsTrend(process.argv[3]); break;
  case 'metrics-query': metricsQuery(process.argv.slice(3).join(' ')); break;

  // CSV Dump (V2 — config-driven)
  case 'csv-dump': {
    const tables = process.argv[3] ? process.argv[3].split(',') : undefined;
    const gzip = process.argv.includes('--gzip');
    csvDump({ tables, gzip });
    break;
  }
  case 'csv-dump-progress': csvDumpProgress(); break;
  case 'csv-dump-cleanup': csvDumpCleanup(); break;
  case 'csv-dump-tables': csvDumpTables(); break;
  case 'csv-full-pipeline': {
    const pipeTables = process.argv[3] && !process.argv[3].startsWith('-') ? process.argv[3].split(',') : undefined;
    const pipeNotify = process.argv.includes('--notify') ? process.argv[process.argv.indexOf('--notify') + 1] : undefined;
    const pipeOut = process.argv.includes('--output') ? process.argv[process.argv.indexOf('--output') + 1] : undefined;
    const skipDump = process.argv.includes('--skip-dump');
    csvFullPipeline({ tables: pipeTables, notifyAgent: pipeNotify, outputDir: pipeOut, skipDump });
    break;
  }
  case 'csv-serve': csvServe(); break;
  case 'csv-serve-stop': csvServeStop(); break;
  case 'csv-download': {
    const dlTables = process.argv[3] && !process.argv[3].startsWith('-') ? process.argv[3].split(',') : undefined;
    const keepServer = process.argv.includes('--keep-server');
    const outDir = process.argv.includes('--output') ? process.argv[process.argv.indexOf('--output') + 1] : undefined;
    const dlToken = process.argv.includes('--token') ? process.argv[process.argv.indexOf('--token') + 1] : undefined;
    const dlChunks = process.argv.includes('--chunks') ? parseInt(process.argv[process.argv.indexOf('--chunks') + 1]) : 8;
    await csvDownload({ tables: dlTables, outputDir: outDir, keepServer, token: dlToken, chunks: dlChunks });
    break;
  }
  case 'csv-verify': csvVerify(process.argv[3]); break;
  case 'csv-download-watch': {
    const watchFile = process.argv[3];
    if (!watchFile) { json({ error: 'Usage: csv-download-watch <filename> [--poll <seconds>] [--notify <agent>]' }); process.exit(1); }
    const pollIdx = process.argv.indexOf('--poll');
    const pollSec = pollIdx !== -1 ? parseInt(process.argv[pollIdx + 1]) : 10;
    const notifyIdx = process.argv.indexOf('--notify');
    const notifyAgent = notifyIdx !== -1 ? process.argv[notifyIdx + 1] : undefined;
    csvDownloadWatch(watchFile, { pollInterval: pollSec, notifyAgent });
    break;
  }

  default:
    json({
      error: `Unknown command: ${command}`,
      commands: {
        'Release & Deploy': ['release', 'watch-build', 'watch-build-notify [run-id] [--notify <agent>]', 'build-status', 'rollout <version>', 'deploy <version>', 'rollback <version>'],
        'Status': ['status', 'health', 'resources', 'memory', 'disk'],
        'Cursors': ['cursor-reset <value>', 'cursor-read', 'cursor-csv'],
        'PG-Sync': ['pg-sync-health', 'pg-sync-logs [pod] [lines]', 'server-logs [pod] [lines]'],
        'Config': ['config-read', 'config-patch <json>'],
        'CSV Dump': ['csv-dump [tables] [--gzip]', 'csv-dump-progress', 'csv-dump-cleanup', 'csv-dump-tables', 'csv-full-pipeline [tables] [--notify <agent>] [--skip-dump]'],
        'CSV Download': ['csv-serve', 'csv-serve-stop', 'csv-download [tables] [--token <t>] [--chunks N] [--output <dir>]', 'csv-download-watch <file> [--poll <sec>] [--notify <agent>]', 'csv-verify [local-dir]'],
        'Tunnels': ['tunnel pg [start|stop|status]', 'tunnel bitdex [start|stop|status]'],
        'Snapshots': ['snapshot-status <session_id>', 'snapshot-download <session_id> [--output <path>]'],
        'Metrics': ['metrics-now', 'metrics-trend [window]', 'metrics-query <promql>'],
        'Data': ['wipe', 'cleanup <captures|load_stage|legacy|bounds>'],
      },
    });
    process.exit(1);
}
