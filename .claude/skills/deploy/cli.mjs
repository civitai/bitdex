#!/usr/bin/env node
/**
 * BitDex Production Deploy CLI
 *
 * Usage: node .claude/skills/deploy/cli.mjs <command> [args]
 */
import { execSync, spawnSync } from 'child_process';
import { readFileSync, writeFileSync, existsSync, createWriteStream, statSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import http from 'http';
import https from 'https';

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

const NS = 'bitdex';
const STS = 'bitdex';
const GHCR = 'ghcr.io/civitai/bitdex';
const DOCKER_WORKFLOW = 'docker.yml';
const NODE = 'talos-fq9-f3k';
const PG_POD = 'cnpg-cluster-nvme0-1';
const PG_NS = 'cnpg-database';
const INDEX_PATH = '/data/indexes/civitai';

function run(cmd, opts = {}) {
  try {
    return execSync(cmd, { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'], ...opts }).trim();
  } catch (e) {
    if (opts.throws !== false) throw e;
    return e.stderr?.trim() || e.message;
  }
}

function kubectl(args) {
  return run(`kubectl ${args} -n ${NS} 2>/dev/null`);
}

function json(data) {
  console.log(JSON.stringify(data, null, 2));
}

function err(msg) {
  console.error(msg);
}

// --- Commands ---

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

  // Find the run ID
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
    if (result.status === 'completed') {
      json({ runId, ...result });
      return;
    }
    spawnSync('sleep', ['15']);
  }
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
  rollout(version);

  // Wait a few seconds for pg-sync to start
  spawnSync('sleep', ['10']);
  pgSyncHealth();
}

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

function cursorReset(value) {
  value = value || process.argv[3];
  if (!value) { json({ error: 'Usage: cursor-reset <value>' }); process.exit(1); }

  // Check pods are down
  const pods = run(`kubectl get pods -n ${NS} -l app=bitdex --no-headers 2>/dev/null`, { throws: false });
  if (pods && !pods.includes('No resources')) {
    json({ error: 'Pods must be scaled to 0 before resetting cursors. Run: deploy scale 0' });
    process.exit(1);
  }

  err(`Resetting cursors to ${value}...`);

  // Reset on both PVCs via transfer pods
  for (const i of [0, 1]) {
    const podName = `cursor-reset-${i}`;
    const cmd = `echo -n ${value} > ${INDEX_PATH}/bitmaps/cursors/pg-sync-bitdex-${i} && cat ${INDEX_PATH}/bitmaps/cursors/pg-sync-bitdex-${i}`;
    const overrides = JSON.stringify({
      spec: {
        containers: [{ name: 'fix', image: 'busybox', command: ['sh', '-c', cmd],
          volumeMounts: [{ name: 'data', mountPath: '/data' }] }],
        volumes: [{ name: 'data', persistentVolumeClaim: { claimName: `data-bitdex-${i}` } }],
        nodeSelector: { 'kubernetes.io/hostname': NODE },
      }
    });
    run(`kubectl run ${podName} -n ${NS} --image=busybox --overrides='${overrides}' --restart=Never 2>/dev/null`);
    run(`kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/${podName} -n ${NS} --timeout=30s 2>/dev/null`);
    const result = run(`kubectl logs -n ${NS} ${podName} 2>/dev/null`);
    err(`  PVC ${i}: ${result}`);
    run(`kubectl delete pod -n ${NS} ${podName} 2>/dev/null`);
  }

  // Reset in PG
  const pgCmd = `UPDATE bitdex_cursors SET last_outbox_id = ${value} WHERE replica_id IN ('pg-sync-bitdex-0', 'pg-sync-bitdex-1');`;
  run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${PG_NS} ${PG_POD} -- psql -U postgres -d civitai -c "${pgCmd}" 2>/dev/null`);
  err(`  PG: updated`);

  json({ cursor: value, pvc0: true, pvc1: true, pg: true });
}

function cursorRead() {
  const cursors = {};
  for (const i of [0, 1]) {
    try {
      const val = run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${NS} bitdex-${i} -c bitdex -- cat ${INDEX_PATH}/bitmaps/cursors/pg-sync-bitdex-${i} 2>/dev/null`);
      cursors[`bitdex-${i}`] = val;
    } catch {
      cursors[`bitdex-${i}`] = null;
    }
  }
  json(cursors);
}

function cursorCsv() {
  try {
    const val = run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${NS} bitdex-0 -c bitdex -- cat ${INDEX_PATH}/load_stage/cursor.txt 2>/dev/null`);
    json({ csvCursor: val });
  } catch {
    json({ csvCursor: null, error: 'Could not read cursor.txt' });
  }
}

function pgSyncHealth() {
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
  json(health);
}

function pgSyncLogs() {
  const pod = process.argv[3] || '0';
  const lines = process.argv[4] || '30';
  const logs = run(`kubectl logs -n ${NS} bitdex-${pod} -c pg-sync --tail=${lines} 2>/dev/null`, { throws: false });
  // Filter out noisy slot-not-found lines
  const filtered = logs.split('\n').filter(l => !l.includes('slot not found') && !l.includes('slow statement')).join('\n');
  console.log(filtered);
}

function serverLogs() {
  const pod = process.argv[3] || '0';
  const lines = process.argv[4] || '20';
  const logs = run(`kubectl logs -n ${NS} bitdex-${pod} -c bitdex --tail=${lines} 2>/dev/null`, { throws: false });
  console.log(logs);
}

function resources() {
  const top = run(`kubectl top pod -n ${NS} 2>/dev/null`, { throws: false });
  console.log(top);
}

function wipe() {
  err('Wiping bitmap/docstore data on both PVCs (keeping CSVs)...');
  for (const i of [0, 1]) {
    const podName = `bitdex-wipe-${i}`;
    const cmd = `rm -rf ${INDEX_PATH}/bitmaps ${INDEX_PATH}/docs ${INDEX_PATH}/bounds ${INDEX_PATH}/slot_arena.bin ${INDEX_PATH}/snapshot.meta && echo wiped-${i}`;
    const overrides = JSON.stringify({
      spec: {
        containers: [{ name: 'wipe', image: 'busybox', command: ['sh', '-c', cmd],
          volumeMounts: [{ name: 'data', mountPath: '/data' }] }],
        volumes: [{ name: 'data', persistentVolumeClaim: { claimName: `data-bitdex-${i}` } }],
        nodeSelector: { 'kubernetes.io/hostname': NODE },
      }
    });
    run(`kubectl run ${podName} -n ${NS} --image=busybox --overrides='${overrides}' --restart=Never 2>/dev/null`);
    run(`kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/${podName} -n ${NS} --timeout=60s 2>/dev/null`);
    err(`  PVC ${i}: ${run(`kubectl logs -n ${NS} ${podName} 2>/dev/null`)}`);
    run(`kubectl delete pod -n ${NS} ${podName} 2>/dev/null`);
  }
  json({ wiped: true });
}

function configRead() {
  const config = run(`MSYS_NO_PATHCONV=1 kubectl exec -n ${NS} bitdex-0 -c bitdex -- cat ${INDEX_PATH}/config.json 2>/dev/null`, { throws: false });
  try {
    json(JSON.parse(config));
  } catch {
    json({ error: 'Could not read config', raw: config });
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

  // Ensure output dir exists
  const outputDir = resolve(dirname(outputPath));
  run(`mkdir -p "${outputDir}"`, { throws: false });

  const url = `${BITDEX_URL}/debug/snapshot/${sessionId}/download`;
  const fullPath = resolve(outputPath);

  // Check for existing partial download (resume support)
  let startByte = 0;
  if (existsSync(fullPath)) {
    startByte = statSync(fullPath).size;
    console.error(`Resuming from byte ${startByte} (${(startByte / 1073741824).toFixed(2)} GB)`);
  }

  console.error(`Downloading snapshot ${sessionId} from ${BITDEX_URL}`);
  console.error(`Output: ${fullPath}`);

  const urlObj = new URL(url);
  const client = urlObj.protocol === 'https:' ? https : http;

  return new Promise((resolveP, rejectP) => {
    const headers = { 'Authorization': `Bearer ${ADMIN_TOKEN}` };
    if (startByte > 0) headers['Range'] = `bytes=${startByte}-`;

    const req = client.get(urlObj, { headers }, (res) => {
      if (res.statusCode === 401 || res.statusCode === 403) {
        console.error(`Auth error: ${res.statusCode}`);
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
        console.error(`\nDownload complete: ${gb} GB → ${fullPath}`);
        json({ downloaded_bytes: downloaded, path: fullPath, session_id: sessionId });
        resolveP();
      });

      res.on('error', (e) => {
        file.end();
        console.error(`\nDownload error at ${(downloaded / 1073741824).toFixed(2)} GB: ${e.message}`);
        console.error('Re-run the same command to resume.');
        json({ error: e.message, downloaded_bytes: downloaded, path: fullPath, resumable: true });
        rejectP(e);
      });
    });

    req.on('error', (e) => {
      console.error(`Connection error: ${e.message}`);
      json({ error: e.message });
      rejectP(e);
    });

    req.on('timeout', () => {
      req.destroy();
      console.error('Connection timed out. Re-run to resume.');
      json({ error: 'timeout', downloaded_bytes: startByte, resumable: true });
      rejectP(new Error('timeout'));
    });
  });
}

// --- Router ---

const command = process.argv[2];
switch (command) {
  case 'release': release(); break;
  case 'watch-build': watchBuild(); break;
  case 'build-status': buildStatus(); break;
  case 'rollout': rollout(process.argv[3]); break;
  case 'deploy': deploy(process.argv[3]); break;
  case 'status': status(); break;
  case 'scale': scale(); break;
  case 'cursor-reset': cursorReset(); break;
  case 'cursor-read': cursorRead(); break;
  case 'cursor-csv': cursorCsv(); break;
  case 'pg-sync-health': pgSyncHealth(); break;
  case 'pg-sync-logs': pgSyncLogs(); break;
  case 'server-logs': serverLogs(); break;
  case 'resources': resources(); break;
  case 'wipe': wipe(); break;
  case 'config-read': configRead(); break;
  case 'snapshot-status': snapshotStatus(process.argv[3]); break;
  case 'snapshot-download': await snapshotDownload(process.argv[3]); break;
  default:
    json({
      error: `Unknown command: ${command}`,
      commands: ['release', 'watch-build', 'build-status', 'rollout', 'deploy', 'status', 'scale',
                 'cursor-reset', 'cursor-read', 'cursor-csv', 'pg-sync-health', 'pg-sync-logs',
                 'server-logs', 'resources', 'wipe', 'config-read',
                 'snapshot-status', 'snapshot-download'],
    });
    process.exit(1);
}
