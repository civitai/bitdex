#!/usr/bin/env node
/**
 * Per-pod redump orchestrator (rolling, no PG nuke).
 *
 * Usage: node .claude/skills/deploy/redump.mjs [step]
 *
 * Steps (run in order):
 *   1. preflight    — list pods, verify both Ready, capture image tag
 *   2. redump-0     — kick redump on bitdex-0, wait for re-ready
 *   3. redump-1     — kick redump on bitdex-1, wait for re-ready
 *   4. all          — runs 1 → 2 → 3 sequentially (default if no arg)
 *
 * What it does per pod:
 *   - POST /api/indexes/civitai/redump with admin token
 *   - Server flips /api/ready to 503, drains, calls sidecar /internal/restart
 *     (which deletes this pod's row in bitdex_cursors), wipes the PVC,
 *     exits → k8s restarts both containers → sidecar re-dumps from PG
 *   - Wait for the pod to come back Ready (`kubectl wait`, 30 min timeout)
 *
 * What it does NOT do:
 *   - Touch PG state (BitdexOps, triggers stay intact)
 *   - Touch Flux (no scale-down; rolling redump keeps one pod serving)
 *   - Coordinate cross-pod dump snapshots (handoff Bug B, separate fix)
 */
import { execSync } from 'child_process';

const NS = 'bitdex';
const STS = 'bitdex';
const K8S_CONTEXT = 'civit-datapacket';
const INDEX = 'civitai';
const DRAIN_SECS = 30;
const REDUMP_WAIT_TIMEOUT = '30m'; // kubectl wait flag

function run(cmd, opts = {}) {
  try {
    return execSync(cmd, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: opts.timeout || 120_000,
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

function log(m) { console.log(`  ✓ ${m}`); }
function warn(m) { console.error(`  ! ${m}`); }
function err(m) { console.error(`  ✗ ${m}`); }
function heading(m) { console.log(`\n=== ${m} ===`); }

function getAdminToken() {
  const b64 = kubectl(
    `get secret bitdex-secrets -o jsonpath='{.data.BITDEX_ADMIN_TOKEN}'`,
  );
  if (!b64) throw new Error('BITDEX_ADMIN_TOKEN secret missing');
  return Buffer.from(b64, 'base64').toString('utf8').trim();
}

function listPods() {
  // ordered by ordinal so redump-0 runs on bitdex-0, redump-1 on bitdex-1
  const raw = kubectl(
    `get pods -l app=bitdex,statefulset.kubernetes.io/pod-name -o jsonpath='{range .items[*]}{.metadata.name}{\"\\n\"}{end}'`,
    { ignoreError: true },
  );
  const fromLabel = raw.split('\n').filter(Boolean);
  if (fromLabel.length) return fromLabel.sort();
  // fallback: list by sts ownership
  const all = kubectl(
    `get pods -o jsonpath='{range .items[?(@.metadata.ownerReferences[0].name=="${STS}")]}{.metadata.name}{\"\\n\"}{end}'`,
  );
  return all.split('\n').filter(Boolean).sort();
}

function podReady(pod) {
  const cond = kubectl(
    `get pod ${pod} -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'`,
    { ignoreError: true },
  );
  return cond === 'True';
}

function step1_preflight() {
  heading('Step 1: Pre-flight');
  const pods = listPods();
  if (!pods.length) throw new Error('No bitdex pods found');
  log(`Pods: ${pods.join(', ')}`);
  for (const p of pods) {
    const ready = podReady(p);
    if (!ready) {
      err(`${p} is NOT Ready — refusing to start redump`);
      throw new Error(`pod ${p} not ready`);
    }
    log(`${p} Ready`);
  }
  const img = kubectl(
    `get sts ${STS} -o jsonpath='{.spec.template.spec.containers[0].image}'`,
  );
  log(`Image: ${img}`);
  return pods;
}

async function redumpPod(pod, token) {
  heading(`Redumping ${pod}`);

  // port-forward in background
  const pf = require('child_process').spawn(
    'kubectl',
    [
      '--context', K8S_CONTEXT,
      '-n', NS,
      'port-forward', `pod/${pod}`, '30290:3000',
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );
  // Wait for the port-forward to be ready
  await new Promise(r => setTimeout(r, 2500));

  let resp;
  try {
    const res = await fetch(`http://localhost:30290/api/indexes/${INDEX}/redump`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ drain_secs: DRAIN_SECS }),
    });
    resp = await res.json();
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${JSON.stringify(resp)}`);
    log(`Redump accepted: ${JSON.stringify(resp)}`);
  } finally {
    pf.kill('SIGTERM');
  }

  // Wait for pod to come back Ready (rolling).
  // The pod will go NotReady within ~10s, restart, then take however long the
  // dump pipeline needs (typically 5-15 min for 107M records).
  log(`Waiting for ${pod} to come back Ready (timeout ${REDUMP_WAIT_TIMEOUT})...`);
  // First wait for unready (so we don't accidentally see the OLD Ready state).
  await new Promise(r => setTimeout(r, (DRAIN_SECS + 10) * 1000));
  // Then wait until Ready again.
  try {
    kubectl(`wait --for=condition=ready pod/${pod} --timeout=${REDUMP_WAIT_TIMEOUT}`, { timeout: 35 * 60 * 1000 });
    log(`${pod} Ready again`);
  } catch (e) {
    err(`${pod} did not return to Ready in ${REDUMP_WAIT_TIMEOUT}: ${e.message}`);
    throw e;
  }
}

async function stepAll() {
  const pods = step1_preflight();
  const token = getAdminToken();
  for (const pod of pods) {
    await redumpPod(pod, token);
  }
  heading('All pods redumped');
  log('Verify with: kubectl -n bitdex get pods');
  log('Smoke test queries before resuming any traffic-shaping work.');
}

async function main() {
  const step = process.argv[2] || 'all';
  switch (step) {
    case 'preflight': step1_preflight(); break;
    case 'redump-0':
    case 'redump-1': {
      const pods = step1_preflight();
      const ordinal = step.endsWith('-0') ? 0 : 1;
      const pod = pods.find(p => p.endsWith(`-${ordinal}`));
      if (!pod) throw new Error(`pod with ordinal ${ordinal} not found`);
      const token = getAdminToken();
      await redumpPod(pod, token);
      break;
    }
    case 'all': await stepAll(); break;
    default:
      err(`Unknown step: ${step}`);
      console.error('Usage: redump.mjs [preflight|redump-0|redump-1|all]');
      process.exit(1);
  }
}

main().catch(e => {
  err(e.message);
  process.exit(1);
});
