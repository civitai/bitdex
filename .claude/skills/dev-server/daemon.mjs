#!/usr/bin/env node
/**
 * BitDex Dev-Server Daemon
 *
 * Background HTTP daemon that coordinates multiple agents working on BitDex.
 * Manages server instances, datasets, build locks, and E2E test locks.
 *
 * Port: 9851 (one above ai-notifications at 9850)
 * State: pure in-memory, no persistence across restarts
 */

import http from 'node:http';
import { spawn, execSync } from 'node:child_process';
import { resolve, dirname, basename, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  mkdirSync, existsSync,
  readdirSync, statSync, unlinkSync,
  copyFileSync,
} from 'node:fs';
import net from 'node:net';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PROJECT_ROOT = resolve(__dirname, '..', '..', '..');

const DAEMON_PORT = 9851;
const MAX_LOG_LINES = 1000;
const HEARTBEAT_INTERVAL = 10_000;
const BUILD_LOCK_TIMEOUT = 600_000;  // 10 minutes
const E2E_LOCK_TIMEOUT = 300_000;    // 5 minutes
const TEST_SLOT_COUNT = 3;
const TEST_SLOT_TIMEOUT = 300_000;   // 5 minutes
const PORT_RANGE_START = 3001;
const PORT_RANGE_END = 3099;
const PORT_E2E_RESERVED = 3100;
const PORT_RESERVATION_TTL = 60_000; // 60 seconds

const IS_WIN = process.platform === 'win32';
const EXE = IS_WIN ? '.exe' : '';

// ─── State ───────────────────────────────────────────────────────

let instances = new Map();    // instanceId → instance object
let datasets = new Map();     // absolute dataDir path → dataset object
let buildLock = { holder: null, target: null, startedAt: null, pid: null, targetDir: null, logBuffer: [], process: null, exitCode: null };
let e2eLock = { holder: null, startedAt: null, pid: null };
let portReservations = new Map(); // port → { reservedAt, reservedBy }
let testSlots = Array.from({length: TEST_SLOT_COUNT}, (_, i) => ({ id: i + 1, holder: null, command: null, startedAt: null, pid: null, process: null, exitCode: null, logBuffer: [], targetDir: resolve(PROJECT_ROOT, `target-test${i + 1}`) }));
let daemonStartedAt = new Date().toISOString();

// SSE clients — connected response objects for event streaming
const sseClients = new Set();

function sseBroadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch { sseClients.delete(res); }
  }
}

// Per-instance log ring buffers (in-memory for fast TUI access)
let logBuffers = new Map();   // instanceId → { lines: [], globalIndex: 0 }
let globalLogIndex = 0;

// ─── Process Utilities ──────────────────────────────────────────

// Non-blocking PID check — uses process.kill(pid, 0) which is instant
// (no execSync/tasklist that blocks the event loop for 200ms+)
function isPidAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0); // signal 0 = check existence, works on Windows Node 18+
    return true;
  } catch {
    return false;
  }
}

function killProcess(pid) {
  if (!pid) return;
  try {
    if (IS_WIN) {
      execSync(`taskkill /PID ${pid} /T /F`, { stdio: 'ignore', windowsHide: true, timeout: 5000 });
    } else {
      process.kill(pid, 'SIGTERM');
      setTimeout(() => {
        try { process.kill(pid, 'SIGKILL'); } catch { /* already dead */ }
      }, 3000);
    }
  } catch { /* already dead */ }
}

// ─── Shadow Copy ────────────────────────────────────────────────
// Runs server from a .active copy so cargo builds don't lock the original.
// On start: copy binary → .active, run .active
// On stop/exit: clean up .active
// Build overwrites original (never locked), restart copies fresh .active

function activePath(binaryPath) {
  // e.g., target/fast/bitdex-server.exe → target/fast/bitdex-server.active.exe
  if (IS_WIN) {
    return binaryPath.replace(/\.exe$/, '.active.exe');
  }
  return binaryPath + '.active';
}

function createShadowCopy(binaryPath) {
  const active = activePath(binaryPath);

  // If .active exists but is locked (orphaned process), try to kill it
  if (existsSync(active)) {
    try {
      unlinkSync(active);
    } catch {
      // File locked — find and kill the orphaned process
      const imageName = basename(active);
      try {
        if (IS_WIN) {
          execSync(`taskkill /IM "${imageName}" /F`, { stdio: 'ignore', windowsHide: true, timeout: 5000 });
        } else {
          execSync(`pkill -f "${imageName}"`, { stdio: 'ignore' });
        }
        // Wait a beat for the process to die, then retry
        try { unlinkSync(active); } catch { /* still locked, will fail on copy */ }
      } catch { /* no orphan found */ }
    }
  }

  copyFileSync(binaryPath, active);
  return active;
}

function cleanShadowCopy(binaryPath) {
  const active = activePath(binaryPath);
  try { unlinkSync(active); } catch { /* already gone or locked */ }
}

// ─── Port Utilities ─────────────────────────────────────────────

function isPortFree(port) {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.once('error', () => resolve(false));
    srv.once('listening', () => { srv.close(); resolve(true); });
    srv.listen(port, '127.0.0.1');
  });
}

async function allocatePort(preferred) {
  // Clean expired reservations
  const now = Date.now();
  for (const [p, r] of portReservations) {
    if (now - new Date(r.reservedAt).getTime() > PORT_RESERVATION_TTL) {
      portReservations.delete(p);
    }
  }

  const usedPorts = new Set([...instances.values()]
    .filter(i => i.status !== 'stopped')
    .map(i => i.port));
  for (const p of portReservations.keys()) usedPorts.add(p);

  if (preferred && !usedPorts.has(preferred)) {
    if (await isPortFree(preferred)) return preferred;
  }

  for (let p = PORT_RANGE_START; p <= PORT_RANGE_END; p++) {
    if (p === PORT_E2E_RESERVED) continue;
    if (usedPorts.has(p)) continue;
    if (await isPortFree(p)) return p;
  }
  return null;
}

// ─── Log Management ─────────────────────────────────────────────

function pushLog(instanceId, level, message) {
  if (!logBuffers.has(instanceId)) {
    logBuffers.set(instanceId, { lines: [], startIndex: 0 });
  }
  const buf = logBuffers.get(instanceId);
  const entry = {
    index: globalLogIndex++,
    timestamp: new Date().toISOString(),
    level,
    message: message.trimEnd(),
  };
  buf.lines.push(entry);
  if (buf.lines.length > MAX_LOG_LINES) {
    buf.startIndex = buf.lines[buf.lines.length - MAX_LOG_LINES].index;
    buf.lines = buf.lines.slice(-MAX_LOG_LINES);
  }
  // Broadcast to SSE clients
  if (sseClients.size > 0) sseBroadcast('log', { instanceId, ...entry });
  return entry;
}

function getLogs(instanceId, { since = 0, before = 0, tail = 100 } = {}) {
  const buf = logBuffers.get(instanceId);
  if (!buf) return [];
  let lines = buf.lines;
  if (before > 0) lines = lines.filter(l => l.index < before);
  else if (since > 0) lines = lines.filter(l => l.index > since);
  if (tail > 0) lines = lines.slice(-tail);
  return lines;
}

// ─── Instance Management ────────────────────────────────────────

function findBinary(worktree) {
  const wt = worktree || PROJECT_ROOT;
  // Try fast profile first (dev build), then release
  const candidates = [
    resolve(wt, 'target', 'fast', `bitdex-server${EXE}`),
    resolve(wt, 'target', 'release', `bitdex-server${EXE}`),
  ];
  for (const c of candidates) {
    if (existsSync(c)) return c;
  }
  return null;
}

function nextInstanceId(port, name) {
  return name || `srv-${port}`;
}

async function waitForHealthAndUpdate(instance, port, dataDir, id) {
  const ready = await waitForHealth(`http://127.0.0.1:${port}/api/health`, 60_000);
  if (ready) {
    instance.status = 'running';
    pushLog(id, 'daemon', 'Server is ready');
    portReservations.delete(port);

    // Query for index stats
    try {
      const res = await fetch(`http://127.0.0.1:${port}/api/indexes`);
      if (res.ok) {
        const body = await res.json();
        const indexes = body.indexes || body;
        if (indexes && indexes.length > 0) {
          const idx = indexes[0];
          instance.dataset = idx.name || 'unknown';
          instance.recordCount = idx.alive_count || idx.record_count || null;
          registerDataset(dataDir, {
            indexName: instance.dataset,
            recordCount: instance.recordCount,
            ownedBy: id,
            skipDiskSize: true,
          });
        }
      }
    } catch { /* stats optional */ }
  } else if (instance.status !== 'stopped' && instance.status !== 'error') {
    instance.status = 'error';
    pushLog(id, 'daemon', 'Server did not become ready within 60s');
  }
}

async function startInstance({ port, dataDir, worktree, binary, name } = {}) {
  const wt = worktree || PROJECT_ROOT;
  const resolvedPort = await allocatePort(port);
  if (!resolvedPort) return { error: 'No free ports in range 3001-3099' };

  const resolvedBinary = binary || findBinary(wt);
  if (!resolvedBinary) {
    return { error: `No bitdex-server binary found. Run 'just build-server' first. Searched in ${wt}/target/{fast,release}/` };
  }

  const resolvedDataDir = dataDir
    ? resolve(wt, dataDir)
    : resolve(PROJECT_ROOT, 'data');

  mkdirSync(resolvedDataDir, { recursive: true });

  const id = nextInstanceId(resolvedPort, name);

  // Check for existing instance on this port
  if (instances.has(id) && instances.get(id).status !== 'stopped') {
    return { error: `Instance ${id} already running on port ${resolvedPort}` };
  }

  // Reserve the port
  portReservations.set(resolvedPort, { reservedAt: new Date().toISOString(), reservedBy: wt });

  // Shadow copy: run from .active so cargo builds don't lock the original
  let runBinary;
  try {
    runBinary = createShadowCopy(resolvedBinary);
    pushLog(id, 'daemon', `Shadow copy: ${basename(resolvedBinary)} → ${basename(runBinary)}`);
  } catch (err) {
    pushLog(id, 'daemon', `Shadow copy failed (${err.message}), running original`);
    runBinary = resolvedBinary;
  }

  const proc = spawn(runBinary, [
    '--port', String(resolvedPort),
    '--data-dir', resolvedDataDir,
    '--log-level', 'debug',
    '--enable-traces',
  ], {
    cwd: wt,
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  });

  const instance = {
    id,
    pid: proc.pid,
    port: resolvedPort,
    dataDir: resolvedDataDir,
    worktree: wt,
    status: 'starting',
    dataset: null,
    recordCount: null,
    startedAt: new Date().toISOString(),
    binary: resolvedBinary,
    activeBinary: runBinary,
    process: proc,
  };

  instances.set(id, instance);

  // Wire stdout/stderr to in-memory log ring buffer (with line buffering)
  let stdoutBuf = '';
  proc.stdout.on('data', (data) => {
    stdoutBuf += data.toString();
    const lines = stdoutBuf.split('\n');
    stdoutBuf = lines.pop(); // keep incomplete last line
    for (const line of lines) {
      if (line) pushLog(id, 'stdout', line);
    }
  });

  let stderrBuf = '';
  proc.stderr.on('data', (data) => {
    stderrBuf += data.toString();
    const lines = stderrBuf.split('\n');
    stderrBuf = lines.pop();
    for (const line of lines) {
      if (line) pushLog(id, 'stderr', line);
    }
  });

  proc.on('error', (err) => {
    pushLog(id, 'daemon', `Process error: ${err.message}`);
    instance.status = 'error';
    instance.process = null;
  });

  proc.on('exit', (code, signal) => {
    pushLog(id, 'daemon', `Exited (code=${code}, signal=${signal})`);
    instance.status = 'stopped';
    instance.exitCode = code;
    instance.process = null;
    portReservations.delete(resolvedPort);
    // Clean up shadow copy
    if (instance.activeBinary && instance.activeBinary !== instance.binary) {
      cleanShadowCopy(instance.binary);
    }
  });



  // Wait for health async — don't block the HTTP response
  waitForHealthAndUpdate(instance, resolvedPort, resolvedDataDir, id);

  return { instance: serializeInstance(instance) };
}

async function stopInstance(idOrPort) {
  const instance = findInstance(idOrPort);
  if (!instance) return { error: `Instance '${idOrPort}' not found` };

  if (instance.process) {
    killProcess(instance.pid);
    instance.process = null;
  } else if (instance.pid && isPidAlive(instance.pid)) {
    killProcess(instance.pid);
  }

  instance.status = 'stopped';
  portReservations.delete(instance.port);

  // Clean up shadow copy
  if (instance.activeBinary && instance.activeBinary !== instance.binary) {
    // Small delay to let the process fully release the file
    setTimeout(() => cleanShadowCopy(instance.binary), 1000);
  }

  // Keep dataset entry — data dir is preserved
  if (datasets.has(instance.dataDir)) {
    const ds = datasets.get(instance.dataDir);
    ds.ownedBy = null;
    ds.lastUsed = new Date().toISOString();
  
  }


  return { stopped: instance.id };
}

function findInstance(idOrPort) {
  if (instances.has(idOrPort)) return instances.get(idOrPort);
  const port = parseInt(idOrPort, 10);
  if (!isNaN(port)) {
    for (const inst of instances.values()) {
      if (inst.port === port) return inst;
    }
  }
  return null;
}

function serializeInstance(i) {
  const { process, activeBinary, ...rest } = i;
  return rest;  // stats included — TUI needs them
}

// ─── Dataset Management ─────────────────────────────────────────

function registerDataset(dataDir, { indexName, recordCount, loadedFrom, ownedBy, skipDiskSize } = {}) {
  const absPath = resolve(dataDir);
  const existing = datasets.get(absPath) || {};
  datasets.set(absPath, {
    path: absPath,
    indexName: indexName || existing.indexName || null,
    recordCount: recordCount || existing.recordCount || null,
    diskBytes: skipDiskSize ? (existing.diskBytes || null) : getDirSize(absPath),
    loadedFrom: loadedFrom || existing.loadedFrom || null,
    lastUsed: new Date().toISOString(),
    ownedBy: ownedBy || existing.ownedBy || null,
  });

}

function getDirSize(dir) {
  try {
    if (!existsSync(dir)) return 0;
    let total = 0;
    const walk = (d) => {
      for (const entry of readdirSync(d, { withFileTypes: true })) {
        const full = resolve(d, entry.name);
        if (entry.isDirectory()) walk(full);
        else total += statSync(full).size;
      }
    };
    walk(dir);
    return total;
  } catch {
    return 0;
  }
}

function scanForDatasets() {
  const patterns = ['data', '.test-data'];
  for (const pat of patterns) {
    const dir = resolve(PROJECT_ROOT, pat);
    if (!existsSync(dir)) continue;
    // Check if this dir itself has indexes
    const indexDir = resolve(dir, 'indexes');
    if (existsSync(indexDir)) {
      registerDataset(dir, { skipDiskSize: true });
    }
    // Check subdirs
    try {
      for (const entry of readdirSync(dir, { withFileTypes: true })) {
        if (entry.isDirectory()) {
          const sub = resolve(dir, entry.name);
          const subIdx = resolve(sub, 'indexes');
          if (existsSync(subIdx)) {
            registerDataset(sub, { skipDiskSize: true });
          }
        }
      }
    } catch { /* ok */ }
  }
}

// ─── Build Pipeline ─────────────────────────────────────────────

// Build lock now includes a log buffer + process, so all agents can watch build output.
// POST /build/run {target, profile, holder} — starts build, returns immediately
// GET /build/status — lock state + exit code
// GET /build/logs?since=N — incremental log polling

function pushBuildLog(level, message) {
  if (!buildLock.logBuffer) return;
  const entry = {
    index: globalLogIndex++,
    timestamp: new Date().toISOString(),
    level,
    message: message.trimEnd(),
  };
  buildLock.logBuffer.push(entry);
  if (buildLock.logBuffer.length > MAX_LOG_LINES) {
    buildLock.logBuffer = buildLock.logBuffer.slice(-MAX_LOG_LINES);
  }
}

function getBuildLogs({ since = 0, tail = 200 } = {}) {
  if (!buildLock.logBuffer) return { logs: [], total: 0 };
  let entries = buildLock.logBuffer;
  if (since > 0) entries = entries.filter(e => e.index > since);
  if (tail > 0) entries = entries.slice(-tail);
  return { logs: entries, total: buildLock.logBuffer.length };
}

function getBuildStatus() {
  if (!buildLock.holder) return { locked: false, exitCode: buildLock.exitCode ?? null };
  return {
    locked: true,
    holder: buildLock.holder,
    target: buildLock.target,
    startedAt: buildLock.startedAt,
    exitCode: buildLock.exitCode ?? null,
    elapsed_s: Math.round((Date.now() - new Date(buildLock.startedAt).getTime()) / 1000),
  };
}

function buildCargoArgs(target, profile) {
  switch (target) {
    case 'bitdex-server':
      return ['build', '--profile', profile || 'fast', '--features', 'server', '--bin', 'bitdex-server'];
    case 'bitdex-loadtest':
      return ['build', '--profile', profile || 'fast', '--features', 'loadtest', '--bin', 'bitdex-loadtest'];
    case 'bitdex-benchmark':
      return ['build', '--release', '--bin', 'bitdex-benchmark'];
    default:
      return ['build', '--profile', profile || 'fast'];
  }
}

async function restartInstancesAfterBuild(target) {
  // Only restart for server builds
  if (target !== 'bitdex-server') return;

  for (const [id, inst] of instances) {
    if (inst.status !== 'running' && inst.status !== 'starting') continue;

    pushBuildLog('daemon', `Restarting ${id} with new binary (shadow copy swap)...`);
    pushLog(id, 'daemon', 'Restarting with new binary after build...');

    // Remember the instance config
    const { port, dataDir, worktree, binary } = inst;

    // Stop the old instance
    killProcess(inst.pid);
    inst.process = null;
    inst.status = 'stopped';

    // Wait a moment for the process to fully exit and release the .active file
    await new Promise(r => setTimeout(r, 1500));
    cleanShadowCopy(binary);

    // Start fresh with the same config
    const result = await startInstance({ port, dataDir, worktree, binary });
    if (result.error) {
      pushBuildLog('daemon', `Failed to restart ${id}: ${result.error}`);
    } else {
      pushBuildLog('daemon', `Restarted ${id} on port ${port}`);
    }
  }
}

async function requestBuild({ target, profile, holder }) {
  const tgt = target || 'bitdex-server';

  // If a build is already running, kill it and restart
  if (buildLock.holder && buildLock.process) {
    pushBuildLog('daemon', `Build interrupted by ${holder || 'unknown'} — restarting`);
    killProcess(buildLock.process.pid);
    buildLock.process = null;
    // Fall through to start new build
  } else if (buildLock.holder) {
    // Lock held but no process — stale, release
    releaseBuildLock();
  }

  buildLock = {
    holder: holder || 'unknown',
    target: tgt,
    startedAt: new Date().toISOString(),
    pid: null,
    targetDir: null,
    logBuffer: buildLock.logBuffer || [], // preserve logs across restarts
    process: null,
    exitCode: null,
  };

  // Clear logs for fresh build
  buildLock.logBuffer = [];


  const cargoArgs = buildCargoArgs(tgt, profile);
  pushBuildLog('daemon', `Building: cargo ${cargoArgs.join(' ')}`);

  const proc = spawn('cargo', cargoArgs, {
    cwd: PROJECT_ROOT,
    shell: IS_WIN,
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env },
  });

  buildLock.process = proc;
  buildLock.pid = proc.pid;

  proc.stdout.on('data', (chunk) => {
    for (const line of chunk.toString().split(/\r?\n/).filter(Boolean)) {
      pushBuildLog('stdout', line);
    }
  });

  proc.stderr.on('data', (chunk) => {
    for (const line of chunk.toString().split(/\r?\n/).filter(Boolean)) {
      pushBuildLog('stderr', line);
    }
  });

  proc.on('exit', (code) => {
    buildLock.exitCode = code;
    buildLock.process = null;
    pushBuildLog('daemon', code === 0 ? 'Build succeeded' : `Build failed (exit ${code})`);

    // Auto-restart running instances that use the built binary
    if (code === 0) {
      restartInstancesAfterBuild(tgt);
    }

    // Auto-release lock after build completes (keep logs for polling)
    const savedLogs = buildLock.logBuffer;
    const savedExit = buildLock.exitCode;
    buildLock = { holder: null, target: null, startedAt: null, pid: null, targetDir: null, logBuffer: savedLogs, exitCode: savedExit, process: null };
  
  });

  proc.on('error', (err) => {
    pushBuildLog('daemon', `Build process error: ${err.message}`);
    buildLock.exitCode = 1;
    buildLock.process = null;
  });

  return { action: 'building', target: tgt };
}

function releaseBuildLock() {
  const savedLogs = buildLock.logBuffer;
  buildLock = { holder: null, target: null, startedAt: null, pid: null, targetDir: null, logBuffer: savedLogs || [], exitCode: null, process: null };

  return { released: true };
}

function acquireE2eLock({ holder, pid }) {
  if (e2eLock.holder) {
    if (!isPidAlive(e2eLock.pid)) {
      pushLog('daemon', 'daemon', `E2E lock auto-released (holder PID ${e2eLock.pid} dead)`);
      releaseE2eLock();
    } else if (Date.now() - new Date(e2eLock.startedAt).getTime() > E2E_LOCK_TIMEOUT) {
      pushLog('daemon', 'daemon', `E2E lock auto-released (timeout after ${E2E_LOCK_TIMEOUT / 1000}s)`);
      releaseE2eLock();
    } else {
      return {
        granted: false,
        holder: e2eLock.holder,
        startedAt: e2eLock.startedAt,
        elapsed_s: Math.round((Date.now() - new Date(e2eLock.startedAt).getTime()) / 1000),
      };
    }
  }

  e2eLock = {
    holder: holder || 'unknown',
    startedAt: new Date().toISOString(),
    pid: pid || null,
  };

  return { granted: true };
}

function releaseE2eLock() {
  e2eLock = { holder: null, startedAt: null, pid: null };

  return { released: true };
}

// ─── Test Slots ─────────────────────────────────────────────────

const MAX_TEST_LOG_LINES = 500;

function getTestSlots() {
  return testSlots.map(s => {
    const { process, ...rest } = s;
    return {
      ...rest,
      alive: !!(process && s.pid && isPidAlive(s.pid)),
      elapsed_s: s.startedAt ? Math.round((Date.now() - new Date(s.startedAt).getTime()) / 1000) : null,
    };
  });
}

function acquireTestSlot({ holder, slot }) {
  let target;
  if (slot != null) {
    const idx = parseInt(slot, 10) - 1;
    if (idx < 0 || idx >= testSlots.length) return { error: `Invalid slot ${slot} (valid: 1-${TEST_SLOT_COUNT})` };
    target = testSlots[idx];
    if (target.holder) return { error: `Slot ${slot} held by '${target.holder}'`, slots: getTestSlots() };
  } else {
    target = testSlots.find(s => !s.holder);
    if (!target) return { error: 'No free test slots', slots: getTestSlots() };
  }

  target.holder = holder || 'unknown';
  target.startedAt = new Date().toISOString();
  target.exitCode = null;
  target.command = null;
  target.logBuffer = [];
  target.pid = null;
  target.process = null;

  sseBroadcast('testSlots', getTestSlots());
  return { slot: target.id, targetDir: target.targetDir };
}

function runTestInSlot(slotId, { command, filter, holder }) {
  const idx = parseInt(slotId, 10) - 1;
  if (idx < 0 || idx >= testSlots.length) return { error: `Invalid slot ${slotId}` };
  const slot = testSlots[idx];

  const filterArgs = filter ? ['--', filter] : [];
  const args = ['test', ...filterArgs];
  const cmdStr = `cargo test${filter ? ' -- ' + filter : ''}`;

  mkdirSync(slot.targetDir, { recursive: true });

  const proc = spawn('cargo', args, {
    cwd: PROJECT_ROOT,
    shell: IS_WIN,
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, CARGO_TARGET_DIR: slot.targetDir },
  });

  slot.process = proc;
  slot.pid = proc.pid;
  slot.command = cmdStr;
  slot.exitCode = null;

  let stdoutBuf = '';
  proc.stdout.on('data', (data) => {
    stdoutBuf += data.toString();
    const lines = stdoutBuf.split('\n');
    stdoutBuf = lines.pop();
    for (const line of lines) {
      if (line) {
        slot.logBuffer.push({
          index: globalLogIndex++,
          timestamp: new Date().toISOString(),
          level: 'stdout',
          message: line.trimEnd(),
        });
        if (slot.logBuffer.length > MAX_TEST_LOG_LINES) {
          slot.logBuffer = slot.logBuffer.slice(-MAX_TEST_LOG_LINES);
        }
      }
    }
  });

  let stderrBuf = '';
  proc.stderr.on('data', (data) => {
    stderrBuf += data.toString();
    const lines = stderrBuf.split('\n');
    stderrBuf = lines.pop();
    for (const line of lines) {
      if (line) {
        slot.logBuffer.push({
          index: globalLogIndex++,
          timestamp: new Date().toISOString(),
          level: 'stderr',
          message: line.trimEnd(),
        });
        if (slot.logBuffer.length > MAX_TEST_LOG_LINES) {
          slot.logBuffer = slot.logBuffer.slice(-MAX_TEST_LOG_LINES);
        }
      }
    }
  });

  proc.on('error', (err) => {
    slot.logBuffer.push({
      index: globalLogIndex++,
      timestamp: new Date().toISOString(),
      level: 'daemon',
      message: `Process error: ${err.message}`,
    });
    slot.exitCode = 1;
    slot.process = null;
    slot.holder = null;
    sseBroadcast('testSlots', getTestSlots());
  });

  proc.on('exit', (code, signal) => {
    slot.exitCode = code;
    slot.process = null;
    slot.holder = null;
    slot.logBuffer.push({
      index: globalLogIndex++,
      timestamp: new Date().toISOString(),
      level: 'daemon',
      message: `Exited (code=${code}, signal=${signal})`,
    });
    sseBroadcast('testSlots', getTestSlots());
  });

  sseBroadcast('testSlots', getTestSlots());
  return { slot: slot.id, targetDir: slot.targetDir, command: cmdStr, pid: proc.pid };
}

function releaseTestSlot(slotId) {
  const idx = parseInt(slotId, 10) - 1;
  if (idx < 0 || idx >= testSlots.length) return { error: `Invalid slot ${slotId}` };
  const slot = testSlots[idx];

  if (slot.process && slot.pid) {
    killProcess(slot.pid);
    slot.process = null;
  }

  slot.holder = null;
  slot.command = null;
  slot.startedAt = null;
  slot.pid = null;
  slot.exitCode = null;

  sseBroadcast('testSlots', getTestSlots());
  return { released: true, slot: slot.id };
}

function getTestSlotLogs(slotId, { since = 0, tail = 200 } = {}) {
  const idx = parseInt(slotId, 10) - 1;
  if (idx < 0 || idx >= testSlots.length) return { error: `Invalid slot ${slotId}` };
  const slot = testSlots[idx];

  let entries = slot.logBuffer;
  if (since > 0) entries = entries.filter(e => e.index > since);
  if (tail > 0) entries = entries.slice(-tail);
  return { logs: entries, total: slot.logBuffer.length, exitCode: slot.exitCode, holder: slot.holder };
}

// ─── Instance Stats Polling ─────────────────────────────────────

async function pollInstanceStats(instance) {
  if (instance.status !== 'running' || !instance.dataset) return;
  try {
    const res = await fetch(`http://127.0.0.1:${instance.port}/api/indexes/${instance.dataset}/stats`, { signal: AbortSignal.timeout(5000) });
    if (res.ok) {
      const stats = await res.json();
      instance.stats = {
        alive_count: stats.alive_count,
        slot_count: stats.slot_count,
        filter_bitmap_mb: (stats.filter_bitmap_bytes / (1024 * 1024)).toFixed(1),
        sort_bitmap_mb: (stats.sort_bitmap_bytes / (1024 * 1024)).toFixed(1),
        slot_bitmap_mb: (stats.slot_bitmap_bytes / (1024 * 1024)).toFixed(1),
        total_bitmap_mb: ((stats.filter_bitmap_bytes + stats.sort_bitmap_bytes + stats.slot_bitmap_bytes) / (1024 * 1024)).toFixed(1),
        cache_bytes: stats.unified_cache_bytes,
        cache_entries: stats.unified_cache_entries,
        cache_hits: stats.unified_cache_hits,
        cache_misses: stats.unified_cache_misses,
        cache_hit_rate: stats.unified_cache_hits + stats.unified_cache_misses > 0
          ? ((stats.unified_cache_hits / (stats.unified_cache_hits + stats.unified_cache_misses)) * 100).toFixed(1) + '%'
          : '—',
        cache_details: stats.unified_cache_entry_details || [],
        flush_cycle: stats.flush_cycle,
      };
    }
  } catch { /* optional */ }
}

// ─── Health Check ───────────────────────────────────────────────

async function waitForHealth(url, timeoutMs) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(url);
      if (res.ok || res.status < 500) return true;
    } catch { /* not ready yet */ }
    await new Promise(r => setTimeout(r, 500));
  }
  return false;
}

// ─── Heartbeat ──────────────────────────────────────────────────

function heartbeat() {
  // Poll stats for running instances
  for (const [id, inst] of instances) {
    if (inst.status === 'running') pollInstanceStats(inst);
  }

  // Check instance liveness
  for (const [id, inst] of instances) {
    if (inst.status === 'stopped') continue;
    if (!isPidAlive(inst.pid)) {
      if (inst.status !== 'stopped') {
        pushLog(id, 'daemon', 'Process died (detected by heartbeat)');
        inst.status = 'stopped';
        inst.process = null;
        portReservations.delete(inst.port);
        if (datasets.has(inst.dataDir)) {
          datasets.get(inst.dataDir).ownedBy = null;
        
        }
      
      }
    }
  }

  // Check build lock liveness
  if (buildLock.holder && buildLock.process) {
    if (!isPidAlive(buildLock.process.pid)) {
      pushBuildLog('daemon', 'Build process died (detected by heartbeat)');
      buildLock.exitCode = 1;
      buildLock.process = null;
      releaseBuildLock();
    } else if (Date.now() - new Date(buildLock.startedAt).getTime() > BUILD_LOCK_TIMEOUT) {
      pushBuildLog('daemon', 'Build auto-killed (timeout)');
      killProcess(buildLock.process.pid);
      buildLock.exitCode = 1;
      buildLock.process = null;
      releaseBuildLock();
    }
  }

  // Check E2E lock liveness
  if (e2eLock.holder) {
    if (!isPidAlive(e2eLock.pid)) {
      pushLog('daemon', 'daemon', `E2E lock auto-released (holder PID ${e2eLock.pid} dead)`);
      releaseE2eLock();
    } else if (Date.now() - new Date(e2eLock.startedAt).getTime() > E2E_LOCK_TIMEOUT) {
      pushLog('daemon', 'daemon', 'E2E lock auto-released (timeout)');
      releaseE2eLock();
    }
  }

  // Check test slot liveness and timeouts
  for (const slot of testSlots) {
    if (!slot.holder) continue;
    if (slot.process && slot.pid) {
      if (!isPidAlive(slot.pid)) {
        slot.logBuffer.push({
          index: globalLogIndex++,
          timestamp: new Date().toISOString(),
          level: 'daemon',
          message: 'Test process died (detected by heartbeat)',
        });
        slot.exitCode = 1;
        slot.process = null;
        slot.holder = null;
        sseBroadcast('testSlots', getTestSlots());
      } else if (Date.now() - new Date(slot.startedAt).getTime() > TEST_SLOT_TIMEOUT) {
        slot.logBuffer.push({
          index: globalLogIndex++,
          timestamp: new Date().toISOString(),
          level: 'daemon',
          message: 'Test auto-killed (timeout)',
        });
        killProcess(slot.pid);
        slot.exitCode = 1;
        slot.process = null;
        slot.holder = null;
        sseBroadcast('testSlots', getTestSlots());
      }
    } else {
      // Holder set but no process — stale, release
      slot.holder = null;
      sseBroadcast('testSlots', getTestSlots());
    }
  }

  // Clean expired port reservations
  const now = Date.now();
  for (const [p, r] of portReservations) {
    if (now - new Date(r.reservedAt).getTime() > PORT_RESERVATION_TTL) {
      portReservations.delete(p);
    }
  }

  // Broadcast status to SSE clients
  if (sseClients.size > 0) sseBroadcast('status', getStatus());
}

// ─── Force Kill ─────────────────────────────────────────────────

function forceKillByName() {
  // Kill ALL bitdex-server processes on the system — managed or orphaned
  const names = ['bitdex-server.exe', 'bitdex-server.active.exe'];
  const killed = [];
  for (const name of names) {
    try {
      if (IS_WIN) {
        execSync(`taskkill /IM "${name}" /F`, { stdio: 'ignore', windowsHide: true, timeout: 5000 });
      } else {
        execSync(`pkill -f "${name}"`, { stdio: 'ignore' });
      }
      killed.push(name);
    } catch { /* no matching processes */ }
  }

  // Mark all tracked instances as stopped
  for (const [id, inst] of instances) {
    if (inst.status !== 'stopped') {
      inst.status = 'stopped';
      inst.process = null;
      pushLog(id, 'daemon', 'Force-killed');
    }
  }


  return { killed, message: `Force-killed all bitdex-server processes (${killed.join(', ') || 'none found'})` };
}

// ─── Status ─────────────────────────────────────────────────────

function getStatus() {
  return {
    daemon: {
      pid: process.pid,
      port: DAEMON_PORT,
      startedAt: daemonStartedAt,
      uptime_s: Math.round((Date.now() - new Date(daemonStartedAt).getTime()) / 1000),
    },
    instances: [...instances.values()].map(serializeInstance),
    datasets: [...datasets.values()],
    build: getBuildStatus(),
    e2e: e2eLock.holder
      ? { locked: true, ...e2eLock, elapsed_s: Math.round((Date.now() - new Date(e2eLock.startedAt).getTime()) / 1000) }
      : { locked: false },
    testSlots: getTestSlots(),
  };
}

// ─── HTTP Server ────────────────────────────────────────────────

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      if (!body) return resolve({});
      try { resolve(JSON.parse(body)); }
      catch { reject(new Error('Invalid JSON')); }
    });
  });
}

function parseQuery(url) {
  const q = {};
  const idx = url.indexOf('?');
  if (idx === -1) return q;
  const params = new URLSearchParams(url.slice(idx));
  for (const [k, v] of params) q[k] = v;
  return q;
}

function json(res, statusCode, data) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

async function handleRequest(req, res) {
  const url = req.url;
  const method = req.method;
  const path = url.split('?')[0];
  const query = parseQuery(url);

  try {
    // GET /health
    if (method === 'GET' && path === '/health') {
      return json(res, 200, { ok: true, pid: process.pid });
    }

    // GET /events — Server-Sent Events stream
    if (method === 'GET' && path === '/events') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      });
      // Send initial status immediately
      res.write(`event: status\ndata: ${JSON.stringify(getStatus())}\n\n`);
      sseClients.add(res);
      req.on('close', () => sseClients.delete(res));
      // Don't end the response — keep it open for streaming
      return;
    }

    // GET /status
    if (method === 'GET' && path === '/status') {
      return json(res, 200, getStatus());
    }

    // GET /instances
    if (method === 'GET' && path === '/instances') {
      return json(res, 200, [...instances.values()].map(serializeInstance));
    }

    // POST /instances — start a new instance
    if (method === 'POST' && path === '/instances') {
      const body = await parseBody(req);
      const result = await startInstance(body);
      return json(res, result.error ? 400 : 201, result);
    }

    // DELETE /instances/:id — stop an instance
    const deleteMatch = path.match(/^\/instances\/([^/]+)$/);
    if (method === 'DELETE' && deleteMatch) {
      const result = await stopInstance(deleteMatch[1]);
      return json(res, result.error ? 404 : 200, result);
    }

    // GET /instances/:id — single instance
    const getInstMatch = path.match(/^\/instances\/([^/]+)$/);
    if (method === 'GET' && getInstMatch) {
      const inst = findInstance(getInstMatch[1]);
      if (!inst) return json(res, 404, { error: `Instance '${getInstMatch[1]}' not found` });
      return json(res, 200, serializeInstance(inst));
    }

    // GET /instances/:id/logs
    const logMatch = path.match(/^\/instances\/([^/]+)\/logs$/);
    if (method === 'GET' && logMatch) {
      const logs = getLogs(logMatch[1], {
        since: parseInt(query.since || '0', 10),
        before: parseInt(query.before || '0', 10),
        tail: parseInt(query.tail || '100', 10),
      });
      return json(res, 200, { logs });
    }

    // GET /datasets
    if (method === 'GET' && path === '/datasets') {
      return json(res, 200, [...datasets.values()]);
    }

    // POST /datasets — register a data directory
    if (method === 'POST' && path === '/datasets') {
      const body = await parseBody(req);
      if (!body.path) return json(res, 400, { error: 'path required' });
      registerDataset(body.path, body);
      return json(res, 200, { registered: body.path });
    }

    // POST /ports/reserve
    if (method === 'POST' && path === '/ports/reserve') {
      const body = await parseBody(req);
      const port = await allocatePort(body.preferred);
      if (!port) return json(res, 503, { error: 'No free ports' });
      portReservations.set(port, {
        reservedAt: new Date().toISOString(),
        reservedBy: body.worktree || 'unknown',
      });
      return json(res, 200, { port });
    }

    // POST /build/run — start a build (daemon runs cargo, captures output)
    if (method === 'POST' && path === '/build/run') {
      const body = await parseBody(req);
      const result = await requestBuild(body);
      return json(res, 200, result);
    }

    // GET /build/status — build lock state + exit code
    if (method === 'GET' && path === '/build/status') {
      return json(res, 200, getBuildStatus());
    }

    // GET /build/logs — incremental build log polling
    if (method === 'GET' && path === '/build/logs') {
      const logs = getBuildLogs({
        since: parseInt(query.since || '0', 10),
        tail: parseInt(query.tail || '200', 10),
      });
      return json(res, 200, logs);
    }

    // Legacy: POST /build/acquire (still works for backward compat)
    if (method === 'POST' && path === '/build/acquire') {
      const body = await parseBody(req);
      // Redirect to new build pipeline
      if (body.target) {
        const result = await requestBuild(body);
        return json(res, 200, { granted: true, ...result });
      }
      return json(res, 200, { granted: !buildLock.holder, ...getBuildStatus() });
    }

    // POST /build/release
    if (method === 'POST' && path === '/build/release') {
      return json(res, 200, releaseBuildLock());
    }

    // GET /build
    if (method === 'GET' && path === '/build') {
      return json(res, 200, getBuildStatus());
    }

    // POST /e2e/acquire
    if (method === 'POST' && path === '/e2e/acquire') {
      const body = await parseBody(req);
      return json(res, 200, acquireE2eLock(body));
    }

    // POST /e2e/release
    if (method === 'POST' && path === '/e2e/release') {
      return json(res, 200, releaseE2eLock());
    }

    // GET /e2e
    if (method === 'GET' && path === '/e2e') {
      return json(res, 200, e2eLock.holder
        ? { locked: true, ...e2eLock, elapsed_s: Math.round((Date.now() - new Date(e2eLock.startedAt).getTime()) / 1000) }
        : { locked: false });
    }

    // GET /test/slots — list all test slots
    if (method === 'GET' && path === '/test/slots') {
      return json(res, 200, getTestSlots());
    }

    // POST /test/run — acquire slot + start cargo test
    if (method === 'POST' && path === '/test/run') {
      const body = await parseBody(req);
      const acq = acquireTestSlot({ holder: body.holder, slot: body.slot });
      if (acq.error) return json(res, 409, acq);
      const result = runTestInSlot(acq.slot, { command: body.command, filter: body.filter, holder: body.holder });
      if (result.error) return json(res, 500, result);
      return json(res, 200, result);
    }

    // GET /test/slots/:id/logs — get logs from a test slot
    const testLogMatch = path.match(/^\/test\/slots\/(\d+)\/logs$/);
    if (method === 'GET' && testLogMatch) {
      const result = getTestSlotLogs(testLogMatch[1], {
        since: parseInt(query.since || '0', 10),
        tail: parseInt(query.tail || '200', 10),
      });
      if (result.error) return json(res, 404, result);
      return json(res, 200, result);
    }

    // POST /test/slots/:id/release — release a test slot
    const testReleaseMatch = path.match(/^\/test\/slots\/(\d+)\/release$/);
    if (method === 'POST' && testReleaseMatch) {
      const result = releaseTestSlot(testReleaseMatch[1]);
      if (result.error) return json(res, 404, result);
      return json(res, 200, result);
    }

    // POST /force-kill — kill ALL bitdex-server processes by name (including orphans)
    if (method === 'POST' && path === '/force-kill') {
      return json(res, 200, forceKillByName());
    }

    // POST /stop-all
    if (method === 'POST' && path === '/stop-all') {
      const results = [];
      for (const [id, inst] of instances) {
        if (inst.status !== 'stopped') {
          results.push(await stopInstance(id));
        }
      }
      return json(res, 200, { stopped: results });
    }

    // POST /shutdown
    if (method === 'POST' && path === '/shutdown') {
      for (const [id, inst] of instances) {
        if (inst.status !== 'stopped') await stopInstance(id);
      }
      json(res, 200, { shutdown: true });
      setTimeout(() => process.exit(0), 500);
      return;
    }

    json(res, 404, { error: `Unknown route: ${method} ${path}` });
  } catch (err) {
    json(res, 500, { error: err.message });
  }
}

// ─── Startup ────────────────────────────────────────────────────

async function main() {
  // Scan for existing datasets on disk
  // Dataset scanning removed — was blocking event loop on large data dirs.
  // Datasets are discovered when instances start and report their index stats.

  const server = http.createServer(handleRequest);

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      // Another daemon already running — exit silently
      process.exit(0);
    }
    console.error('Daemon error:', err.message);
    process.exit(1);
  });

  server.listen(DAEMON_PORT, '127.0.0.1', () => {
    console.log(`BitDex dev-server daemon running on port ${DAEMON_PORT} (PID ${process.pid})`);
  });

  // Heartbeat
  setInterval(heartbeat, HEARTBEAT_INTERVAL);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
