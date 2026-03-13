#!/usr/bin/env node
/**
 * BitDex Dev-Server CLI
 *
 * Agent-facing CLI for coordinating BitDex development.
 * Talks to the daemon at http://localhost:9851.
 * Auto-starts the daemon if not running.
 *
 * Usage:
 *   node .claude/skills/dev-server/cli.mjs <command> [options]
 *
 * Commands:
 *   status                              Overview of instances, datasets, locks
 *   start [--port N] [--data-dir D]     Start a managed server instance
 *   stop <id|port>                      Stop an instance (preserves data)
 *   logs <id|port> [--tail N]           View instance logs
 *   datasets                            List known data directories
 *   reserve-port                        Reserve next available port
 *   build [--target T] [--profile P]    Acquire lock, build, release
 *   test-e2e [--port N]                 Acquire lock, run E2E, release
 *   dash                                TUI dashboard
 *   shutdown                            Kill all instances + stop daemon
 *   help                                Show this help
 */

import { spawn, execSync } from 'node:child_process';
import { resolve, dirname, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { existsSync, readFileSync } from 'node:fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const DAEMON_SCRIPT = resolve(__dirname, 'daemon.mjs');
const PID_FILE = resolve(__dirname, 'state', 'daemon.pid');
const PROJECT_ROOT = resolve(__dirname, '..', '..', '..');
const DAEMON_URL = 'http://127.0.0.1:9851';

// ─── Argument Parsing ───────────────────────────────────────────

function getArg(name, defaultVal) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return defaultVal;
  return process.argv[idx + 1];
}

function hasFlag(name) {
  return process.argv.includes(name);
}

const command = process.argv[2] || 'help';

// ─── Daemon Communication ───────────────────────────────────────

async function daemonFetch(path, { method = 'GET', body } = {}) {
  const opts = { method };
  if (body) {
    opts.headers = { 'Content-Type': 'application/json' };
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`${DAEMON_URL}${path}`, opts);
  return res.json();
}

async function isDaemonRunning() {
  try {
    const res = await fetch(`${DAEMON_URL}/health`, { signal: AbortSignal.timeout(2000) });
    return res.ok;
  } catch {
    return false;
  }
}

async function ensureDaemon() {
  if (await isDaemonRunning()) return true;

  // Check stale PID file
  if (existsSync(PID_FILE)) {
    try {
      const pid = JSON.parse(readFileSync(PID_FILE, 'utf8'));
      // PID file exists but daemon not responding — it's stale
    } catch { /* corrupt PID file */ }
  }

  // Start daemon — detached, no console window, no stdio
  // (matches ai-notifications pattern: process.execPath, no shell)
  const child = spawn(process.execPath, [DAEMON_SCRIPT], {
    detached: true,
    stdio: 'ignore',
    windowsHide: true,
    cwd: PROJECT_ROOT,
  });
  child.unref();

  // Poll for up to 5 seconds
  for (let i = 0; i < 25; i++) {
    await new Promise(r => setTimeout(r, 200));
    if (await isDaemonRunning()) return true;
  }

  console.error('Failed to start daemon');
  return false;
}

function getWorktree() {
  try {
    return execSync('git rev-parse --show-toplevel', {
      encoding: 'utf8',
      cwd: process.cwd(),
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
      timeout: 5000,
    }).trim().replace(/\\/g, '/');
  } catch {
    return process.cwd().replace(/\\/g, '/');
  }
}

// ─── Commands ───────────────────────────────────────────────────

async function cmdStatus() {
  const status = await daemonFetch('/status');
  console.log(JSON.stringify(status, null, 2));
}

function printCheatSheet() {
  const D = '\x1b[2m', R = '\x1b[0m', B = '\x1b[1m', C = '\x1b[36m';
  console.error('');
  console.error(`${B}Quick reference:${R}`);
  console.error(`  ${C}just dev${R}           Start server (or show status if running)`);
  console.error(`  ${C}just dev-dash${R}       Live TUI dashboard`);
  console.error(`  ${C}just dev-status${R}     Status overview (JSON)`);
  console.error(`  ${C}just dev-logs ID${R}    View instance logs`);
  console.error(`  ${C}just dev-stop ID${R}    Stop an instance`);
  console.error(`  ${C}just dev-build${R}      Coordinated cargo build`);
  console.error(`  ${C}just dev-test-e2e${R}   Run E2E tests (with lock)`);
  console.error(`  ${C}just dev-shutdown${R}   Kill everything`);
  console.error('');
}

function printInstanceStatus(instances) {
  const G = '\x1b[32m', Y = '\x1b[33m', R = '\x1b[0m', D = '\x1b[2m', B = '\x1b[1m';
  for (const inst of instances) {
    const sc = inst.status === 'running' ? G : inst.status === 'starting' ? Y : D;
    const icon = (inst.status === 'running' || inst.status === 'starting') ? '●' : '○';
    const records = inst.recordCount ? `${(inst.recordCount / 1e6).toFixed(1)}M records` : '';
    const dataRel = relative(PROJECT_ROOT, inst.dataDir).replace(/\\/g, '/');
    console.error(`  ${sc}${icon}${R} ${B}${inst.id}${R} — port ${inst.port}, ${sc}${inst.status}${R} ${records}`);
    console.error(`    ${D}data: ${dataRel}${R}`);
  }
}

// Default command: idempotent "ensure server is running"
async function cmdStart() {
  const forceNew = hasFlag('--new');
  const port = getArg('--port');
  const dataDir = getArg('--data-dir');
  const worktree = getWorktree();

  // Check if anything is already running (unless --new forces a new instance)
  if (!forceNew) {
    const status = await daemonFetch('/status');
    const running = status.instances.filter(i => i.status === 'running' || i.status === 'starting');

    if (running.length > 0) {
      console.error('BitDex server already running:');
      printInstanceStatus(running);
      printCheatSheet();
      // JSON output for agents
      console.log(JSON.stringify({ already_running: true, instances: running }));
      return;
    }
  }

  // Start a new instance
  const name = getArg('--name');
  const body = { worktree };
  if (port) body.port = parseInt(port, 10);
  if (name) body.name = name;
  body.dataDir = dataDir || resolve(PROJECT_ROOT, 'data');

  console.error('Starting BitDex server...');
  const result = await daemonFetch('/instances', { method: 'POST', body });

  if (result.error) {
    console.error(`Error: ${result.error}`);
    console.log(JSON.stringify(result));
    return;
  }

  console.error(`Started ${result.instance.id} on port ${result.instance.port} (status: ${result.instance.status})`);
  printInstanceStatus([result.instance]);
  printCheatSheet();
  console.log(JSON.stringify(result));
}

// Agent command: always starts a new instance (explicit port/data-dir)
async function cmdNew() {
  const port = getArg('--port');
  const dataDir = getArg('--data-dir');
  const worktree = getWorktree();

  const result = await daemonFetch('/instances', {
    method: 'POST',
    body: {
      port: port ? parseInt(port, 10) : undefined,
      dataDir: dataDir || undefined,
      worktree,
    },
  });

  console.log(JSON.stringify(result, null, 2));
}

async function resolveTarget(explicit) {
  if (explicit) return explicit;
  // Default to first running instance
  const status = await daemonFetch('/status');
  const running = status.instances.filter(i => i.status === 'running' || i.status === 'starting');
  if (running.length === 0) {
    console.error('No running instances. Start one with: just dev');
    process.exit(1);
  }
  return running[0].id;
}

async function cmdStop() {
  const target = await resolveTarget(process.argv[3]);
  const result = await daemonFetch(`/instances/${target}`, { method: 'DELETE' });
  console.log(JSON.stringify(result, null, 2));
}

async function cmdLogs() {
  const target = await resolveTarget(process.argv[3]);
  const tail = parseInt(getArg('--tail', '50'), 10);
  const since = parseInt(getArg('--since', '0'), 10);
  const result = await daemonFetch(`/instances/${target}/logs?tail=${tail}&since=${since}`);
  console.log(JSON.stringify(result, null, 2));
}

async function cmdDatasets() {
  const result = await daemonFetch('/datasets');
  console.log(JSON.stringify(result, null, 2));
}

async function cmdReservePort() {
  const preferred = getArg('--preferred');
  const result = await daemonFetch('/ports/reserve', {
    method: 'POST',
    body: {
      preferred: preferred ? parseInt(preferred, 10) : undefined,
      worktree: getWorktree(),
    },
  });
  console.log(JSON.stringify(result, null, 2));
}

const ERROR_PATTERNS = /\berror\b|\bpanic\b|\bfailed\b|\bFATAL\b/i;

async function cmdBuild() {
  const target = getArg('--target', 'bitdex-server');
  const profile = getArg('--profile', 'fast');
  const worktree = getWorktree();

  // Request build via daemon (it runs cargo and captures output)
  const result = await daemonFetch('/build/run', {
    method: 'POST',
    body: { target, profile, holder: worktree },
  });

  if (result.action === 'building') {
    console.error(`Building ${target}...`);
  } else if (result.error) {
    console.error(`Error: ${result.error}`);
    console.log(JSON.stringify(result));
    process.exit(1);
  }

  // Poll build logs until done
  let cursor = 0;
  while (true) {
    await new Promise(r => setTimeout(r, 1000));

    const bStatus = await daemonFetch('/build/status');
    const logs = await daemonFetch(`/build/logs?since=${cursor}`);

    for (const entry of logs.logs || []) {
      const ts = entry.timestamp.split('T')[1]?.slice(0, 8) || '';
      const lvl = ERROR_PATTERNS.test(entry.message) ? '\x1b[31mERR\x1b[0m' : '   ';
      // Build logs go to stderr so JSON output stays clean
      console.error(`${ts} ${lvl} ${entry.message}`);
      cursor = entry.index;
    }

    if (!bStatus.locked) {
      if (bStatus.exitCode === 0) {
        console.error('\x1b[32mBuild succeeded.\x1b[0m');
        console.log(JSON.stringify({ success: true, target }));
      } else {
        console.error(`\x1b[31mBuild failed (exit ${bStatus.exitCode}).\x1b[0m`);
        console.log(JSON.stringify({ success: false, target, exitCode: bStatus.exitCode }));
        process.exit(1);
      }
      break;
    }
  }
}

async function cmdTestE2e() {
  const port = getArg('--port', '3100');
  const worktree = getWorktree();

  // Acquire E2E lock
  console.error('Acquiring E2E test lock...');
  let attempts = 0;
  while (true) {
    const result = await daemonFetch('/e2e/acquire', {
      method: 'POST',
      body: { holder: worktree, pid: process.pid },
    });

    if (result.granted) {
      console.error('E2E lock acquired');
      break;
    }

    attempts++;
    if (attempts > 60) { // 5 minutes at 5s intervals
      console.error('Timed out waiting for E2E lock');
      console.log(JSON.stringify({ error: 'E2E lock timeout', holder: result.holder }));
      process.exit(1);
    }

    console.error(`E2E lock held by '${result.holder}' (${result.elapsed_s}s). Waiting...`);
    await new Promise(r => setTimeout(r, 5000));
  }

  const e2eScript = resolve(worktree, 'tests', 'e2e', 'run-e2e.mjs');

  console.error(`Running E2E tests on port ${port}...`);

  try {
    const proc = spawn('node', [e2eScript, '--port', port, '--skip-build'], {
      cwd: worktree,
      stdio: 'inherit',
      shell: process.platform === 'win32',
    });

    const exitCode = await new Promise((resolve) => {
      proc.on('exit', (code) => resolve(code || 0));
      proc.on('error', () => resolve(1));
    });

    console.log(JSON.stringify({ success: exitCode === 0, exitCode }));
  } finally {
    await daemonFetch('/e2e/release', { method: 'POST' });
    console.error('E2E lock released');
  }
}

async function cmdShutdown() {
  const result = await daemonFetch('/shutdown', { method: 'POST' });
  console.log(JSON.stringify(result, null, 2));
}

// ─── TUI Dashboard ──────────────────────────────────────────────

// ANSI sequences
const ALT_ON = '\x1b[?1049h';    // Enter alternate screen buffer
const ALT_OFF = '\x1b[?1049l';   // Exit alternate screen buffer
const CUR_HIDE = '\x1b[?25l';
const CUR_SHOW = '\x1b[?25h';
const HOME = '\x1b[H';            // Cursor to (0,0)
const CLR_LINE = '\x1b[2K';       // Clear entire line
const CLR_BELOW = '\x1b[J';       // Clear from cursor to end of screen

const DIM = '\x1b[2m';
const R = '\x1b[0m';
const B = '\x1b[1m';
const GRN = '\x1b[32m';
const RED = '\x1b[31m';
const YLW = '\x1b[33m';
const CYN = '\x1b[36m';

const write = (s) => process.stdout.write(s);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function fmtUp(startedAt) {
  if (!startedAt) return '—';
  const s = Math.round((Date.now() - new Date(startedAt).getTime()) / 1000);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m${s % 60}s`;
  return `${Math.floor(s / 3600)}h${Math.floor((s % 3600) / 60)}m`;
}

function fmtBytes(bytes) {
  if (!bytes || bytes === 0) return '—';
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}K`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}M`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)}G`;
}

function fmtCount(n) {
  if (!n) return '—';
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function sColor(status) {
  if (status === 'running') return GRN;
  if (status === 'starting' || status === 'loading') return YLW;
  if (status === 'error') return RED;
  return DIM;
}

function sIcon(status) {
  return (status === 'running' || status === 'starting' || status === 'loading') ? '●' : '○';
}

function pad(str, w) {
  const vis = str.replace(/\x1b\[[0-9;]*m/g, '');
  return str + ' '.repeat(Math.max(0, w - vis.length));
}

const ANSI_RE = /\x1b\[[0-9;]*[a-zA-Z]/g;
function stripAnsi(s) { return s.replace(ANSI_RE, ''); }

function trunc(str, n) {
  return str.length <= n ? str : str.slice(0, n - 1) + '…';
}

async function cmdDashboard() {
  if (!process.stdin.isTTY) {
    console.error('Dashboard requires a TTY');
    process.exit(1);
  }

  let cols = process.stdout.columns || 100;
  let rows = process.stdout.rows || 30;
  process.stdout.on('resize', () => {
    cols = process.stdout.columns || 100;
    rows = process.stdout.rows || 30;
  });

  process.stdin.setRawMode(true);
  process.stdin.resume();
  process.stdin.setEncoding('utf8');
  write(ALT_ON + CUR_HIDE);

  let selectedInstance = null;
  let logLines = [];
  let logCursor = -1;
  let lastLogTarget = null;
  let running = true;
  let actionMsg = '';
  let inputMode = null;  // null or { prompt, buffer, callback }

  function flash(msg) { actionMsg = msg; setTimeout(() => { actionMsg = ''; }, 4000); }

  function render(status) {
    const buf = [];
    buf.push(HOME);

    // ── Header
    const hdr = ` BitDex Dev Server ${DIM}│${R} up ${fmtUp(status.daemon.startedAt)} ${DIM}│${R} PID ${status.daemon.pid}`;
    buf.push(CLR_LINE + B + CYN + hdr + R + '\n');
    buf.push(CLR_LINE + DIM + '─'.repeat(cols) + R + '\n');

    // ── Instances (hide stopped ones unless they're the only ones)
    const liveInstances = status.instances.filter(i => i.status !== 'stopped');
    const showInstances = liveInstances.length > 0 ? liveInstances : status.instances.slice(-1);
    buf.push(CLR_LINE + B + ' Instances' + R + '\n');
    if (showInstances.length === 0) {
      buf.push(CLR_LINE + `  ${DIM}(none — press ${R}${B}n${R}${DIM} to start one)${R}` + '\n');
    } else {
      buf.push(CLR_LINE + `  ${DIM}${pad('ID', 14)}${pad('Port', 6)}${pad('Status', 10)}${pad('Index', 10)}${pad('Records', 10)}${pad('Data Dir', 24)}${pad('Up', 8)}${R}` + '\n');
      for (const inst of showInstances) {
        const sc = sColor(inst.status);
        const sel = selectedInstance === inst.id ? `${B}▸ ` : '  ';
        const dr = relative(PROJECT_ROOT, inst.dataDir).replace(/\\/g, '/') || inst.dataDir;
        const isUp = inst.status === 'running' || inst.status === 'starting';
        const idx = isUp ? (inst.dataset || DIM + '...' + R) : DIM + '—' + R;
        const uptime = isUp ? fmtUp(inst.startedAt) : '—';
        const records = isUp ? fmtCount(inst.recordCount) : '—';
        buf.push(CLR_LINE + `${sel}${sc}${sIcon(inst.status)}${R} ${pad(inst.id, 13)}${pad(String(inst.port), 6)}${sc}${pad(inst.status, 10)}${R}${pad(idx, 10)}${pad(records, 10)}${pad(trunc(dr, 22), 24)}${pad(uptime, 8)}` + '\n');
      }
    }
    buf.push(CLR_LINE + DIM + '─'.repeat(cols) + R + '\n');

    // ── Datasets + Locks
    const hw = Math.floor(cols / 2);
    buf.push(CLR_LINE + B + ' Datasets' + R + ' '.repeat(Math.max(0, hw - 10)) + B + 'Locks' + R + '\n');

    const dsL = status.datasets.length === 0
      ? [`  ${DIM}(none)${R}`]
      : status.datasets.map(ds => {
          const rel = relative(PROJECT_ROOT, ds.path).replace(/\\/g, '/') || ds.path;
          return `  ${pad(trunc(rel, hw - 20), hw - 18)}${pad(fmtCount(ds.recordCount), 8)}${pad(fmtBytes(ds.diskBytes), 8)}`;
        });

    const lkL = [];
    lkL.push(status.build.locked
      ? `  Build: ${YLW}HELD${R} by ${trunc(status.build.holder, 20)} (${status.build.elapsed_s}s)`
      : `  Build: ${GRN}FREE${R}`);
    lkL.push(status.e2e.locked
      ? `  E2E:   ${YLW}HELD${R} by ${trunc(status.e2e.holder, 20)} (${status.e2e.elapsed_s}s)`
      : `  E2E:   ${GRN}FREE${R}`);

    for (let i = 0; i < Math.max(dsL.length, lkL.length); i++) {
      const dl = dsL[i] || '';
      const ll = lkL[i] || '';
      const dv = dl.replace(/\x1b\[[0-9;]*m/g, '');
      buf.push(CLR_LINE + dl + ' '.repeat(Math.max(0, hw - dv.length)) + ll + '\n');
    }

    // ── Build status
    const bld = status.build;
    if (bld.locked) {
      buf.push(CLR_LINE + `  ${YLW}◈${R} ${YLW}building: ${bld.target}${R} ${DIM}(${bld.elapsed_s}s)${R}` + '\n');
    } else if (bld.exitCode !== null && bld.exitCode !== undefined) {
      const ec = bld.exitCode === 0 ? GRN : RED;
      buf.push(CLR_LINE + `  ${ec}◈${R} build: ${ec}exit ${bld.exitCode}${R}` + '\n');
    } else {
      buf.push(CLR_LINE + `  ${DIM}◈ build: idle${R}` + '\n');
    }

    // ── Log separator
    const logTarget = selectedInstance || (status.instances[0]?.id) || null;
    const logLabel = logTarget || '(no instance)';
    const sep = `── logs: ${logLabel} `;
    buf.push(CLR_LINE + DIM + sep + '─'.repeat(Math.max(0, cols - sep.length)) + R + '\n');

    // ── Log area (fixed row count, matches ai-notifications pattern)
    const HEADER_ROWS = 11;
    const FOOTER_ROWS = 2;
    const logAreaRows = Math.max(1, rows - HEADER_ROWS - FOOTER_ROWS);
    const visible = logLines.slice(-logAreaRows);

    for (let i = 0; i < logAreaRows; i++) {
      const entry = visible[i];
      if (entry) {
        const ts = new Date(entry.timestamp).toLocaleTimeString('en-US', { hour12: false });
        const rawMsg = stripAnsi(entry.message || '');
        const isErr = /\berror\b|\bpanic\b|\bfailed\b|\bFATAL\b/i.test(rawMsg);
        const lvl = isErr ? `${RED}ERR${R}` : '   ';
        const msgMax = cols - 16;
        const msg = rawMsg.length > msgMax ? rawMsg.slice(0, msgMax) : rawMsg;
        buf.push(CLR_LINE + `  ${DIM}${ts}${R} ${lvl} ${msg}` + '\n');
      } else {
        buf.push(CLR_LINE + '\n');
      }
    }

    // ── Footer
    buf.push(CLR_LINE + DIM + '─'.repeat(cols) + R + '\n');
    if (actionMsg) {
      buf.push(CLR_LINE + ` ${YLW}${actionMsg}${R}` + CLR_BELOW);
    } else {
      buf.push(CLR_LINE + `  ${DIM}1-9${R} select  ${B}b${R}${DIM}uild${R}  ${B}n${R}${DIM}ew${R}  ${B}s${R}${DIM}top${R}  ${B}r${R}${DIM}estart${R}  ${B}k${R}${DIM}ill all${R}  ${B}q${R}${DIM}uit${R}` + CLR_BELOW);
    }

    write(buf.join(''));
  }

  // Keyboard handler
  process.stdin.on('data', async (key) => {
    if (!running) return;

    // Input mode: collecting a text value (e.g. data-dir path)
    if (inputMode) {
      if (key === '\x1b' || key === '\x03') {
        // ESC or Ctrl+C: cancel input
        inputMode = null;
        actionMsg = '';
        return;
      }
      if (key === '\r' || key === '\n') {
        // Enter: submit
        const cb = inputMode.callback;
        const val = inputMode.buffer.trim();
        inputMode = null;
        actionMsg = '';
        await cb(val);
        return;
      }
      if (key === '\x7f' || key === '\b') {
        // Backspace
        inputMode.buffer = inputMode.buffer.slice(0, -1);
      } else if (key.length >= 1 && key.charCodeAt(0) >= 32) {
        // Printable chars (including pasted paths)
        inputMode.buffer += key;
      }
      actionMsg = `${inputMode.prompt}${inputMode.buffer}█`;
      return;
    }

    // Ignore multi-byte escape sequences (arrow keys etc.)
    if (key.length > 1 && key[0] === '\x1b') return;

    if (key === 'q' || key === '\x03') {
      running = false;
      write(CUR_SHOW + ALT_OFF);
      process.stdin.setRawMode(false);
      process.exit(0);
    }

    if (key === 's' && selectedInstance) {
      flash(`Stopping ${selectedInstance}...`);
      await daemonFetch(`/instances/${selectedInstance}`, { method: 'DELETE' });
      selectedInstance = null;
    }

    if (key === 'k') {
      flash('Stopping all...');
      await daemonFetch('/stop-all', { method: 'POST' });
      selectedInstance = null;
    }

    if (key === 'b') {
      flash('Building bitdex-server...');
      try {
        await daemonFetch('/build/run', { method: 'POST', body: { target: 'bitdex-server', profile: 'fast', holder: 'dashboard' } });
      } catch (e) { flash(`Build error: ${e.message}`); }
    }

    if (key === 'r' && selectedInstance) {
      flash(`Restarting ${selectedInstance}...`);
      const inst = (await daemonFetch('/status')).instances.find(i => i.id === selectedInstance);
      if (inst) {
        await daemonFetch(`/instances/${selectedInstance}`, { method: 'DELETE' });
        await sleep(1500);
        await daemonFetch('/instances', { method: 'POST', body: { port: inst.port, dataDir: inst.dataDir, worktree: inst.worktree } });
        flash(`Restarted ${selectedInstance}`);
      }
    }

    if (key === 'n') {
      inputMode = {
        prompt: 'New instance data-dir (Enter=./data): ',
        buffer: '',
        callback: async (dataDir) => {
          const dir = dataDir || 'data';
          flash(`Starting instance on ${dir}...`);
          try {
            const result = await daemonFetch('/instances', {
              method: 'POST',
              body: { dataDir: resolve(PROJECT_ROOT, dir), worktree: getWorktree() },
            });
            if (result.error) flash(`Error: ${result.error}`);
            else flash(`Started ${result.instance.id} on port ${result.instance.port}`);
          } catch (e) { flash(`Error: ${e.message}`); }
        },
      };
      actionMsg = `${inputMode.prompt}█`;
      return;
    }

    const num = parseInt(key, 10);
    if (num >= 1 && num <= 9) {
      try {
        const st = await daemonFetch('/status');
        if (st.instances[num - 1]) {
          selectedInstance = st.instances[num - 1].id;
          logLines = []; logCursor = -1; lastLogTarget = null;
        }
      } catch {}
    }
  });

  // Main poll loop
  while (running) {
    try {
      const status = await daemonFetch('/status');

      // Fetch logs for selected instance
      const logTarget = selectedInstance || (status.instances[0]?.id) || null;
      if (logTarget) {
        // Reset cursor when switching instances
        if (logTarget !== lastLogTarget) {
          logLines = [];
          logCursor = -1;
          lastLogTarget = logTarget;
        }
        try {
          const ld = await daemonFetch(`/instances/${logTarget}/logs?tail=100&since=${logCursor}`);
          for (const entry of ld.logs || []) {
            logLines.push(entry);
            logCursor = entry.index;
          }
          if (logLines.length > 500) logLines = logLines.slice(-300);
        } catch {}
      }

      render(status);
    } catch {
      write(HOME + CLR_LINE + `${RED}Daemon not responding${R}` + CLR_BELOW);
    }

    await sleep(1000);
  }
}

// ─── Help ───────────────────────────────────────────────────────

function cmdHelp() {
  console.log(`BitDex Dev-Server CLI

Usage: node .claude/skills/dev-server/cli.mjs <command> [options]

Commands:
  status                              Overview of instances, datasets, locks
  start [--port N] [--data-dir D]     Ensure server is running (idempotent)
  start --new [--port N] [--data-dir] Spin up an additional instance
  new [--port N] [--data-dir D]       Spin up an additional instance (agent use)
  stop <id|port>                      Stop an instance (preserves data)
  logs <id|port> [--tail N]           View instance logs
  datasets                            List known data directories
  reserve-port [--preferred N]        Reserve next available port
  build [--target T] [--profile P]    Acquire lock, build, release (default: fast profile)
  test-e2e [--port N]                 Acquire lock, run E2E suite, release
  dash                                TUI dashboard
  shutdown                            Kill all instances + stop daemon
  help                                Show this help

Targets for build:
  bitdex-server    (default) Server binary
  bitdex-loadtest  Loadtest binary
  bitdex-benchmark Benchmark binary (always --release)

All commands except 'dash' and 'help' output JSON to stdout.
Status messages go to stderr.`);
}

// ─── Main ───────────────────────────────────────────────────────

async function main() {
  if (command === 'help') return cmdHelp();

  if (!await ensureDaemon()) {
    console.log(JSON.stringify({ error: 'Could not connect to daemon' }));
    process.exit(1);
  }

  switch (command) {
    case 'status': return cmdStatus();
    case 'start': return cmdStart();
    case 'new': return cmdNew();
    case 'stop': return cmdStop();
    case 'logs': return cmdLogs();
    case 'datasets': return cmdDatasets();
    case 'reserve-port': return cmdReservePort();
    case 'build': return cmdBuild();
    case 'test-e2e': return cmdTestE2e();
    case 'dash': case 'dashboard': return cmdDashboard();
    case 'shutdown': return cmdShutdown();
    default:
      console.error(`Unknown command: ${command}`);
      cmdHelp();
      process.exit(1);
  }
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
