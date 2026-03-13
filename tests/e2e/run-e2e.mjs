#!/usr/bin/env node
/**
 * BitDex E2E Test Runner
 *
 * Starts a fresh server, runs all self-contained E2E test suites, produces
 * structured JSON results, and cleans up.
 *
 * Usage:
 *   node tests/e2e/run-e2e.mjs [--port 3100] [--skip-build] [--keep] [--verbose]
 *
 * Suites run:
 *   - e2e-write-handling.mjs         (self-contained)
 *   - e2e-eviction.mjs               (self-contained)
 *   - e2e-query-operators.mjs        (self-contained)
 *   - e2e-error-handling.mjs         (self-contained)
 *   - e2e-pagination-overhead.mjs    (self-contained)
 *   - e2e-save-unload-lazy.mjs       (self-contained)
 *   - e2e-low-cardinality-string.mjs (self-contained)
 *   - e2e-delisting.mjs              (self-contained)
 *   - e2e-schema-versioning.mjs      (self-contained)
 *   - e2e-unload-memory.mjs          (self-contained)
 *
 * NOT run automatically (requires production data):
 *   - e2e-unified-cache.mjs   (use --data-dir with loaded data manually)
 */

import { spawn, execSync } from 'node:child_process';
import { mkdirSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PROJECT_ROOT = resolve(__dirname, '..', '..');

// --- CLI args ---

function getArg(name, defaultVal) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return defaultVal;
  return process.argv[idx + 1];
}

const PORT = Number(getArg('--port', '3100'));
const SKIP_BUILD = process.argv.includes('--skip-build');
const KEEP = process.argv.includes('--keep');
const VERBOSE = process.argv.includes('--verbose');
const DATA_DIR = resolve(PROJECT_ROOT, 'test-e2e-data');
const RESULTS_DIR = resolve(PROJECT_ROOT, 'docs', 'test-results');
const SERVER_URL = `http://localhost:${PORT}`;

// --- Self-contained suites ---

const SUITES = [
  {
    name: 'write-handling',
    file: 'tests/e2e/e2e-write-handling.mjs',
    description: 'Write correctness (insert, upsert, delete, concurrent, multi-value)',
  },
  {
    name: 'eviction',
    file: 'tests/e2e/e2e-eviction.mjs',
    description: 'Idle eviction lifecycle (load, idle, evict, reload, existence set)',
  },
  {
    name: 'query-operators',
    file: 'tests/e2e/e2e-query-operators.mjs',
    description: 'Query operators (Range Gt/Gte/Lt/Lte, NotEq, combined range+filter)',
  },
  {
    name: 'error-handling',
    file: 'tests/e2e/e2e-error-handling.mjs',
    description: 'Error handling (invalid JSON, unknown index, empty index, slot recycling)',
  },
  {
    name: 'pagination-overhead',
    file: 'tests/e2e/e2e-pagination-overhead.mjs',
    description: 'Pagination & overhead (cursor correctness, cache acceleration, expansion, structural overhead)',
  },
  {
    name: 'save-unload-lazy',
    file: 'tests/e2e/e2e-save-unload-lazy.mjs',
    description: 'Save-snapshot lifecycle (snapshot save, query correctness, mutation survival, stats)',
  },
  {
    name: 'low-cardinality-string',
    file: 'tests/e2e/e2e-low-cardinality-string.mjs',
    description: 'LowCardinalityString fields (auto-dictionary, case-insensitive, upsert, doc serving)',
  },
  {
    name: 'delisting',
    file: 'tests/e2e/e2e-delisting.mjs',
    description: 'Document delisting / visibility (availability, blockedFor, delist/relist, combined)',
  },
  {
    name: 'schema-versioning',
    file: 'tests/e2e/e2e-schema-versioning.mjs',
    description: 'Schema versioning & field elision (defaults, reconstruction, null handling, round-trip)',
  },
  {
    name: 'unload-memory',
    file: 'tests/e2e/e2e-unload-memory.mjs',
    description: 'Bitmap memory drops after save_and_unload, stays low, queries work via lazy reload',
  },
];

// Suites that require production data (documented but not auto-run)
const MANUAL_SUITES = [
  {
    name: 'unified-cache',
    file: 'tests/e2e/e2e-unified-cache.mjs',
    description: 'Unified cache: population, pagination, mutation/delete maintenance, min_filter_size',
    note: 'Requires --data-dir with loaded production data',
  },
];

// --- Logging ---

function log(...args) { console.log('[run-e2e]', ...args); }
function logErr(...args) { console.error('[run-e2e]', ...args); }

// --- Build ---

function buildServer() {
  if (SKIP_BUILD) {
    log('Skipping build (--skip-build)');
    return;
  }
  log('Building server (cargo build --release --features server --bin server)...');
  try {
    execSync('cargo build --release --features server --bin server', {
      cwd: PROJECT_ROOT,
      stdio: 'inherit',
      timeout: 300_000, // 5 minutes
    });
    log('Build complete');
  } catch (e) {
    logErr('Build failed:', e.message);
    process.exit(1);
  }
}

// --- Server lifecycle ---

let serverProcess = null;
let serverStdout = '';
let serverStderr = '';

function startServer() {
  // Clean data dir
  if (existsSync(DATA_DIR)) {
    rmSync(DATA_DIR, { recursive: true, force: true });
  }
  mkdirSync(DATA_DIR, { recursive: true });

  log(`Starting server on port ${PORT} with data-dir ${DATA_DIR}`);

  const args = [
    '--release', '--features', 'server', '--bin', 'server', '--',
    '--port', String(PORT),
    '--data-dir', DATA_DIR,
  ];

  serverProcess = spawn('cargo', ['run', ...args], {
    cwd: PROJECT_ROOT,
    stdio: ['ignore', 'pipe', 'pipe'],
    // On Windows, shell: true is needed for cargo
    shell: process.platform === 'win32',
  });

  serverProcess.stdout.on('data', (data) => {
    const text = data.toString();
    serverStdout += text;
    if (VERBOSE) process.stdout.write(`[server] ${text}`);
  });

  serverProcess.stderr.on('data', (data) => {
    const text = data.toString();
    serverStderr += text;
    if (VERBOSE) process.stderr.write(`[server] ${text}`);
  });

  serverProcess.on('error', (err) => {
    logErr('Server process error:', err.message);
  });

  serverProcess.on('exit', (code, signal) => {
    if (code !== null && code !== 0) {
      logErr(`Server exited with code ${code}`);
    } else if (signal) {
      log(`Server killed with signal ${signal}`);
    }
  });
}

async function waitForServer(timeoutMs = 60_000) {
  const start = Date.now();
  log(`Waiting for server at ${SERVER_URL}...`);

  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${SERVER_URL}/api/indexes`);
      if (res.ok) {
        log('Server is ready');
        return true;
      }
    } catch {
      // Server not yet up
    }
    await new Promise(r => setTimeout(r, 500));
  }

  logErr(`Server did not become ready within ${timeoutMs / 1000}s`);
  logErr('Server stderr:', serverStderr.slice(-2000));
  return false;
}

function killServer() {
  if (!serverProcess) return;
  log('Stopping server...');
  try {
    if (process.platform === 'win32') {
      // On Windows, spawn with shell creates a cmd.exe wrapper.
      // We need to kill the process tree.
      try {
        execSync(`taskkill /PID ${serverProcess.pid} /T /F`, { stdio: 'ignore' });
      } catch {
        // Process may already be dead
      }
    } else {
      serverProcess.kill('SIGTERM');
      // Give it 3 seconds to shut down gracefully, then SIGKILL
      setTimeout(() => {
        if (serverProcess && !serverProcess.killed) {
          serverProcess.kill('SIGKILL');
        }
      }, 3000);
    }
  } catch (e) {
    logErr('Error killing server:', e.message);
  }
  serverProcess = null;
}

function cleanup() {
  killServer();
  if (!KEEP && existsSync(DATA_DIR)) {
    log(`Removing test data dir: ${DATA_DIR}`);
    try {
      rmSync(DATA_DIR, { recursive: true, force: true });
    } catch (e) {
      logErr('Error cleaning data dir:', e.message);
    }
  } else if (KEEP) {
    log(`Keeping test data dir: ${DATA_DIR}`);
  }
}

// --- Run a single suite ---

/**
 * Runs a single E2E test suite and returns structured results.
 */
async function runSuite(suite) {
  const suiteStart = Date.now();
  log(`\n${'='.repeat(60)}`);
  log(`Running: ${suite.name} (${suite.file})`);
  log(`${'='.repeat(60)}`);

  const filePath = resolve(PROJECT_ROOT, suite.file);
  const args = ['--url', SERVER_URL, '--results-dir', RESULTS_DIR];
  if (VERBOSE) args.push('--verbose');

  return new Promise((resolvePromise) => {
    let stdout = '';
    let stderr = '';

    const child = spawn('node', [filePath, ...args], {
      cwd: PROJECT_ROOT,
      stdio: ['ignore', 'pipe', 'pipe'],
      shell: process.platform === 'win32',
    });

    child.stdout.on('data', (data) => {
      const text = data.toString();
      stdout += text;
      process.stdout.write(text);
    });

    child.stderr.on('data', (data) => {
      const text = data.toString();
      stderr += text;
      process.stderr.write(text);
    });

    child.on('error', (err) => {
      resolvePromise({
        name: suite.name,
        file: suite.file,
        status: 'error',
        error: err.message,
        groups: [],
        duration_ms: Date.now() - suiteStart,
        stdout,
        stderr,
      });
    });

    child.on('exit', (code) => {
      const duration_ms = Date.now() - suiteStart;
      const status = code === 0 ? 'pass' : 'fail';

      // Parse groups from stdout. Each test logs lines like:
      //   PASS: A. Fresh insert appears in query
      //   FAIL: B. Upsert updates filter values
      const groups = [];
      const passRe = /^\s*PASS:\s+(\S+)\.\s+(.+)$/gm;
      const failRe = /^\s*FAIL:\s+(\S+)\.\s+(.+)$/gm;
      let match;
      while ((match = passRe.exec(stdout)) !== null) {
        groups.push({ id: match[1], name: match[2].trim(), status: 'pass' });
      }
      while ((match = failRe.exec(stdout)) !== null) {
        groups.push({ id: match[1], name: match[2].trim(), status: 'fail' });
      }

      log(`\n  ${suite.name}: ${status.toUpperCase()} (${duration_ms}ms, exit code ${code})`);

      resolvePromise({
        name: suite.name,
        file: suite.file,
        status,
        groups,
        duration_ms,
        stdout,
        stderr,
      });
    });
  });
}

// --- Main ---

async function main() {
  const timestamp = new Date().toISOString();
  log(`BitDex E2E Test Runner`);
  log(`Timestamp: ${timestamp}`);
  log(`Server URL: ${SERVER_URL}`);
  log(`Data dir: ${DATA_DIR}`);
  log(`Results dir: ${RESULTS_DIR}`);

  // Ensure results dir exists
  mkdirSync(RESULTS_DIR, { recursive: true });

  // Build
  buildServer();

  // Start server
  startServer();

  // Register cleanup for unexpected exits
  process.on('SIGINT', () => { cleanup(); process.exit(130); });
  process.on('SIGTERM', () => { cleanup(); process.exit(143); });
  process.on('uncaughtException', (err) => {
    logErr('Uncaught exception:', err.message);
    cleanup();
    process.exit(1);
  });

  // Wait for server readiness
  const ready = await waitForServer();
  if (!ready) {
    cleanup();
    process.exit(1);
  }

  // Run suites sequentially
  const suiteResults = [];
  for (const suite of SUITES) {
    const result = await runSuite(suite);
    suiteResults.push(result);
  }

  // Cleanup
  cleanup();

  // Compute summary
  let totalPassed = 0;
  let totalFailed = 0;
  for (const r of suiteResults) {
    const p = r.groups.filter(g => g.status === 'pass').length;
    const f = r.groups.filter(g => g.status === 'fail').length;
    totalPassed += p;
    totalFailed += f;
  }

  const summary = {
    total: totalPassed + totalFailed,
    passed: totalPassed,
    failed: totalFailed,
  };

  // Build JSON results
  const results = {
    timestamp,
    server_version: 'bitdex-v2',
    suites: suiteResults.map(r => ({
      name: r.name,
      file: r.file,
      status: r.status,
      groups: r.groups,
      duration_ms: r.duration_ms,
      stdout: r.stdout,
      stderr: r.stderr,
    })),
    summary,
    manual_suites: MANUAL_SUITES.map(s => ({
      name: s.name,
      file: s.file,
      description: s.description,
      note: s.note,
    })),
  };

  // Write JSON results
  const resultsFile = resolve(RESULTS_DIR, `e2e-${timestamp.replace(/[:.]/g, '-')}.json`);
  writeFileSync(resultsFile, JSON.stringify(results, null, 2));
  log(`\nResults written to: ${resultsFile}`);

  // Print human-readable summary
  log(`\n${'='.repeat(60)}`);
  log('E2E Test Summary');
  log(`${'='.repeat(60)}`);
  for (const r of suiteResults) {
    const icon = r.status === 'pass' ? 'PASS' : 'FAIL';
    const groupSummary = r.groups.length > 0
      ? ` (${r.groups.filter(g => g.status === 'pass').length}/${r.groups.length} groups)`
      : '';
    log(`  ${icon}: ${r.name}${groupSummary} [${r.duration_ms}ms]`);
    for (const g of r.groups) {
      const gIcon = g.status === 'pass' ? '  +' : '  X';
      log(`    ${gIcon} ${g.id}. ${g.name}`);
    }
  }
  log(`${'='.repeat(60)}`);
  log(`Total: ${summary.passed} passed, ${summary.failed} failed out of ${summary.total}`);

  if (MANUAL_SUITES.length > 0) {
    log(`\nSkipped (require production data):`);
    for (const s of MANUAL_SUITES) {
      log(`  - ${s.name}: ${s.note}`);
    }
  }

  log(`${'='.repeat(60)}`);

  process.exit(summary.failed > 0 ? 1 : 0);
}

main();
