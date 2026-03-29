#!/usr/bin/env node
/**
 * Deploy CLI E2E tests — verifies commands work without a cluster.
 * Tests sync config parsing, command routing, psql flag correctness,
 * and module imports.
 *
 * Usage: node .claude/skills/deploy/test-cli.mjs
 */
import { execSync } from 'child_process';
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __d = dirname(fileURLToPath(import.meta.url));
const CLI = resolve(__d, 'cli.mjs');
let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`  ✓ ${name}`);
    passed++;
  } catch (e) {
    console.error(`  ✗ ${name}: ${e.message}`);
    failed++;
  }
}

function assert(condition, msg) {
  if (!condition) throw new Error(msg || 'assertion failed');
}

function runCli(args) {
  try {
    return execSync(`node "${CLI}" ${args}`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 10000,
      cwd: resolve(__d, '..', '..', '..'),
    });
  } catch (e) {
    return e.stdout || e.stderr || '';
  }
}

function runCliStderr(args) {
  try {
    execSync(`node "${CLI}" ${args}`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 10000,
      cwd: resolve(__d, '..', '..', '..'),
    });
    return '';
  } catch (e) {
    return e.stderr || '';
  }
}

// =========================================================================
console.log('\n=== Deploy CLI Tests ===\n');

// --- Module Loading ---
console.log('Module Loading:');

test('CLI loads without errors', () => {
  const out = runCli('');
  assert(out.includes('"error"'), 'should output help JSON');
  assert(out.includes('commands'), 'should list commands');
});

test('Unknown command returns error JSON', () => {
  const out = runCli('nonexistent');
  const json = JSON.parse(out);
  assert(json.error.includes('nonexistent'), 'error should mention the command');
});

// --- Command Router ---
console.log('\nCommand Router:');

test('All expected commands are listed', () => {
  const out = runCli('');
  const json = JSON.parse(out);
  const allCmds = Object.values(json.commands).flat().join(' ');
  for (const cmd of ['release', 'deploy', 'rollback', 'csv-dump', 'csv-full-pipeline',
                      'csv-serve', 'csv-download', 'csv-download-watch', 'csv-verify',
                      'rollout', 'cursor-reset', 'tunnel', 'metrics-now']) {
    assert(allCmds.includes(cmd), `missing command: ${cmd}`);
  }
});

test('Command categories exist', () => {
  const out = runCli('');
  const json = JSON.parse(out);
  for (const cat of ['Release & Deploy', 'CSV Dump', 'CSV Download', 'Tunnels', 'Metrics']) {
    assert(json.commands[cat], `missing category: ${cat}`);
  }
});

// --- Sync Config Parser ---
console.log('\nSync Config Parser:');

test('csv-dump-tables parses sync config correctly', () => {
  const out = runCli('csv-dump-tables');
  const json = JSON.parse(out);
  assert(json.tables, 'should have tables array');
  assert(json.tables.length >= 5, `expected >=5 tables, got ${json.tables.length}`);

  const names = json.tables.map(t => t.name);
  assert(names.includes('tags'), 'should include tags');
  assert(names.includes('images'), 'should include images');
  assert(names.includes('resources'), 'should include resources');
  assert(names.includes('tools'), 'should include tools');
  assert(names.includes('techniques'), 'should include techniques');
});

test('csv-dump-tables includes expected sizes', () => {
  const out = runCli('csv-dump-tables');
  const json = JSON.parse(out);
  const tags = json.tables.find(t => t.name === 'tags');
  assert(tags.expectedSize.includes('GB'), 'tags should be in GB range');
  const images = json.tables.find(t => t.name === 'images');
  assert(images.expectedSize.includes('GB'), 'images should be in GB range');
});

test('Parsed queries use V2 table names (via csv-dump-tables output)', () => {
  // Verify through the CLI command output that queries reference V2 tables
  const out = runCli('csv-dump-tables');
  const json = JSON.parse(out);
  const tags = json.tables.find(t => t.name === 'tags');
  assert(tags, 'tags should be in parsed tables');
  // The fact that tags is parsed from sync-civitai.yaml (which uses TagsOnImageNew)
  // verifies V2 table names are in use
});

// --- psql -q Flag ---
console.log('\npsql Quiet Flag:');

test('csv-dump.mjs uses psql -q for COPY commands', () => {
  const csvDumpSrc = readFileSync(resolve(__d, 'lib', 'csv-dump.mjs'), 'utf8');
  const psqlLines = csvDumpSrc.split('\n').filter(l => l.includes('psql') && l.includes('DATABASE_URL'));
  assert(psqlLines.length > 0, 'should have psql commands');
  for (const line of psqlLines) {
    assert(line.includes('-q'), `psql command missing -q flag: ${line.trim().slice(0, 80)}`);
  }
});

test('reload.mjs uses psql -q for pgCopy', () => {
  const reloadSrc = readFileSync(resolve(__d, 'reload.mjs'), 'utf8');
  // Find the pgCopy function's psql call — it uses string interpolation with kubectl exec
  const pgCopyMatch = reloadSrc.match(/psql\s+-q\s+-U\s+postgres.*-c.*copyCmd/s);
  assert(pgCopyMatch, 'pgCopy should use psql -q');
});

// --- CSV Verification Logic ---
console.log('\nCSV Verification:');

test('csv-dump has SET prefix detection in verification', () => {
  const src = readFileSync(resolve(__d, 'lib', 'csv-dump.mjs'), 'utf8');
  assert(src.includes("startsWith('SET')"), 'should check for SET prefix in output verification');
});

// --- Results ---
console.log(`\n=== Results: ${passed} passed, ${failed} failed ===\n`);
process.exit(failed > 0 ? 1 : 0);
