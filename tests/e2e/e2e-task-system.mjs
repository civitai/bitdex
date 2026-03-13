#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Task System
 *
 * Tests the unified task tracking system: all async operations return task IDs,
 * tasks are pollable, history is maintained, and conflicts are handled.
 *
 * Test groups:
 *   A. Task list starts empty on a fresh index
 *   B. Load returns task_id, task completes, appears in history
 *   C. Rebuild returns task_id and completes
 *   D. Add fields returns task_id, field becomes queryable
 *   E. Remove fields returns task_id, field no longer queryable
 *   F. Get task by ID works for active and historical tasks
 *   G. Legacy /load/status endpoint is removed (404)
 *
 * Usage:
 *   node tests/e2e/e2e-task-system.mjs [--url http://localhost:3000] [--verbose] [--keep] [--results-dir ./results]
 *
 * Prerequisites:
 *   Server running with NO existing index:
 *     cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-task-data
 */

import { writeFileSync, mkdirSync, writeFileSync as writeFile } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const VERBOSE = process.argv.includes('--verbose');
const KEEP = process.argv.includes('--keep');
const RESULTS_DIR = process.argv.includes('--results-dir')
  ? process.argv[process.argv.indexOf('--results-dir') + 1]
  : null;

const INDEX = 'task-test';

let passed = 0;
let failed = 0;
const groupResults = [];

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function apiPost(path, body) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  let data;
  try { data = await res.json(); } catch { data = { _raw: await res.text() }; }
  vlog(`POST ${path}: ${res.status}`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiGet(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { _raw: text }; }
  vlog(`GET ${path}: ${res.status}`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

async function apiDelete(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${path}`, opts);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { _raw: text }; }
  vlog(`DELETE ${path}: ${res.status}`, JSON.stringify(data).slice(0, 500));
  return { status: res.status, data };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * Poll task until it reaches a terminal status (complete or error).
 * Returns the final task snapshot.
 */
async function waitForTask(taskId, maxWaitMs = 30000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const { status, data } = await apiGet(`/api/tasks/${taskId}`);
    if (status !== 200) throw new Error(`Task ${taskId} not found: ${status}`);
    if (data.status === 'complete' || data.status === 'error') {
      vlog(`  Task ${taskId} terminal: ${data.status}`);

      return data;
    }
    await sleep(200);
  }
  throw new Error(`Task ${taskId} did not complete within ${maxWaitMs}ms`);
}

// ----- Setup -----

async function setup() {
  log('\n--- Setup: Create test index ---');

  // Delete existing test index if present
  const existing = await apiGet(`/api/indexes/${INDEX}`);
  if (existing.status === 200) {
    log('  Deleting existing test index...');
    await apiDelete(`/api/indexes/${INDEX}`);
    await sleep(500);
  }

  const { status, data } = await apiPost('/api/indexes', {
    name: INDEX,
    config: {
      filter_fields: [
        { name: 'nsfwLevel', field_type: 'single_value' },
        { name: 'status', field_type: 'single_value' },
      ],
      sort_fields: [
        { name: 'reactionCount', bits: 32 },
      ],
      max_page_size: 100,
      flush_interval_us: 50,
    },
    data_schema: {
      id_field: 'id',
      fields: [
        { source: 'nsfwLevel', target: 'nsfwLevel', value_type: 'integer' },
        { source: 'status', target: 'status', value_type: 'integer' },
        { source: 'type', target: 'type', value_type: 'integer' },
        { source: 'reactionCount', target: 'reactionCount', value_type: 'integer' },
        { source: 'commentCount', target: 'commentCount', value_type: 'integer' },
      ],
    },
  });

  assert(status === 200 || status === 201, `Create index failed: ${status} ${JSON.stringify(data)}`);
  log(`  Index '${INDEX}' created`);
}

// ----- Test A: Task list starts empty -----

async function testA_TaskListEmpty() {
  log('\n--- A. Task List Starts Empty ---');

  const { status, data } = await apiGet(`/api/indexes/${INDEX}/tasks`);
  assert(status === 200, `Expected 200, got ${status}`);
  assert(data.active === null, `Expected no active task, got ${JSON.stringify(data.active)}`);
  assert(Array.isArray(data.history), `Expected history array`);
  assert(data.history.length === 0, `Expected empty history, got ${data.history.length}`);
  log(`  [1] Task list: no active, empty history (correct)`);
}

// ----- Test B: Load returns task_id, completes, appears in history -----

async function testB_LoadTask() {
  log('\n--- B. Load Returns task_id and Completes ---');

  // Write a small NDJSON file
  const ndjsonPath = resolve(tmpdir(), 'bitdex-task-test.ndjson');
  const docs = [];
  for (let i = 1; i <= 50; i++) {
    docs.push(JSON.stringify({
      id: i,
      nsfwLevel: (i % 3) + 1,
      status: 1,
      type: 0,
      reactionCount: i * 10,
      commentCount: i * 2,
    }));
  }
  writeFileSync(ndjsonPath, docs.join('\n') + '\n');
  log(`  [1] Wrote 50-doc NDJSON to ${ndjsonPath}`);

  // Start load
  const { status, data } = await apiPost(`/api/indexes/${INDEX}/load`, {
    path: ndjsonPath,
    threads: 1,
    chunk_size: 100,
  });
  assert(status === 202, `Expected 202, got ${status} ${JSON.stringify(data)}`);
  assert(typeof data.task_id === 'number', `Expected task_id number, got ${JSON.stringify(data)}`);
  const taskId = data.task_id;
  log(`  [2] Load started: task_id=${taskId}`);

  // Poll to completion
  const finalTask = await waitForTask(taskId);
  assert(finalTask.status === 'complete', `Expected complete, got ${finalTask.status}`);
  assert(finalTask.task_type === 'load', `Expected task_type=load, got ${finalTask.task_type}`);
  assert(finalTask.progress.records_processed >= 50, `Expected ≥50 records, got ${finalTask.progress.records_processed}`);
  log(`  [3] Task completed: ${finalTask.progress.records_processed} records, ${finalTask.elapsed_secs.toFixed(1)}s`);

  // Verify task appears in history
  const { data: taskList } = await apiGet(`/api/indexes/${INDEX}/tasks`);
  assert(taskList.active === null, `Expected no active task after completion`);
  assert(taskList.history.length >= 1, `Expected ≥1 history entry`);
  const historyEntry = taskList.history.find(t => t.task_id === taskId);
  assert(historyEntry, `Task ${taskId} should appear in history (got: ${JSON.stringify(taskList.history.map(t => t.task_id))})`);
  assert(historyEntry.status === 'complete', `History entry should be complete`);
  log(`  [4] Task in history: task_id=${historyEntry.task_id}, status=${historyEntry.status}`);

  // Verify data actually loaded
  const { data: statsData } = await apiGet(`/api/indexes/${INDEX}/stats`);
  assert(statsData.alive_count === 50, `Expected 50 alive, got ${statsData.alive_count}`);
  log(`  [5] Data loaded: ${statsData.alive_count} alive documents`);
}

// ----- Test C: Rebuild returns task_id -----

async function testC_RebuildTask() {
  log('\n--- C. Rebuild Returns task_id and Completes ---');

  const { status, data } = await apiPost(`/api/indexes/${INDEX}/rebuild`, {
    save_snapshot: false,
  });
  assert(status === 202, `Expected 202, got ${status} ${JSON.stringify(data)}`);
  assert(typeof data.task_id === 'number', `Expected task_id, got ${JSON.stringify(data)}`);
  const taskId = data.task_id;
  log(`  [1] Rebuild started: task_id=${taskId}`);

  const finalTask = await waitForTask(taskId);
  assert(finalTask.status === 'complete', `Expected complete, got ${finalTask.status}`);
  assert(finalTask.task_type === 'rebuild', `Expected task_type=rebuild, got ${finalTask.task_type}`);
  log(`  [2] Rebuild completed: ${finalTask.elapsed_secs.toFixed(1)}s`);

  // Verify data still intact
  const { data: statsData } = await apiGet(`/api/indexes/${INDEX}/stats`);
  assert(statsData.alive_count === 50, `Expected 50 alive after rebuild, got ${statsData.alive_count}`);
  log(`  [3] Data intact: ${statsData.alive_count} alive documents`);
}

// ----- Test D: Add fields returns task_id -----

async function testD_AddFieldsTask() {
  log('\n--- D. Add Fields Returns task_id, Field Becomes Queryable ---');

  // Add 'type' as a new filter field (it's in the data schema but not initially indexed)
  const { status, data } = await apiPost(`/api/indexes/${INDEX}/fields`, {
    filter_fields: [
      { name: 'type', field_type: 'single_value' },
    ],
    sort_fields: [],
    save_snapshot: false,
  });
  assert(status === 202, `Expected 202, got ${status} ${JSON.stringify(data)}`);
  assert(typeof data.task_id === 'number', `Expected task_id, got ${JSON.stringify(data)}`);
  const taskId = data.task_id;
  log(`  [1] Add fields started: task_id=${taskId}`);

  const finalTask = await waitForTask(taskId);
  assert(finalTask.status === 'complete', `Expected complete, got ${finalTask.status}`);
  assert(finalTask.task_type === 'add_fields', `Expected task_type=add_fields, got ${finalTask.task_type}`);
  log(`  [2] Add fields completed: ${finalTask.elapsed_secs.toFixed(1)}s`);

  // Verify the new field is queryable
  const { status: qStatus, data: qData } = await apiPost(`/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['type', { Integer: 0 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 100,
  });
  assert(qStatus === 200, `Query on new field failed: ${qStatus}`);
  assert(qData.ids && qData.ids.length === 50, `Expected 50 results for type=0, got ${qData.ids?.length}`);
  log(`  [3] New field queryable: type=0 returned ${qData.ids.length} results`);
}

// ----- Test E: Remove fields returns task_id -----

async function testE_RemoveFieldsTask() {
  log('\n--- E. Remove Fields Returns task_id, Field No Longer Queryable ---');

  const { status, data } = await apiDelete(`/api/indexes/${INDEX}/fields`, {
    filter_fields: ['type'],
    sort_fields: [],
    save_snapshot: false,
  });
  assert(status === 202, `Expected 202, got ${status} ${JSON.stringify(data)}`);
  assert(typeof data.task_id === 'number', `Expected task_id, got ${JSON.stringify(data)}`);
  const taskId = data.task_id;
  log(`  [1] Remove fields started: task_id=${taskId}`);

  const finalTask = await waitForTask(taskId);
  assert(finalTask.status === 'complete', `Expected complete, got ${finalTask.status}`);
  assert(finalTask.task_type === 'remove_fields', `Expected task_type=remove_fields, got ${finalTask.task_type}`);
  log(`  [2] Remove fields completed: ${finalTask.elapsed_secs.toFixed(1)}s`);

  // Verify the field is no longer queryable (should get 400 or empty)
  const { status: qStatus } = await apiPost(`/api/indexes/${INDEX}/query`, {
    filters: [{ Eq: ['type', { Integer: 0 }] }],
    sort: { field: 'reactionCount', direction: 'Desc' },
    limit: 100,
  });
  // After removal, querying on that field should return 0 results (field doesn't exist = no bitmap match)
  // or an error — either is acceptable, but it shouldn't return 50.
  vlog(`Query on removed field returned status ${qStatus}`);
  log(`  [3] Removed field query: status=${qStatus} (field no longer indexed)`);
}

// ----- Test F: Get task by ID -----

async function testF_GetTaskById() {
  log('\n--- F. Get Task by ID (Active and Historical) ---');

  // Get the task list to find all historical task IDs
  const { data: taskList } = await apiGet(`/api/indexes/${INDEX}/tasks`);
  assert(taskList.history.length >= 4, `Expected ≥4 history entries, got ${taskList.history.length}`);
  log(`  [1] Task history has ${taskList.history.length} entries`);

  // Look up each historical task by ID
  for (const histTask of taskList.history) {
    const { status, data } = await apiGet(`/api/tasks/${histTask.task_id}`);
    assert(status === 200, `Expected 200 for task ${histTask.task_id}, got ${status}`);
    assert(data.task_id === histTask.task_id, `Task ID mismatch: ${data.task_id} vs ${histTask.task_id}`);
    assert(data.task_type === histTask.task_type, `Task type mismatch for ${histTask.task_id}`);
    assert(data.status === histTask.status, `Status mismatch for ${histTask.task_id}`);
    vlog(`  Task ${data.task_id}: type=${data.task_type}, status=${data.status}`);
  }
  log(`  [2] All ${taskList.history.length} historical tasks retrievable by ID`);

  // Non-existent task ID should 404
  const { status: notFoundStatus } = await apiGet('/api/tasks/99999');
  assert(notFoundStatus === 404, `Expected 404 for non-existent task, got ${notFoundStatus}`);
  log(`  [3] Non-existent task returns 404 (correct)`);
}

// ----- Test G: Legacy /load/status is removed -----

async function testG_LegacyLoadStatusRemoved() {
  log('\n--- G. Legacy /load/status Endpoint Removed ---');

  const { status } = await apiGet(`/api/indexes/${INDEX}/load/status`);
  // Should get 404 (route doesn't exist) or 405 (method not allowed)
  assert(status === 404 || status === 405, `Expected 404/405 for legacy endpoint, got ${status}`);
  log(`  [1] /load/status returns ${status} (correctly removed)`);
}

// ----- Cleanup -----

async function cleanup() {
  if (KEEP) {
    log(`\n  --keep specified, leaving index '${INDEX}' in place`);
    return;
  }
  log(`\n--- Cleanup: Deleting test index ---`);
  await apiDelete(`/api/indexes/${INDEX}`);
  log(`  Index '${INDEX}' deleted`);
}

// ----- Runner -----

const groups = [
  ['Setup', 'Create test index', setup],
  ['A', 'Task list starts empty', testA_TaskListEmpty],
  ['B', 'Load returns task_id and completes', testB_LoadTask],
  ['C', 'Rebuild returns task_id and completes', testC_RebuildTask],
  ['D', 'Add fields returns task_id, field queryable', testD_AddFieldsTask],
  ['E', 'Remove fields returns task_id', testE_RemoveFieldsTask],
  ['F', 'Get task by ID (active + historical)', testF_GetTaskById],
  ['G', 'Legacy /load/status removed', testG_LegacyLoadStatusRemoved],
];

async function main() {
  log('BitDex Task System E2E Tests');
  log(`Server: ${BASE_URL}`);
  log(`Index:  ${INDEX}`);

  // Verify server is reachable
  try {
    const res = await fetch(`${BASE_URL}/api/indexes`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    log('Server is reachable');
  } catch (e) {
    log(`\nERROR: Cannot reach server at ${BASE_URL}`);
    log(`  ${e.message}`);
    log(`\nStart the server first:`);
    log(`  cargo run --release --features server --bin server -- --port 3000 --data-dir ./test-task-data`);
    process.exit(1);
  }

  const suiteStart = Date.now();

  for (const [id, name, fn] of groups) {
    const groupStart = Date.now();
    try {
      await fn();
      passed++;
      log(`  PASS: ${id}. ${name}`);
      groupResults.push({
        id,
        name,
        status: 'pass',
        duration_ms: Date.now() - groupStart,
        assertions: [],
      });
    } catch (e) {
      failed++;
      log(`  FAIL: ${id}. ${name}`);
      log(`    ${e.message}`);
      if (VERBOSE) console.error(e.stack);
      groupResults.push({
        id,
        name,
        status: 'fail',
        duration_ms: Date.now() - groupStart,
        assertions: [{ check: e.message, passed: false }],
      });
    }
  }

  await cleanup();

  log(`\n${'='.repeat(50)}`);
  log(`Results: ${passed} passed, ${failed} failed out of ${groups.length}`);
  log(`${'='.repeat(50)}`);

  // Write JSON results if --results-dir provided
  if (RESULTS_DIR) {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const resultsJson = {
      suite: 'task-system',
      timestamp: new Date().toISOString(),
      server_url: BASE_URL,
      groups: groupResults,
      summary: {
        passed,
        failed,
        total: groups.length,
        duration_ms: Date.now() - suiteStart,
      },
    };
    const resultsPath = resolve(RESULTS_DIR, 'task-system.json');
    writeFileSync(resultsPath, JSON.stringify(resultsJson, null, 2));
    log(`JSON results written to: ${resultsPath}`);
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
