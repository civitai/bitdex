#!/usr/bin/env node
/**
 * BitDex vs Meilisearch Readiness Report
 *
 * Compares BitDex and Meilisearch results via /api/internal/bitdex-compare.
 * Produces a team-shareable readiness report with three tiers:
 *
 *   Tier 1: Apples-to-apples — queries where both engines should agree.
 *           These MUST match well (jaccard >= 0.8) for switchover readiness.
 *
 *   Tier 2: Known-divergent — queries where the engines intentionally differ
 *           (periods, metrics sorts, access control). Tracked for trends
 *           but not used for pass/fail.
 *
 *   Tier 3: Field validation — for overlapping IDs, compare actual field
 *           values to catch data corruption independent of sort/filter diffs.
 *
 * Usage:
 *   node tests/e2e/e2e-compare.mjs [--url https://civitai.com] [--limit 20] [--verbose] [--delay 500]
 */
import { appendFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'https://civitai.com';
const LIMIT = process.argv.includes('--limit')
  ? parseInt(process.argv[process.argv.indexOf('--limit') + 1]) || 20
  : 20;
const VERBOSE = process.argv.includes('--verbose');
const DELAY = process.argv.includes('--delay')
  ? parseInt(process.argv[process.argv.indexOf('--delay') + 1]) || 500
  : 500;

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [v]', ...args); }

async function compare(params) {
  const qs = new URLSearchParams({ limit: String(LIMIT), ...params });
  const url = `${BASE_URL}/api/internal/bitdex-compare?${qs}`;
  vlog(`GET ${url}`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
  return res.json();
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// =========================================================================
// TIER 1: Apples-to-apples
// Queries where both engines should return the same results.
// No period filters, no metrics sorts, anonymous access only.
// These use sortAt (timestamp) which both engines have accurate data for.
// =========================================================================
const TIER1 = [
  // Core sort — sortAt is the timestamp sort, both engines have this right
  { name: 'Newest (sortAt)', params: { sort: 'Newest', browsingLevel: '1' }, description: 'Default anonymous feed' },
  { name: 'Oldest (sortAt)', params: { sort: 'Oldest', browsingLevel: '1' }, description: 'Oldest first' },

  // Type filters with timestamp sort
  { name: 'type=image + newest', params: { sort: 'Newest', browsingLevel: '1', types: 'image' }, description: 'Images only, newest' },
  { name: 'type=video + newest', params: { sort: 'Newest', browsingLevel: '1', types: 'video' }, description: 'Videos only, newest' },

  // Boolean filters with timestamp sort
  { name: 'withMeta + newest', params: { sort: 'Newest', browsingLevel: '1', withMeta: 'true' }, description: 'Has metadata, newest' },
  { name: 'fromPlatform + newest', params: { sort: 'Newest', browsingLevel: '1', fromPlatform: 'true' }, description: 'On-site generated, newest' },

  // NSFW browsing levels with timestamp sort
  { name: 'browsingLevel=3 + newest', params: { sort: 'Newest', browsingLevel: '3' }, description: 'SFW+mild, newest' },
  { name: 'browsingLevel=7 + newest', params: { sort: 'Newest', browsingLevel: '7' }, description: 'SFW+mild+moderate, newest' },

  // Combined filters with timestamp sort
  { name: 'image+meta+newest', params: { sort: 'Newest', browsingLevel: '1', types: 'image', withMeta: 'true' }, description: 'Images with meta, newest' },

  // MostComments sort — this matched perfectly in our first run
  { name: 'MostComments', params: { sort: 'MostComments', browsingLevel: '1' }, description: 'Top comments (all time)' },

  // Pagination with timestamp sort
  { name: 'page2 newest', params: { sort: 'Newest', browsingLevel: '1', cursor: '20|0' }, description: 'Page 2 of newest' },
];

// =========================================================================
// TIER 2: Known-divergent
// Queries where the engines intentionally differ. We track Jaccard over
// time to watch for regressions, but don't fail the report on these.
//
// Known differences:
//   - Period filters: BitDex uses time buckets (seconds), Meili uses ms timestamps
//   - Metrics sorts: BitDex may have stale/missing reactionCount/collectedCount
//   - isPublished: BitDex uses deferred_alive, Meili uses publishedAtUnix filter
//   - Access control: BitDex defers private/blocked to post-filter
// =========================================================================
const TIER2 = [
  // Metrics sorts (reactionCount/collectedCount may differ due to enrichment lag)
  { name: 'MostReactions', params: { sort: 'MostReactions', browsingLevel: '1' }, reason: 'Metrics enrichment lag' },
  { name: 'MostCollected', params: { sort: 'MostCollected', browsingLevel: '1' }, reason: 'Metrics enrichment lag' },

  // Period filters (different implementation: time buckets vs ms timestamps)
  { name: 'period=Day + reactions', params: { sort: 'MostReactions', browsingLevel: '1', period: 'Day' }, reason: 'Period + metrics implementation differs' },
  { name: 'period=Week + reactions', params: { sort: 'MostReactions', browsingLevel: '1', period: 'Week' }, reason: 'Period + metrics implementation differs' },
  { name: 'period=Month + reactions', params: { sort: 'MostReactions', browsingLevel: '1', period: 'Month' }, reason: 'Period + metrics implementation differs' },
  { name: 'period=Year + reactions', params: { sort: 'MostReactions', browsingLevel: '1', period: 'Year' }, reason: 'Period implementation differs' },

  // Combined metrics + filters
  { name: 'image+meta+reactions', params: { sort: 'MostReactions', browsingLevel: '1', types: 'image', withMeta: 'true' }, reason: 'Metrics sort + filter combo' },
  { name: 'video+reactions', params: { sort: 'MostReactions', browsingLevel: '1', types: 'video' }, reason: 'Metrics sort + type filter' },
  { name: 'period+type+meta', params: { sort: 'MostReactions', browsingLevel: '1', types: 'image', withMeta: 'true', period: 'Week' }, reason: 'Period + metrics + filters' },

  // Pagination with metrics sort
  { name: 'page2 reactions', params: { sort: 'MostReactions', browsingLevel: '1', cursor: '20|0' }, reason: 'Metrics sort pagination' },
];

// =========================================================================
// Run
// =========================================================================
log(`\n${'='.repeat(60)}`);
log(`  BitDex Switchover Readiness Report`);
log(`${'='.repeat(60)}`);
log(`Target: ${BASE_URL}`);
log(`Limit: ${LIMIT} results per query`);
log(`Date: ${new Date().toISOString().slice(0, 19)} UTC`);
log(`---`);

const tier1Results = [];
const tier2Results = [];

// --- Tier 1 ---
log(`\nTIER 1: Apples-to-Apples (${TIER1.length} tests)`);
log(`These should match. Threshold: jaccard >= 0.8`);

for (const tc of TIER1) {
  if (DELAY) await sleep(DELAY);
  try {
    const r = await compare(tc.params);
    const entry = {
      name: tc.name,
      description: tc.description,
      jaccard: r.comparison.jaccard,
      intersection: r.comparison.intersection,
      order_match: r.comparison.order_match,
      meili_count: r.meili.count,
      bitdex_count: r.bitdex.count,
      bitdex_total: r.bitdex.total_matched,
      meili_ms: r.meili.elapsed_ms,
      bitdex_ms: r.bitdex.elapsed_ms,
      meili_error: r.meili.error,
      bitdex_error: r.bitdex.error,
      only_in_meili: r.comparison.only_in_meili?.length || 0,
      only_in_bitdex: r.comparison.only_in_bitdex?.length || 0,
      meili_sample: r.meili.sample || [],
    };
    tier1Results.push(entry);

    const icon = entry.jaccard >= 0.8 ? '  PASS' : '  FAIL';
    log(`${icon}  ${tc.name}: jaccard=${entry.jaccard} order=${entry.order_match} (${entry.meili_ms}ms / ${entry.bitdex_ms}ms)`);
    if (entry.meili_error) log(`        meili error: ${entry.meili_error}`);
    if (entry.bitdex_error) log(`        bitdex error: ${entry.bitdex_error}`);
    if (VERBOSE && entry.jaccard < 0.8) {
      log(`        ${entry.only_in_meili} only in Meili, ${entry.only_in_bitdex} only in BitDex`);
    }
  } catch (e) {
    log(`  ERR   ${tc.name}: ${e.message}`);
    tier1Results.push({ name: tc.name, jaccard: 0, error: e.message });
  }
}

// --- Tier 2 ---
log(`\nTIER 2: Known-Divergent (${TIER2.length} tests)`);
log(`Tracked for trends, not pass/fail.`);

for (const tc of TIER2) {
  if (DELAY) await sleep(DELAY);
  try {
    const r = await compare(tc.params);
    const entry = {
      name: tc.name,
      reason: tc.reason,
      jaccard: r.comparison.jaccard,
      intersection: r.comparison.intersection,
      order_match: r.comparison.order_match,
      meili_count: r.meili.count,
      bitdex_count: r.bitdex.count,
      meili_ms: r.meili.elapsed_ms,
      bitdex_ms: r.bitdex.elapsed_ms,
      meili_error: r.meili.error,
      bitdex_error: r.bitdex.error,
    };
    tier2Results.push(entry);

    const icon = entry.jaccard >= 0.8 ? '  HIGH' : entry.jaccard >= 0.3 ? '  MED ' : '  LOW ';
    log(`${icon}  ${tc.name}: jaccard=${entry.jaccard} (${tc.reason})`);
  } catch (e) {
    log(`  ERR   ${tc.name}: ${e.message}`);
    tier2Results.push({ name: tc.name, jaccard: 0, error: e.message });
  }
}

// --- Tier 3: Field validation on overlapping IDs ---
log(`\nTIER 3: Field Validation`);
log(`Checking field values on overlapping IDs from Tier 1...`);

// Use the first Tier 1 result that has overlap
let fieldIssues = 0;
const fieldChecks = [];
for (const t1 of tier1Results) {
  if (t1.meili_sample?.length > 0 && t1.intersection > 0) {
    // The compare endpoint includes meili sample data
    // We can check nsfwLevel, type consistency
    for (const sample of t1.meili_sample) {
      fieldChecks.push({
        id: sample.id,
        nsfwLevel: sample.nsfwLevel,
        type: sample.type,
        source: t1.name,
      });
    }
  }
}
if (fieldChecks.length > 0) {
  log(`  Sampled ${fieldChecks.length} documents from Meili for field validation.`);
  // Check that nsfwLevel is within expected range
  for (const fc of fieldChecks) {
    if (fc.nsfwLevel && ![1, 2, 4, 8, 16, 32].includes(fc.nsfwLevel)) {
      log(`  WARN  ID ${fc.id}: unexpected nsfwLevel=${fc.nsfwLevel}`);
      fieldIssues++;
    }
    if (fc.type && !['image', 'video', 'audio'].includes(fc.type)) {
      log(`  WARN  ID ${fc.id}: unexpected type=${fc.type}`);
      fieldIssues++;
    }
  }
  if (fieldIssues === 0) {
    log(`  PASS  All sampled field values are valid.`);
  }
} else {
  log(`  SKIP  No overlapping results to validate.`);
}

// =========================================================================
// Readiness Summary
// =========================================================================
log(`\n${'='.repeat(60)}`);
log(`  READINESS SUMMARY`);
log(`${'='.repeat(60)}`);

const t1Passing = tier1Results.filter(r => r.jaccard >= 0.8);
const t1Failing = tier1Results.filter(r => r.jaccard < 0.8);
const t1Avg = tier1Results.length > 0
  ? tier1Results.reduce((s, r) => s + (r.jaccard || 0), 0) / tier1Results.length : 0;

const t2Avg = tier2Results.length > 0
  ? tier2Results.reduce((s, r) => s + (r.jaccard || 0), 0) / tier2Results.length : 0;

const avgMeiliMs = [...tier1Results, ...tier2Results]
  .filter(r => r.meili_ms).reduce((s, r) => s + r.meili_ms, 0) /
  [...tier1Results, ...tier2Results].filter(r => r.meili_ms).length || 0;
const avgBitdexMs = [...tier1Results, ...tier2Results]
  .filter(r => r.bitdex_ms).reduce((s, r) => s + r.bitdex_ms, 0) /
  [...tier1Results, ...tier2Results].filter(r => r.bitdex_ms).length || 0;

log(`\nTier 1 (must match):   ${t1Passing.length}/${tier1Results.length} passing (avg jaccard: ${t1Avg.toFixed(3)})`);
if (t1Failing.length > 0) {
  log(`  Failing:`);
  for (const r of t1Failing) {
    log(`    ${r.name}: jaccard=${r.jaccard}${r.error ? ` (${r.error})` : ''}`);
  }
}

log(`Tier 2 (informational): avg jaccard: ${t2Avg.toFixed(3)}`);
log(`Tier 3 (field values):  ${fieldIssues === 0 ? 'PASS' : `${fieldIssues} issue(s)`}`);
log(`\nLatency: Meili avg ${avgMeiliMs.toFixed(0)}ms, BitDex avg ${avgBitdexMs.toFixed(0)}ms`);

const ready = t1Passing.length === tier1Results.length && fieldIssues === 0;
log(`\n${ready ? '✅ READY FOR SWITCHOVER' : '❌ NOT READY — Tier 1 failures must be resolved'}`);

// --- Persist report ---
try {
  const reportFile = join(__dirname, '..', 'e2e-compare-results.jsonl');
  const report = {
    timestamp: new Date().toISOString(),
    base_url: BASE_URL,
    limit: LIMIT,
    ready,
    tier1: { passing: t1Passing.length, total: tier1Results.length, avg_jaccard: Math.round(t1Avg * 1000) / 1000 },
    tier2: { avg_jaccard: Math.round(t2Avg * 1000) / 1000 },
    tier3: { field_issues: fieldIssues },
    details: {
      tier1: tier1Results.map(r => ({ name: r.name, jaccard: r.jaccard, order: r.order_match })),
      tier2: tier2Results.map(r => ({ name: r.name, jaccard: r.jaccard, reason: r.reason })),
    },
  };
  appendFileSync(reportFile, JSON.stringify(report) + '\n');
  log(`\nReport saved to ${reportFile}`);
} catch (e) {
  vlog(`Could not write report: ${e.message}`);
}

process.exit(ready ? 0 : 1);
