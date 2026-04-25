#!/usr/bin/env node
// Synthetic long-tail probe: fires the exact `postId In [single]` + safety
// prefilter shape from `lazy-load-localization-2026-04-25.md`. Used because
// kubectl PF + SSE consumer is unstable; this drives the same surface
// directly without requiring the relay path.
//
// Usage: node probe-postid-tail.mjs [count] [concurrency]

import http from 'node:http';

const COUNT = parseInt(process.argv[2] || '2000');
const CONC = parseInt(process.argv[3] || '8');
const TARGET_HOST = '127.0.0.1';
const TARGET_PORT = 3002;

// Spread postIds across the entire 22.8M-id space → hits every postId bucket
// → maximizes lazy-load surface, exactly the cold-path postId In [single]
// shape from production traces.
const MAX_POST_ID = 22_500_000;

function randomPostId() {
  return Math.floor(Math.random() * MAX_POST_ID) + 1;
}

const SAFETY_PREFILTER = [
  { Not: { Eq: ["availability", { String: "Private" }] } },
  { NotIn: ["blockedFor", [{ String: "tos" }, { String: "moderated" }, { String: "CSAM" }, { String: "AiNotVerified" }]] },
  { IsNotNull: "postId" },
  { Not: { Eq: ["poi", { Bool: true }] } },
  { Not: { Eq: ["minor", { Bool: true }] } },
  { In: ["nsfwLevel", [{ Integer: 1 }, { Integer: 2 }, { Integer: 4 }, { Integer: 8 }, { Integer: 16 }]] },
  { Not: { And: [
    { In: ["nsfwLevel", [{ Integer: 4 }, { Integer: 8 }, { Integer: 16 }, { Integer: 32 }]] },
    { In: ["baseModel", [{ String: "SD 3" }, { String: "SD 3.5" }, { String: "SD 3.5 Medium" }, { String: "SD 3.5 Large" }, { String: "SD 3.5 Large Turbo" }, { String: "SDXL Turbo" }, { String: "SVD" }, { String: "SVD XT" }, { String: "Stable Cascade" }]] },
  ]}},
  { Eq: ["isPublished", { Bool: true }] },
];

function buildBody(postId) {
  return JSON.stringify({
    filters: [
      ...SAFETY_PREFILTER,
      { In: ["postId", [{ Integer: postId }]] },
    ],
    include_docs: true,
    limit: 50,
    sort: { direction: "Desc", field: "reactionCount" },
  });
}

function fireOne(body) {
  return new Promise((resolve) => {
    const start = process.hrtime.bigint();
    const req = http.request(
      {
        hostname: TARGET_HOST,
        port: TARGET_PORT,
        path: "/api/indexes/civitai/query",
        method: "POST",
        headers: {
          "content-type": "application/json",
          "content-length": Buffer.byteLength(body),
        },
        agent: false,
      },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const elapsed_ms = Number(process.hrtime.bigint() - start) / 1e6;
          resolve({ status: res.statusCode, elapsed_ms });
        });
      }
    );
    req.on("error", (e) => {
      const elapsed_ms = Number(process.hrtime.bigint() - start) / 1e6;
      resolve({ status: 0, elapsed_ms, err: e.message });
    });
    req.write(body);
    req.end();
  });
}

async function worker(queue, results) {
  while (true) {
    const id = queue.pop();
    if (id == null) return;
    const body = buildBody(id);
    const r = await fireOne(body);
    results.push(r);
  }
}

const queue = [];
for (let i = 0; i < COUNT; i++) queue.push(randomPostId());

const results = [];
console.log(`firing ${COUNT} postId-in-single queries (cold-path, distinct postIds across 22.5M space) — concurrency ${CONC}`);
const t0 = Date.now();
const workers = Array.from({ length: CONC }, () => worker(queue, results));
await Promise.all(workers);
const elapsed_s = (Date.now() - t0) / 1000;

const oks = results.filter((r) => r.status === 200);
const errs = results.filter((r) => r.status !== 200);
const sorted = oks.map((r) => r.elapsed_ms).sort((a, b) => a - b);

function pct(p) {
  const idx = Math.floor((sorted.length - 1) * p);
  return sorted[idx];
}

const over1s = sorted.filter((ms) => ms > 1000).length;
const over500ms = sorted.filter((ms) => ms > 500).length;
const max = sorted.length ? sorted[sorted.length - 1] : 0;

console.log(`done in ${elapsed_s.toFixed(1)}s, ${oks.length} ok / ${errs.length} err`);
console.log(`P50=${pct(0.5).toFixed(1)}ms  P90=${pct(0.9).toFixed(1)}ms  P95=${pct(0.95).toFixed(1)}ms  P99=${pct(0.99).toFixed(1)}ms  P99.9=${pct(0.999).toFixed(1)}ms  max=${max.toFixed(1)}ms`);
console.log(`>500ms: ${over500ms} (${(100 * over500ms / oks.length).toFixed(2)}%)`);
console.log(`>1000ms: ${over1s} (${(100 * over1s / oks.length).toFixed(2)}%)`);
if (errs.length) {
  console.log(`errors: ${errs.slice(0, 5).map((e) => e.err || e.status).join(", ")}`);
}
