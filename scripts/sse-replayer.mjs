#!/usr/bin/env node
// SSE Query Replayer — reads from stdin (piped from curl SSE), replays to local BitDex.
// Usage: curl -N http://localhost:3099/debug/queries/stream -H "Authorization: Bearer TOKEN" | node scripts/sse-replayer.mjs
//
// Replays each query to BITDEX_URL (default http://localhost:3002) and tracks latency stats.

const http = require('http');
const readline = require('readline');

const TARGET = process.env.BITDEX_URL || 'http://localhost:3002';
const INDEX = process.env.BITDEX_INDEX || 'civitai';
const url = new URL(`${TARGET}/api/indexes/${INDEX}/query`);

let total = 0, errors = 0, inFlight = 0;
const latencies = [];
const startTime = Date.now();

function replay(queryBody) {
  const body = JSON.stringify(queryBody);
  inFlight++;
  total++;
  const t0 = Date.now();

  const req = http.request({
    hostname: url.hostname,
    port: url.port,
    path: url.pathname,
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    timeout: 30000,
  }, (res) => {
    let data = '';
    res.on('data', c => data += c);
    res.on('end', () => {
      inFlight--;
      const ms = Date.now() - t0;
      latencies.push(ms);
      try {
        const j = JSON.parse(data);
        if (total % 50 === 0) {
          printStats();
        }
      } catch {}
    });
  });
  req.on('error', () => { inFlight--; errors++; });
  req.on('timeout', () => { req.destroy(); inFlight--; errors++; });
  req.write(body);
  req.end();
}

function printStats() {
  if (latencies.length === 0) return;
  const sorted = [...latencies].sort((a, b) => a - b);
  const n = sorted.length;
  const elapsed = (Date.now() - startTime) / 1000;
  const qps = (n / elapsed).toFixed(1);
  const p50 = sorted[Math.floor(n * 0.50)];
  const p95 = sorted[Math.floor(n * 0.95)];
  const p99 = sorted[Math.floor(n * 0.99)];
  const under1 = sorted.filter(x => x < 1).length;
  process.stderr.write(
    `[${n} queries, ${qps} QPS] P50=${p50}ms P95=${p95}ms P99=${p99}ms ` +
    `<1ms=${(under1/n*100).toFixed(0)}% errors=${errors} inflight=${inFlight}\n`
  );
}

// Read SSE from stdin
const rl = readline.createInterface({ input: process.stdin });
rl.on('line', (line) => {
  if (!line.startsWith('data: ')) return;
  try {
    const event = JSON.parse(line.slice(6));
    if (!event.body) return;
    // Strip include_docs to avoid doc fetch overhead in benchmarks
    const q = { ...event.body };
    delete q.include_docs;
    replay(q);
  } catch {}
});

rl.on('close', () => {
  // Wait for in-flight to drain
  const drain = setInterval(() => {
    if (inFlight === 0) {
      clearInterval(drain);
      printStats();
      process.stderr.write(`\nDone. ${total} queries replayed, ${errors} errors.\n`);
      process.exit(0);
    }
  }, 100);
});

// Print stats on Ctrl+C
process.on('SIGINT', () => {
  process.stderr.write('\n--- Final Stats ---\n');
  printStats();
  process.exit(0);
});
