#!/usr/bin/env node
// Dual-stream SSE consumer: subscribes to a BitDex relay's `/events/queries`
// and `/events/ops` channels and replays each event's `body` against a local
// BitDex instance. Used to drive a local perf rig with real prod traffic
// without owning prod compute.
//
// Usage:
//   node scripts/replay-prod-via-relay.mjs [duration_minutes]
//
// Env vars:
//   RELAY_URL          base URL of relay (default https://bitdex.civitai.com)
//   BITDEX_URL         local target (default http://localhost:3002)
//   BITDEX_ADMIN_TOKEN bearer for /events/* (required)
//   QUERY_CONCURRENCY  max parallel local query POSTs (default 16)
//   OPS_CONCURRENCY    max parallel local ops POSTs    (default 4)
//   QUERY_QUEUE_MAX    soft drop threshold for queries (default 2000)
//   OPS_QUEUE_MAX      soft drop threshold for ops     (default 5000)
//   STATS_INTERVAL_MS  stats print interval            (default 10000)
//
// Behavior:
//   - Two independent SSE consumers (one per channel).
//   - Each event payload is a JSON string with shape
//       {"seq_id":N,"ts_ms":N,"index":"X","body":<original-request-body>}.
//     `body` is replayed against the matching local route.
//   - `:lagged N` SSE comments → counted as drops, no replay attempted.
//   - Sequence-id gaps tracked + reported (relay drops messages, not subscribers).
//   - Reconnect with exponential backoff + jitter, capped at 60s.
//   - SIGINT prints final stats and exits.

import http from 'node:http';
import https from 'node:https';

const RELAY_BASE = (process.env.RELAY_URL || 'https://bitdex.civitai.com').replace(/\/$/, '');
const TARGET     = (process.env.BITDEX_URL || 'http://localhost:3002').replace(/\/$/, '');
const TOKEN      = process.env.BITDEX_ADMIN_TOKEN || '';
const Q_MAX      = parseInt(process.env.QUERY_CONCURRENCY || '16', 10);
const O_MAX      = parseInt(process.env.OPS_CONCURRENCY    || '4',  10);
const Q_QMAX     = parseInt(process.env.QUERY_QUEUE_MAX    || '2000', 10);
const O_QMAX     = parseInt(process.env.OPS_QUEUE_MAX      || '5000', 10);
const STATS_MS   = parseInt(process.env.STATS_INTERVAL_MS  || '10000', 10);
const DUR_MIN    = parseInt(process.argv[2] || '30', 10);
const DUR_MS     = DUR_MIN * 60 * 1000;

if (!TOKEN) {
  console.error('BITDEX_ADMIN_TOKEN env var required (relay /events/* always bearer-gated).');
  process.exit(1);
}

const targetUrl = new URL(TARGET);
const targetGetter = targetUrl.protocol === 'https:' ? https : http;

const startedAt = Date.now();

// -------- Per-channel consumer ---------------------------------------------

function makeConsumer({ channel, localPath, maxConc, queueMax }) {
  const state = {
    channel,
    localPath,
    maxConc,
    queueMax,
    inFlight: 0,
    queue: [],
    received: 0,
    replayed: 0,
    errors: 0,
    drops: 0,           // queue overflow
    laggedDropped: 0,   // sum of :lagged N counts from relay
    gaps: 0,
    gapTotal: 0,
    latencies: [],
    lastSeqId: null,
    backoffMs: 1000,
  };

  function replayOne(event) {
    state.inFlight++;
    const t0 = Date.now();
    const payload = JSON.stringify(event.body);
    const tgt = new URL(localPath, TARGET);
    const req = targetGetter.request(
      {
        protocol: tgt.protocol,
        hostname: tgt.hostname,
        port: tgt.port,
        path: tgt.pathname,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
        },
        timeout: 30_000,
      },
      (res) => {
        // Drain so the socket can be reused by the agent's pool.
        res.on('data', () => {});
        res.on('end', () => {
          state.inFlight--;
          state.replayed++;
          state.latencies.push(Date.now() - t0);
          if (res.statusCode >= 400) state.errors++;
          drain();
        });
      },
    );
    req.on('error', () => {
      state.inFlight--;
      state.errors++;
      drain();
    });
    req.on('timeout', () => {
      req.destroy();
      state.inFlight--;
      state.errors++;
      drain();
    });
    req.write(payload);
    req.end();
  }

  function drain() {
    while (state.queue.length > 0 && state.inFlight < state.maxConc) {
      replayOne(state.queue.shift());
    }
  }

  function onEvent(event) {
    state.received++;

    if (state.lastSeqId != null && event.seq_id != null) {
      const expected = state.lastSeqId + 1;
      if (event.seq_id > expected) {
        state.gaps++;
        state.gapTotal += event.seq_id - expected;
      }
    }
    state.lastSeqId = event.seq_id ?? state.lastSeqId;

    if (!event.body) return;

    if (state.inFlight < state.maxConc) {
      replayOne(event);
    } else if (state.queue.length < state.queueMax) {
      state.queue.push(event);
    } else {
      state.drops++;
    }
  }

  function onLagged(n) {
    state.laggedDropped += n;
  }

  function snapshot() {
    const lat = state.latencies;
    if (lat.length === 0) {
      return {
        channel,
        received: state.received,
        replayed: 0,
        errors: state.errors,
        drops: state.drops,
        lagged_dropped: state.laggedDropped,
        gaps: state.gaps,
        gap_total: state.gapTotal,
        in_flight: state.inFlight,
        queued: state.queue.length,
      };
    }
    const sorted = [...lat].sort((a, b) => a - b);
    const pick = (q) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * q))];
    return {
      channel,
      received: state.received,
      replayed: state.replayed,
      errors: state.errors,
      drops: state.drops,
      lagged_dropped: state.laggedDropped,
      gaps: state.gaps,
      gap_total: state.gapTotal,
      in_flight: state.inFlight,
      queued: state.queue.length,
      p50: pick(0.50),
      p90: pick(0.90),
      p95: pick(0.95),
      p99: pick(0.99),
    };
  }

  return { state, onEvent, onLagged, snapshot };
}

// -------- SSE plumbing -----------------------------------------------------

function connectSSE(consumer) {
  const url = new URL(`${RELAY_BASE}/events/${consumer.state.channel}`);
  const getter = url.protocol === 'https:' ? https : http;
  let buffer = '';

  console.error(`[${consumer.state.channel}] connecting → ${url.href}`);
  const req = getter.get(
    url,
    {
      headers: {
        Authorization: `Bearer ${TOKEN}`,
        Accept: 'text/event-stream',
      },
      timeout: DUR_MS + 60_000,
    },
    (res) => {
      if (res.statusCode !== 200) {
        console.error(
          `[${consumer.state.channel}] non-200 from relay: ${res.statusCode}; will reconnect`,
        );
        res.resume();
        scheduleReconnect(consumer);
        return;
      }
      console.error(`[${consumer.state.channel}] connected`);
      consumer.state.backoffMs = 1000; // reset backoff on success

      res.on('data', (chunk) => {
        buffer += chunk.toString('utf8');
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const raw of lines) {
          const line = raw.trimEnd();
          if (line.startsWith(':')) {
            // Comment line — relay sends `: lagged N` for tokio broadcast lag.
            const m = line.match(/lagged\s+(\d+)/);
            if (m) consumer.onLagged(parseInt(m[1], 10));
            continue;
          }
          if (!line.startsWith('data: ')) continue;
          const body = line.slice(6);
          try {
            const parsed = JSON.parse(body);
            consumer.onEvent(parsed);
          } catch {
            // Bad frame; skip.
          }
        }
      });
      res.on('end', () => {
        console.error(`[${consumer.state.channel}] stream ended; reconnecting`);
        scheduleReconnect(consumer);
      });
    },
  );
  req.on('error', (e) => {
    console.error(`[${consumer.state.channel}] socket error: ${e.message}; reconnecting`);
    scheduleReconnect(consumer);
  });
  req.on('timeout', () => {
    console.error(`[${consumer.state.channel}] request timeout; reconnecting`);
    req.destroy();
    scheduleReconnect(consumer);
  });
  return req;
}

function scheduleReconnect(consumer) {
  const jitter = Math.floor(Math.random() * 250);
  const delay = Math.min(60_000, consumer.state.backoffMs + jitter);
  consumer.state.backoffMs = Math.min(60_000, consumer.state.backoffMs * 2);
  setTimeout(() => connectSSE(consumer), delay);
}

// -------- Drivers ----------------------------------------------------------

const queryConsumer = makeConsumer({
  channel: 'queries',
  localPath: '/api/indexes/civitai/query',
  maxConc: Q_MAX,
  queueMax: Q_QMAX,
});
const opsConsumer = makeConsumer({
  channel: 'ops',
  localPath: '/api/indexes/civitai/ops',
  maxConc: O_MAX,
  queueMax: O_QMAX,
});

console.error(`Replay: ${RELAY_BASE}/events/{queries,ops} → ${TARGET}`);
console.error(`Duration ${DUR_MIN} min; conc=q:${Q_MAX}/o:${O_MAX}; queue=q:${Q_QMAX}/o:${O_QMAX}`);

connectSSE(queryConsumer);
connectSSE(opsConsumer);

const statsTimer = setInterval(() => {
  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(0);
  const q = queryConsumer.snapshot();
  const o = opsConsumer.snapshot();
  process.stderr.write(
    `[${elapsed}s] q recv=${q.received} done=${q.replayed} err=${q.errors} drop=${q.drops} lag=${q.lagged_dropped} gap=${q.gaps}/${q.gap_total} qd=${q.queued} p50=${q.p50 ?? '-'} p99=${q.p99 ?? '-'} | ` +
    `o recv=${o.received} done=${o.replayed} err=${o.errors} drop=${o.drops} lag=${o.lagged_dropped} gap=${o.gaps}/${o.gap_total} qd=${o.queued} p50=${o.p50 ?? '-'} p99=${o.p99 ?? '-'}\n`,
  );
}, STATS_MS);

const stopTimer = setTimeout(() => {
  clearInterval(statsTimer);
  console.error('\n=== FINAL ===');
  console.error('queries:', JSON.stringify(queryConsumer.snapshot(), null, 2));
  console.error('ops:    ', JSON.stringify(opsConsumer.snapshot(), null, 2));
  process.exit(0);
}, DUR_MS);

process.on('SIGINT', () => {
  clearTimeout(stopTimer);
  clearInterval(statsTimer);
  console.error('\n=== INTERRUPTED ===');
  console.error('queries:', JSON.stringify(queryConsumer.snapshot(), null, 2));
  console.error('ops:    ', JSON.stringify(opsConsumer.snapshot(), null, 2));
  process.exit(0);
});
