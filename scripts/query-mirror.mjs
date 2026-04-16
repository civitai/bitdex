#!/usr/bin/env node
/**
 * query-mirror.mjs — Mirror live prod query traffic to a local BitDex instance.
 *
 * Connects to the SSE stream on the source server and replays each query body
 * against the target server. Drops events when the concurrency cap is saturated
 * so the prod SSE stream is never backpressured.
 *
 * Usage:
 *   BITDEX_ADMIN_TOKEN=<token> node scripts/query-mirror.mjs [options]
 *
 * Options:
 *   --source  <url>   Source server URL  (default: BITDEX_PROD_URL or https://bitdex.civitai.com)
 *   --target  <url>   Target server URL  (default: http://localhost:3001)
 *   --index   <name>  Only mirror queries for this index (optional)
 *   --concurrency <n> Max in-flight POSTs to the target server (default: 4)
 */

import { createInterface } from 'readline';

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    source: process.env.BITDEX_PROD_URL || 'https://bitdex.civitai.com',
    target: 'http://localhost:3001',
    index: null,
    concurrency: 4,
  };
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--source':      opts.source      = args[++i]; break;
      case '--target':      opts.target      = args[++i]; break;
      case '--index':       opts.index       = args[++i]; break;
      case '--concurrency': opts.concurrency = parseInt(args[++i], 10); break;
      default:
        console.error(`Unknown argument: ${args[i]}`);
        process.exit(1);
    }
  }
  return opts;
}

// ---------------------------------------------------------------------------
// SSE reader using fetch streaming (works with Bearer auth)
// ---------------------------------------------------------------------------

async function* readSseStream(url, token) {
  const resp = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'text/event-stream',
      'Cache-Control': 'no-cache',
    },
  });

  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`SSE connect failed: HTTP ${resp.status} — ${body}`);
  }

  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });

    // SSE events are separated by \n\n
    const parts = buf.split('\n\n');
    buf = parts.pop(); // keep partial event in buffer

    for (const part of parts) {
      const dataLine = part.split('\n').find(l => l.startsWith('data:'));
      if (dataLine) {
        const data = dataLine.slice('data:'.length).trim();
        if (data) yield data;
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const opts = parseArgs();
  const token = process.env.BITDEX_ADMIN_TOKEN;
  if (!token) {
    console.error('BITDEX_ADMIN_TOKEN env var is required');
    process.exit(1);
  }

  // Build SSE URL with optional index filter
  const streamUrl = new URL('/debug/queries/stream', opts.source);
  if (opts.index) streamUrl.searchParams.set('index', opts.index);

  console.log(`Connecting to SSE stream: ${streamUrl}`);
  console.log(`Mirroring to:            ${opts.target}`);
  console.log(`Concurrency cap:         ${opts.concurrency}`);
  if (opts.index) console.log(`Index filter:            ${opts.index}`);
  console.log('Press Ctrl+C to stop.\n');

  let totalReceived = 0;
  let totalProcessed = 0;
  let totalDropped = 0;
  let inFlight = 0;
  let lastLogAt = 0;

  // Graceful shutdown
  let shuttingDown = false;
  process.on('SIGINT', () => {
    shuttingDown = true;
    console.log(`\nShutting down...`);
    console.log(`Summary: received=${totalReceived} processed=${totalProcessed} dropped=${totalDropped}`);
    process.exit(0);
  });

  for await (const data of readSseStream(streamUrl.toString(), token)) {
    if (shuttingDown) break;

    totalReceived++;

    let event;
    try {
      event = JSON.parse(data);
    } catch {
      console.warn('Skipping non-JSON SSE data:', data.slice(0, 80));
      continue;
    }

    // Drop if concurrency saturated
    if (inFlight >= opts.concurrency) {
      totalDropped++;
      continue;
    }

    inFlight++;

    // Fire-and-forget POST to target; errors logged but not fatal
    const targetUrl = `${opts.target}/api/indexes/${encodeURIComponent(event.index)}/query`;
    const body = JSON.stringify(event.body);

    (async () => {
      try {
        const resp = await fetch(targetUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body,
        });
        if (!resp.ok) {
          const text = await resp.text().catch(() => '');
          console.warn(`[mirror] POST ${targetUrl} → HTTP ${resp.status}: ${text.slice(0, 120)}`);
        } else {
          totalProcessed++;
        }
      } catch (err) {
        console.warn(`[mirror] POST ${targetUrl} failed: ${err.message}`);
      } finally {
        inFlight--;
      }
    })();

    // Progress log every 1000 received events
    if (totalReceived - lastLogAt >= 1000) {
      lastLogAt = totalReceived;
      const lag = totalReceived - totalProcessed - totalDropped - inFlight;
      console.log(
        `[progress] received=${totalReceived} processed=${totalProcessed} ` +
        `dropped=${totalDropped} in_flight=${inFlight} lag=${lag}`
      );
    }
  }

  console.log(`Stream ended. received=${totalReceived} processed=${totalProcessed} dropped=${totalDropped}`);
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
