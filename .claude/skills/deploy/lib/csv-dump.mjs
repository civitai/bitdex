/**
 * Config-driven CSV dump from PostgreSQL to BitDex PVC.
 *
 * Fixes from Aidan's session (2026-03-28):
 * - Uses separate -c flags to avoid SET+COPY gzip corruption
 * - Writes raw CSV (no gzip) for V2 dump processor compatibility
 * - Reads queries from config/sync-civitai.yaml (no hardcoded V1 queries)
 * - Uses transfer pod pattern (not kubectl pipe, which corrupts >1GB)
 * - Built-in progress polling
 */
import { existsSync, statSync } from 'fs';
import { resolve } from 'path';
import { createTransferPod, deletePod, kubectlExecPod, portForward, killPortForward, run, K8S_CONTEXT, NS, NODE, INDEX_PATH } from './kubectl.mjs';
import { loadDumpQueries, EXPECTED_SIZES } from './sync-config.mjs';
import { multipartDownload } from './multipart-download.mjs';

const LOAD_STAGE = `${INDEX_PATH}/load_stage`;
const POD_NAME = 'csv-dump-v2';

/**
 * Run a full CSV dump to the PVC.
 * Creates a transfer pod, dumps all tables from sync config, reports progress.
 *
 * @param {object} opts
 * @param {string} [opts.configPath] - Path to sync config YAML
 * @param {number} [opts.pvcIndex=0] - Which PVC to dump to
 * @param {string[]} [opts.tables] - Subset of table names to dump (default: all)
 * @param {boolean} [opts.gzip=false] - Whether to gzip output (default: raw CSV for V2)
 */
export function csvDump({ configPath, pvcIndex = 0, tables, gzip = false } = {}) {
  const queries = loadDumpQueries(configPath);
  const filtered = tables
    ? queries.filter(q => tables.includes(q.name))
    : queries;

  if (filtered.length === 0) {
    console.log(JSON.stringify({ error: 'No matching tables found', available: queries.map(q => q.name) }, null, 2));
    process.exit(1);
  }

  console.error(`Creating transfer pod (PVC ${pvcIndex})...`);
  createTransferPod(POD_NAME, { pvcIndex });

  // Ensure load_stage directory exists
  kubectlExecPod(POD_NAME, `mkdir -p ${LOAD_STAGE}`);

  const results = [];
  try {
    for (let i = 0; i < filtered.length; i++) {
      const { name, query, file } = filtered[i];
      const outFile = gzip ? `${LOAD_STAGE}/${name}.csv.gz` : `${LOAD_STAGE}/${name}.csv`;

      console.error(`[${i + 1}/${filtered.length}] Dumping ${name}...`);

      // CRITICAL: Use --quiet (-q) to suppress "SET" output from psql.
      // Without -q, psql writes "SET\n" to stdout even with separate -c flags,
      // corrupting CSV output. Confirmed by Aidan — all 9 CSVs had SET prefix.
      const dumpCmd = gzip
        ? `psql -q "$DATABASE_URL" -c "SET statement_timeout = 0" -c "${escapeQuery(query)}" | gzip > ${outFile}`
        : `psql -q "$DATABASE_URL" -c "SET statement_timeout = 0" -c "${escapeQuery(query)}" > ${outFile}`;

      kubectlExecPod(POD_NAME, `bash -c '${dumpCmd}'`, { timeout: 3600000 });

      // Verify file was created and get size
      const sizeOutput = kubectlExecPod(POD_NAME, `ls -lh ${outFile} 2>/dev/null`);
      const sizeMatch = sizeOutput?.match(/\S+\s+\S+\s+\S+\s+\S+\s+(\S+)/);
      const size = sizeMatch ? sizeMatch[1] : '?';

      // Verify file isn't corrupt (gzip check or CSV header check)
      let verified = true;
      if (gzip) {
        const gzCheck = kubectlExecPod(POD_NAME, `gzip -t ${outFile} 2>&1`);
        if (gzCheck && gzCheck.includes('not in gzip')) {
          console.error(`  WARNING: ${name}.csv.gz failed gzip verification!`);
          verified = false;
        }
      } else {
        // For raw CSV, check first line looks like data (not SET output)
        const head = kubectlExecPod(POD_NAME, `head -c 50 ${outFile}`);
        if (head && head.startsWith('SET')) {
          console.error(`  WARNING: ${name}.csv starts with SET — dump may be corrupt!`);
          verified = false;
        }
      }

      console.error(`  ${name}: ${size}${verified ? '' : ' (VERIFICATION FAILED)'}`);
      results.push({ name, file: outFile, size, verified });
    }

    // Final listing
    console.error('\nLoad stage contents:');
    const listing = kubectlExecPod(POD_NAME, `ls -lh ${LOAD_STAGE}`);
    if (listing) console.error(listing);

    console.log(JSON.stringify({ dumped: true, tables: results, podName: POD_NAME }, null, 2));
  } catch (e) {
    console.error(`Dump failed: ${e.message}`);
    console.log(JSON.stringify({ error: e.message, partial: results, podName: POD_NAME }, null, 2));
  }
  // Note: we intentionally do NOT delete the pod here.
  // The pod stays alive for progress polling and manual inspection.
  // Use csv-dump-cleanup to remove it when done.
}

/**
 * Poll dump progress on the transfer pod.
 * Shows file sizes and estimated completion percentages.
 */
export function csvDumpProgress() {
  const listing = kubectlExecPod(POD_NAME, `ls -lh ${LOAD_STAGE} 2>/dev/null`);
  if (!listing || listing.includes('No such file')) {
    console.log(JSON.stringify({ error: 'No load_stage directory found. Is a dump running?' }, null, 2));
    return;
  }

  const files = [];
  for (const line of listing.split('\n')) {
    const match = line.match(/(\S+)\s+\S+\s+\S+\s+(\S+)\s+\S+\s+\S+\s+\S+\s+(\S+)/);
    if (!match) continue;
    const [, , sizeStr, filename] = match;
    if (!filename || filename === 'total') continue;

    const name = filename.replace(/\.csv(\.gz)?$/, '');
    const expected = EXPECTED_SIZES[name];

    // Parse size to bytes for percentage
    const sizeBytes = parseSizeToBytes(sizeStr);
    const pct = expected && sizeBytes ? `${((sizeBytes / expected) * 100).toFixed(0)}%` : '?';

    files.push({ name: filename, size: sizeStr, pct });
    console.error(`  ${filename}: ${sizeStr} (${pct})`);
  }

  // Check if dump pod is still running
  const podStatus = kubectlExecPod(POD_NAME, 'echo alive');
  const running = podStatus?.includes('alive');

  console.log(JSON.stringify({ files, podRunning: running }, null, 2));
}

/** Clean up the dump transfer pod. */
export function csvDumpCleanup() {
  console.error(`Deleting transfer pod ${POD_NAME}...`);
  deletePod(POD_NAME);
  console.log(JSON.stringify({ cleaned: true, pod: POD_NAME }, null, 2));
}

/** List available tables from sync config. */
export function csvDumpTables(configPath) {
  const queries = loadDumpQueries(configPath);
  console.log(JSON.stringify({
    tables: queries.map(q => ({
      name: q.name,
      file: q.file,
      expectedSize: formatBytes(EXPECTED_SIZES[q.name]),
    })),
  }, null, 2));
}

// --- HTTP Download Server ---

const DL_POD = 'csv-dl';
const DL_PORT = 8888;
const DL_LOCAL_PORT = 8899;

/**
 * Start an HTTP server on the PVC for fast CSV downloads.
 * Uses python3 http.server inside a pod with PVC mounted.
 * Port-forwards to local for curl access at ~63 MB/s.
 */
export function csvServe({ pvcIndex = 0 } = {}) {
  const overrides = JSON.stringify({
    spec: {
      containers: [{
        name: 'serve',
        image: 'python:3.12-slim',
        command: ['python3', '-m', 'http.server', String(DL_PORT), '--directory', LOAD_STAGE],
        ports: [{ containerPort: DL_PORT }],
        volumeMounts: [{ name: 'data', mountPath: '/data' }],
      }],
      volumes: [{
        name: 'data',
        persistentVolumeClaim: { claimName: `data-bitdex-${pvcIndex}` },
      }],
      nodeSelector: { 'kubernetes.io/hostname': NODE },
    },
  });

  // Clean up any existing server pod
  run(`kubectl delete pod -n ${NS} --context ${K8S_CONTEXT} ${DL_POD} --grace-period=0`, { throws: false });
  run(`kubectl wait --for=delete pod/${DL_POD} -n ${NS} --context ${K8S_CONTEXT} --timeout=30s`, { throws: false });

  console.error('Starting HTTP download server...');
  // Same Windows quirk as createTransferPod: PodSecurity warning on stderr can
  // make `kubectl run` look like it failed even when the pod was created.
  // Don't redirect stderr; verify pod existence after the run instead.
  run(`kubectl run ${DL_POD} -n ${NS} --context ${K8S_CONTEXT} --image=python:3.12-slim --overrides='${overrides}' --restart=Never`,
      { throws: false });
  const podPhase = run(
    `kubectl get pod ${DL_POD} -n ${NS} --context ${K8S_CONTEXT} -o jsonpath='{.status.phase}'`,
    { throws: false },
  );
  if (!podPhase || podPhase.includes('NotFound') || podPhase.includes('error')) {
    console.error(`csvServe: ${DL_POD} not found after kubectl run. phase=${podPhase || '<empty>'}`);
    console.log(JSON.stringify({ error: `Pod ${DL_POD} did not start` }, null, 2));
    process.exit(1);
  }
  run(`kubectl wait --for=condition=Ready pod/${DL_POD} -n ${NS} --context ${K8S_CONTEXT} --timeout=120s`,
      { throws: false });

  // Port-forward
  console.error(`Port-forwarding ${DL_LOCAL_PORT} → ${DL_PORT}...`);
  const ready = portForward({
    target: `pod/${DL_POD}`,
    localPort: DL_LOCAL_PORT,
    remotePort: DL_PORT,
    healthCmd: `curl -sf http://localhost:${DL_LOCAL_PORT}/ 2>/dev/null | head -c 20`,
  });

  if (ready) {
    // List available files
    const listing = kubectlExecPod(DL_POD, `ls -lh ${LOAD_STAGE} 2>/dev/null`);
    if (listing) console.error(`\nAvailable files:\n${listing}`);

    console.log(JSON.stringify({
      status: 'serving',
      url: `http://localhost:${DL_LOCAL_PORT}`,
      pod: DL_POD,
      pvcIndex,
      hint: `curl -O http://localhost:${DL_LOCAL_PORT}/images.csv`,
    }, null, 2));
  } else {
    console.error('Failed to start download server');
    console.log(JSON.stringify({ error: 'Port-forward failed' }, null, 2));
    process.exit(1);
  }
}

/** Stop the HTTP download server. */
export function csvServeStop() {
  killPortForward(DL_LOCAL_PORT, DL_PORT);
  deletePod(DL_POD);
  console.log(JSON.stringify({ status: 'stopped', pod: DL_POD }, null, 2));
}

// --- Ingress Download (bitdex-dl pod) ---

const DL_INGRESS_URL = 'https://bitdex.civitai.com/downloads';

function getDlToken() {
  return process.env.BITDEX_DL_TOKEN || null;
}

/**
 * Check if the ingress download endpoint is available.
 * @returns {{ available: boolean, token: string|null }}
 */
function checkIngressDownload() {
  const token = getDlToken();
  if (!token) return { available: false, token: null };

  const check = run(
    `curl -sf -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${token}" "${DL_INGRESS_URL}/" 2>/dev/null`,
    { throws: false }
  );
  return { available: check === '200' || check === '301' || check === '403', token };
}

/**
 * Download a single file via ingress HTTPS.
 * Uses multi-part parallel download for files >1GB, single curl otherwise.
 * @returns {Promise<{ size: number, sizeHuman: string } | null>}
 */
async function downloadViaIngress(filename, localPath, token, { chunks = 8 } = {}) {
  const url = `${DL_INGRESS_URL}/${filename}`;
  const name = filename.replace(/\.csv(\.gz)?$/, '');
  const expectedSize = EXPECTED_SIZES[name] || 0;

  // Use multi-part for files expected >1GB
  if (expectedSize > 1024 ** 3) {
    try {
      const result = await multipartDownload(url, localPath, { chunks, token });
      return { size: result.size, sizeHuman: formatBytes(result.size), rate: result.rate };
    } catch (e) {
      console.error(`  Multi-part failed (${e.message}), falling back to single download`);
    }
  }

  // Single download (small files or multi-part fallback)
  const resumeFlag = existsSync(localPath) ? '-C -' : '';
  run(
    `curl -# ${resumeFlag} -o "${localPath}" -H "Authorization: Bearer ${token}" "${url}" 2>&1`,
    { throws: false, timeout: 7200000 }
  );

  if (existsSync(localPath)) {
    const size = statSync(localPath).size;
    return { size, sizeHuman: formatBytes(size) };
  }
  return null;
}

/**
 * Download CSVs — prefers ingress (HTTPS, no downtime) over port-forward.
 *
 * Download methods (tried in order):
 * 1. **Ingress** (bitdex-dl pod via https://bitdex.civitai.com/downloads/) — requires BITDEX_DL_TOKEN
 * 2. **Port-forward** (csv-serve pod) — fallback, slower, may drop on large files
 *
 * @param {object} opts
 * @param {string[]} [opts.tables] - Specific tables to download (default: all from sync config)
 * @param {string} [opts.outputDir] - Local output directory (default: data/load_stage)
 * @param {boolean} [opts.keepServer=false] - Keep port-forward server running after download
 * @param {string} [opts.token] - Override download token (default: BITDEX_DL_TOKEN env)
 */
export async function csvDownload({ tables, outputDir, keepServer = false, token, chunks = 8 } = {}) {
  const outDir = outputDir || resolve(process.cwd(), 'data', 'load_stage');
  run(`mkdir -p "${outDir}"`, { throws: false });

  // Override token if provided via flag
  if (token) process.env.BITDEX_DL_TOKEN = token;

  // Determine download method:
  // If token is available (via flag or env), USE ingress directly — skip availability check.
  // The HEAD check fails when bitdex-dl is scaled to 0, but the download itself may
  // still work if the pod scales up or the URL is reachable.
  const dlToken = getDlToken();
  const useIngress = !!dlToken;

  if (useIngress) {
    console.error(`Using ingress download (${DL_INGRESS_URL}) — token provided, HTTP/2 + multi-part for large files`);
  } else {
    console.error('No download token (set BITDEX_DL_TOKEN or use --token). Falling back to port-forward...');
    // Fall back to port-forward server
    const check = run(`curl -sf http://localhost:${DL_LOCAL_PORT}/ 2>/dev/null | head -c 20`, { throws: false });
    if (!check || check.includes('refused')) {
      console.error('Starting download server pod...');
      csvServe();
    }
  }

  // Get file list
  let files;
  if (tables) {
    files = tables.map(t => t.endsWith('.csv') ? t : `${t}.csv`);
  } else {
    // Discover files from sync config
    const queries = loadDumpQueries();
    files = queries.map(q => q.file);
  }

  console.error(`Downloading ${files.length} files to ${outDir}...`);
  const results = [];

  for (const file of files) {
    const localPath = resolve(outDir, file);
    console.error(`  ${file}...`);

    let dl;
    if (useIngress) {
      dl = await downloadViaIngress(file, localPath, dlToken, { chunks });
    } else {
      // Port-forward download
      run(
        `curl -# -o "${localPath}" http://localhost:${DL_LOCAL_PORT}/${file} 2>&1`,
        { throws: false, timeout: 3600000 }
      );
      if (existsSync(localPath)) {
        const size = statSync(localPath).size;
        dl = { size, sizeHuman: formatBytes(size) };
      }
    }

    if (dl) {
      console.error(`    ${dl.sizeHuman}`);
      results.push({ file, localPath, ...dl, method: useIngress ? 'ingress' : 'port-forward' });
    } else {
      console.error(`    FAILED`);
      results.push({ file, error: 'Download failed' });
    }
  }

  if (!useIngress && !keepServer) {
    console.error('Stopping download server...');
    csvServeStop();
  }

  console.log(JSON.stringify({
    downloaded: results.filter(r => !r.error).length,
    method: useIngress ? 'ingress' : 'port-forward',
    files: results,
    outputDir: outDir,
  }, null, 2));
}

/**
 * Verify local CSVs match PVC versions.
 * Checks: file size, row count, first/last line.
 *
 * @param {string} [localDir] - Local CSV directory (default: data/load_stage)
 */
export function csvVerify(localDir) {
  const dir = localDir || resolve(process.cwd(), 'data', 'load_stage');

  // Check if we can reach the PVC (try dump pod first, then a new ephemeral check)
  let podName = POD_NAME;
  let podAlive = kubectlExecPod(podName, 'echo alive')?.includes('alive');
  if (!podAlive) {
    podName = DL_POD;
    podAlive = kubectlExecPod(podName, 'echo alive')?.includes('alive');
  }

  if (!podAlive) {
    console.log(JSON.stringify({ error: 'No running pod with PVC access. Start csv-serve or csv-dump first.' }, null, 2));
    return;
  }

  // List local CSV files
  const localFiles = run(`ls "${dir}"/*.csv 2>/dev/null`, { throws: false });
  if (!localFiles) {
    console.log(JSON.stringify({ error: `No CSV files found in ${dir}` }, null, 2));
    return;
  }

  const results = [];
  for (const localPath of localFiles.split('\n').filter(Boolean)) {
    const filename = localPath.split('/').pop().split('\\').pop();
    const remotePath = `${LOAD_STAGE}/${filename}`;

    const localSize = existsSync(localPath) ? statSync(localPath).size : 0;
    // Use ls -ln for portable size extraction (stat -c not available in all containers)
    const remoteLs = kubectlExecPod(podName, `ls -ln ${remotePath} 2>/dev/null`);
    const remoteSizeMatch = remoteLs?.match(/\S+\s+\S+\s+\S+\s+\S+\s+(\d+)/);
    const remoteSizeNum = remoteSizeMatch ? parseInt(remoteSizeMatch[1]) : 0;

    const localLines = run(`wc -l < "${localPath}" 2>/dev/null`, { throws: false })?.trim();
    const remoteLines = kubectlExecPod(podName, `wc -l ${remotePath} 2>/dev/null`)?.match(/(\d+)/)?.[1] || '0';

    const localHead = run(`head -1 "${localPath}" 2>/dev/null`, { throws: false })?.slice(0, 80);
    const remoteHead = kubectlExecPod(podName, `head -1 ${remotePath} 2>/dev/null`)?.slice(0, 80);

    const sizeMatch = localSize === remoteSizeNum;
    const lineMatch = localLines === remoteLines;
    const headMatch = localHead === remoteHead;
    const ok = sizeMatch && lineMatch && headMatch;

    const status = ok ? 'OK' : 'MISMATCH';
    console.error(`  ${filename}: ${status}${!sizeMatch ? ` (size: ${localSize} vs ${remoteSizeNum})` : ''}${!lineMatch ? ` (lines: ${localLines} vs ${remoteLines})` : ''}${!headMatch ? ' (header differs)' : ''}`);

    results.push({
      file: filename,
      status,
      local: { size: localSize, lines: localLines },
      remote: { size: remoteSizeNum, lines: remoteLines },
      sizeMatch,
      lineMatch,
      headMatch,
    });
  }

  const allOk = results.every(r => r.status === 'OK');
  console.log(JSON.stringify({ verified: allOk, files: results }, null, 2));
}

// --- Full Pipeline ---

/**
 * Full pipeline: dump → serve → download → verify → cleanup → notify.
 * One command, walk away, get notified when done.
 *
 * @param {object} opts
 * @param {string[]} [opts.tables] - Specific tables (default: all)
 * @param {string} [opts.outputDir] - Local output directory
 * @param {string} [opts.notifyAgent] - Agent to notify via mailbox on completion
 * @param {boolean} [opts.skipDump=false] - Skip dump step (use existing PVC files)
 */
export function csvFullPipeline({ tables, outputDir, notifyAgent, skipDump = false } = {}) {
  const outDir = outputDir || resolve(process.cwd(), 'data', 'load_stage');
  const startTime = Date.now();

  const steps = [
    { name: 'dump', label: 'Dumping CSVs to PVC' },
    { name: 'serve', label: 'Starting HTTP download server' },
    { name: 'download', label: 'Downloading CSVs locally' },
    { name: 'verify', label: 'Verifying integrity' },
    { name: 'cleanup', label: 'Cleaning up pods' },
  ];

  const results = { steps: {}, startTime: new Date().toISOString() };

  try {
    // Step 1: Dump (unless skipped)
    if (!skipDump) {
      console.error('\n=== Step 1/5: Dumping CSVs to PVC ===');
      csvDump({ tables, gzip: false });
      results.steps.dump = 'ok';
    } else {
      console.error('\n=== Step 1/5: Dump SKIPPED (using existing PVC files) ===');
      results.steps.dump = 'skipped';
    }

    // Step 2: Start HTTP server
    console.error('\n=== Step 2/5: Starting HTTP download server ===');
    csvServe();
    results.steps.serve = 'ok';

    // Step 3: Download
    console.error('\n=== Step 3/5: Downloading CSVs locally ===');
    csvDownload({ tables, outputDir: outDir, keepServer: true });
    results.steps.download = 'ok';

    // Step 4: Verify
    console.error('\n=== Step 4/5: Verifying integrity ===');
    csvVerify(outDir);
    results.steps.verify = 'ok';

    // Step 5: Cleanup
    console.error('\n=== Step 5/5: Cleaning up ===');
    csvServeStop();
    if (!skipDump) csvDumpCleanup();
    results.steps.cleanup = 'ok';

    const elapsed = Math.round((Date.now() - startTime) / 1000);
    results.elapsed = `${Math.round(elapsed / 60)}m ${elapsed % 60}s`;
    results.status = 'complete';
    results.outputDir = outDir;

    console.error(`\n=== Pipeline complete in ${results.elapsed} ===`);
    console.log(JSON.stringify(results, null, 2));

    // Notify
    if (notifyAgent) {
      try {
        run(
          `node ~/.claude/skills/mailbox/query.mjs send ${notifyAgent} "CSV full pipeline complete in ${results.elapsed}. ${Object.keys(results.steps).length} steps finished. Files in ${outDir}"`,
          { throws: false }
        );
        console.error(`Notified ${notifyAgent}`);
      } catch {}
    }
  } catch (e) {
    results.status = 'failed';
    results.error = e.message;
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    results.elapsed = `${Math.round(elapsed / 60)}m ${elapsed % 60}s`;

    console.error(`\n=== Pipeline FAILED at ${results.elapsed}: ${e.message} ===`);
    console.log(JSON.stringify(results, null, 2));

    // Notify on failure too
    if (notifyAgent) {
      try {
        run(
          `node ~/.claude/skills/mailbox/query.mjs send ${notifyAgent} "CSV pipeline FAILED after ${results.elapsed}: ${e.message}"`,
          { throws: false }
        );
      } catch {}
    }
  }
}

// --- Watch Mode ---

/**
 * Watch a local file download, polling size and showing progress.
 * Exits with a notification when the file stops growing (download complete)
 * or reaches expected size.
 *
 * Usage: csv-download-watch <filename> [--poll <seconds>] [--notify <agent>]
 *
 * @param {string} filename - File to watch (e.g., 'tags.csv')
 * @param {object} opts
 * @param {number} [opts.pollInterval=10] - Seconds between polls
 * @param {string} [opts.notifyAgent] - Agent name to notify via mailbox on completion
 * @param {string} [opts.localDir] - Directory containing the file
 */
export function csvDownloadWatch(filename, { pollInterval = 10, notifyAgent, localDir } = {}) {
  const dir = localDir || resolve(process.cwd(), 'data', 'load_stage');
  const filePath = resolve(dir, filename);
  const name = filename.replace(/\.csv(\.gz)?$/, '');
  const expected = EXPECTED_SIZES[name] || null;

  let lastSize = 0;
  let lastTime = Date.now();
  let stableCount = 0;

  console.error(`Watching ${filename}...`);
  if (expected) console.error(`  Expected size: ${formatBytes(expected)}`);
  console.error(`  Polling every ${pollInterval}s. Will notify when complete.\n`);

  const poll = () => {
    let currentSize = 0;
    try {
      if (existsSync(filePath)) {
        currentSize = statSync(filePath).size;
      }
    } catch { /* file may be locked */ }

    const now = Date.now();
    const elapsed = (now - lastTime) / 1000;
    const bytesPerSec = elapsed > 0 ? (currentSize - lastSize) / elapsed : 0;
    const rate = bytesPerSec > 0 ? formatBytes(bytesPerSec) + '/s' : 'stalled';

    const pct = expected ? `${((currentSize / expected) * 100).toFixed(1)}%` : '?';
    const eta = expected && bytesPerSec > 0
      ? formatDuration((expected - currentSize) / bytesPerSec)
      : '?';

    const sizeHuman = formatBytes(currentSize);
    const line = `  ${sizeHuman} (${pct}) — ${rate} — ETA: ${eta}`;
    console.error(line);

    // Check if download is complete (file stopped growing for 2 consecutive polls)
    if (currentSize > 0 && currentSize === lastSize) {
      stableCount++;
      if (stableCount >= 2) {
        console.error(`\n  Download complete: ${sizeHuman}`);

        // Notify via mailbox if requested
        if (notifyAgent) {
          try {
            run(
              `node ~/.claude/skills/mailbox/query.mjs send ${notifyAgent} "${filename} download complete — ${sizeHuman}"`,
              { throws: false }
            );
            console.error(`  Notified ${notifyAgent} via mailbox`);
          } catch {}
        }

        console.log(JSON.stringify({
          status: 'complete',
          file: filename,
          size: currentSize,
          sizeHuman,
          path: filePath,
        }, null, 2));
        return; // Exit polling
      }
    } else {
      stableCount = 0;
    }

    lastSize = currentSize;
    lastTime = now;

    // Schedule next poll
    setTimeout(poll, pollInterval * 1000);
  };

  poll();
}

// --- Helpers ---

function escapeQuery(query) {
  // Escape single quotes for bash -c wrapping
  return query.replace(/'/g, "'\"'\"'");
}

function parseSizeToBytes(s) {
  const match = s.match(/^([\d.]+)([KMGTP]?)$/i);
  if (!match) return 0;
  const num = parseFloat(match[1]);
  const unit = match[2].toUpperCase();
  const multipliers = { '': 1, 'K': 1024, 'M': 1024 ** 2, 'G': 1024 ** 3, 'T': 1024 ** 4 };
  return num * (multipliers[unit] || 1);
}

function formatBytes(bytes) {
  if (!bytes) return '?';
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GB`;
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(0)} MB`;
  return `${(bytes / 1024).toFixed(0)} KB`;
}

function formatDuration(seconds) {
  if (!seconds || seconds <= 0) return '?';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}
