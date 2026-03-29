/**
 * Multi-part parallel HTTP downloader using Range requests.
 * Spawns N concurrent curl processes, each downloading a chunk,
 * then concatenates into the final file.
 *
 * Requires: endpoint supports Accept-Ranges: bytes
 */
import { execSync, spawn } from 'child_process';
import { existsSync, statSync, createWriteStream, createReadStream, unlinkSync, mkdirSync } from 'fs';
import { resolve, dirname } from 'path';

/**
 * Download a file using parallel Range requests.
 *
 * @param {string} url - Full URL to download
 * @param {string} outputPath - Local file path
 * @param {object} opts
 * @param {number} [opts.chunks=8] - Number of parallel connections
 * @param {string} [opts.token] - Bearer token for auth
 * @param {number} [opts.minChunkSize=10*1024*1024] - Min chunk size (10MB) — don't split tiny files
 * @returns {Promise<{ size: number, elapsed: number, rate: string }>}
 */
export async function multipartDownload(url, outputPath, { chunks = 8, token, minChunkSize = 10 * 1024 * 1024 } = {}) {
  const dir = dirname(outputPath);
  mkdirSync(dir, { recursive: true });

  // Step 1: HEAD request to get content-length and verify range support
  const authHeader = token ? `-H "Authorization: Bearer ${token}"` : '';
  const headResult = execSync(
    `curl -sI ${authHeader} "${url}"`,
    { encoding: 'utf8', timeout: 15000 }
  );

  const contentLength = parseInt(headResult.match(/content-length:\s*(\d+)/i)?.[1] || '0');
  const acceptRanges = /accept-ranges:\s*bytes/i.test(headResult);

  if (!contentLength) {
    throw new Error(`Cannot determine file size from HEAD request. Headers:\n${headResult.slice(0, 500)}`);
  }

  if (!acceptRanges) {
    console.error('  Server does not support range requests — falling back to single download');
    execSync(`curl -# ${authHeader} -o "${outputPath}" "${url}"`, { stdio: 'inherit', timeout: 7200000 });
    return { size: contentLength, elapsed: 0, rate: '?' };
  }

  const sizeGB = (contentLength / (1024 ** 3)).toFixed(2);
  console.error(`  File size: ${sizeGB} GB (${contentLength} bytes)`);

  // Don't split files smaller than minChunkSize * chunks
  const effectiveChunks = contentLength < minChunkSize * chunks
    ? Math.max(1, Math.floor(contentLength / minChunkSize))
    : chunks;

  if (effectiveChunks <= 1) {
    console.error('  File too small for multi-part — using single download');
    execSync(`curl -# ${authHeader} -o "${outputPath}" "${url}"`, { stdio: 'inherit', timeout: 7200000 });
    const size = existsSync(outputPath) ? statSync(outputPath).size : 0;
    return { size, elapsed: 0, rate: '?' };
  }

  const chunkSize = Math.ceil(contentLength / effectiveChunks);
  console.error(`  Splitting into ${effectiveChunks} chunks of ~${(chunkSize / (1024 ** 2)).toFixed(0)} MB each`);

  const startTime = Date.now();
  const chunkDir = `${outputPath}.parts`;
  mkdirSync(chunkDir, { recursive: true });

  // Step 2: Download chunks in parallel
  const chunkPromises = [];
  for (let i = 0; i < effectiveChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize - 1, contentLength - 1);
    const chunkPath = resolve(chunkDir, `chunk_${String(i).padStart(3, '0')}`);

    chunkPromises.push(downloadChunk(url, chunkPath, start, end, token, i, effectiveChunks));
  }

  const results = await Promise.all(chunkPromises);
  const allOk = results.every(r => r.ok);
  if (!allOk) {
    const failed = results.filter(r => !r.ok).map(r => r.index);
    throw new Error(`Chunks failed: ${failed.join(', ')}`);
  }

  // Step 3: Concatenate chunks
  console.error('  Concatenating chunks...');
  const writeStream = createWriteStream(outputPath);
  for (let i = 0; i < effectiveChunks; i++) {
    const chunkPath = resolve(chunkDir, `chunk_${String(i).padStart(3, '0')}`);
    await new Promise((res, rej) => {
      const readStream = createReadStream(chunkPath);
      readStream.pipe(writeStream, { end: false });
      readStream.on('end', res);
      readStream.on('error', rej);
    });
  }
  writeStream.end();
  await new Promise(res => writeStream.on('finish', res));

  // Step 4: Verify size
  const finalSize = statSync(outputPath).size;
  if (finalSize !== contentLength) {
    console.error(`  WARNING: Size mismatch! Expected ${contentLength}, got ${finalSize}`);
  }

  // Cleanup chunks
  for (let i = 0; i < effectiveChunks; i++) {
    const chunkPath = resolve(chunkDir, `chunk_${String(i).padStart(3, '0')}`);
    try { unlinkSync(chunkPath); } catch {}
  }
  try { execSync(`rmdir "${chunkDir}" 2>/dev/null`, { encoding: 'utf8' }); } catch {}

  const elapsed = (Date.now() - startTime) / 1000;
  const rateMBs = (finalSize / (1024 ** 2)) / elapsed;
  const rate = `${rateMBs.toFixed(1)} MB/s`;

  console.error(`  Done: ${sizeGB} GB in ${elapsed.toFixed(0)}s (${rate})`);

  return { size: finalSize, elapsed, rate };
}

/**
 * Download a single chunk via curl with Range header.
 * @returns {Promise<{ ok: boolean, index: number }>}
 */
function downloadChunk(url, chunkPath, start, end, token, index, total) {
  return new Promise((resolve) => {
    const args = [
      '-sf',
      '-o', chunkPath,
      '-r', `${start}-${end}`,
    ];
    if (token) {
      args.push('-H', `Authorization: Bearer ${token}`);
    }
    args.push(url);

    const proc = spawn('curl', args, { stdio: ['pipe', 'pipe', 'pipe'] });
    let stderr = '';

    proc.stderr.on('data', d => stderr += d.toString());

    proc.on('close', (code) => {
      if (code === 0 && existsSync(chunkPath)) {
        const size = statSync(chunkPath).size;
        const expected = end - start + 1;
        if (size === expected) {
          process.stderr.write(`  chunk ${index + 1}/${total}: ${(size / (1024 ** 2)).toFixed(0)} MB ✓\n`);
          resolve({ ok: true, index });
        } else {
          process.stderr.write(`  chunk ${index + 1}/${total}: size mismatch (${size} vs ${expected}) ✗\n`);
          resolve({ ok: false, index });
        }
      } else {
        process.stderr.write(`  chunk ${index + 1}/${total}: failed (exit ${code}) ✗\n`);
        resolve({ ok: false, index });
      }
    });

    proc.on('error', () => {
      resolve({ ok: false, index });
    });
  });
}
