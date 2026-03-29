#!/usr/bin/env node
import { execSync, spawn } from 'child_process';
import { createWriteStream, statSync, unlinkSync, existsSync } from 'fs';
import { join } from 'path';

const URL = 'https://bitdex.civitai.com/downloads/tags.csv';
const TOKEN = process.argv[2] || process.env.BITDEX_DL_TOKEN;
const CHUNKS = parseInt(process.argv[3] || '16', 10);
const OUTPUT = process.argv[4] || 'data/load_stage/tags.csv';

if (!TOKEN) { console.error('Usage: node multipart-dl.mjs <token> [chunks] [output]'); process.exit(1); }

// Get file size via HEAD
const headers = execSync(`curl -sI -H "Authorization: Bearer ${TOKEN}" "${URL}"`, { encoding: 'utf8' });
const sizeMatch = headers.match(/content-length:\s*(\d+)/i);
if (!sizeMatch) { console.error('Could not get content-length'); process.exit(1); }
const totalSize = BigInt(sizeMatch[1]);
console.log(`Total: ${totalSize} bytes (${(Number(totalSize) / 1073741824).toFixed(1)} GB)`);
console.log(`Chunks: ${CHUNKS}, ~${(Number(totalSize) / CHUNKS / 1073741824).toFixed(1)} GB each`);

const chunkSize = totalSize / BigInt(CHUNKS);
const startTime = Date.now();

// Download all chunks in parallel
const promises = [];
for (let i = 0; i < CHUNKS; i++) {
  const start = chunkSize * BigInt(i);
  const end = i === CHUNKS - 1 ? totalSize - 1n : chunkSize * BigInt(i + 1) - 1n;
  const partFile = `${OUTPUT}.part${i}`;

  promises.push(new Promise((resolve, reject) => {
    const args = ['-s', '-H', `Authorization: Bearer ${TOKEN}`, '-r', `${start}-${end}`, '-o', partFile, URL];
    const proc = spawn('curl', args);
    proc.on('close', code => {
      if (code !== 0) reject(new Error(`Chunk ${i} failed with code ${code}`));
      else {
        const size = statSync(partFile).size;
        const expected = Number(end - start + 1n);
        console.log(`  Chunk ${i}: ${(size / 1073741824).toFixed(2)} GB ✓`);
        if (size !== expected) reject(new Error(`Chunk ${i} size mismatch: ${size} vs ${expected}`));
        else resolve();
      }
    });
    proc.on('error', reject);
  }));
}

console.log(`Downloading ${CHUNKS} chunks in parallel...`);
try {
  await Promise.all(promises);
} catch (e) {
  console.error('Download failed:', e.message);
  process.exit(1);
}

const dlTime = ((Date.now() - startTime) / 1000).toFixed(1);
console.log(`All chunks done in ${dlTime}s. Concatenating...`);

// Concatenate chunks
const ws = createWriteStream(OUTPUT);
for (let i = 0; i < CHUNKS; i++) {
  const partFile = `${OUTPUT}.part${i}`;
  const data = execSync(`cat "${partFile}"`, { maxBuffer: 20 * 1024 * 1024 * 1024 });
  ws.write(data);
  unlinkSync(partFile);
}
ws.end();

ws.on('finish', () => {
  const finalSize = statSync(OUTPUT).size;
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Done! ${OUTPUT}: ${(finalSize / 1073741824).toFixed(1)} GB in ${totalTime}s`);
  if (BigInt(finalSize) !== totalSize) {
    console.error(`WARNING: Size mismatch! Expected ${totalSize}, got ${finalSize}`);
  }
});
