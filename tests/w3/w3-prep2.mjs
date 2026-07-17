#!/usr/bin/env node
// W3 gate-1 images prep (quote-aware + integrated dedup). Posts.w3.csv already built & verified.
//   Reads posts.csv -> publishedAt lookup. Streams images{,-small}.csv, inserts sortAt after
//   field 9 (createdAtSecs) by locating the top-level comma boundary (respecting quoted fields
//   like the comma-containing blurhash), preserving original bytes. Dedups to first-occurrence.
//   Output: images{,-small}.w3d.csv (unique ids, 14 cols).
//   node w3-prep2.mjs <full|small>
import fs from 'node:fs';
const MODE = process.argv[2] || 'small';
const STAGE = 'C:/Dev/Repos/open-source/bitdex-v2/data/load_stage';
const imgIn  = `${STAGE}/images${MODE === 'small' ? '-small' : ''}.csv`;
const imgOut = `${STAGE}/images${MODE === 'small' ? '-small' : ''}.w3d.csv`;
const postsIn = `${STAGE}/posts.csv`;

const NL = 10;
function forEachLine(path, onLine) {
  const fd = fs.openSync(path, 'r'); const BUF = Buffer.allocUnsafe(1 << 22);
  let leftover = Buffer.alloc(0), n;
  while ((n = fs.readSync(fd, BUF, 0, BUF.length, null)) > 0) {
    let data = leftover.length ? Buffer.concat([leftover, BUF.subarray(0, n)]) : Buffer.from(BUF.subarray(0, n));
    let s = 0, nl;
    while ((nl = data.indexOf(NL, s)) !== -1) { onLine(data.subarray(s, nl)); s = nl + 1; }
    leftover = data.subarray(s);
  }
  if (leftover.length) onLine(leftover);
  fs.closeSync(fd);
}

// ---- pass 1: posts.csv -> maxPostId + publishedAt lookup (posts has no quoted fields) ----
console.error(`[prep2 ${MODE}] pass1 posts`);
let maxPostId = 0;
forEachLine(postsIn, (line) => { const c = line.indexOf(44); const id = parseInt((c === -1 ? line : line.subarray(0, c)).toString(), 10); if (id > maxPostId) maxPostId = id; });
const pub = new Int32Array(maxPostId + 2);
forEachLine(postsIn, (line) => {
  const s = line.toString(); const p = s.split(',');
  const id = parseInt(p[0], 10); if (!Number.isFinite(id)) return;
  const pa = parseInt(p[1], 10); if (Number.isFinite(pa)) pub[id] = pa;
});
console.error(`[prep2 ${MODE}] maxPostId=${maxPostId}`);

// find top-level comma positions in a CSV line string; returns array up to `need` commas.
function topCommas(str, need) {
  const pos = []; let q = false;
  for (let i = 0; i < str.length; i++) {
    const ch = str.charCodeAt(i);
    if (q) { if (ch === 34) { if (str.charCodeAt(i + 1) === 34) i++; else q = false; } }
    else { if (ch === 34) q = true; else if (ch === 44) { pos.push(i); if (pos.length >= need) return pos; } }
  }
  return pos;
}

// ---- pass 2: images -> insert sortAt + dedup ----
console.error(`[prep2 ${MODE}] pass2 images -> ${imgOut}`);
const MAXID = 260_000_000;
const seen = new Uint8Array((MAXID >> 3) + 1);
const ofd = fs.openSync(imgOut, 'w');
let obuf = [], rows = 0, kept = 0, malformed = 0;
const flush = () => { if (obuf.length) { fs.writeSync(ofd, obuf.join('')); obuf = []; } };
forEachLine(imgIn, (line) => {
  rows++;
  const s = line.toString();
  const cp = topCommas(s, 11); // need commas 0..10 -> indices for fields 7,8,9,10 boundaries
  if (cp.length < 11) { malformed++; return; }
  const id = parseInt(s.slice(0, cp[0]), 10);
  // header row (id non-numeric) -> skip (full file has none; small file's header dropped)
  if (!Number.isFinite(id)) { return; }
  if (id >= 0 && id < MAXID) { const b = id >> 3, m = 1 << (id & 7); if (seen[b] & m) return; seen[b] |= m; }
  const scanned = parseInt(s.slice(cp[7] + 1, cp[8]), 10) || 0;
  const created = parseInt(s.slice(cp[8] + 1, cp[9]), 10) || 0;
  const postId  = parseInt(s.slice(cp[9] + 1, cp[10]), 10);
  const pa = (Number.isFinite(postId) && postId >= 0 && postId <= maxPostId) ? pub[postId] : 0;
  let sortAt = scanned; if (created > sortAt) sortAt = created; if (pa > sortAt) sortAt = pa;
  // splice ",sortAt" right after field9 (at cp[9], the comma between createdAtSecs and postId)
  obuf.push(s.slice(0, cp[9]) + ',' + sortAt + s.slice(cp[9]) + '\n');
  kept++;
  if (obuf.length >= 20000) flush();
});
flush(); fs.closeSync(ofd);
console.error(`[prep2 ${MODE}] rows=${rows} kept=${kept} malformed=${malformed}`);
