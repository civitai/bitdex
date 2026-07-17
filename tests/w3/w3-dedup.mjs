#!/usr/bin/env node
// Dedup a prepped images CSV to unique slot ids (first occurrence wins), matching prod
// reality (each Image id appears once). Bitset by id — tiny memory.
//   node w3-dedup.mjs <in.csv> <out.csv>
import fs from 'node:fs';
const [inP, outP] = process.argv.slice(2);
if (!inP || !outP) { console.error('usage: w3-dedup.mjs <in> <out>'); process.exit(2); }
const MAXID = 260_000_000; // image ids < ~140M in prod; headroom
const seen = new Uint8Array((MAXID >> 3) + 1);
const fd = fs.openSync(inP, 'r');
const ofd = fs.openSync(outP, 'w');
const B = Buffer.allocUnsafe(1 << 22);
let left = Buffer.alloc(0), br, n = 0, kept = 0, skipped = 0, obuf = [];
const flush = () => { if (obuf.length) { fs.writeSync(ofd, obuf.join('')); obuf = []; } };
while ((br = fs.readSync(fd, B, 0, B.length, null)) > 0) {
  let d = left.length ? Buffer.concat([left, B.subarray(0, br)]) : Buffer.from(B.subarray(0, br));
  let s = 0, nl;
  while ((nl = d.indexOf(10, s)) !== -1) {
    const line = d.subarray(s, nl);
    const c = d.indexOf(44, s);
    const id = parseInt(d.subarray(s, c === -1 || c > nl ? nl : c).toString(), 10);
    n++;
    if (Number.isFinite(id) && id >= 0 && id < MAXID) {
      const byte = id >> 3, bit = 1 << (id & 7);
      if (seen[byte] & bit) { skipped++; } else { seen[byte] |= bit; obuf.push(line.toString() + '\n'); kept++; }
    } else { obuf.push(line.toString() + '\n'); kept++; } // non-numeric header etc — keep
    if (obuf.length >= 20000) flush();
    s = nl + 1;
  }
  left = d.subarray(s);
}
if (left.length) { obuf.push(left.toString() + '\n'); kept++; }
flush();
fs.closeSync(fd); fs.closeSync(ofd);
console.error(`dedup ${inP}: rows=${n} kept=${kept} skipped_dups=${skipped}`);
