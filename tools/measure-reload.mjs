// measure-reload.mjs — Measure per-value bitmap reload latency on live 105M Civitai dataset
// Tests whether single-value lazy reload is viable in the query hot path

const BASE = "http://localhost:3001";
const INDEX = "civitai";
const URL = `${BASE}/api/indexes/${INDEX}/query`;

async function query(tagId) {
  const body = {
    filters: [{ In: ["tagIds", [{ Integer: tagId }]] }],
    sort: { field: "sortAt", direction: "Desc" },
    limit: 1,
  };
  const t0 = performance.now();
  const res = await fetch(URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const wallMs = performance.now() - t0;
  const data = await res.json();
  return {
    tagId,
    serverUs: data.elapsed_us,
    wallMs: wallMs.toFixed(2),
    totalMatched: data.total_matched,
    ids: data.ids?.length ?? 0,
  };
}

async function section(label) {
  console.log(`\n${"=".repeat(60)}`);
  console.log(label);
  console.log("=".repeat(60));
}

async function main() {
  console.log("BitDex per-value bitmap reload latency measurement");
  console.log(`Target: ${URL}`);
  console.log(`Date: ${new Date().toISOString()}\n`);

  // --- Popular tag (tagId 5) — warm-up and cached ---
  await section("1. Popular tag (tagId=5) — first load vs cached");

  const first5 = await query(5);
  console.log(`  First query:  server=${first5.serverUs}us  wall=${first5.wallMs}ms  matched=${first5.totalMatched}`);

  const cached5 = await query(5);
  console.log(`  Cached query: server=${cached5.serverUs}us  wall=${cached5.wallMs}ms  matched=${cached5.totalMatched}`);

  // Third hit to confirm steady state
  const steady5 = await query(5);
  console.log(`  Steady state: server=${steady5.serverUs}us  wall=${steady5.wallMs}ms  matched=${steady5.totalMatched}`);

  // --- Rare tags (50000-50009) — likely cold, triggers per-value lazy load ---
  await section("2. Rare tags (50000–50009) — cold per-value loads");

  const rareTags = [];
  for (let i = 50000; i < 50010; i++) {
    const r = await query(i);
    rareTags.push(r);
    const label = r.totalMatched === 0 ? "(empty)" : "";
    console.log(`  tagId=${i}: server=${r.serverUs}us  wall=${r.wallMs}ms  matched=${r.totalMatched} ${label}`);
  }

  // --- Very rare / nonexistent tags (200000+) ---
  await section("3. Very rare tags (200000–200009) — likely nonexistent bitmaps");

  const veryRare = [];
  for (let i = 200000; i < 200010; i++) {
    const r = await query(i);
    veryRare.push(r);
    const label = r.totalMatched === 0 ? "(empty)" : "";
    console.log(`  tagId=${i}: server=${r.serverUs}us  wall=${r.wallMs}ms  matched=${r.totalMatched} ${label}`);
  }

  // --- Re-query rare tags (should now be cached/loaded) ---
  await section("4. Re-query rare tags (50000–50009) — should be warm now");

  const rareWarm = [];
  for (let i = 50000; i < 50010; i++) {
    const r = await query(i);
    rareWarm.push(r);
    console.log(`  tagId=${i}: server=${r.serverUs}us  wall=${r.wallMs}ms  matched=${r.totalMatched}`);
  }

  // --- Rapid-fire 50 queries on a single cached tag ---
  await section("5. Rapid-fire 50 queries on cached tagId=5");

  const rapidResults = [];
  for (let i = 0; i < 50; i++) {
    const r = await query(5);
    rapidResults.push(r.serverUs);
  }
  const sorted = [...rapidResults].sort((a, b) => a - b);
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p95 = sorted[Math.floor(sorted.length * 0.95)];
  const p99 = sorted[Math.floor(sorted.length * 0.99)];
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  console.log(`  n=50  min=${min}us  p50=${p50}us  p95=${p95}us  p99=${p99}us  max=${max}us`);

  // --- Moderate popularity tags ---
  await section("6. Moderate tags (1000–1009) — medium cardinality");

  for (let i = 1000; i < 1010; i++) {
    const cold = await query(i);
    const warm = await query(i);
    console.log(`  tagId=${i}: cold=${cold.serverUs}us  warm=${warm.serverUs}us  matched=${cold.totalMatched}`);
  }

  // --- Summary ---
  await section("SUMMARY");

  const rareNonEmpty = rareTags.filter(r => r.totalMatched > 0);
  const rareWarmNonEmpty = rareWarm.filter(r => r.totalMatched > 0);
  const rareEmptyCount = rareTags.filter(r => r.totalMatched === 0).length;

  console.log(`  Popular tag (5) first load:    ${first5.serverUs} us`);
  console.log(`  Popular tag (5) cached:        ${cached5.serverUs} us`);
  console.log(`  Popular tag (5) steady:        ${steady5.serverUs} us`);
  console.log(`  Rapid-fire p50:                ${p50} us`);
  console.log(`  Rapid-fire p95:                ${p95} us`);
  if (rareNonEmpty.length > 0) {
    const avgCold = Math.round(rareNonEmpty.reduce((s, r) => s + r.serverUs, 0) / rareNonEmpty.length);
    const avgWarm = Math.round(rareWarmNonEmpty.reduce((s, r) => s + r.serverUs, 0) / rareWarmNonEmpty.length);
    console.log(`  Rare tags cold avg (non-empty): ${avgCold} us  (n=${rareNonEmpty.length})`);
    console.log(`  Rare tags warm avg (non-empty): ${avgWarm} us  (n=${rareWarmNonEmpty.length})`);
  }
  console.log(`  Rare tags empty count:         ${rareEmptyCount}/10`);

  const verdict =
    p50 < 1000
      ? "VERDICT: Sub-millisecond cached queries. Eviction+reload looks viable."
      : p50 < 10000
        ? "VERDICT: Low-millisecond queries. Eviction+reload may be viable with prefetch."
        : "VERDICT: High latency. Eviction+reload needs careful design.";
  console.log(`\n  ${verdict}`);
}

main().catch(console.error);
