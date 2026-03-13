// Poll bulk load progress endpoint
const port = process.argv[2] || 9092;
const url = `http://localhost:${port}/status`;

async function poll() {
  try {
    const res = await fetch(url);
    const s = await res.json();
    const lines = [];
    for (const [k, v] of Object.entries(s.streams)) {
      if (v.rows > 0 || v.done === false) {
        const done = v.done ? ' DONE' : '';
        lines.push(`  ${k}: ${v.rows.toLocaleString()} rows (${Math.round(v.rate).toLocaleString()}/s)${done}`);
      }
    }
    console.log(`Phase: ${s.phase} | Elapsed: ${Math.round(s.elapsed_secs)}s (${(s.elapsed_secs/60).toFixed(1)}m) | Done: ${s.streams_done}/5`);
    lines.forEach(l => console.log(l));
    const img = s.streams.images;
    if (img && img.rate > 0) {
      console.log(`  >> Images ETA: ${Math.round((105e6 - img.rows) / img.rate / 60)}m`);
    }
  } catch (e) {
    console.log('Connection error:', e.message);
  }
}

poll();
