/// Microbenchmark for dump pipeline bottleneck analysis.
///
/// Simulates the tags dump pipeline at different scales to measure:
/// 1. CSV line parsing throughput
/// 2. Bitmap insertion throughput
/// 3. Vec<RoaringBitmap> allocation overhead (300K entries × N threads)
/// 4. Parallel reduce/merge throughput
/// 5. Overall pipeline throughput
///
/// Goal: identify which phase is the bottleneck at 5.5M rows/sec target.
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::hint::black_box;
use std::time::Instant;

const MAX_TAG_ID: usize = 30_000; // 28K real distinct tags
const SLOTS: u32 = 109_000_000; // 109M records

fn main() {
    println!("=== Dump Pipeline Microbenchmark ===\n");

    // Generate synthetic tag data: each slot has ~40 tags (4.5B / 109M ≈ 41)
    let num_rows: usize = 10_000_000; // 10M rows for quick benchmarking
    println!("Generating {} synthetic tag rows...", num_rows);
    let t = Instant::now();
    let data: Vec<(u32, u16)> = (0..num_rows)
        .map(|i| {
            let slot = (i as u32 * 7 + 13) % SLOTS; // scattered slots
            let tag = (i as u16 * 31 + 5) % MAX_TAG_ID as u16;
            (slot, tag)
        })
        .collect();
    println!("  Generated in {:.1}ms\n", t.elapsed().as_secs_f64() * 1000.0);

    // ── Bench 1: CSV line parsing throughput ──
    // Simulate parsing "imageId,tagId\n12345,678\n..." lines
    println!("--- Bench 1: CSV Line Parsing ---");
    let csv_data: String = data.iter().map(|(s, t)| format!("{},{}\n", s, t)).collect();
    let csv_bytes = csv_data.as_bytes();
    println!("  CSV size: {:.1} MB", csv_bytes.len() as f64 / 1e6);

    let t = Instant::now();
    let mut parse_count = 0u64;
    for line in csv_bytes.split(|&b| b == b'\n') {
        if line.is_empty() { continue; }
        // Fast two-column parse (no allocation)
        if let Some(comma) = line.iter().position(|&b| b == b',') {
            let _slot = fast_parse_u32(&line[..comma]);
            let _tag = fast_parse_u32(&line[comma+1..]);
            parse_count += 1;
        }
    }
    let parse_elapsed = t.elapsed();
    let parse_rate = parse_count as f64 / parse_elapsed.as_secs_f64();
    println!("  {} rows in {:.1}ms ({:.1}M rows/sec)\n",
        parse_count, parse_elapsed.as_secs_f64() * 1000.0, parse_rate / 1e6);

    // ── Bench 2: Bitmap insertion throughput (single thread) ──
    println!("--- Bench 2: Bitmap Insertion (single thread) ---");
    let t = Instant::now();
    let mut bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect();
    for &(slot, tag) in &data {
        bitmaps[tag as usize].insert(slot);
    }
    let insert_elapsed = t.elapsed();
    let insert_rate = data.len() as f64 / insert_elapsed.as_secs_f64();
    println!("  {} inserts in {:.1}ms ({:.1}M/sec)",
        data.len(), insert_elapsed.as_secs_f64() * 1000.0, insert_rate / 1e6);
    let non_empty = bitmaps.iter().filter(|b| !b.is_empty()).count();
    println!("  {} non-empty bitmaps\n", non_empty);

    // ── Bench 3: Vec<RoaringBitmap> allocation cost ──
    println!("--- Bench 3: Vec<RoaringBitmap> Allocation (32 threads) ---");
    let t = Instant::now();
    let num_threads = 32usize;
    let vecs: Vec<Vec<RoaringBitmap>> = (0..num_threads)
        .into_par_iter()
        .map(|_| (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect())
        .collect();
    let alloc_elapsed = t.elapsed();
    let total_bitmaps = num_threads * MAX_TAG_ID;
    println!("  {} threads × {}K bitmaps = {} total in {:.1}ms",
        num_threads, MAX_TAG_ID / 1000, total_bitmaps,
        alloc_elapsed.as_secs_f64() * 1000.0);
    // Estimate memory
    let empty_size = std::mem::size_of::<RoaringBitmap>();
    println!("  Estimated memory: {:.1} MB ({}B × {})\n",
        (total_bitmaps * empty_size) as f64 / 1e6, empty_size, total_bitmaps);
    drop(vecs);

    // ── Bench 4: Full pipeline (parse + insert + merge) parallel ──
    println!("--- Bench 4: Full Pipeline (parallel parse + bitmap insert + reduce) ---");
    let num_threads = rayon::current_num_threads();
    println!("  Using {} rayon threads", num_threads);

    let chunk_size = data.len() / num_threads;
    let t = Instant::now();

    // Phase A: parallel parse + insert (simulating the .collect() path)
    let ta = Instant::now();
    let thread_results: Vec<Vec<RoaringBitmap>> = (0..num_threads)
        .into_par_iter()
        .map(|tid| {
            let start = tid * chunk_size;
            let end = if tid == num_threads - 1 { data.len() } else { (tid + 1) * chunk_size };
            let mut bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect();
            for &(slot, tag) in &data[start..end] {
                bitmaps[tag as usize].insert(slot);
            }
            bitmaps
        })
        .collect();
    let phase_a = ta.elapsed();
    println!("  Phase A (parse+insert): {:.1}ms", phase_a.as_secs_f64() * 1000.0);

    // Phase B: reduce (merge all thread bitmaps)
    let tb = Instant::now();
    let merged = thread_results
        .into_par_iter()
        .reduce(
            || (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect::<Vec<_>>(),
            |mut dst, src| {
                for (i, bm) in src.into_iter().enumerate() {
                    if !bm.is_empty() {
                        dst[i] |= bm;
                    }
                }
                dst
            },
        );
    let phase_b = tb.elapsed();
    println!("  Phase B (reduce):       {:.1}ms", phase_b.as_secs_f64() * 1000.0);

    let total_elapsed = t.elapsed();
    let total_rate = data.len() as f64 / total_elapsed.as_secs_f64();
    println!("  Total: {:.1}ms ({:.1}M rows/sec)", total_elapsed.as_secs_f64() * 1000.0, total_rate / 1e6);
    let non_empty = merged.iter().filter(|b| !b.is_empty()).count();
    println!("  {} non-empty bitmaps\n", non_empty);
    black_box(&merged);

    // ── Bench 5: Alternative — DashMap instead of collect+reduce ──
    println!("--- Bench 5: DashMap Alternative (no collect, direct shared insert) ---");
    let shared: dashmap::DashMap<u16, RoaringBitmap> = dashmap::DashMap::new();
    // Pre-populate keys to avoid lock contention on insert
    for i in 0..MAX_TAG_ID as u16 {
        shared.insert(i, RoaringBitmap::new());
    }

    let t = Instant::now();
    data.par_chunks(chunk_size.max(1))
        .for_each(|chunk| {
            for &(slot, tag) in chunk {
                if let Some(mut bm) = shared.get_mut(&tag) {
                    bm.insert(slot);
                }
            }
        });
    let dashmap_elapsed = t.elapsed();
    let dashmap_rate = data.len() as f64 / dashmap_elapsed.as_secs_f64();
    println!("  {} rows in {:.1}ms ({:.1}M rows/sec)\n",
        data.len(), dashmap_elapsed.as_secs_f64() * 1000.0, dashmap_rate / 1e6);
    black_box(&shared);

    // ── Bench 6: Streaming reduce (no intermediate Vec<Vec<RoaringBitmap>>) ──
    println!("--- Bench 6: Streaming Reduce (fold+reduce, no .collect()) ---");
    let t = Instant::now();
    let merged2: Vec<RoaringBitmap> = (0..num_threads)
        .into_par_iter()
        .map(|tid| {
            let start = tid * chunk_size;
            let end = if tid == num_threads - 1 { data.len() } else { (tid + 1) * chunk_size };
            let mut bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect();
            for &(slot, tag) in &data[start..end] {
                bitmaps[tag as usize].insert(slot);
            }
            bitmaps
        })
        .reduce(
            || (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect::<Vec<_>>(),
            |mut dst, src| {
                for (i, bm) in src.into_iter().enumerate() {
                    if !bm.is_empty() {
                        dst[i] |= bm;
                    }
                }
                dst
            },
        );
    let streaming_elapsed = t.elapsed();
    let streaming_rate = data.len() as f64 / streaming_elapsed.as_secs_f64();
    println!("  {} rows in {:.1}ms ({:.1}M rows/sec)\n",
        data.len(), streaming_elapsed.as_secs_f64() * 1000.0, streaming_rate / 1e6);
    black_box(&merged2);

    // ── Summary ──
    println!("=== SUMMARY ===");
    println!("  Parse only:          {:.1}M rows/sec", parse_rate / 1e6);
    println!("  Insert only:         {:.1}M rows/sec", insert_rate / 1e6);
    println!("  Collect+Reduce:      {:.1}M rows/sec", total_rate / 1e6);
    println!("  DashMap shared:      {:.1}M rows/sec", dashmap_rate / 1e6);
    println!("  Streaming reduce:    {:.1}M rows/sec", streaming_rate / 1e6);
    println!("  Target:              5.5M rows/sec");
    println!("");
    println!("  At 4.5B rows:");
    println!("    Collect+Reduce:  {:.0}s", 4.5e9 / total_rate);
    println!("    DashMap:         {:.0}s", 4.5e9 / dashmap_rate);
    println!("    Streaming:       {:.0}s", 4.5e9 / streaming_rate);
    println!("    Target (5.5M/s): {:.0}s", 4.5e9 / 5.5e6);
}

fn fast_parse_u32(bytes: &[u8]) -> u32 {
    let mut result = 0u32;
    for &b in bytes {
        if b >= b'0' && b <= b'9' {
            result = result * 10 + (b - b'0') as u32;
        }
    }
    result
}
