//! Microbench: isolate bottlenecks in tag bitmap building pipeline.
//!
//! Tests: pure bitmap insert, parse+insert, Vec alloc, merge cost,
//! and multi-threaded scaling (1/4/8/16/28 threads).
//!
//! Run: cargo run -p scratch --release --bin bench_tag_pipeline

use std::hint::black_box;
use std::time::Instant;
use roaring::RoaringBitmap;

const NUM_ROWS: usize = 50_000_000; // 50M rows per test
const NUM_TAGS: usize = 31_000;
const MAX_TAG_ID: usize = 300_000;
const MAX_SLOT: u32 = 124_000_000;

fn main() {
    println!("=== Tag Pipeline Microbench ===");
    println!("Rows per test: {}M", NUM_ROWS / 1_000_000);
    println!("Distinct tags: {}", NUM_TAGS);
    println!();

    // Pre-generate test data: (tag_id, slot) pairs
    let mut rng_state: u64 = 42;
    let tag_ids: Vec<u32> = (0..NUM_TAGS as u32).collect();
    let pairs: Vec<(u32, u32)> = (0..NUM_ROWS)
        .map(|_| {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let tag = tag_ids[(rng_state >> 33) as usize % NUM_TAGS];
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let slot = (rng_state >> 32) as u32 % MAX_SLOT;
            (tag, slot)
        })
        .collect();

    // --- Test 1: Pure bitmap insert (Vec<RoaringBitmap>, direct index) ---
    {
        let mut bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID)
            .map(|_| RoaringBitmap::new())
            .collect();

        let start = Instant::now();
        for &(tag, slot) in &pairs {
            bitmaps[tag as usize].insert(slot);
        }
        let elapsed = start.elapsed();
        let rate = NUM_ROWS as f64 / elapsed.as_secs_f64();
        println!("1. Pure Vec bitmap insert:     {:.1}M/s  ({:.1}s, {} ns/op)",
            rate / 1e6, elapsed.as_secs_f64(),
            elapsed.as_nanos() / NUM_ROWS as u128);
        black_box(&bitmaps);
        drop(bitmaps);
    }

    // --- Test 2: Parse + bitmap insert (simulated) ---
    // Simulate parsing two tab-separated ints from bytes
    {
        // Pre-build byte lines
        let lines: Vec<Vec<u8>> = pairs.iter().take(NUM_ROWS)
            .map(|(tag, slot)| format!("{}\t{}", tag, slot).into_bytes())
            .collect();

        let mut bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID)
            .map(|_| RoaringBitmap::new())
            .collect();

        let start = Instant::now();
        for line in &lines {
            if let Some(tab) = line.iter().position(|&b| b == b'\t') {
                let tag: u32 = fast_atoi(&line[..tab]);
                let slot: u32 = fast_atoi(&line[tab+1..]);
                if (tag as usize) < MAX_TAG_ID {
                    bitmaps[tag as usize].insert(slot);
                }
            }
        }
        let elapsed = start.elapsed();
        let rate = NUM_ROWS as f64 / elapsed.as_secs_f64();
        println!("2. Parse + Vec bitmap insert:  {:.1}M/s  ({:.1}s, {} ns/op)",
            rate / 1e6, elapsed.as_secs_f64(),
            elapsed.as_nanos() / NUM_ROWS as u128);
        black_box(&bitmaps);
        drop(bitmaps);
    }

    // --- Test 3: Vec<RoaringBitmap> allocation cost ---
    {
        let start = Instant::now();
        let bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID)
            .map(|_| RoaringBitmap::new())
            .collect();
        let elapsed = start.elapsed();
        println!("3. Vec alloc ({}K bitmaps):  {:.1}ms",
            MAX_TAG_ID / 1000, elapsed.as_secs_f64() * 1000.0);
        black_box(&bitmaps);
        drop(bitmaps);
    }

    // --- Test 4: Multi-threaded scaling ---
    println!("\n--- Multi-threaded scaling (pure bitmap insert, {}M rows) ---", NUM_ROWS / 1_000_000);
    for num_threads in [1, 4, 8, 16, 28] {
        let chunk_size = NUM_ROWS / num_threads;
        let pairs_ref = &pairs;

        let start = Instant::now();
        let thread_results: Vec<Vec<RoaringBitmap>> = (0..num_threads)
            .into_iter()
            .map(|t| {
                let range_start = t * chunk_size;
                let range_end = if t == num_threads - 1 { NUM_ROWS } else { range_start + chunk_size };
                (range_start, range_end)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|(rs, re)| {
                std::thread::scope(|_| {
                    // Can't easily use thread::scope with collect, use sequential for 1 thread
                    let mut bm: Vec<RoaringBitmap> = (0..MAX_TAG_ID)
                        .map(|_| RoaringBitmap::new())
                        .collect();
                    for &(tag, slot) in &pairs_ref[rs..re] {
                        bm[tag as usize].insert(slot);
                    }
                    bm
                })
            })
            .collect();

        // Merge
        let merge_start = Instant::now();
        let mut merged = thread_results.into_iter().reduce(|mut dst, src| {
            for (i, bm) in src.into_iter().enumerate() {
                if !bm.is_empty() {
                    dst[i] |= bm;
                }
            }
            dst
        }).unwrap();
        let merge_elapsed = merge_start.elapsed();
        let total_elapsed = start.elapsed();

        let rate = NUM_ROWS as f64 / total_elapsed.as_secs_f64();
        println!("  {:2} threads: {:.1}M/s  (total {:.1}s, merge {:.1}s)",
            num_threads, rate / 1e6, total_elapsed.as_secs_f64(), merge_elapsed.as_secs_f64());
        black_box(&merged);
        drop(merged);
    }

    // --- Test 5: Actual parallel with std::thread::scope ---
    println!("\n--- True parallel (std::thread::scope, {}M rows) ---", NUM_ROWS / 1_000_000);
    for num_threads in [4, 8, 16, 28] {
        let chunk_size = NUM_ROWS / num_threads;

        let start = Instant::now();
        let results: Vec<Vec<RoaringBitmap>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let rs = t * chunk_size;
                    let re = if t == num_threads - 1 { NUM_ROWS } else { rs + chunk_size };
                    let chunk = &pairs[rs..re];
                    s.spawn(move || {
                        let mut bm: Vec<RoaringBitmap> = (0..MAX_TAG_ID)
                            .map(|_| RoaringBitmap::new())
                            .collect();
                        for &(tag, slot) in chunk {
                            bm[tag as usize].insert(slot);
                        }
                        bm
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let parallel_elapsed = start.elapsed();

        // Merge
        let merge_start = Instant::now();
        let merged = results.into_iter().reduce(|mut dst, src| {
            for (i, bm) in src.into_iter().enumerate() {
                if !bm.is_empty() {
                    dst[i] |= bm;
                }
            }
            dst
        }).unwrap();
        let merge_elapsed = merge_start.elapsed();
        let total = start.elapsed();

        let rate = NUM_ROWS as f64 / total.as_secs_f64();
        println!("  {:2} threads: {:.1}M/s  (parallel {:.1}s, merge {:.1}s, total {:.1}s)",
            num_threads, rate / 1e6,
            parallel_elapsed.as_secs_f64(), merge_elapsed.as_secs_f64(),
            total.as_secs_f64());
        black_box(&merged);
    }

    // --- Test 6: Merge cost isolation ---
    println!("\n--- Merge cost (28 pre-built Vecs, 31K non-empty bitmaps each) ---");
    {
        let vecs: Vec<Vec<RoaringBitmap>> = (0..28)
            .map(|t| {
                let mut bm: Vec<RoaringBitmap> = (0..MAX_TAG_ID)
                    .map(|_| RoaringBitmap::new())
                    .collect();
                // Insert some data to make bitmaps non-trivial
                let base = t * (NUM_ROWS / 28);
                for i in 0..(NUM_ROWS / 28) {
                    let (tag, slot) = pairs[base + i];
                    bm[tag as usize].insert(slot);
                }
                bm
            })
            .collect();

        let start = Instant::now();
        let merged = vecs.into_iter().reduce(|mut dst, src| {
            for (i, bm) in src.into_iter().enumerate() {
                if !bm.is_empty() {
                    dst[i] |= bm;
                }
            }
            dst
        }).unwrap();
        let elapsed = start.elapsed();
        println!("  28-way merge: {:.1}s ({:.0} bitmaps OR'd)",
            elapsed.as_secs_f64(), NUM_TAGS as f64 * 27.0);
        black_box(&merged);
    }
}

#[inline]
fn fast_atoi(bytes: &[u8]) -> u32 {
    let mut n: u32 = 0;
    for &b in bytes {
        if b >= b'0' && b <= b'9' {
            n = n * 10 + (b - b'0') as u32;
        }
    }
    n
}
