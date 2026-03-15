//! Bench: batched Vec push → sorted roaring build, 8 threads, small dataset.
//! Uses HashMap (not pre-allocated Vec) to keep memory tight.
//!
//! Run: cargo run -p scratch --release --bin bench_batched_flush

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;
use roaring::RoaringBitmap;

const ROWS_PER_THREAD: usize = 2_000_000;
const NUM_TAGS: usize = 31_000;
const MAX_SLOT: u32 = 124_000_000;
const NUM_THREADS: usize = 8;
const BATCH_SIZE: usize = 500_000;

fn main() {
    println!("=== Batched Flush: {} threads × {}M rows, flush every {}K ===",
        NUM_THREADS, ROWS_PER_THREAD / 1_000_000, BATCH_SIZE / 1_000);
    println!("Total: {}M rows\n", ROWS_PER_THREAD * NUM_THREADS / 1_000_000);

    let start = Instant::now();
    let results: Vec<HashMap<u32, RoaringBitmap>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..NUM_THREADS).map(|t| {
            s.spawn(move || {
                let mut bitmaps: HashMap<u32, RoaringBitmap> = HashMap::new();
                let mut vecs: HashMap<u32, Vec<u32>> = HashMap::new();
                let mut rng: u64 = 42 + t as u64 * 7919;
                let mut count = 0usize;
                let mut flush_count = 0usize;

                for _ in 0..ROWS_PER_THREAD {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let tag = (rng >> 33) as u32 % NUM_TAGS as u32;
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let slot = (rng >> 32) as u32 % MAX_SLOT;

                    vecs.entry(tag).or_default().push(slot);
                    count += 1;

                    if count % BATCH_SIZE == 0 {
                        // Flush
                        for (tag_id, v) in vecs.iter_mut() {
                            if !v.is_empty() {
                                v.sort_unstable();
                                v.dedup();
                                let batch = RoaringBitmap::from_sorted_iter(v.drain(..)).unwrap();
                                bitmaps.entry(*tag_id).and_modify(|b| *b |= &batch).or_insert(batch);
                            }
                        }
                        flush_count += 1;
                        eprintln!("  thread {}: flush {} ({:.1}M rows)", t, flush_count, count as f64 / 1e6);
                    }
                }
                // Final flush
                for (tag_id, mut v) in vecs {
                    if !v.is_empty() {
                        v.sort_unstable();
                        v.dedup();
                        let batch = RoaringBitmap::from_sorted_iter(v.into_iter()).unwrap();
                        bitmaps.entry(tag_id).and_modify(|b| *b |= &batch).or_insert(batch);
                    }
                }
                eprintln!("  thread {} done: {} tags, {} flushes", t, bitmaps.len(), flush_count + 1);
                bitmaps
            })
        }).collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let parallel = start.elapsed();

    // Merge
    let merge_start = Instant::now();
    let mut merged: HashMap<u32, RoaringBitmap> = HashMap::new();
    for thread_map in results {
        for (tag, bm) in thread_map {
            merged.entry(tag).and_modify(|b| *b |= &bm).or_insert(bm);
        }
    }
    let merge_elapsed = merge_start.elapsed();
    let total = start.elapsed();

    let total_rows = ROWS_PER_THREAD * NUM_THREADS;
    let rate = total_rows as f64 / total.as_secs_f64();
    println!("\nRate: {:.1}M/s  (parallel {:.2}s, merge {:.2}s, total {:.2}s)",
        rate / 1e6, parallel.as_secs_f64(), merge_elapsed.as_secs_f64(), total.as_secs_f64());
    println!("Tags: {}", merged.len());
    println!("Extrapolated 4.48B: {:.0}s", 4_480_000_000.0 / rate);
    black_box(&merged);
}
