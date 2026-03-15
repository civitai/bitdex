//! Compare bitmap insert strategies:
//! A) Direct roaring insert (random order) — current approach
//! B) Vec<u32> push + sort + from_sorted_iter — deferred roaring construction
//! C) Vec<u32> push (unsorted) + from_iter — no sort
//!
//! Run: cargo run -p scratch --release --bin bench_insert_strategy

use std::hint::black_box;
use std::time::Instant;
use roaring::RoaringBitmap;

const NUM_INSERTS: usize = 10_000_000; // 10M per tag (realistic for popular tags)
const MAX_SLOT: u32 = 124_000_000;
const NUM_TAGS: usize = 100; // test with 100 tags to see aggregate

fn main() {
    println!("=== Insert Strategy Comparison ===");
    println!("Inserts per tag: {}M, Tags: {}, Max slot: {}M\n",
        NUM_INSERTS / 1_000_000, NUM_TAGS, MAX_SLOT / 1_000_000);

    // Generate random (tag, slot) pairs
    let mut rng: u64 = 42;
    let pairs: Vec<(usize, u32)> = (0..NUM_INSERTS * NUM_TAGS / 10) // 100M total, 1M per tag avg
        .map(|_| {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let tag = (rng >> 33) as usize % NUM_TAGS;
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let slot = (rng >> 32) as u32 % MAX_SLOT;
            (tag, slot)
        })
        .collect();
    let total = pairs.len();
    println!("Total pairs: {}M\n", total / 1_000_000);

    // --- Strategy A: Direct roaring insert ---
    {
        let mut bitmaps: Vec<RoaringBitmap> = (0..NUM_TAGS)
            .map(|_| RoaringBitmap::new())
            .collect();

        let start = Instant::now();
        for &(tag, slot) in &pairs {
            bitmaps[tag].insert(slot);
        }
        let elapsed = start.elapsed();
        let rate = total as f64 / elapsed.as_secs_f64();
        println!("A) Direct roaring insert:       {:.1}M/s  ({:.1}s, {} ns/op)",
            rate / 1e6, elapsed.as_secs_f64(), elapsed.as_nanos() / total as u128);
        black_box(&bitmaps);
    }

    // --- Strategy B: Vec push + sort + from_sorted_iter ---
    {
        let mut vecs: Vec<Vec<u32>> = (0..NUM_TAGS).map(|_| Vec::new()).collect();

        let push_start = Instant::now();
        for &(tag, slot) in &pairs {
            vecs[tag].push(slot);
        }
        let push_elapsed = push_start.elapsed();

        let sort_start = Instant::now();
        for v in &mut vecs {
            v.sort_unstable();
            v.dedup();
        }
        let sort_elapsed = sort_start.elapsed();

        let build_start = Instant::now();
        let bitmaps: Vec<RoaringBitmap> = vecs.into_iter()
            .map(|v| RoaringBitmap::from_sorted_iter(v.into_iter()).unwrap())
            .collect();
        let build_elapsed = build_start.elapsed();

        let total_elapsed = push_start.elapsed();
        let rate = total as f64 / total_elapsed.as_secs_f64();
        println!("B) Vec push+sort+from_sorted:   {:.1}M/s  (push {:.1}s, sort {:.1}s, build {:.1}s, total {:.1}s, {} ns/op)",
            rate / 1e6, push_elapsed.as_secs_f64(), sort_elapsed.as_secs_f64(),
            build_elapsed.as_secs_f64(), total_elapsed.as_secs_f64(),
            total_elapsed.as_nanos() / total as u128);
        black_box(&bitmaps);
    }

    // --- Strategy C: Vec push (no sort) + from_iter ---
    {
        let mut vecs: Vec<Vec<u32>> = (0..NUM_TAGS).map(|_| Vec::new()).collect();

        let push_start = Instant::now();
        for &(tag, slot) in &pairs {
            vecs[tag].push(slot);
        }
        let push_elapsed = push_start.elapsed();

        let build_start = Instant::now();
        let bitmaps: Vec<RoaringBitmap> = vecs.into_iter()
            .map(|v| v.into_iter().collect::<RoaringBitmap>())
            .collect();
        let build_elapsed = build_start.elapsed();

        let total_elapsed = push_start.elapsed();
        let rate = total as f64 / total_elapsed.as_secs_f64();
        println!("C) Vec push+from_iter (unsort):  {:.1}M/s  (push {:.1}s, build {:.1}s, total {:.1}s, {} ns/op)",
            rate / 1e6, push_elapsed.as_secs_f64(),
            build_elapsed.as_secs_f64(), total_elapsed.as_secs_f64(),
            total_elapsed.as_nanos() / total as u128);
        black_box(&bitmaps);
    }

    // --- Strategy D: Just Vec push (no bitmap at all) - measures pure accumulation speed ---
    {
        let mut vecs: Vec<Vec<u32>> = (0..NUM_TAGS).map(|_| Vec::new()).collect();

        let start = Instant::now();
        for &(tag, slot) in &pairs {
            vecs[tag].push(slot);
        }
        let elapsed = start.elapsed();
        let rate = total as f64 / elapsed.as_secs_f64();
        println!("D) Pure Vec push (no bitmap):   {:.1}M/s  ({:.1}s, {} ns/op)",
            rate / 1e6, elapsed.as_secs_f64(), elapsed.as_nanos() / total as u128);

        let total_bytes: usize = vecs.iter().map(|v| v.len() * 4).sum();
        println!("   Memory: {:.1} GB", total_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
        black_box(&vecs);
    }
}
