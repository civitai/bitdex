/// Benchmark post-pass bitmap inversion with synthetic data at production scale.
/// 109M slots, 28K distinct tags, ~40 tags per slot average.
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const MAX_SLOT: u32 = 126_000_000; // realistic max (not u32::MAX)
const NUM_TAGS: usize = 28_000;
const SHARD_SIZE: u32 = 1_000_000;

fn main() {
    println!("=== Synthetic Post-Pass Benchmark ===");
    println!("  Slots: {}M, Tags: {}K, Shard: {}M\n", MAX_SLOT / 1_000_000, NUM_TAGS / 1000, SHARD_SIZE / 1_000_000);

    // Build synthetic bitmaps: each tag has ~4000 random slots (109M × 40 tags / 28K tags)
    println!("Building synthetic bitmaps...");
    let t = Instant::now();
    let mut bitmaps: Vec<RoaringBitmap> = Vec::with_capacity(NUM_TAGS);
    let mut rng_state: u64 = 12345;
    let slots_per_tag = 4000usize; // avg

    for _ in 0..NUM_TAGS {
        let mut bm = RoaringBitmap::new();
        for _ in 0..slots_per_tag {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let slot = (rng_state >> 33) as u32 % MAX_SLOT;
            bm.insert(slot);
        }
        bitmaps.push(bm);
    }
    let non_empty: Vec<usize> = (0..NUM_TAGS).filter(|&i| !bitmaps[i].is_empty()).collect();
    let total_bits: u64 = bitmaps.iter().map(|b| b.len()).sum();
    println!("  {} bitmaps, {} total bits in {:.1}s\n", non_empty.len(), total_bits, t.elapsed().as_secs_f64());

    // Pre-compute tag ranges
    let tag_ranges: Vec<(usize, u32, u32)> = non_empty.iter()
        .filter_map(|&tag| {
            let bm = &bitmaps[tag];
            Some((tag, bm.min()?, bm.max()?))
        })
        .collect();

    let num_shards = (MAX_SLOT / SHARD_SIZE) + 1;
    println!("Shards: {}\n", num_shards);

    // Sequential post-pass
    println!("--- Sequential Post-Pass ---");
    let t = Instant::now();
    let mut seq_docs = 0u64;
    let mut seq_tags = 0u64;

    for shard_idx in 0..num_shards {
        let shard_start = shard_idx * SHARD_SIZE;
        let shard_end = shard_start + SHARD_SIZE;

        let relevant: Vec<usize> = tag_ranges.iter()
            .filter(|&&(_, min, max)| max >= shard_start && min < shard_end)
            .map(|&(tag, _, _)| tag)
            .collect();
        if relevant.is_empty() { continue; }

        let mut counts = vec![0u32; SHARD_SIZE as usize];
        for &tag_id in &relevant {
            for slot in bitmaps[tag_id].iter() {
                if slot < shard_start { continue; }
                if slot >= shard_end { break; }
                counts[(slot - shard_start) as usize] += 1;
            }
        }

        let total: u32 = counts.iter().sum();
        seq_tags += total as u64;
        seq_docs += counts.iter().filter(|&&c| c > 0).count() as u64;
        black_box(&counts);
    }
    let seq_time = t.elapsed();
    println!("  {} docs, {} tags in {:.1}s ({:.0} docs/sec)\n",
        seq_docs, seq_tags, seq_time.as_secs_f64(),
        seq_docs as f64 / seq_time.as_secs_f64());

    // Parallel post-pass
    println!("--- Parallel Post-Pass ({} threads) ---", rayon::current_num_threads());
    let t = Instant::now();
    let par_docs = AtomicU64::new(0);
    let par_tags = AtomicU64::new(0);

    (0..num_shards).into_par_iter().for_each(|shard_idx| {
        let shard_start = shard_idx * SHARD_SIZE;
        let shard_end = shard_start + SHARD_SIZE;

        let relevant: Vec<usize> = tag_ranges.iter()
            .filter(|&&(_, min, max)| max >= shard_start && min < shard_end)
            .map(|&(tag, _, _)| tag)
            .collect();
        if relevant.is_empty() { return; }

        let mut counts = vec![0u32; SHARD_SIZE as usize];
        for &tag_id in &relevant {
            for slot in bitmaps[tag_id].iter() {
                if slot < shard_start { continue; }
                if slot >= shard_end { break; }
                counts[(slot - shard_start) as usize] += 1;
            }
        }

        let total: u32 = counts.iter().sum();
        par_tags.fetch_add(total as u64, Ordering::Relaxed);
        par_docs.fetch_add(counts.iter().filter(|&&c| c > 0).count() as u64, Ordering::Relaxed);
        black_box(&counts);
    });
    let par_time = t.elapsed();
    let pd = par_docs.load(Ordering::Relaxed);
    println!("  {} docs, {} tags in {:.1}s ({:.0} docs/sec)",
        pd, par_tags.load(Ordering::Relaxed), par_time.as_secs_f64(),
        pd as f64 / par_time.as_secs_f64());
    println!("  Speedup: {:.1}x\n", seq_time.as_secs_f64() / par_time.as_secs_f64());

    println!("=== At full scale (109M slots, 4.5B tag entries) ===");
    let scale = 4.5e9 / total_bits as f64;
    println!("  Sequential: {:.0}s ({:.1} min)", seq_time.as_secs_f64() * scale, seq_time.as_secs_f64() * scale / 60.0);
    println!("  Parallel:   {:.0}s ({:.1} min)", par_time.as_secs_f64() * scale, par_time.as_secs_f64() * scale / 60.0);
}
