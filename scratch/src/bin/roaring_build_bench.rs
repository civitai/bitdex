/// Benchmark: fastest way to build RoaringBitmaps during bulk loading.
///
/// Context: The BitDex dump pipeline does ~467M individual insert() calls per sort field per
/// phase (32 bits × 14.6M rows = 467M insertions). This is ~30% of parse time at 107M scale.
///
/// This benchmark measures 6 build strategies across 4 workloads:
///   Sequential 1M, Random scattered 1M, Sequential 10M, Random scattered 10M
///
/// Key finding from roaring-rs source analysis:
///   - insert()     => binary_search per call = O(log n_containers) per insert
///   - extend()     => caches current container key, avoids re-search on same container
///   - collect()    => identical to extend() (FromIterator delegates to extend)
///   - append()     => push_unchecked: only checks last() container = O(1), requires sorted input
///   - from_sorted_iter => wraps append(), same O(1) container cost
///
/// For slot IDs in [0, 110M):
///   - There are ceil(110M / 65536) = 1678 containers
///   - Random values frequently cross container boundaries → high binary_search cost
///   - Sort-then-insert means each boundary crossing is encountered once, then amortized
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use roaring::RoaringBitmap;
use std::hint::black_box;
use std::time::{Duration, Instant};

const ITERS: u32 = 100;
const UNIVERSE: u32 = 10_000_000; // 10M universe for random scatter (realistic slot range for 1M test)
const UNIVERSE_LARGE: u32 = 110_000_000; // 110M universe for 10M test (production-like density)

fn gen_sequential(n: usize) -> Vec<u32> {
    (0u32..n as u32).collect()
}

fn gen_random(n: usize, universe: u32) -> Vec<u32> {
    let mut rng = StdRng::seed_from_u64(0xdeadbeef_u64);
    (0..n).map(|_| rng.gen_range(0..universe)).collect()
}

fn gen_random_unique(n: usize, universe: u32) -> Vec<u32> {
    // Build a unique random set (realistic: each slot appears once per bit layer)
    use std::collections::HashSet;
    let mut rng = StdRng::seed_from_u64(0xdeadbeef_u64);
    let mut set = HashSet::with_capacity(n);
    while set.len() < n {
        set.insert(rng.gen_range(0..universe));
    }
    set.into_iter().collect()
}

// --- Approach A: individual insert() calls (current approach) ---
fn approach_a(values: &[u32]) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    for &v in values {
        bm.insert(v);
    }
    bm
}

// --- Approach B: collect() from iterator ---
fn approach_b(values: &[u32]) -> RoaringBitmap {
    values.iter().copied().collect()
}

// --- Approach C: insert_range for contiguous ranges, individual for scattered ---
// For the sort bitmap case, values within a single container (same high 16 bits) CAN be ranged,
// but we need to detect runs. This simulates the sequential case using full insert_range.
fn approach_c_sequential(values: &[u32]) -> RoaringBitmap {
    // For a perfectly sequential set, one insert_range covers everything
    let mut bm = RoaringBitmap::new();
    if values.is_empty() {
        return bm;
    }
    // Detect runs and insert_range each
    let mut start = values[0];
    let mut prev = values[0];
    for &v in &values[1..] {
        if v == prev + 1 {
            prev = v;
        } else {
            bm.insert_range(start..=prev);
            start = v;
            prev = v;
        }
    }
    bm.insert_range(start..=prev);
    bm
}

fn approach_c_scattered(values: &[u32]) -> RoaringBitmap {
    // For scattered values, run-detection over unsorted input finds no runs — falls back to insert()
    // This represents the real cost of "try insert_range, fall back to insert" for random data
    let mut bm = RoaringBitmap::new();
    if values.is_empty() {
        return bm;
    }
    let mut start = values[0];
    let mut prev = values[0];
    for &v in &values[1..] {
        if v == prev + 1 {
            prev = v;
        } else {
            if start == prev {
                bm.insert(start);
            } else {
                bm.insert_range(start..=prev);
            }
            start = v;
            prev = v;
        }
    }
    if start == prev {
        bm.insert(start);
    } else {
        bm.insert_range(start..=prev);
    }
    bm
}

// --- Approach D: sort first, then collect() ---
fn approach_d(values: &[u32]) -> RoaringBitmap {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted.iter().copied().collect()
}

// --- Approach E: sort first, then from_sorted_iter (uses append/push_unchecked) ---
fn approach_e(values: &[u32]) -> RoaringBitmap {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    RoaringBitmap::from_sorted_iter(sorted.iter().copied()).unwrap()
}

// --- Approach F: extend() (no sort, pre-allocates nothing but caches container key) ---
fn approach_f(values: &[u32]) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    bm.extend(values.iter().copied());
    bm
}

// --- Approach G: BONUS — sort + dedup + from_sorted_iter (dedup is free via from_sorted_iter) ---
// In the dump pipeline, sort layer bitmaps are sets — duplicates are possible if a slot appears
// multiple times in a phase. sort_unstable + dedup is a no-op cost when there are few/no dupes.
fn approach_g_sort_dedup_from_sorted(values: &[u32]) -> RoaringBitmap {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    RoaringBitmap::from_sorted_iter(sorted.iter().copied()).unwrap()
}

// --- Approach H: BONUS — pre-sort into per-container buckets, then bulk insert per bucket ---
// Avoids the sort allocation by bucketing via high 16 bits — but adds allocation overhead.
// Included to test whether the sort dominates or the insert dominates.
fn approach_h_bucket_insert(values: &[u32]) -> RoaringBitmap {
    // Group by high 16 bits (container key), sort each group, insert_range runs
    // This is O(n) bucketing + O(k log k) sort per container k
    let mut bm = RoaringBitmap::new();
    if values.is_empty() {
        return bm;
    }
    // Bucket into 1678 possible containers (for 0..110M universe)
    const N_BUCKETS: usize = 1680;
    let mut buckets: Vec<Vec<u16>> = vec![Vec::new(); N_BUCKETS];
    for &v in values {
        let hi = (v >> 16) as usize;
        let lo = (v & 0xFFFF) as u16;
        if hi < N_BUCKETS {
            buckets[hi].push(lo);
        }
    }
    for (hi, bucket) in buckets.iter_mut().enumerate() {
        if bucket.is_empty() {
            continue;
        }
        bucket.sort_unstable();
        bucket.dedup();
        let base = (hi as u32) << 16;
        let values_u32: Vec<u32> = bucket.iter().map(|&lo| base | lo as u32).collect();
        bm.extend(values_u32.iter().copied());
    }
    bm
}

fn bench<F: Fn(&[u32]) -> RoaringBitmap>(
    name: &str,
    values: &[u32],
    iters: u32,
    f: F,
) -> Duration {
    // Warmup
    let _ = black_box(f(values));
    let _ = black_box(f(values));

    let start = Instant::now();
    for _ in 0..iters {
        let _ = black_box(f(values));
    }
    let total = start.elapsed();
    let avg = total / iters;
    println!("  {:45} {:>10.3} ms/iter  ({:.1} M inserts/sec)",
        name,
        avg.as_secs_f64() * 1000.0,
        values.len() as f64 / avg.as_secs_f64() / 1_000_000.0
    );
    avg
}

fn run_workload(label: &str, values: &[u32], iters: u32, is_sequential: bool) {
    println!("\n--- {} ({} values, {} iters) ---", label, values.len(), iters);

    let a = bench("A: individual insert()", values, iters, approach_a);
    let b = bench("B: collect() [= extend unsorted]", values, iters, approach_b);
    if is_sequential {
        let c = bench("C: insert_range (run-detect, sequential)", values, iters, approach_c_sequential);
        let speedup_c = a.as_secs_f64() / c.as_secs_f64();
        println!("     C vs A speedup: {:.2}x", speedup_c);
    } else {
        let c = bench("C: insert_range fallback (scattered)", values, iters, approach_c_scattered);
        let speedup_c = a.as_secs_f64() / c.as_secs_f64();
        println!("     C vs A speedup: {:.2}x", speedup_c);
    }
    let d = bench("D: sort_unstable + collect()", values, iters, approach_d);
    let e = bench("E: sort_unstable + from_sorted_iter", values, iters, approach_e);
    let f_ = bench("F: extend() [explicit call]", values, iters, approach_f);
    let g = bench("G: sort + dedup + from_sorted_iter", values, iters, approach_g_sort_dedup_from_sorted);
    let h = bench("H: per-container bucket-sort + extend", values, iters, approach_h_bucket_insert);

    println!();
    println!("  Speedup vs A (individual insert):");
    println!("    B (collect/extend unsorted):   {:.2}x", a.as_secs_f64() / b.as_secs_f64());
    println!("    D (sort + collect):             {:.2}x", a.as_secs_f64() / d.as_secs_f64());
    println!("    E (sort + from_sorted_iter):    {:.2}x", a.as_secs_f64() / e.as_secs_f64());
    println!("    F (extend explicit):            {:.2}x", a.as_secs_f64() / f_.as_secs_f64());
    println!("    G (sort+dedup+from_sorted):     {:.2}x", a.as_secs_f64() / g.as_secs_f64());
    println!("    H (bucket-sort+extend):         {:.2}x", a.as_secs_f64() / h.as_secs_f64());
}

fn main() {
    println!("=== RoaringBitmap Build Strategy Benchmark ===");
    println!();
    println!("Context: BitDex dump pipeline builds sort bitmaps via ~467M individual insert()");
    println!("calls per sort field (32 bits × 14.6M rows). This benchmark finds the fastest");
    println!("alternative.");
    println!();
    println!("roaring-rs internal analysis:");
    println!("  insert()          => binary_search on containers Vec per call = O(log k)");
    println!("  extend()          => caches current container key, avoids re-search within container");
    println!("  from_sorted_iter  => push_unchecked: checks last() only = O(1) per insert");
    println!("  sort_unstable     => O(n log n) but cache-friendly, ~300 MB/s for u32");
    println!();

    // Workload 1: 1M sequential
    let seq_1m = gen_sequential(1_000_000);
    run_workload("1M sequential (0..1_000_000)", &seq_1m, ITERS, true);

    // Workload 2: 1M random scattered in 10M universe (~10% density)
    let rand_1m = gen_random_unique(1_000_000, UNIVERSE);
    run_workload("1M random unique in 0..10_000_000", &rand_1m, ITERS, false);

    // Workload 3: 10M sequential — scale test
    let seq_10m = gen_sequential(10_000_000);
    run_workload("10M sequential (0..10_000_000)", &seq_10m, 20, true);

    // Workload 4: 10M random scattered in 110M universe (production-like: ~9% density)
    println!("\nGenerating 10M unique random values in 0..110M universe (production-like)...");
    let rand_10m = gen_random_unique(10_000_000, UNIVERSE_LARGE);
    run_workload("10M random unique in 0..110_000_000 (production-like)", &rand_10m, 20, false);

    // --- Extra analysis: what fraction of time is sort vs insert? ---
    println!("\n=== Sort cost isolation (10M random, 20 iters) ===");
    {
        let values = &rand_10m;
        let iters = 20u32;

        // Time just the sort
        let mut dummy = values.to_vec();
        let start = Instant::now();
        for _ in 0..iters {
            dummy = values.to_vec();
            dummy.sort_unstable();
            let _ = black_box(&dummy);
        }
        let sort_only = start.elapsed() / iters;
        println!("  sort_unstable only (clone+sort):  {:.3} ms", sort_only.as_secs_f64() * 1000.0);

        // Time just the from_sorted_iter on pre-sorted data
        dummy.sort_unstable();
        let sorted = dummy.clone();
        let start = Instant::now();
        for _ in 0..iters {
            let _ = black_box(RoaringBitmap::from_sorted_iter(sorted.iter().copied()).unwrap());
        }
        let insert_sorted = start.elapsed() / iters;
        println!("  from_sorted_iter (already sorted): {:.3} ms", insert_sorted.as_secs_f64() * 1000.0);

        // Time just insert() on same data (unsorted)
        let start = Instant::now();
        for _ in 0..iters {
            let _ = black_box(approach_a(values));
        }
        let insert_unsorted = start.elapsed() / iters;
        println!("  insert() loop (unsorted):          {:.3} ms", insert_unsorted.as_secs_f64() * 1000.0);

        println!();
        println!("  Sort cost as fraction of E total: {:.1}%",
            sort_only.as_secs_f64() / (sort_only + insert_sorted).as_secs_f64() * 100.0);
        println!("  from_sorted_iter speedup vs insert(): {:.2}x",
            insert_unsorted.as_secs_f64() / insert_sorted.as_secs_f64());
    }

    // --- Production model: 32-bit sort field at 14.6M rows ---
    println!("\n=== Production model: 1 sort field, 32 bit layers, 14.6M rows ===");
    println!("(simulates one sort bitmap build pass in the images phase)");
    {
        // At 14.6M rows, each bit layer has ~7.3M set bits on average (half the rows have bit N set).
        // Values are scattered across 0..110M (slot IDs).
        // We simulate 32 bitmaps, each built from ~7.3M values.
        let n_bit_layers = 32usize;
        let rows = 14_600_000usize;
        let n_per_layer = rows / 2; // ~half set per bit on average

        println!("  Generating {} layers × {} values each...", n_bit_layers, n_per_layer);
        let layers: Vec<Vec<u32>> = (0..n_bit_layers)
            .map(|i| {
                // Each layer gets a different seed to simulate independent bit distributions
                let mut rng = StdRng::seed_from_u64(0xbeef0000_u64 + i as u64);
                let mut vals: Vec<u32> = (0..n_per_layer)
                    .map(|_| rng.gen_range(0..UNIVERSE_LARGE))
                    .collect();
                vals.sort_unstable();
                vals.dedup();
                vals
            })
            .collect();

        let total_inserts: usize = layers.iter().map(|l: &Vec<u32>| l.len()).sum();
        println!("  Total unique values across all layers: {} ({:.1}M)", total_inserts, total_inserts as f64 / 1_000_000.0);

        // A (insert loop) — production baseline
        let start = Instant::now();
        for layer in &layers {
            let _ = black_box(approach_a(layer));
        }
        let time_a = start.elapsed();
        println!("  A (insert loop):               {:>8.1} ms total  ({:.0}M inserts/sec)",
            time_a.as_secs_f64() * 1000.0,
            total_inserts as f64 / time_a.as_secs_f64() / 1_000_000.0);

        // E (sort + from_sorted_iter) — best bet for random data
        let start = Instant::now();
        for layer in &layers {
            let _ = black_box(approach_e(layer));
        }
        let time_e = start.elapsed();
        println!("  E (sort+from_sorted_iter):     {:>8.1} ms total  ({:.0}M inserts/sec)  speedup: {:.2}x",
            time_e.as_secs_f64() * 1000.0,
            total_inserts as f64 / time_e.as_secs_f64() / 1_000_000.0,
            time_a.as_secs_f64() / time_e.as_secs_f64());

        // G (sort + dedup + from_sorted_iter)
        let start = Instant::now();
        for layer in &layers {
            let _ = black_box(approach_g_sort_dedup_from_sorted(layer));
        }
        let time_g = start.elapsed();
        println!("  G (sort+dedup+from_sorted):    {:>8.1} ms total  ({:.0}M inserts/sec)  speedup: {:.2}x",
            time_g.as_secs_f64() * 1000.0,
            total_inserts as f64 / time_g.as_secs_f64() / 1_000_000.0,
            time_a.as_secs_f64() / time_g.as_secs_f64());

        // F (extend unsorted) — check if extend caching helps without sort cost
        let start = Instant::now();
        for layer in &layers {
            let _ = black_box(approach_f(layer));
        }
        let time_f = start.elapsed();
        println!("  F (extend unsorted):           {:>8.1} ms total  ({:.0}M inserts/sec)  speedup: {:.2}x",
            time_f.as_secs_f64() * 1000.0,
            total_inserts as f64 / time_f.as_secs_f64() / 1_000_000.0,
            time_a.as_secs_f64() / time_f.as_secs_f64());
    }

    println!("\n=== Summary ===");
    println!("See printed speedup numbers above for recommendations.");
    println!("Expected winner for dump pipeline (random slot IDs): approach E or G.");
    println!("insert() has O(log k) binary search per call; from_sorted_iter has O(1) push_unchecked.");
    println!("For 110M universe, k = ~1678 containers => binary search = ~11 comparisons per insert.");
}
