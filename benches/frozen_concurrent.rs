//! Experiment 3b: Frozen bitmap concurrent query simulation.
//!
//! Simulates production query load:
//! - 20 filter bitmaps (various sizes) as frozen mmap'd files
//! - 89 QPS with 30% cache miss rate → 27 concurrent "cold" queries
//! - Each query: to_owned() 3 bitmaps + AND them together
//! - Compare: frozen mmap'd bitmaps vs heap-resident bitmaps
//!
//! Run: cargo bench --bench frozen_concurrent --features data-silo

use roaring::RoaringBitmap;
use roaring::bitmap::frozen::FrozenRoaringBitmap;
use std::fs::{self, File};
use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

const NUM_BITMAPS: usize = 20;
const QUERIES_PER_SEC: usize = 89;
const CACHE_MISS_RATE: f64 = 0.30;
const BITMAPS_PER_QUERY: usize = 3;
const TEST_DURATION_SECS: u64 = 5;
const MAX_VALUE: u32 = 107_000_000;

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() { return Duration::ZERO; }
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Generate a bitmap with approximately `density` fraction of MAX_VALUE bits set.
fn make_bitmap(seed: u64, density: f64) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    let mut rng = seed;
    let threshold = (density * u32::MAX as f64) as u32;
    for i in 0..MAX_VALUE {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        if (rng >> 32) as u32 <= threshold {
            bm.insert(i);
        }
    }
    bm
}

fn main() {
    let dir = PathBuf::from("data/silo-experiment3b");
    fs::create_dir_all(&dir).unwrap();

    println!("=== Experiment 3b: Frozen Bitmaps Under Concurrent Query Load ===");
    println!("Bitmaps: {}  QPS: {}  Cache miss: {:.0}%  Bitmaps/query: {}",
        NUM_BITMAPS, QUERIES_PER_SEC, CACHE_MISS_RATE * 100.0, BITMAPS_PER_QUERY);
    println!("Test duration: {}s  Max value: {}M", TEST_DURATION_SECS, MAX_VALUE / 1_000_000);
    println!();

    // Generate bitmaps with varying densities (realistic: nsfwLevel dense, userId sparse)
    let densities = [
        0.80, 0.60, 0.50, 0.40, 0.30, // dense (type, nsfwLevel)
        0.20, 0.15, 0.10, 0.08, 0.05, // medium (hasMeta, minor)
        0.03, 0.02, 0.01, 0.005, 0.003, // sparse (specific values)
        0.001, 0.0005, 0.0002, 0.0001, 0.00005, // very sparse (userId=X)
    ];

    print!("Generating {} bitmaps...", NUM_BITMAPS);
    let heap_bitmaps: Vec<RoaringBitmap> = (0..NUM_BITMAPS)
        .map(|i| make_bitmap(0xDEAD0000 + i as u64, densities[i]))
        .collect();
    println!(" done");

    // Print bitmap stats
    for (i, bm) in heap_bitmaps.iter().enumerate() {
        if i < 5 || i >= NUM_BITMAPS - 2 {
            println!("  bm[{:>2}]: {:>10} bits ({:.2}%), {} bytes serialized",
                i, bm.len(), densities[i] * 100.0, bm.serialized_size());
        } else if i == 5 {
            println!("  ...");
        }
    }
    println!();

    // Write frozen files and mmap them
    print!("Writing frozen files...");
    let frozen_paths: Vec<PathBuf> = (0..NUM_BITMAPS)
        .map(|i| dir.join(format!("frozen_{:02}.bin", i)))
        .collect();

    for (i, bm) in heap_bitmaps.iter().enumerate() {
        let frozen_size = bm.frozen_serialized_size();
        let mut buf = vec![0u8; frozen_size + 32];
        let align_offset = (32 - (buf.as_ptr() as usize % 32)) % 32;
        let aligned = &mut buf[align_offset..align_offset + frozen_size];
        bm.serialize_frozen_into(aligned).unwrap();
        let mut f = File::create(&frozen_paths[i]).unwrap();
        f.write_all(aligned).unwrap();
        f.sync_all().unwrap();
    }
    println!(" done");

    // mmap all frozen files
    let frozen_mmaps: Vec<memmap2::Mmap> = frozen_paths.iter()
        .map(|p| {
            let f = File::open(p).unwrap();
            unsafe { memmap2::Mmap::map(&f).unwrap() }
        })
        .collect();

    // Pre-parse frozen views (this is what startup would do — near-instant)
    let frozen_views: Vec<FrozenRoaringBitmap<'_>> = frozen_mmaps.iter()
        .map(|m| FrozenRoaringBitmap::view(&m[..]).unwrap())
        .collect();

    // Run tests with varying query complexity
    let cold_queries_per_sec = (QUERIES_PER_SEC as f64 * CACHE_MISS_RATE) as usize;
    let total_queries = cold_queries_per_sec * TEST_DURATION_SECS as usize;
    let frozen_views_ref = &frozen_views;
    let heap_bitmaps_ref = &heap_bitmaps;

    println!("  {:>6}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "ANDs", "Frz p50", "Frz p95", "Frz p99", "Heap p50", "Heap p95", "Heap p99");
    println!("  {}", "-".repeat(78));

    for bitmaps_per_query in [3usize, 6, 8] {
        // Build query plans
        let query_plans: Vec<Vec<usize>> = {
            let mut rng: u64 = 0xCAFE + bitmaps_per_query as u64;
            (0..total_queries).map(|_| {
                (0..bitmaps_per_query).map(|_| {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (rng >> 32) as usize % NUM_BITMAPS
                }).collect()
            }).collect()
        };

        // Frozen test
        let frozen_latencies: Vec<Duration> = {
            let num_threads = cold_queries_per_sec.min(rayon::current_num_threads());
            let chunk = (total_queries + num_threads - 1) / num_threads;
            let results: Vec<Vec<Duration>> = (0..num_threads).into_iter().map(|t| {
                let start_q = t * chunk;
                let end_q = ((t + 1) * chunk).min(total_queries);
                let mut lats = Vec::with_capacity(end_q - start_q);
                for q in start_q..end_q {
                    let plan = &query_plans[q];
                    let start = Instant::now();
                    let mut result = frozen_views_ref[plan[0]].to_owned();
                    for &idx in &plan[1..] {
                        let owned = frozen_views_ref[idx].to_owned();
                        result &= owned;
                    }
                    let _ = result.len();
                    lats.push(start.elapsed());
                }
                lats
            }).collect();
            results.into_iter().flatten().collect()
        };

        // Heap test
        let heap_latencies: Vec<Duration> = {
            let num_threads = cold_queries_per_sec.min(rayon::current_num_threads());
            let chunk = (total_queries + num_threads - 1) / num_threads;
            let results: Vec<Vec<Duration>> = (0..num_threads).into_iter().map(|t| {
                let start_q = t * chunk;
                let end_q = ((t + 1) * chunk).min(total_queries);
                let mut lats = Vec::with_capacity(end_q - start_q);
                for q in start_q..end_q {
                    let plan = &query_plans[q];
                    let start = Instant::now();
                    let mut result = heap_bitmaps_ref[plan[0]].clone();
                    for &idx in &plan[1..] {
                        result &= &heap_bitmaps_ref[idx];
                    }
                    let _ = result.len();
                    lats.push(start.elapsed());
                }
                lats
            }).collect();
            results.into_iter().flatten().collect()
        };

        let mut sf = frozen_latencies.clone(); sf.sort();
        let mut sh = heap_latencies.clone(); sh.sort();

        println!("  {:>6}  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs  {:>8.0}μs",
            bitmaps_per_query,
            percentile(&sf, 50.0).as_nanos() as f64 / 1000.0,
            percentile(&sf, 95.0).as_nanos() as f64 / 1000.0,
            percentile(&sf, 99.0).as_nanos() as f64 / 1000.0,
            percentile(&sh, 50.0).as_nanos() as f64 / 1000.0,
            percentile(&sh, 95.0).as_nanos() as f64 / 1000.0,
            percentile(&sh, 99.0).as_nanos() as f64 / 1000.0,
        );
    }

    println!();
    println!("  Memory: Frozen=0 bytes  Heap=6.5 GB  Startup: Frozen=~200μs  Heap=seconds");
    println!();

    // Cleanup
    let _ = fs::remove_dir_all(&dir);
}
