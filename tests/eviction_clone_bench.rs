//! Microbenchmark: HashMap<u64, AtomicU64> clone cost for idle eviction design.
//!
//! During snapshot publish, BitDex clones FilterField which would contain a
//! HashMap<u64, AtomicU64> tracking last-access times per value. The clone must
//! load each AtomicU64 (Relaxed) and create a new AtomicU64 in the cloned map.
//! This happens on the flush thread hot path.
//!
//! Threshold: clone >1ms at 31K = problem for flush loop.
//!            clone >10ms at 553K = rules out eviction on userId.
//!
//! Run: cargo test --release --test eviction_clone_bench -- --nocapture

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Manually clone a HashMap<u64, AtomicU64> by loading each atomic (Relaxed)
/// and inserting into a new map.
fn clone_atomic_map(src: &HashMap<u64, AtomicU64>) -> HashMap<u64, AtomicU64> {
    let mut dst = HashMap::with_capacity(src.len());
    for (&k, v) in src.iter() {
        dst.insert(k, AtomicU64::new(v.load(Ordering::Relaxed)));
    }
    dst
}

/// Build a test map with `n` entries. Keys are hash-scattered to avoid
/// unrealistic sequential bucket layout.
fn build_map(n: u64) -> HashMap<u64, AtomicU64> {
    let prime: u64 = 2654435761;
    let now = 1_700_000_000u64; // fake epoch seconds
    let mut map = HashMap::with_capacity(n as usize);
    for i in 0..n {
        let key = i.wrapping_mul(prime);
        // Spread timestamps across last ~24h so eviction sweep has realistic filtering
        let ts = now - (i % 86400);
        map.insert(key, AtomicU64::new(ts));
    }
    map
}

/// Eviction sweep: iterate all entries, load atomic, collect keys older than threshold.
fn eviction_sweep(map: &HashMap<u64, AtomicU64>, threshold: u64) -> Vec<u64> {
    let mut evict = Vec::new();
    for (&k, v) in map.iter() {
        if v.load(Ordering::Relaxed) < threshold {
            evict.push(k);
        }
    }
    evict
}

fn run_clone_bench(label: &str, n: u64, iterations: u32) {
    let map = build_map(n);
    assert_eq!(map.len(), n as usize);

    // Warmup
    for _ in 0..5 {
        let _ = clone_atomic_map(&map);
    }

    let mut durations = Vec::with_capacity(iterations as usize);
    for _ in 0..iterations {
        let start = Instant::now();
        let cloned = clone_atomic_map(&map);
        let elapsed = start.elapsed();
        // Prevent optimizer from eliding the clone
        assert_eq!(cloned.len(), map.len());
        durations.push(elapsed);
    }

    durations.sort();
    let min = durations[0];
    let max = durations[durations.len() - 1];
    let p50 = durations[durations.len() / 2];
    let p99 = durations[(durations.len() as f64 * 0.99) as usize];
    let total: Duration = durations.iter().sum();
    let mean = total / iterations;

    println!(
        "  {label:>12} ({n:>7} entries): min={min:>8.1?}  p50={p50:>8.1?}  mean={mean:>8.1?}  p99={p99:>8.1?}  max={max:>8.1?}"
    );
}

fn run_sweep_bench(label: &str, n: u64, evict_fraction: f64, iterations: u32) {
    let map = build_map(n);
    let now = 1_700_000_000u64;
    // Threshold: entries with ts < threshold get evicted
    // evict_fraction of entries have ts in [now-86399, now], so set threshold to evict that fraction
    let threshold = now - ((1.0 - evict_fraction) * 86400.0) as u64;

    // Warmup
    for _ in 0..5 {
        let _ = eviction_sweep(&map, threshold);
    }

    let mut durations = Vec::with_capacity(iterations as usize);
    let mut evicted_count = 0;
    for _ in 0..iterations {
        let start = Instant::now();
        let evicted = eviction_sweep(&map, threshold);
        let elapsed = start.elapsed();
        evicted_count = evicted.len();
        durations.push(elapsed);
    }

    durations.sort();
    let min = durations[0];
    let max = durations[durations.len() - 1];
    let p50 = durations[durations.len() / 2];
    let p99 = durations[(durations.len() as f64 * 0.99) as usize];
    let total: Duration = durations.iter().sum();
    let mean = total / iterations;

    println!(
        "  {label:>12} ({n:>7} entries, {evicted_count:>6} evicted): min={min:>8.1?}  p50={p50:>8.1?}  mean={mean:>8.1?}  p99={p99:>8.1?}  max={max:>8.1?}"
    );
}

#[test]
fn bench_eviction_clone() {
    println!("\n=== Eviction HashMap<u64, AtomicU64> Clone Microbenchmark ===\n");

    println!("--- Clone (full map copy) — 1000 iterations ---");
    run_clone_bench("tagIds", 31_000, 1000);
    run_clone_bench("userId", 553_000, 1000);

    println!();
    println!("--- Eviction sweep (scan + filter) — 1000 iterations ---");
    // 10% eviction rate (typical idle cleanup)
    run_sweep_bench("tagIds 10%", 31_000, 0.10, 1000);
    run_sweep_bench("userId 10%", 553_000, 0.10, 1000);
    // 50% eviction rate (aggressive cleanup)
    run_sweep_bench("tagIds 50%", 31_000, 0.50, 1000);
    run_sweep_bench("userId 50%", 553_000, 0.50, 1000);

    println!();
    println!("--- Threshold analysis ---");
    println!("  Clone >1ms at 31K entries  → problem for flush loop");
    println!("  Clone >10ms at 553K entries → rules out eviction on userId");
    println!();
}
