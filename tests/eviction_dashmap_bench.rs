//! Microbenchmark: DashMap stamping overhead in the query hot path.
//!
//! Measures the cost of stamping `DashMap<(String, u64), AtomicU64>` entries,
//! which is the proposed eviction tracking mechanism. The go/no-go threshold
//! is <750ns per stamp to stay well under 10% overhead on 11us cached queries.
//!
//! Run: cargo test --test eviction_dashmap_bench --release -- --nocapture

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const NUM_ENTRIES: u64 = 31_000; // tagIds scale
const ITERATIONS: u64 = 1_000_000;
const NUM_THREADS: usize = 4;

fn make_key(i: u64) -> (String, u64) {
    // Reuse a fixed field name to simulate the real pattern (one field, many values)
    ("tagIds".to_string(), i)
}

/// Pre-populate a DashMap with 31K entries.
fn build_dashmap() -> DashMap<(String, u64), AtomicU64> {
    let map = DashMap::with_capacity(NUM_ENTRIES as usize);
    for i in 0..NUM_ENTRIES {
        map.insert(make_key(i), AtomicU64::new(0));
    }
    map
}

/// Pre-populate a HashMap with 31K entries.
fn build_hashmap() -> HashMap<(String, u64), u64> {
    let mut map = HashMap::with_capacity(NUM_ENTRIES as usize);
    for i in 0..NUM_ENTRIES {
        map.insert(make_key(i), 0u64);
    }
    map
}

/// Simple xorshift64 for deterministic, fast random numbers (no allocations).
struct Xorshift64(u64);

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

#[test]
fn eviction_dashmap_bench() {
    println!("\n{}", "=".repeat(70));
    println!("  Eviction DashMap Stamping Microbenchmark");
    println!("  Entries: {NUM_ENTRIES}  |  Iterations: {ITERATIONS}  |  Threads: {NUM_THREADS}");
    println!("{}\n", "=".repeat(70));

    // ---------------------------------------------------------------
    // 1. DashMap: get() + relaxed store (existing entry lookup)
    // ---------------------------------------------------------------
    {
        let map = build_dashmap();
        let mut rng = Xorshift64::new(0xDEAD_BEEF);
        let cycle: u64 = 42;

        // Warm up
        for _ in 0..10_000 {
            let idx = rng.next() % NUM_ENTRIES;
            if let Some(entry) = map.get(&make_key(idx)) {
                entry.value().store(cycle, Ordering::Relaxed);
            }
        }

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let idx = rng.next() % NUM_ENTRIES;
            if let Some(entry) = map.get(&make_key(idx)) {
                entry.value().store(cycle, Ordering::Relaxed);
            }
        }
        let elapsed = start.elapsed();
        let ns_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;

        println!("1) DashMap get() + store          : {ns_per_op:>8.1} ns/op   ({:.2}ms total)", elapsed.as_secs_f64() * 1000.0);
        assert!(ns_per_op < 750.0, "FAIL: DashMap get+store {ns_per_op:.1}ns exceeds 750ns threshold");
    }

    // ---------------------------------------------------------------
    // 2. DashMap: entry().or_insert_with() + store (actual code path)
    // ---------------------------------------------------------------
    {
        let map = build_dashmap();
        let mut rng = Xorshift64::new(0xCAFE_BABE);
        let cycle: u64 = 42;

        // Warm up
        for _ in 0..10_000 {
            let idx = rng.next() % NUM_ENTRIES;
            map.entry(make_key(idx))
                .or_insert_with(|| AtomicU64::new(cycle))
                .store(cycle, Ordering::Relaxed);
        }

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let idx = rng.next() % NUM_ENTRIES;
            map.entry(make_key(idx))
                .or_insert_with(|| AtomicU64::new(cycle))
                .store(cycle, Ordering::Relaxed);
        }
        let elapsed = start.elapsed();
        let ns_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;

        println!("2) DashMap entry().or_insert+store : {ns_per_op:>8.1} ns/op   ({:.2}ms total)", elapsed.as_secs_f64() * 1000.0);
        assert!(ns_per_op < 750.0, "FAIL: DashMap entry+store {ns_per_op:.1}ns exceeds 750ns threshold");
    }

    // ---------------------------------------------------------------
    // 3. DashMap: 4-thread concurrent stamping (entry pattern)
    // ---------------------------------------------------------------
    {
        let map = Arc::new(build_dashmap());
        let cycle: u64 = 42;

        // Warm up
        {
            let mut rng = Xorshift64::new(0x1234);
            for _ in 0..10_000 {
                let idx = rng.next() % NUM_ENTRIES;
                map.entry(make_key(idx))
                    .or_insert_with(|| AtomicU64::new(cycle))
                    .store(cycle, Ordering::Relaxed);
            }
        }

        let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));
        let mut handles = Vec::new();

        for t in 0..NUM_THREADS {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let mut rng = Xorshift64::new(0xBEEF_0000 + t as u64);
                barrier.wait();
                let start = Instant::now();
                for _ in 0..ITERATIONS {
                    let idx = rng.next() % NUM_ENTRIES;
                    map.entry(make_key(idx))
                        .or_insert_with(|| AtomicU64::new(cycle))
                        .store(cycle, Ordering::Relaxed);
                }
                start.elapsed()
            }));
        }

        let thread_times: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let max_elapsed = thread_times.iter().max().unwrap();
        let total_ops = ITERATIONS * NUM_THREADS as u64;
        let ns_per_op = max_elapsed.as_nanos() as f64 / ITERATIONS as f64;
        let throughput = total_ops as f64 / max_elapsed.as_secs_f64();

        println!("3) DashMap {NUM_THREADS}-thread entry+store    : {ns_per_op:>8.1} ns/op   ({:.2}ms wall, {:.1}M ops/s)",
            max_elapsed.as_secs_f64() * 1000.0, throughput / 1e6);
        assert!(ns_per_op < 750.0, "FAIL: DashMap concurrent {ns_per_op:.1}ns exceeds 750ns threshold");
    }

    // ---------------------------------------------------------------
    // 4. Baseline: Mutex<HashMap> single-threaded
    // ---------------------------------------------------------------
    {
        let map = Mutex::new(build_hashmap());
        let mut rng = Xorshift64::new(0xF00D_FACE);
        let cycle: u64 = 42;

        // Warm up
        for _ in 0..10_000 {
            let idx = rng.next() % NUM_ENTRIES;
            let mut guard = map.lock().unwrap();
            guard.insert(make_key(idx), cycle);
        }

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let idx = rng.next() % NUM_ENTRIES;
            let mut guard = map.lock().unwrap();
            guard.insert(make_key(idx), cycle);
        }
        let elapsed = start.elapsed();
        let ns_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;

        println!("4) Mutex<HashMap> single-thread    : {ns_per_op:>8.1} ns/op   ({:.2}ms total)", elapsed.as_secs_f64() * 1000.0);
    }

    // ---------------------------------------------------------------
    // 5. Baseline: Mutex<HashMap> 4-thread concurrent
    // ---------------------------------------------------------------
    {
        let map = Arc::new(Mutex::new(build_hashmap()));
        let cycle: u64 = 42;
        let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));
        let mut handles = Vec::new();

        for t in 0..NUM_THREADS {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let mut rng = Xorshift64::new(0xBEEF_0000 + t as u64);
                barrier.wait();
                let start = Instant::now();
                for _ in 0..ITERATIONS {
                    let idx = rng.next() % NUM_ENTRIES;
                    let mut guard = map.lock().unwrap();
                    guard.insert(make_key(idx), cycle);
                }
                start.elapsed()
            }));
        }

        let thread_times: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let max_elapsed = thread_times.iter().max().unwrap();
        let total_ops = ITERATIONS * NUM_THREADS as u64;
        let ns_per_op = max_elapsed.as_nanos() as f64 / ITERATIONS as f64;
        let throughput = total_ops as f64 / max_elapsed.as_secs_f64();

        println!("5) Mutex<HashMap> {NUM_THREADS}-thread          : {ns_per_op:>8.1} ns/op   ({:.2}ms wall, {:.1}M ops/s)",
            max_elapsed.as_secs_f64() * 1000.0, throughput / 1e6);
    }

    // ---------------------------------------------------------------
    // Summary
    // ---------------------------------------------------------------
    println!("\n--- Verdict ---");
    println!("Threshold: <750ns/stamp to stay well under 10% overhead on 11us cached queries.");
    println!("If all DashMap tests passed, stamping is safe for the query hot path.\n");
}
