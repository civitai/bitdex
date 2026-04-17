//! Write-path microbenchmark: isolate where time goes in shard append operations.
//!
//! Tests:
//! 1. Raw fsync cost — open pre-existing files, append bytes, fsync
//! 2. Batch fsync — append N ops then one fsync
//! 3. No-fsync ceiling — skip fsync entirely
//! 4. File open/close overhead — open+close without writing
//! 5. DashMap lock overhead — acquire/release parking_lot RwLock
//! 6. Codec encode overhead — encode ops to bytes

use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const BENCH_DIR: &str = "C:/Dev/Repos/open-source/bitdex-v2/scratch/bench_tmp";
const NUM_SHARDS: usize = 500;
const OPS_PER_SHARD: usize = 5;
const OP_PAYLOAD_BYTES: usize = 100; // Typical small op entry size

fn setup_shard_files(dir: &Path, count: usize) -> Vec<PathBuf> {
    fs::create_dir_all(dir).unwrap();
    let mut paths = Vec::with_capacity(count);
    for i in 0..count {
        let p = dir.join(format!("shard_{:04}.bin", i));
        // Write a minimal header (28 bytes like ShardStore) + some initial data
        let mut f = File::create(&p).unwrap();
        let header = [0u8; 28];
        f.write_all(&header).unwrap();
        f.sync_all().unwrap();
        paths.push(p);
    }
    paths
}

fn cleanup(dir: &Path) {
    let _ = fs::remove_dir_all(dir);
}

fn make_payload(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}

fn median(times: &mut Vec<f64>) -> f64 {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}

fn fmt(times: &[f64]) -> String {
    times.iter().map(|t| format!("{:.2}", t)).collect::<Vec<_>>().join(", ")
}

fn main() {
    println!("\n========================================");
    println!("  Write-Path Microbenchmark (NTFS)");
    println!("========================================\n");

    let dir = Path::new(BENCH_DIR);
    cleanup(dir);

    // ================================================================
    // Test 1: Raw per-file fsync cost
    // ================================================================
    println!("--- Test 1: Raw per-file fsync (append {}B + fsync, {} files) ---\n", OP_PAYLOAD_BYTES, NUM_SHARDS);

    let paths = setup_shard_files(dir, NUM_SHARDS);
    let payload = make_payload(OP_PAYLOAD_BYTES);

    let iters = 5;
    let mut iter_times = Vec::new();
    for _ in 0..iters {
        let start = Instant::now();
        for p in &paths {
            let mut f = OpenOptions::new().read(true).write(true).open(p).unwrap();
            f.seek(SeekFrom::End(0)).unwrap();
            f.write_all(&payload).unwrap();
            f.sync_all().unwrap();
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        iter_times.push(elapsed_ms);
    }
    let med = median(&mut iter_times);
    let ops_per_sec = NUM_SHARDS as f64 / (med / 1000.0);
    println!("  {} files × (open+append+fsync+close): {:.1}ms median → {:.0} ops/s", NUM_SHARDS, med, ops_per_sec);
    println!("  Per-file: {:.2}ms", med / NUM_SHARDS as f64);
    println!("  [{}]\n", fmt(&iter_times));

    // ================================================================
    // Test 2: Batch fsync — append N ops to each file, one fsync at end
    // ================================================================
    println!("--- Test 2: Batch append ({}×{}B per file, 1 fsync, {} files) ---\n", OPS_PER_SHARD, OP_PAYLOAD_BYTES, NUM_SHARDS);

    cleanup(dir);
    let paths = setup_shard_files(dir, NUM_SHARDS);
    let batch_payload = make_payload(OP_PAYLOAD_BYTES * OPS_PER_SHARD);

    let mut iter_times = Vec::new();
    for _ in 0..iters {
        let start = Instant::now();
        for p in &paths {
            let mut f = OpenOptions::new().read(true).write(true).open(p).unwrap();
            f.seek(SeekFrom::End(0)).unwrap();
            f.write_all(&batch_payload).unwrap();
            f.sync_all().unwrap();
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        iter_times.push(elapsed_ms);
    }
    let med = median(&mut iter_times);
    let total_ops = NUM_SHARDS * OPS_PER_SHARD;
    let ops_per_sec = total_ops as f64 / (med / 1000.0);
    println!("  {} files × (open+append {}B+fsync+close): {:.1}ms median → {:.0} logical ops/s", NUM_SHARDS, batch_payload.len(), med, ops_per_sec);
    println!("  Per-file: {:.2}ms", med / NUM_SHARDS as f64);
    println!("  [{}]\n", fmt(&iter_times));

    // ================================================================
    // Test 3: No-fsync ceiling
    // ================================================================
    println!("--- Test 3: No fsync (append {}B, {} files) ---\n", OP_PAYLOAD_BYTES, NUM_SHARDS);

    cleanup(dir);
    let paths = setup_shard_files(dir, NUM_SHARDS);

    let mut iter_times = Vec::new();
    for _ in 0..iters {
        let start = Instant::now();
        for p in &paths {
            let mut f = OpenOptions::new().read(true).write(true).open(p).unwrap();
            f.seek(SeekFrom::End(0)).unwrap();
            f.write_all(&payload).unwrap();
            // NO fsync
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        iter_times.push(elapsed_ms);
    }
    let med = median(&mut iter_times);
    let ops_per_sec = NUM_SHARDS as f64 / (med / 1000.0);
    println!("  {} files × (open+append+close, NO fsync): {:.1}ms median → {:.0} ops/s", NUM_SHARDS, med, ops_per_sec);
    println!("  Per-file: {:.3}ms", med / NUM_SHARDS as f64);
    println!("  [{}]\n", fmt(&iter_times));

    // ================================================================
    // Test 4: File open/close overhead only
    // ================================================================
    println!("--- Test 4: File open+close only ({} files) ---\n", NUM_SHARDS);

    let mut iter_times = Vec::new();
    for _ in 0..iters {
        let start = Instant::now();
        for p in &paths {
            let _f = OpenOptions::new().read(true).write(true).open(p).unwrap();
            // open + drop (close)
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        iter_times.push(elapsed_ms);
    }
    let med = median(&mut iter_times);
    let ops_per_sec = NUM_SHARDS as f64 / (med / 1000.0);
    println!("  {} files × (open+close): {:.1}ms median → {:.0} ops/s", NUM_SHARDS, med, ops_per_sec);
    println!("  Per-file: {:.3}ms", med / NUM_SHARDS as f64);
    println!("  [{}]\n", fmt(&iter_times));

    // ================================================================
    // Test 5: DashMap + RwLock overhead
    // ================================================================
    println!("--- Test 5: DashMap lock acquire/release ({} shards) ---\n", NUM_SHARDS);

    let locks: DashMap<usize, Arc<RwLock<()>>> = DashMap::new();
    for i in 0..NUM_SHARDS {
        locks.insert(i, Arc::new(RwLock::new(())));
    }

    let mut iter_times = Vec::new();
    for _ in 0..iters {
        let start = Instant::now();
        for i in 0..NUM_SHARDS {
            let lock = locks.get(&i).unwrap().clone();
            let _guard = lock.read();
            std::hint::black_box(&_guard);
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        iter_times.push(elapsed_ms);
    }
    let med = median(&mut iter_times);
    println!("  {} locks × (DashMap get + RwLock read): {:.3}ms median", NUM_SHARDS, med);
    println!("  Per-lock: {:.0}ns", med * 1_000_000.0 / NUM_SHARDS as f64);
    println!("  [{}]\n", fmt(&iter_times));

    // ================================================================
    // Test 6: Header read + modify + write-back (what append_ops_to_shard does)
    // ================================================================
    println!("--- Test 6: Header read-modify-write ({} files, with fsync) ---\n", NUM_SHARDS);

    cleanup(dir);
    let paths = setup_shard_files(dir, NUM_SHARDS);

    let mut iter_times = Vec::new();
    for _ in 0..iters {
        let start = Instant::now();
        for p in &paths {
            let mut f = OpenOptions::new().read(true).write(true).open(p).unwrap();
            // Read 28-byte header
            let mut header_buf = [0u8; 28];
            f.read_exact(&mut header_buf).unwrap();
            // Seek to end, append payload
            f.seek(SeekFrom::End(0)).unwrap();
            f.write_all(&payload).unwrap();
            // Write back modified header field (ops_count at offset 20)
            f.seek(SeekFrom::Start(20)).unwrap();
            let count: u32 = 1;
            f.write_all(&count.to_le_bytes()).unwrap();
            // fsync
            f.sync_all().unwrap();
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        iter_times.push(elapsed_ms);
    }
    let med = median(&mut iter_times);
    let ops_per_sec = NUM_SHARDS as f64 / (med / 1000.0);
    println!("  {} files × (open+read_hdr+append+write_hdr+fsync+close): {:.1}ms → {:.0} ops/s", NUM_SHARDS, med, ops_per_sec);
    println!("  Per-file: {:.2}ms", med / NUM_SHARDS as f64);
    println!("  [{}]\n", fmt(&iter_times));

    // ================================================================
    // Test 7: Parallel via rayon (simulates current par_iter approach)
    // ================================================================
    println!("--- Test 7: Parallel append+fsync (rayon, {} files) ---\n", NUM_SHARDS);

    cleanup(dir);
    let paths = setup_shard_files(dir, NUM_SHARDS);

    use rayon::prelude::*;
    let mut iter_times = Vec::new();
    for _ in 0..iters {
        let start = Instant::now();
        paths.par_iter().for_each(|p| {
            let mut f = OpenOptions::new().read(true).write(true).open(p).unwrap();
            let mut header_buf = [0u8; 28];
            f.read_exact(&mut header_buf).unwrap();
            f.seek(SeekFrom::End(0)).unwrap();
            f.write_all(&payload).unwrap();
            f.seek(SeekFrom::Start(20)).unwrap();
            let count: u32 = 1;
            f.write_all(&count.to_le_bytes()).unwrap();
            f.sync_all().unwrap();
        });
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        iter_times.push(elapsed_ms);
    }
    let med = median(&mut iter_times);
    let ops_per_sec = NUM_SHARDS as f64 / (med / 1000.0);
    println!("  {} files × parallel (open+read_hdr+append+write_hdr+fsync+close): {:.1}ms → {:.0} ops/s", NUM_SHARDS, med, ops_per_sec);
    println!("  Per-file effective: {:.2}ms", med / NUM_SHARDS as f64);
    println!("  [{}]\n", fmt(&iter_times));

    // ================================================================
    // Test 8: Batched fsync — write all, then fsync all
    // ================================================================
    println!("--- Test 8: Write-all-then-fsync-all ({} files) ---\n", NUM_SHARDS);

    cleanup(dir);
    let paths = setup_shard_files(dir, NUM_SHARDS);

    let mut iter_times = Vec::new();
    for _ in 0..iters {
        let start = Instant::now();
        // Phase 1: write all (keep files open)
        let mut files: Vec<File> = Vec::with_capacity(NUM_SHARDS);
        for p in &paths {
            let mut f = OpenOptions::new().read(true).write(true).open(p).unwrap();
            let mut header_buf = [0u8; 28];
            f.read_exact(&mut header_buf).unwrap();
            f.seek(SeekFrom::End(0)).unwrap();
            f.write_all(&payload).unwrap();
            f.seek(SeekFrom::Start(20)).unwrap();
            let count: u32 = 1;
            f.write_all(&count.to_le_bytes()).unwrap();
            files.push(f);
        }
        // Phase 2: fsync all
        for f in &files {
            f.sync_all().unwrap();
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        iter_times.push(elapsed_ms);
    }
    let med = median(&mut iter_times);
    let ops_per_sec = NUM_SHARDS as f64 / (med / 1000.0);
    println!("  {} files × (write-all then fsync-all): {:.1}ms → {:.0} ops/s", NUM_SHARDS, med, ops_per_sec);
    println!("  Per-file: {:.2}ms", med / NUM_SHARDS as f64);
    println!("  [{}]\n", fmt(&iter_times));

    // ================================================================
    // Summary
    // ================================================================
    println!("========================================");
    println!("  Summary");
    println!("========================================\n");
    println!("  If raw fsync is the floor, Tests 1 vs 3 tell you.");
    println!("  If open/close is expensive, Test 4 tells you.");
    println!("  If batching helps, Test 2 vs 1 tells you.");
    println!("  If parallelism helps, Test 7 vs 6 tells you.");
    println!("  If deferred-fsync helps, Test 8 vs 6 tells you.");

    cleanup(dir);
}
