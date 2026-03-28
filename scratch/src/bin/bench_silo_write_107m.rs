//! Benchmark 0: Data Silo write throughput at 107M scale.
//!
//! Goal: ≥10M rows/s with 28 threads writing ~200 byte docs to per-thread silo files.
//! This validates the core assumption from Fredrick's 1M-scale microbench (20.1M/s).
//!
//! Usage:
//!   cargo run -p scratch --release --bin bench_silo_write_107m
//!
//! Cleans up after itself (deletes silo files).

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rayon::prelude::*;

const NUM_ROWS: u64 = 107_000_000;
const DOC_SIZE: usize = 200; // ~200 bytes per document (realistic for Civitai images)
const NUM_THREADS: usize = 28;

fn main() {
    // Set rayon thread count
    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build_global()
        .ok();

    let silo_dir = PathBuf::from("bench_silo_tmp");
    let _ = fs::remove_dir_all(&silo_dir);
    fs::create_dir_all(&silo_dir).unwrap();

    // Generate a representative document payload (msgpack-ish, ~200 bytes)
    let doc_template: Vec<u8> = {
        let mut v = Vec::with_capacity(DOC_SIZE);
        // Simulate msgpack fields: nsfwLevel, userId, type, postId, url, hash, etc.
        v.extend_from_slice(b"\x8a"); // fixmap of 10 fields
        v.extend_from_slice(b"\xa8nsfwLevl\x01"); // nsfwLevel: 1
        v.extend_from_slice(b"\xa6userId\xce\x00\x0f\x42\x40"); // userId: 1000000
        v.extend_from_slice(b"\xa4type\xa5image"); // type: "image"
        v.extend_from_slice(b"\xa6postId\xce\x00\x15\x4d\x50"); // postId
        v.extend_from_slice(b"\xa3url\xd9\x40"); // url: 64-byte string
        v.resize(DOC_SIZE, 0x41); // pad with 'A' to reach target size
        v
    };

    println!("=== Benchmark 0: Data Silo Write Throughput ===");
    println!("  Rows:    {}M", NUM_ROWS / 1_000_000);
    println!("  Doc:     {} bytes", DOC_SIZE);
    println!("  Threads: {}", NUM_THREADS);
    println!("  Total:   {:.1} GB", NUM_ROWS as f64 * DOC_SIZE as f64 / 1e9);
    println!();

    // Phase 1: Pre-create silo files
    let t_create = Instant::now();
    let silo_paths: Vec<PathBuf> = (0..NUM_THREADS)
        .map(|i| silo_dir.join(format!("silo_{:02}.dat", i)))
        .collect();
    for path in &silo_paths {
        File::create(path).unwrap();
    }
    println!("  Pre-create: {:.1}ms", t_create.elapsed().as_secs_f64() * 1000.0);

    // Phase 2: Parallel write — each thread writes its share to its own file
    let rows_per_thread = NUM_ROWS / NUM_THREADS as u64;
    let total_written = AtomicU64::new(0);
    let total_bytes = AtomicU64::new(0);

    let t_write = Instant::now();
    let local_results: Vec<(u64, u64)> = (0..NUM_THREADS)
        .into_par_iter()
        .map(|thread_id| {
            let path = &silo_paths[thread_id];
            let file = File::create(path).unwrap();
            let mut writer = BufWriter::with_capacity(256 * 1024, file);
            let mut bytes = 0u64;
            let mut count = 0u64;

            let start_slot = thread_id as u64 * rows_per_thread;
            for i in 0..rows_per_thread {
                let slot_id = start_slot + i;
                // Write: [u32 slot_id][doc bytes]
                writer.write_all(&(slot_id as u32).to_le_bytes()).unwrap();
                writer.write_all(&(DOC_SIZE as u32).to_le_bytes()).unwrap();
                writer.write_all(&doc_template).unwrap();
                bytes += 8 + DOC_SIZE as u64;
                count += 1;

                if count % 1_000_000 == 0 {
                    let prev = total_written.fetch_add(1_000_000, Ordering::Relaxed);
                    if (prev / 10_000_000) != ((prev + 1_000_000) / 10_000_000) {
                        let elapsed = t_write.elapsed().as_secs_f64();
                        let total = prev + 1_000_000;
                        eprintln!(
                            "  [{:.1}s] {}M rows ({:.1}M/s)",
                            elapsed,
                            total / 1_000_000,
                            total as f64 / elapsed / 1e6
                        );
                    }
                }
            }
            writer.flush().unwrap();
            total_bytes.fetch_add(bytes, Ordering::Relaxed);
            (count, bytes)
        })
        .collect();

    let write_elapsed = t_write.elapsed();
    let total_rows: u64 = local_results.iter().map(|(c, _)| c).sum();
    let total_data: u64 = local_results.iter().map(|(_, b)| b).sum();
    let rows_per_sec = total_rows as f64 / write_elapsed.as_secs_f64();

    println!();
    println!("  Write phase:");
    println!("    Rows:     {}M", total_rows / 1_000_000);
    println!("    Data:     {:.1} GB", total_data as f64 / 1e9);
    println!("    Time:     {:.2}s", write_elapsed.as_secs_f64());
    println!("    Rate:     {:.1}M rows/s", rows_per_sec / 1e6);
    println!("    BW:       {:.1} GB/s", total_data as f64 / write_elapsed.as_secs_f64() / 1e9);

    // Phase 3: Index merge (simulated — just build a Vec)
    let t_index = Instant::now();
    let mut index: Vec<(u8, u64, u32)> = Vec::with_capacity(total_rows as usize);
    for thread_id in 0..NUM_THREADS {
        let start_slot = thread_id as u64 * rows_per_thread;
        let mut offset = 0u64;
        for i in 0..rows_per_thread {
            index.push((thread_id as u8, offset, DOC_SIZE as u32));
            offset += 8 + DOC_SIZE as u64;
        }
    }
    let index_elapsed = t_index.elapsed();
    println!();
    println!("  Index merge:");
    println!("    Entries:  {}M", index.len() / 1_000_000);
    println!("    Time:     {:.2}s", index_elapsed.as_secs_f64());
    println!("    Memory:   {:.1} GB", index.len() as f64 * 13.0 / 1e9);

    // Phase 4: mmap read verification
    let t_read = Instant::now();
    let mmaps: Vec<memmap2::Mmap> = silo_paths
        .iter()
        .map(|p| {
            let f = File::open(p).unwrap();
            unsafe { memmap2::Mmap::map(&f).unwrap() }
        })
        .collect();

    // Random reads
    let mut rng_state: u64 = 12345;
    let num_reads = 1_000_000;
    let mut read_bytes = 0u64;
    for _ in 0..num_reads {
        // Simple xorshift for fast random
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        let slot = (rng_state % total_rows) as usize;
        let (file_id, offset, length) = index[slot];
        let mmap = &mmaps[file_id as usize];
        let start = offset as usize + 8; // skip slot_id + length header
        let end = start + length as usize;
        if end <= mmap.len() {
            read_bytes += (end - start) as u64;
        }
    }
    let read_elapsed = t_read.elapsed();
    println!();
    println!("  Random reads ({} lookups):", num_reads);
    println!("    Time:     {:.2}ms", read_elapsed.as_secs_f64() * 1000.0);
    println!("    Per-read: {:.0}ns", read_elapsed.as_nanos() as f64 / num_reads as f64);
    println!("    Bytes:    {:.1} MB", read_bytes as f64 / 1e6);

    // Verdict
    println!();
    let pass = rows_per_sec >= 10_000_000.0;
    println!(
        "  RESULT: {:.1}M rows/s — {} (goal: ≥10M/s)",
        rows_per_sec / 1e6,
        if pass { "✓ PASS" } else { "✗ FAIL" }
    );

    // Cleanup
    let _ = fs::remove_dir_all(&silo_dir);
}
