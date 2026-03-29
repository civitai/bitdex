//! Benchmark 3: mmap Index Startup
//!
//! Validates that mmap'ing a 1.4 GB index file (107M entries × 13 bytes)
//! and performing random lookups meets the data silo design goals:
//!   - mmap creation time: <100ms
//!   - First random access: <1ms
//!   - 10K random accesses: <1ms total (7ns each expected)
//!
//! Index format (matches data_silo.rs DocIndex):
//!   [u32 num_entries][N × (u8 file_id, u64 offset, u32 length)]
//!
//! Usage:
//!   cargo run -p scratch --release --bin bench_silo_mmap_index

use memmap2::Mmap;
use rand::prelude::*;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

const NUM_ENTRIES: u32 = 107_000_000;
const ENTRY_SIZE: usize = 13; // u8 + u64 + u32
const HEADER_SIZE: usize = 4; // u32 num_entries

fn main() {
    let dir = std::env::temp_dir().join("bitdex_bench_silo_mmap");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("doc_index.bin");

    println!("=== Benchmark 3: mmap Index Startup ===");
    println!("Entries: {NUM_ENTRIES} ({:.2} GB)", file_size_gb());
    println!();

    // Step 1: Generate the index file
    generate_index_file(&index_path);

    // Step 2: mmap the file and measure timing
    bench_mmap_and_reads(&index_path);

    // Step 3: Run multiple iterations for stability
    println!("\n--- Stability check (5 iterations) ---");
    for i in 1..=5 {
        print!("  Run {i}: ");
        bench_mmap_quick(&index_path);
    }

    // Cleanup
    let _ = fs::remove_dir_all(&dir);
    println!("\nDone. Temp files cleaned up.");
}

fn file_size_gb() -> f64 {
    (HEADER_SIZE + NUM_ENTRIES as usize * ENTRY_SIZE) as f64 / (1024.0 * 1024.0 * 1024.0)
}

fn generate_index_file(path: &Path) {
    println!("Generating {:.2} GB index file...", file_size_gb());
    let start = Instant::now();

    let total_bytes = HEADER_SIZE + NUM_ENTRIES as usize * ENTRY_SIZE;
    let mut buf = Vec::with_capacity(total_bytes);

    // Header: num_entries as u32 LE
    buf.extend_from_slice(&NUM_ENTRIES.to_le_bytes());

    // Entries: simulate realistic data
    // 28 silo files (file_id 0-27), offsets increasing per file, lengths ~200 bytes
    let mut rng = StdRng::seed_from_u64(42);
    let num_files: u8 = 28;
    let mut file_offsets = vec![0u64; num_files as usize];

    for _ in 0..NUM_ENTRIES {
        let file_id = rng.gen_range(0..num_files);
        let length: u32 = rng.gen_range(100..300); // ~200 byte docs
        let offset = file_offsets[file_id as usize];
        file_offsets[file_id as usize] += length as u64;

        buf.push(file_id);
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&length.to_le_bytes());
    }

    let gen_elapsed = start.elapsed();
    println!("  Generated in memory: {:.2}s", gen_elapsed.as_secs_f64());

    let write_start = Instant::now();
    let mut file = fs::File::create(path).unwrap();
    file.write_all(&buf).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let write_elapsed = write_start.elapsed();
    let total = start.elapsed();
    println!(
        "  Written to disk: {:.2}s ({:.2} GB/s)",
        write_elapsed.as_secs_f64(),
        file_size_gb() / write_elapsed.as_secs_f64()
    );
    println!("  Total generation: {:.2}s", total.as_secs_f64());
    println!();
}

fn bench_mmap_and_reads(path: &Path) {
    let file_size = fs::metadata(path).unwrap().len();
    println!(
        "Index file: {:.2} GB ({} bytes)",
        file_size as f64 / (1024.0 * 1024.0 * 1024.0),
        file_size
    );

    // --- mmap creation ---
    let mmap_start = Instant::now();
    let file = fs::File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let mmap_elapsed = mmap_start.elapsed();
    println!(
        "\nmmap creation: {:.3}ms  (goal: <100ms)  {}",
        mmap_elapsed.as_secs_f64() * 1000.0,
        pass_fail(mmap_elapsed.as_secs_f64() * 1000.0, 100.0)
    );

    // Verify header
    let num_entries = u32::from_le_bytes(mmap[0..4].try_into().unwrap());
    assert_eq!(num_entries, NUM_ENTRIES, "header mismatch");

    // --- First random access ---
    let mut rng = StdRng::seed_from_u64(123);
    let slot: u32 = rng.gen_range(0..NUM_ENTRIES);
    let first_start = Instant::now();
    let entry = read_entry(&mmap, slot);
    let first_elapsed = first_start.elapsed();
    println!(
        "First random access (slot {}): {:.3}ms  (goal: <1ms)  {}",
        slot,
        first_elapsed.as_secs_f64() * 1000.0,
        pass_fail(first_elapsed.as_secs_f64() * 1000.0, 1.0)
    );
    println!(
        "  -> file_id={}, offset={}, length={}",
        entry.0, entry.1, entry.2
    );

    // --- 10K random accesses ---
    let slots: Vec<u32> = (0..10_000).map(|_| rng.gen_range(0..NUM_ENTRIES)).collect();

    let batch_start = Instant::now();
    let mut checksum: u64 = 0;
    for &s in &slots {
        let (fid, off, len) = read_entry(&mmap, s);
        checksum = checksum.wrapping_add(fid as u64 + off + len as u64);
    }
    let batch_elapsed = batch_start.elapsed();
    println!(
        "10K random accesses: {:.3}ms  ({:.1}ns/access)  (goal: <1ms total)  {}",
        batch_elapsed.as_secs_f64() * 1000.0,
        batch_elapsed.as_nanos() as f64 / 10_000.0,
        pass_fail(batch_elapsed.as_secs_f64() * 1000.0, 1.0)
    );
    println!("  checksum (prevent optimization): {checksum}");

    // --- 100K random accesses (extra data point) ---
    let slots_100k: Vec<u32> = (0..100_000).map(|_| rng.gen_range(0..NUM_ENTRIES)).collect();
    let batch100k_start = Instant::now();
    let mut checksum2: u64 = 0;
    for &s in &slots_100k {
        let (fid, off, len) = read_entry(&mmap, s);
        checksum2 = checksum2.wrapping_add(fid as u64 + off + len as u64);
    }
    let batch100k_elapsed = batch100k_start.elapsed();
    println!(
        "100K random accesses: {:.3}ms  ({:.1}ns/access)",
        batch100k_elapsed.as_secs_f64() * 1000.0,
        batch100k_elapsed.as_nanos() as f64 / 100_000.0,
    );
    println!("  checksum: {checksum2}");

    // --- Sequential scan (page cache friendly) ---
    let seq_start = Instant::now();
    let mut seq_checksum: u64 = 0;
    for s in 0..10_000u32 {
        let (fid, off, len) = read_entry(&mmap, s);
        seq_checksum = seq_checksum.wrapping_add(fid as u64 + off + len as u64);
    }
    let seq_elapsed = seq_start.elapsed();
    println!(
        "10K sequential accesses: {:.3}ms  ({:.1}ns/access)",
        seq_elapsed.as_secs_f64() * 1000.0,
        seq_elapsed.as_nanos() as f64 / 10_000.0,
    );
    println!("  checksum: {seq_checksum}");

    // --- Warmup: touch all pages, then re-measure random access ---
    println!("\n--- After full warmup (touching all pages) ---");
    let warmup_start = Instant::now();
    let mut warmup_cs: u64 = 0;
    // Read every 4096th entry to touch each 4KB page (each entry=13 bytes, ~315 entries/page)
    let stride = 4096 / ENTRY_SIZE; // ~315
    let mut s = 0u32;
    while s < NUM_ENTRIES {
        let (fid, off, len) = read_entry(&mmap, s);
        warmup_cs = warmup_cs.wrapping_add(fid as u64 + off + len as u64);
        s += stride as u32;
    }
    let warmup_elapsed = warmup_start.elapsed();
    println!(
        "Warmup (touch all pages): {:.3}ms  cs={warmup_cs}",
        warmup_elapsed.as_secs_f64() * 1000.0
    );

    // Re-measure 10K random after warmup
    let slots_warm: Vec<u32> = (0..10_000)
        .map(|_| rng.gen_range(0..NUM_ENTRIES))
        .collect();
    let warm_start = Instant::now();
    let mut warm_cs: u64 = 0;
    for &s in &slots_warm {
        let (fid, off, len) = read_entry(&mmap, s);
        warm_cs = warm_cs.wrapping_add(fid as u64 + off + len as u64);
    }
    let warm_elapsed = warm_start.elapsed();
    println!(
        "10K random (warm): {:.3}ms  ({:.1}ns/access)  (goal: <1ms)  {}",
        warm_elapsed.as_secs_f64() * 1000.0,
        warm_elapsed.as_nanos() as f64 / 10_000.0,
        pass_fail(warm_elapsed.as_secs_f64() * 1000.0, 1.0)
    );
    println!("  checksum: {warm_cs}");

    // 100K random after warmup
    let slots_warm_100k: Vec<u32> = (0..100_000)
        .map(|_| rng.gen_range(0..NUM_ENTRIES))
        .collect();
    let warm100k_start = Instant::now();
    let mut warm100k_cs: u64 = 0;
    for &s in &slots_warm_100k {
        let (fid, off, len) = read_entry(&mmap, s);
        warm100k_cs = warm100k_cs.wrapping_add(fid as u64 + off + len as u64);
    }
    let warm100k_elapsed = warm100k_start.elapsed();
    println!(
        "100K random (warm): {:.3}ms  ({:.1}ns/access)",
        warm100k_elapsed.as_secs_f64() * 1000.0,
        warm100k_elapsed.as_nanos() as f64 / 100_000.0,
    );
    println!("  checksum: {warm100k_cs}");
}

/// Quick iteration for stability check
fn bench_mmap_quick(path: &Path) {
    let mut rng = StdRng::seed_from_u64(999);

    let mmap_start = Instant::now();
    let file = fs::File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let mmap_us = mmap_start.elapsed().as_micros();

    let slots: Vec<u32> = (0..10_000).map(|_| rng.gen_range(0..NUM_ENTRIES)).collect();
    let batch_start = Instant::now();
    let mut cs: u64 = 0;
    for &s in &slots {
        let (fid, off, len) = read_entry(&mmap, s);
        cs = cs.wrapping_add(fid as u64 + off + len as u64);
    }
    let batch_ns = batch_start.elapsed().as_nanos();

    println!(
        "mmap={mmap_us}us  10K_random={:.3}ms ({:.1}ns/ea)  cs={cs}",
        batch_ns as f64 / 1_000_000.0,
        batch_ns as f64 / 10_000.0
    );
}

/// Read a single index entry by slot_id from the mmap'd buffer.
#[inline]
fn read_entry(mmap: &[u8], slot_id: u32) -> (u8, u64, u32) {
    let base = HEADER_SIZE + slot_id as usize * ENTRY_SIZE;
    let file_id = mmap[base];
    let offset = u64::from_le_bytes(mmap[base + 1..base + 9].try_into().unwrap());
    let length = u32::from_le_bytes(mmap[base + 9..base + 13].try_into().unwrap());
    (file_id, offset, length)
}

fn pass_fail(actual: f64, goal: f64) -> &'static str {
    if actual <= goal { "PASS" } else { "FAIL" }
}
