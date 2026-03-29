//! Benchmark 0 variant: mmap writes for silo files
//!
//! Tests whether mmap writes (pre-allocate + memcpy) beat BufWriter for
//! bulk silo writes at 107M scale. This helps diagnose whether the B0
//! throughput gap (1.8M/s actual vs 10M/s goal) is syscall overhead or
//! NVMe bandwidth.
//!
//! Approach:
//!   1. Pre-allocate each silo file to expected size (~795 MB per file)
//!   2. mmap writable
//!   3. memcpy docs sequentially into the mapped region (per-thread)
//!   4. msync/flush at the end
//!
//! Also tests BufWriter baseline for direct comparison on the same machine.
//!
//! Usage:
//!   cargo run -p scratch --release --bin bench_silo_mmap_write

use memmap2::MmapMut;
use rand::prelude::*;
use rayon::prelude::*;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const TOTAL_ROWS: u64 = 107_000_000;
const NUM_THREADS: usize = 28;
const DOC_SIZE: usize = 208; // average doc size from design doc

fn main() {
    let dir = std::env::temp_dir().join("bitdex_bench_silo_mmap_write");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let rows_per_thread = TOTAL_ROWS / NUM_THREADS as u64;
    let file_size = rows_per_thread as usize * DOC_SIZE;

    println!("=== Benchmark 0 Variant: mmap Write vs BufWriter ===");
    println!("Total rows: {TOTAL_ROWS}");
    println!("Threads: {NUM_THREADS}");
    println!("Doc size: {DOC_SIZE} bytes");
    println!("Per-file: {rows_per_thread} rows × {DOC_SIZE} = {:.2} MB", file_size as f64 / (1024.0 * 1024.0));
    println!("Total data: {:.2} GB", (TOTAL_ROWS as f64 * DOC_SIZE as f64) / (1024.0 * 1024.0 * 1024.0));
    println!();

    // Generate a doc template (random-ish bytes, same per thread for fairness)
    let doc: Vec<u8> = {
        let mut rng = StdRng::seed_from_u64(42);
        (0..DOC_SIZE).map(|_| rng.gen()).collect()
    };

    // --- Test 1: mmap writes ---
    println!("--- Test 1: mmap writes (pre-allocate + memcpy) ---");
    let mmap_dir = dir.join("mmap");
    fs::create_dir_all(&mmap_dir).unwrap();

    let mmap_rate = bench_mmap_write(&mmap_dir, &doc, rows_per_thread, file_size);
    println!();

    // Clean up mmap files before BufWriter test
    let _ = fs::remove_dir_all(&mmap_dir);

    // --- Test 2: BufWriter (256KB buffer, matching data_silo.rs) ---
    println!("--- Test 2: BufWriter (256KB, current data_silo.rs) ---");
    let buf_dir = dir.join("bufwriter_256k");
    fs::create_dir_all(&buf_dir).unwrap();

    let buf256k_rate = bench_bufwriter(&buf_dir, &doc, rows_per_thread, 256 * 1024);
    println!();

    let _ = fs::remove_dir_all(&buf_dir);

    // --- Test 3: BufWriter (8MB buffer, Josh's variant) ---
    println!("--- Test 3: BufWriter (8MB, Josh's variant) ---");
    let buf8m_dir = dir.join("bufwriter_8m");
    fs::create_dir_all(&buf8m_dir).unwrap();

    let buf8m_rate = bench_bufwriter(&buf8m_dir, &doc, rows_per_thread, 8 * 1024 * 1024);
    println!();

    let _ = fs::remove_dir_all(&buf8m_dir);

    // --- Test 4: Direct write (no BufWriter, raw File::write_all) ---
    println!("--- Test 4: Direct write (no BufWriter) ---");
    let direct_dir = dir.join("direct");
    fs::create_dir_all(&direct_dir).unwrap();

    let direct_rate = bench_direct_write(&direct_dir, &doc, rows_per_thread);
    println!();

    let _ = fs::remove_dir_all(&direct_dir);

    // --- Test 5: Single thread mmap (NVMe bandwidth baseline) ---
    println!("--- Test 5: Single thread mmap (NVMe bandwidth ceiling) ---");
    let single_dir = dir.join("single_mmap");
    fs::create_dir_all(&single_dir).unwrap();

    let single_rate = bench_single_thread_mmap(&single_dir, &doc, TOTAL_ROWS as usize, TOTAL_ROWS as usize * DOC_SIZE);
    println!();

    // Summary
    println!("=== SUMMARY ===");
    println!("| Approach                | Rate (M rows/s) | vs 10M/s goal |");
    println!("|-------------------------|------------------|---------------|");
    println!("| mmap write (28 threads) | {:>16.2} | {:>13.1}x |", mmap_rate, mmap_rate / 10.0);
    println!("| BufWriter 256KB (28 th) | {:>16.2} | {:>13.1}x |", buf256k_rate, buf256k_rate / 10.0);
    println!("| BufWriter 8MB (28 th)   | {:>16.2} | {:>13.1}x |", buf8m_rate, buf8m_rate / 10.0);
    println!("| Direct write (28 th)    | {:>16.2} | {:>13.1}x |", direct_rate, direct_rate / 10.0);
    println!("| Single thread mmap      | {:>16.2} | {:>13.1}x |", single_rate, single_rate / 10.0);

    // Cleanup
    let _ = fs::remove_dir_all(&dir);
    println!("\nDone. Temp files cleaned up.");
}

fn bench_mmap_write(dir: &Path, doc: &[u8], rows_per_thread: u64, file_size: usize) -> f64 {
    // Pre-allocate files
    let pre_start = Instant::now();
    let files: Vec<PathBuf> = (0..NUM_THREADS)
        .map(|i| {
            let path = dir.join(format!("silo_{:02}.dat", i));
            let file = File::create(&path).unwrap();
            file.set_len(file_size as u64).unwrap();
            path
        })
        .collect();
    println!("  Pre-allocate: {:.2}ms", pre_start.elapsed().as_secs_f64() * 1000.0);

    // mmap and write in parallel
    let total_written = AtomicU64::new(0);
    let write_start = Instant::now();

    files.par_iter().for_each(|path| {
        let file = OpenOptions::new().read(true).write(true).open(path).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file) }.unwrap();

        let mut offset = 0usize;
        for _ in 0..rows_per_thread {
            mmap[offset..offset + doc.len()].copy_from_slice(doc);
            offset += doc.len();
        }

        mmap.flush().unwrap();
        total_written.fetch_add(rows_per_thread, Ordering::Relaxed);
    });

    let write_elapsed = write_start.elapsed();
    let total = total_written.load(Ordering::Relaxed);
    let rate = total as f64 / write_elapsed.as_secs_f64() / 1_000_000.0;
    let throughput_gb = (total as f64 * doc.len() as f64) / (1024.0 * 1024.0 * 1024.0) / write_elapsed.as_secs_f64();

    println!("  Write: {:.2}s — {:.2}M rows/s — {:.2} GB/s", write_elapsed.as_secs_f64(), rate, throughput_gb);
    println!("  Total rows: {total}");
    rate
}

fn bench_bufwriter(dir: &Path, doc: &[u8], rows_per_thread: u64, buf_size: usize) -> f64 {
    let total_written = AtomicU64::new(0);
    let write_start = Instant::now();

    (0..NUM_THREADS).into_par_iter().for_each(|i| {
        let path = dir.join(format!("silo_{:02}.dat", i));
        let file = File::create(&path).unwrap();
        let mut writer = BufWriter::with_capacity(buf_size, file);

        for _ in 0..rows_per_thread {
            writer.write_all(doc).unwrap();
        }
        writer.flush().unwrap();
        total_written.fetch_add(rows_per_thread, Ordering::Relaxed);
    });

    let write_elapsed = write_start.elapsed();
    let total = total_written.load(Ordering::Relaxed);
    let rate = total as f64 / write_elapsed.as_secs_f64() / 1_000_000.0;
    let throughput_gb = (total as f64 * doc.len() as f64) / (1024.0 * 1024.0 * 1024.0) / write_elapsed.as_secs_f64();

    println!("  Write: {:.2}s — {:.2}M rows/s — {:.2} GB/s", write_elapsed.as_secs_f64(), rate, throughput_gb);
    println!("  Total rows: {total}");
    rate
}

fn bench_direct_write(dir: &Path, doc: &[u8], rows_per_thread: u64) -> f64 {
    let total_written = AtomicU64::new(0);
    let write_start = Instant::now();

    (0..NUM_THREADS).into_par_iter().for_each(|i| {
        let path = dir.join(format!("silo_{:02}.dat", i));
        let mut file = File::create(&path).unwrap();

        for _ in 0..rows_per_thread {
            file.write_all(doc).unwrap();
        }
        file.flush().unwrap();
        total_written.fetch_add(rows_per_thread, Ordering::Relaxed);
    });

    let write_elapsed = write_start.elapsed();
    let total = total_written.load(Ordering::Relaxed);
    let rate = total as f64 / write_elapsed.as_secs_f64() / 1_000_000.0;
    let throughput_gb = (total as f64 * doc.len() as f64) / (1024.0 * 1024.0 * 1024.0) / write_elapsed.as_secs_f64();

    println!("  Write: {:.2}s — {:.2}M rows/s — {:.2} GB/s", write_elapsed.as_secs_f64(), rate, throughput_gb);
    println!("  Total rows: {total}");
    rate
}

fn bench_single_thread_mmap(dir: &Path, doc: &[u8], total_rows: usize, file_size: usize) -> f64 {
    let path = dir.join("silo_single.dat");
    let file = File::create(&path).unwrap();
    file.set_len(file_size as u64).unwrap();

    let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file) }.unwrap();

    let write_start = Instant::now();
    let mut offset = 0usize;
    for _ in 0..total_rows {
        mmap[offset..offset + doc.len()].copy_from_slice(doc);
        offset += doc.len();
    }
    mmap.flush().unwrap();

    let write_elapsed = write_start.elapsed();
    let rate = total_rows as f64 / write_elapsed.as_secs_f64() / 1_000_000.0;
    let throughput_gb = (total_rows as f64 * doc.len() as f64) / (1024.0 * 1024.0 * 1024.0) / write_elapsed.as_secs_f64();

    println!("  Write: {:.2}s — {:.2}M rows/s — {:.2} GB/s", write_elapsed.as_secs_f64(), rate, throughput_gb);
    println!("  Total rows: {total_rows}");
    rate
}
