//! Benchmark 1: Phase Ordering Cost (read-merge-write)
//!
//! Validates that cross-phase merging adds <10% overhead vs pure append.
//! Simulates the dump processor pattern where:
//!   Phase 1 (images): writes ~200 byte docs for each slot
//!   Phase 2 (resources): reads existing doc, merges ~50 bytes of new fields, rewrites
//!
//! The critical question: does the read-back cost of mmap'd Phase 1 data
//! significantly impact Phase 2 write throughput?
//!
//! Usage:
//!   cargo run -p scratch --release --bin bench_silo_phase_merge

use memmap2::Mmap;
use rand::prelude::*;
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const NUM_ROWS: u64 = 10_000_000;
const NUM_THREADS: usize = 28;
const PHASE1_DOC_SIZE: usize = 200; // images fields
const PHASE2_EXTRA_SIZE: usize = 50; // resources fields added
const BUF_SIZE: usize = 8 * 1024 * 1024; // 8MB BufWriter (approved approach)

fn main() {
    let dir = std::env::temp_dir().join("bitdex_bench_silo_phase_merge");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let rows_per_thread = NUM_ROWS / NUM_THREADS as u64;

    println!("=== Benchmark 1: Phase Ordering Cost ===");
    println!("Rows: {NUM_ROWS} across {NUM_THREADS} threads ({rows_per_thread}/thread)");
    println!("Phase 1 doc: {PHASE1_DOC_SIZE} bytes, Phase 2 adds: {PHASE2_EXTRA_SIZE} bytes");
    println!("BufWriter: {BUF_SIZE} bytes");
    println!();

    // Generate doc templates
    let mut rng = StdRng::seed_from_u64(42);
    let phase1_doc: Vec<u8> = (0..PHASE1_DOC_SIZE).map(|_| rng.gen()).collect();
    let phase2_extra: Vec<u8> = (0..PHASE2_EXTRA_SIZE).map(|_| rng.gen()).collect();

    // --- Phase 1: Pure append (baseline) ---
    println!("--- Phase 1: Pure append (images) ---");
    let phase1_dir = dir.join("phase1");
    fs::create_dir_all(&phase1_dir).unwrap();

    let (phase1_rate, phase1_indexes) = bench_phase1_append(
        &phase1_dir, &phase1_doc, rows_per_thread,
    );
    println!();

    // --- Phase 2a: Read-merge-write (hot cache) ---
    // Files are still in page cache from Phase 1 write
    println!("--- Phase 2a: Read-merge-write (hot cache) ---");
    let phase2_hot_dir = dir.join("phase2_hot");
    fs::create_dir_all(&phase2_hot_dir).unwrap();

    let phase2_hot_rate = bench_phase2_merge(
        &phase1_dir, &phase2_hot_dir, &phase1_indexes,
        &phase2_extra, rows_per_thread,
    );
    let hot_overhead = (1.0 - phase2_hot_rate / phase1_rate) * 100.0;
    println!(
        "  Overhead vs Phase 1: {:.1}% (goal: <10%)  {}",
        hot_overhead,
        if hot_overhead.abs() < 10.0 { "PASS" } else { "FAIL" }
    );
    println!();

    // --- Phase 2b: Pure append of same total size (control) ---
    // Shows what Phase 2 would look like without the read cost
    println!("--- Phase 2b: Pure append control (same total doc size, no read) ---");
    let control_dir = dir.join("control");
    fs::create_dir_all(&control_dir).unwrap();

    let merged_doc: Vec<u8> = phase1_doc.iter()
        .chain(phase2_extra.iter())
        .copied()
        .collect();
    let (control_rate, _) = bench_phase1_append(
        &control_dir, &merged_doc, rows_per_thread,
    );
    let merge_cost = (1.0 - phase2_hot_rate / control_rate) * 100.0;
    println!("  Read-merge overhead vs pure append of same size: {:.1}%", merge_cost);
    println!();

    // Summary
    println!("=== SUMMARY ===");
    println!("| Phase | Rate (M rows/s) | Doc size |");
    println!("|-------|-----------------|----------|");
    println!("| Phase 1 (pure append) | {:>15.2} | {} bytes |", phase1_rate, PHASE1_DOC_SIZE);
    println!("| Phase 2 (read+merge, hot) | {:>15.2} | {} bytes |", phase2_hot_rate, PHASE1_DOC_SIZE + PHASE2_EXTRA_SIZE);
    println!("| Control (pure append, same size) | {:>15.2} | {} bytes |", control_rate, PHASE1_DOC_SIZE + PHASE2_EXTRA_SIZE);
    println!();
    println!("Phase 2 overhead vs Phase 1: {:.1}%  {}", hot_overhead, if hot_overhead.abs() < 10.0 { "PASS" } else { "FAIL" });
    println!("Read-merge cost (Phase 2 vs control): {:.1}%", merge_cost);

    // Cleanup
    let _ = fs::remove_dir_all(&dir);
    println!("\nDone. Temp files cleaned up.");
}

/// Phase 1: Pure append, returns rate and per-thread index data.
fn bench_phase1_append(
    dir: &Path,
    doc: &[u8],
    rows_per_thread: u64,
) -> (f64, Vec<Vec<(u64, u32)>>) {
    let total_written = AtomicU64::new(0);
    let indexes: Vec<_> = (0..NUM_THREADS)
        .map(|_| std::sync::Mutex::new(Vec::with_capacity(rows_per_thread as usize)))
        .collect();

    let write_start = Instant::now();

    (0..NUM_THREADS).into_par_iter().for_each(|i| {
        let path = dir.join(format!("silo_{:02}.dat", i));
        let file = File::create(&path).unwrap();
        let mut writer = BufWriter::with_capacity(BUF_SIZE, file);

        let mut local_index = Vec::with_capacity(rows_per_thread as usize);
        let mut offset = 0u64;

        for _ in 0..rows_per_thread {
            writer.write_all(doc).unwrap();
            local_index.push((offset, doc.len() as u32));
            offset += doc.len() as u64;
        }
        writer.flush().unwrap();
        total_written.fetch_add(rows_per_thread, Ordering::Relaxed);

        *indexes[i].lock().unwrap() = local_index;
    });

    let elapsed = write_start.elapsed();
    let total = total_written.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    println!("  Write: {:.2}s — {:.2}M rows/s", elapsed.as_secs_f64(), rate);

    let indexes: Vec<Vec<(u64, u32)>> = indexes.into_iter()
        .map(|m| m.into_inner().unwrap())
        .collect();

    (rate, indexes)
}

/// Phase 2: Read existing doc from Phase 1 silo, merge new fields, write to Phase 2 silo.
fn bench_phase2_merge(
    phase1_dir: &Path,
    phase2_dir: &Path,
    phase1_indexes: &[Vec<(u64, u32)>],
    extra_data: &[u8],
    rows_per_thread: u64,
) -> f64 {
    // mmap Phase 1 files for reading
    let mmaps: Vec<Mmap> = (0..NUM_THREADS)
        .map(|i| {
            let path = phase1_dir.join(format!("silo_{:02}.dat", i));
            let file = File::open(&path).unwrap();
            unsafe { Mmap::map(&file) }.unwrap()
        })
        .collect();

    let total_written = AtomicU64::new(0);
    let write_start = Instant::now();

    (0..NUM_THREADS).into_par_iter().for_each(|i| {
        let path = phase2_dir.join(format!("silo_{:02}.dat", i));
        let file = File::create(&path).unwrap();
        let mut writer = BufWriter::with_capacity(BUF_SIZE, file);

        let mmap = &mmaps[i];
        let index = &phase1_indexes[i];

        for j in 0..rows_per_thread as usize {
            let (offset, length) = index[j];
            let existing = &mmap[offset as usize..(offset as usize + length as usize)];

            // Read-merge-write: existing doc + new fields
            writer.write_all(existing).unwrap();
            writer.write_all(extra_data).unwrap();
        }
        writer.flush().unwrap();
        total_written.fetch_add(rows_per_thread, Ordering::Relaxed);
    });

    let elapsed = write_start.elapsed();
    let total = total_written.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    println!("  Write: {:.2}s — {:.2}M rows/s", elapsed.as_secs_f64(), rate);
    rate
}
