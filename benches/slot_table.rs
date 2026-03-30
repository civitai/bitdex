//! Experiment 6: Thin slot table — variable-size doc bulk writes + reads.
//!
//! Compares variable-size docs with a 12-byte slot table (u64 offset + u32 length)
//! against the fixed-offset baseline (slot_id * 256).
//!
//! Run: cargo bench --bench slot_table --features data-silo

use std::fs::{self, File};
use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const NUM_DOCS: usize = 1_000_000;
const NUM_THREADS: usize = 32;
const DOCS_PER_THREAD: usize = NUM_DOCS / NUM_THREADS;
const FIXED_SLOT_SIZE: usize = 256;

// Slot table entry: 12 bytes per slot
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct SlotEntry {
    offset: u64,
    length: u32,
}

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() { return Duration::ZERO; }
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Generate a random doc size between min_size and max_size bytes.
fn random_doc_size(seed: u64, min_size: usize, max_size: usize) -> usize {
    let range = max_size - min_size;
    min_size + (seed as usize % (range + 1))
}

fn main() {
    let dir = PathBuf::from("data/silo-experiment6");
    fs::create_dir_all(&dir).unwrap();

    println!("=== Experiment 6: Thin Slot Table (Variable-Size Docs) ===");
    println!("Docs: {}  Threads: {}  Doc sizes: 100-500 bytes", NUM_DOCS, NUM_THREADS);
    println!();

    // Pre-generate doc sizes and content
    let mut doc_sizes = Vec::with_capacity(NUM_DOCS);
    let mut rng: u64 = 0xDEADBEEF;
    for _ in 0..NUM_DOCS {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        doc_sizes.push(random_doc_size(rng >> 32, 100, 500));
    }

    let avg_size: f64 = doc_sizes.iter().sum::<usize>() as f64 / NUM_DOCS as f64;
    let total_data_bytes: usize = doc_sizes.iter().sum();
    println!("  Avg doc size: {:.0} bytes", avg_size);
    println!("  Total data: {:.1} MB", total_data_bytes as f64 / 1_048_576.0);
    println!();

    // =====================================================================
    // Test 1: Variable-size docs with slot table
    // =====================================================================
    println!("--- Test 1: Variable-size + slot table ---");

    let data_path = dir.join("varsize_data.dat");
    let table_path = dir.join("slot_table.dat");

    // Pre-compute per-thread regions and per-slot offsets
    let mut slot_offsets: Vec<u64> = vec![0u64; NUM_DOCS];
    let mut thread_regions: Vec<(u64, usize)> = Vec::with_capacity(NUM_THREADS); // (start_offset, region_size)

    {
        let mut global_offset: u64 = 0;
        for t in 0..NUM_THREADS {
            let start_slot = t * DOCS_PER_THREAD;
            let end_slot = start_slot + DOCS_PER_THREAD;
            let region_start = global_offset;
            for slot in start_slot..end_slot {
                slot_offsets[slot] = global_offset;
                global_offset += doc_sizes[slot] as u64;
            }
            let region_size = (global_offset - region_start) as usize;
            thread_regions.push((region_start, region_size));
        }
    }

    // Create data file
    {
        let f = File::create(&data_path).unwrap();
        f.set_len(total_data_bytes as u64).unwrap();
        f.sync_all().unwrap();
    }

    // Parallel mmap write
    let t0 = Instant::now();
    {
        let f = File::options().read(true).write(true).open(&data_path).unwrap();
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };
        let ptr = mmap.as_mut_ptr();
        let slot_offsets_ref = &slot_offsets;
        let doc_sizes_ref = &doc_sizes;

        std::thread::scope(|s| {
            for t in 0..NUM_THREADS {
                let start_slot = t * DOCS_PER_THREAD;
                let end_slot = start_slot + DOCS_PER_THREAD;
                let ptr = ptr as usize; // Send as usize

                s.spawn(move || {
                    let ptr = ptr as *mut u8;
                    let mut rng: u64 = 0xCAFE0000 + t as u64;
                    for slot in start_slot..end_slot {
                        let offset = slot_offsets_ref[slot] as usize;
                        let size = doc_sizes_ref[slot];
                        // Write pseudo-random data
                        unsafe {
                            let dst = ptr.add(offset);
                            for i in 0..size {
                                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                                *dst.add(i) = (rng >> 56) as u8;
                            }
                        }
                    }
                });
            }
        });

        mmap.flush().unwrap();
    }
    let write_elapsed = t0.elapsed();

    // Build slot table
    let t1 = Instant::now();
    {
        let table_size = NUM_DOCS * 12; // 12 bytes per entry
        let mut table_data = vec![0u8; table_size];
        for slot in 0..NUM_DOCS {
            let entry_offset = slot * 12;
            table_data[entry_offset..entry_offset + 8].copy_from_slice(&slot_offsets[slot].to_le_bytes());
            table_data[entry_offset + 8..entry_offset + 12].copy_from_slice(&(doc_sizes[slot] as u32).to_le_bytes());
        }
        let mut f = File::create(&table_path).unwrap();
        f.write_all(&table_data).unwrap();
        f.sync_all().unwrap();
    }
    let table_build_elapsed = t1.elapsed();
    let total_write_elapsed = write_elapsed + table_build_elapsed;

    let varsize_write_rate = NUM_DOCS as f64 / total_write_elapsed.as_secs_f64();
    println!("  Write: {:.0}μs ({:.2}M docs/s)", total_write_elapsed.as_micros(), varsize_write_rate / 1_000_000.0);
    println!("    Data write: {:.0}μs, Table build: {:.0}μs",
        write_elapsed.as_micros(), table_build_elapsed.as_micros());

    // Random reads via slot table
    let data_mmap = {
        let f = File::open(&data_path).unwrap();
        unsafe { memmap2::Mmap::map(&f).unwrap() }
    };
    let table_mmap = {
        let f = File::open(&table_path).unwrap();
        unsafe { memmap2::Mmap::map(&f).unwrap() }
    };

    let num_reads = 100_000;
    let mut read_lats: Vec<Duration> = Vec::with_capacity(num_reads);
    let mut rng: u64 = 0xBEEF;
    let mut checksum: u64 = 0;

    for _ in 0..num_reads {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let slot = (rng >> 32) as usize % NUM_DOCS;

        let start = Instant::now();
        // Table lookup
        let entry_offset = slot * 12;
        let data_offset = u64::from_le_bytes(table_mmap[entry_offset..entry_offset + 8].try_into().unwrap()) as usize;
        let data_length = u32::from_le_bytes(table_mmap[entry_offset + 8..entry_offset + 12].try_into().unwrap()) as usize;
        // Data read
        let doc = &data_mmap[data_offset..data_offset + data_length];
        checksum = checksum.wrapping_add(doc[0] as u64);
        read_lats.push(start.elapsed());
    }

    read_lats.sort();
    println!("  Read ({} random): p50={:.0}ns  p95={:.0}ns  p99={:.0}ns  (checksum={})",
        num_reads,
        percentile(&read_lats, 50.0).as_nanos(),
        percentile(&read_lats, 95.0).as_nanos(),
        percentile(&read_lats, 99.0).as_nanos(),
        checksum,
    );
    println!("  Table overhead: {:.1} MB ({} slots × 12 bytes)",
        NUM_DOCS as f64 * 12.0 / 1_048_576.0, NUM_DOCS);
    println!();

    // =====================================================================
    // Test 2: Fixed-offset baseline (256 bytes/slot)
    // =====================================================================
    println!("--- Test 2: Fixed-offset baseline (256 bytes/slot) ---");

    let fixed_path = dir.join("fixed_data.dat");
    let fixed_total = NUM_DOCS * FIXED_SLOT_SIZE;

    {
        let f = File::create(&fixed_path).unwrap();
        f.set_len(fixed_total as u64).unwrap();
        f.sync_all().unwrap();
    }

    let t2 = Instant::now();
    {
        let f = File::options().read(true).write(true).open(&fixed_path).unwrap();
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };
        let ptr = mmap.as_mut_ptr();
        let doc_sizes_ref = &doc_sizes;

        std::thread::scope(|s| {
            for t in 0..NUM_THREADS {
                let start_slot = t * DOCS_PER_THREAD;
                let end_slot = start_slot + DOCS_PER_THREAD;
                let ptr = ptr as usize;

                s.spawn(move || {
                    let ptr = ptr as *mut u8;
                    let mut rng: u64 = 0xCAFE0000 + t as u64;
                    for slot in start_slot..end_slot {
                        let offset = slot * FIXED_SLOT_SIZE;
                        let size = doc_sizes_ref[slot].min(FIXED_SLOT_SIZE);
                        unsafe {
                            let dst = ptr.add(offset);
                            for i in 0..size {
                                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                                *dst.add(i) = (rng >> 56) as u8;
                            }
                        }
                    }
                });
            }
        });

        mmap.flush().unwrap();
    }
    let fixed_write_elapsed = t2.elapsed();
    let fixed_write_rate = NUM_DOCS as f64 / fixed_write_elapsed.as_secs_f64();
    println!("  Write: {:.0}μs ({:.2}M docs/s)", fixed_write_elapsed.as_micros(), fixed_write_rate / 1_000_000.0);

    // Fixed random reads
    let fixed_mmap = {
        let f = File::open(&fixed_path).unwrap();
        unsafe { memmap2::Mmap::map(&f).unwrap() }
    };

    let mut fixed_read_lats: Vec<Duration> = Vec::with_capacity(num_reads);
    rng = 0xBEEF;
    checksum = 0;

    for _ in 0..num_reads {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let slot = (rng >> 32) as usize % NUM_DOCS;

        let start = Instant::now();
        let offset = slot * FIXED_SLOT_SIZE;
        let doc = &fixed_mmap[offset..offset + FIXED_SLOT_SIZE];
        checksum = checksum.wrapping_add(doc[0] as u64);
        fixed_read_lats.push(start.elapsed());
    }

    fixed_read_lats.sort();
    println!("  Read ({} random): p50={:.0}ns  p95={:.0}ns  p99={:.0}ns  (checksum={})",
        num_reads,
        percentile(&fixed_read_lats, 50.0).as_nanos(),
        percentile(&fixed_read_lats, 95.0).as_nanos(),
        percentile(&fixed_read_lats, 99.0).as_nanos(),
        checksum,
    );
    println!("  Disk: {:.1} MB ({} slots × {} bytes)",
        fixed_total as f64 / 1_048_576.0, NUM_DOCS, FIXED_SLOT_SIZE);
    println!();

    // =====================================================================
    // Summary
    // =====================================================================
    println!("=== Comparison ===");
    println!("  {:>20}  {:>12}  {:>12}  {:>12}  {:>12}",
        "", "Write rate", "Read p50", "Read p99", "Disk");
    println!("  {}", "-".repeat(72));
    println!("  {:>20}  {:>10.2}M/s  {:>10.0}ns  {:>10.0}ns  {:>10.1}MB",
        "Slot table (var)",
        varsize_write_rate / 1_000_000.0,
        percentile(&read_lats, 50.0).as_nanos(),
        percentile(&read_lats, 99.0).as_nanos(),
        (total_data_bytes as f64 + NUM_DOCS as f64 * 12.0) / 1_048_576.0,
    );
    println!("  {:>20}  {:>10.2}M/s  {:>10.0}ns  {:>10.0}ns  {:>10.1}MB",
        "Fixed (256B/slot)",
        fixed_write_rate / 1_000_000.0,
        percentile(&fixed_read_lats, 50.0).as_nanos(),
        percentile(&fixed_read_lats, 99.0).as_nanos(),
        fixed_total as f64 / 1_048_576.0,
    );
    println!();
    println!("  Table overhead: {:.1} MB for {} slots at 107M scale: {:.1} MB",
        NUM_DOCS as f64 * 12.0 / 1_048_576.0, NUM_DOCS,
        107_000_000.0 * 12.0 / 1_048_576.0,
    );

    // Cleanup
    let _ = fs::remove_dir_all(&dir);
}
