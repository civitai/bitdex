//! Experiment 2/2b: Deterministic-offset doc storage benchmark.
//!
//! Concept: offset = slot_id * SLOT_SIZE. No index. Zero memory overhead.
//!
//! Exp 2b adds write optimization strategies:
//!   1. Baseline: seek+write per doc (Exp 2 original)
//!   2. Batched pwrite: accumulate N docs, write batch in one pass
//!   3. mmap writes: memcpy to mmap'd file at deterministic offsets
//!   4. Multi-threaded mmap: parallel rayon writes to non-overlapping regions
//!
//! Run: cargo bench --bench deterministic_offset --features data-silo

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const SLOT_SIZE: usize = 256;
const HEADER_SIZE: usize = 4; // u32 LE doc_len
const DOC_SIZE: usize = 230;
const DEFAULT_WRITE_COUNT: usize = 1_000_000;
const DEFAULT_READ_COUNT: usize = 10_000;

// ---------------------------------------------------------------------------
// Doc generation
// ---------------------------------------------------------------------------

fn generate_doc(slot_id: u32) -> Vec<u8> {
    let mut doc = Vec::with_capacity(DOC_SIZE);
    doc.extend_from_slice(&slot_id.to_le_bytes());
    let remaining = DOC_SIZE - 4;
    for i in 0..remaining {
        doc.push(((slot_id as usize + i) % 256) as u8);
    }
    doc
}

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

// ---------------------------------------------------------------------------
// Strategy 1: Baseline seek+write (from Exp 2)
// ---------------------------------------------------------------------------

fn bench_seek_write(path: &Path, count: usize) -> (Duration, u64) {
    let mut file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(path).unwrap();

    let start = Instant::now();
    for slot in 0..count as u32 {
        let doc = generate_doc(slot);
        let offset = slot as u64 * SLOT_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&(doc.len() as u32).to_le_bytes()).unwrap();
        file.write_all(&doc).unwrap();
    }
    file.sync_all().unwrap();
    let elapsed = start.elapsed();
    let size = fs::metadata(path).unwrap().len();
    (elapsed, size)
}

// ---------------------------------------------------------------------------
// Strategy 2: Batched pwrite
// ---------------------------------------------------------------------------

fn bench_batched_write(path: &Path, count: usize, batch_size: usize) -> (Duration, u64) {
    let mut file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(path).unwrap();

    // Pre-allocate the file
    let total_size = count as u64 * SLOT_SIZE as u64;
    file.set_len(total_size).unwrap();

    let start = Instant::now();
    let mut batch_buf: Vec<(u64, Vec<u8>)> = Vec::with_capacity(batch_size);

    for slot in 0..count as u32 {
        let doc = generate_doc(slot);
        let offset = slot as u64 * SLOT_SIZE as u64;
        let mut slot_buf = Vec::with_capacity(SLOT_SIZE);
        slot_buf.extend_from_slice(&(doc.len() as u32).to_le_bytes());
        slot_buf.extend_from_slice(&doc);
        batch_buf.push((offset, slot_buf));

        if batch_buf.len() >= batch_size {
            // Write batch — sorted by offset for sequential I/O
            for (off, data) in &batch_buf {
                file.seek(SeekFrom::Start(*off)).unwrap();
                file.write_all(data).unwrap();
            }
            batch_buf.clear();
        }
    }
    // Flush remaining
    for (off, data) in &batch_buf {
        file.seek(SeekFrom::Start(*off)).unwrap();
        file.write_all(data).unwrap();
    }
    file.sync_all().unwrap();
    let elapsed = start.elapsed();
    let size = fs::metadata(path).unwrap().len();
    (elapsed, size)
}

// ---------------------------------------------------------------------------
// Strategy 3: mmap writes (single-threaded)
// ---------------------------------------------------------------------------

fn bench_mmap_write(path: &Path, count: usize) -> (Duration, u64) {
    let file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(path).unwrap();

    let total_size = count as u64 * SLOT_SIZE as u64;
    file.set_len(total_size).unwrap();

    // Safety: we own the file exclusively, writing to non-overlapping regions
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };

    let start = Instant::now();
    for slot in 0..count as u32 {
        let doc = generate_doc(slot);
        let offset = slot as usize * SLOT_SIZE;
        let dest = &mut mmap[offset..offset + SLOT_SIZE];
        // Write header
        dest[..4].copy_from_slice(&(doc.len() as u32).to_le_bytes());
        // Write doc
        dest[4..4 + doc.len()].copy_from_slice(&doc);
    }
    mmap.flush().unwrap();
    let elapsed = start.elapsed();
    let size = fs::metadata(path).unwrap().len();
    (elapsed, size)
}

// ---------------------------------------------------------------------------
// Strategy 4: Multi-threaded mmap writes
// ---------------------------------------------------------------------------

fn bench_mmap_parallel(path: &Path, count: usize) -> (Duration, u64) {
    let file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(path).unwrap();

    let total_size = count as u64 * SLOT_SIZE as u64;
    file.set_len(total_size).unwrap();

    let mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    // Get a raw pointer for cross-thread access. Safe because each thread writes
    // to non-overlapping slot regions (offset = slot_id * SLOT_SIZE).
    let mmap_ptr = mmap.as_ptr() as usize;
    let mmap_len = mmap.len();

    let start = Instant::now();
    let num_threads = rayon::current_num_threads();
    let chunk_size = (count + num_threads - 1) / num_threads;

    rayon::scope(|s| {
        for thread_idx in 0..num_threads {
            let start_slot = thread_idx * chunk_size;
            let end_slot = ((thread_idx + 1) * chunk_size).min(count);
            let ptr = mmap_ptr;
            let len = mmap_len;

            s.spawn(move |_| {
                for slot in start_slot..end_slot {
                    let doc = generate_doc(slot as u32);
                    let offset = slot * SLOT_SIZE;
                    if offset + SLOT_SIZE <= len {
                        // Safety: non-overlapping regions, each thread owns its slot range
                        unsafe {
                            let dest = (ptr as *mut u8).add(offset);
                            std::ptr::copy_nonoverlapping(
                                (doc.len() as u32).to_le_bytes().as_ptr(),
                                dest,
                                4,
                            );
                            std::ptr::copy_nonoverlapping(
                                doc.as_ptr(),
                                dest.add(4),
                                doc.len(),
                            );
                        }
                    }
                }
            });
        }
    });

    // Need to drop mmap to flush, but we need explicit flush first
    drop(mmap);
    // Reopen for sync
    let file = OpenOptions::new().read(true).write(true).open(path).unwrap();
    file.sync_all().unwrap();
    let elapsed = start.elapsed();
    let size = fs::metadata(path).unwrap().len();
    (elapsed, size)
}

// ---------------------------------------------------------------------------
// Read benchmark (shared across all strategies)
// ---------------------------------------------------------------------------

fn bench_reads(path: &Path, max_slot: u32, count: usize) -> Vec<Duration> {
    let mut file = File::open(path).unwrap();

    let mut rng_state: u64 = 0xDEADBEEF;
    let slots: Vec<u32> = (0..count)
        .map(|_| {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (rng_state >> 33) as u32 % max_slot
        })
        .collect();

    let mut latencies = Vec::with_capacity(count);
    let mut hdr = [0u8; HEADER_SIZE];
    for &slot in &slots {
        let start = Instant::now();
        let offset = slot as u64 * SLOT_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut hdr).unwrap();
        let doc_len = u32::from_le_bytes(hdr) as usize;
        if doc_len > 0 && doc_len + HEADER_SIZE <= SLOT_SIZE {
            let mut buf = vec![0u8; doc_len];
            file.read_exact(&mut buf).unwrap();
            let stored_slot = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            assert_eq!(stored_slot, slot, "data corruption at slot {}", slot);
        }
        latencies.push(start.elapsed());
    }
    latencies
}

fn print_write_result(name: &str, elapsed: Duration, count: usize, file_size: u64) {
    let docs_per_sec = count as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (count as f64 * DOC_SIZE as f64) / elapsed.as_secs_f64() / 1e6;
    println!("  {:<30} {:>8.0} docs/s  {:>6.0} MB/s  {:.2}s  {:.2} GB",
        name, docs_per_sec, mb_per_sec, elapsed.as_secs_f64(), file_size as f64 / 1e9);
}

fn print_read_result(latencies: &mut Vec<Duration>) {
    latencies.sort();
    println!("  Read p50={:.1}μs  p95={:.1}μs  p99={:.1}μs  mean={:.1}μs",
        percentile(latencies, 50.0).as_nanos() as f64 / 1000.0,
        percentile(latencies, 95.0).as_nanos() as f64 / 1000.0,
        percentile(latencies, 99.0).as_nanos() as f64 / 1000.0,
        latencies.iter().map(|d| d.as_nanos()).sum::<u128>() as f64 / latencies.len() as f64 / 1000.0,
    );
}

fn main() {
    let count = std::env::var("BENCH_WRITE_COUNT")
        .ok().and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_WRITE_COUNT);
    let read_count = std::env::var("BENCH_READ_COUNT")
        .ok().and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_READ_COUNT);

    let dir = PathBuf::from("data/silo-experiment2");
    fs::create_dir_all(&dir).unwrap();

    println!("=== Experiment 2b: Deterministic-Offset Write Optimization ===");
    println!("Docs: {}  Reads: {}  Doc size: {}B  Slot size: {}B  Threads: {}",
        count, read_count, DOC_SIZE, SLOT_SIZE, rayon::current_num_threads());
    println!("Target: beat 405K docs/s (DocStore V2 baseline)");
    println!();
    println!("  {:<30} {:>12}  {:>8}  {:>6}  {:>7}", "Strategy", "Throughput", "MB/s", "Time", "Size");
    println!("  {}", "-".repeat(75));

    // Strategy 1: Baseline seek+write
    let path = dir.join("bench_seekwrite.dat");
    let (elapsed, size) = bench_seek_write(&path, count);
    print_write_result("1. Seek+write (baseline)", elapsed, count, size);
    let mut lats = bench_reads(&path, count as u32, read_count);
    print_read_result(&mut lats);
    let _ = fs::remove_file(&path);
    println!();

    // Strategy 2: Batched pwrite
    for batch_size in [64, 256, 1024] {
        let path = dir.join(format!("bench_batch_{}.dat", batch_size));
        let (elapsed, size) = bench_batched_write(&path, count, batch_size);
        print_write_result(&format!("2. Batched (n={})", batch_size), elapsed, count, size);
        let mut lats = bench_reads(&path, count as u32, read_count);
        print_read_result(&mut lats);
        let _ = fs::remove_file(&path);
    }
    println!();

    // Strategy 3: mmap single-threaded
    let path = dir.join("bench_mmap_st.dat");
    let (elapsed, size) = bench_mmap_write(&path, count);
    print_write_result("3. mmap (single-thread)", elapsed, count, size);
    let mut lats = bench_reads(&path, count as u32, read_count);
    print_read_result(&mut lats);
    let _ = fs::remove_file(&path);
    println!();

    // Strategy 4: mmap multi-threaded
    let path = dir.join("bench_mmap_mt.dat");
    let (elapsed, size) = bench_mmap_parallel(&path, count);
    print_write_result("4. mmap (multi-thread)", elapsed, count, size);
    let mut lats = bench_reads(&path, count as u32, read_count);
    print_read_result(&mut lats);
    let _ = fs::remove_file(&path);
    println!();

    // Summary
    println!("--- Projections at 107M scale ---");
    println!("  Disk: {:.1} GB (256B slots)", 107_000_000u64 as f64 * 256.0 / 1e9);
    println!("  Memory: 0 bytes (no index)");
    println!();
    println!("--- DocStore V2 Baseline ---");
    println!("  Write: 405K docs/s  |  Read: 10μs warm  |  Memory: ~1.6 GB index");
}
