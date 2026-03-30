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

    // =========================================================================
    // Experiment 2d: Sharded mmap write contention test
    // =========================================================================
    println!("=== Experiment 2d: Sharded mmap Write Contention ===");
    let num_shards: usize = 32;
    println!("Shards: {}  Threads: {}  Sharding: slot_id % {}",
        num_shards, rayon::current_num_threads(), num_shards);
    println!("Each thread writes to ALL shards (modulo distribution).");
    println!();

    {
        let shard_dir = dir.join("sharded_test");
        fs::create_dir_all(&shard_dir).unwrap();

        // Calculate slots per shard: count / num_shards (round up)
        let slots_per_shard = (count + num_shards - 1) / num_shards;
        let shard_file_size = slots_per_shard as u64 * SLOT_SIZE as u64;

        // Create and mmap all shard files
        let shard_files: Vec<File> = (0..num_shards)
            .map(|i| {
                let p = shard_dir.join(format!("silo_{:02}.dat", i));
                let f = OpenOptions::new()
                    .read(true).write(true).create(true).truncate(true)
                    .open(&p).unwrap();
                f.set_len(shard_file_size).unwrap();
                f
            })
            .collect();
        let shard_mmaps: Vec<memmap2::MmapMut> = shard_files.iter()
            .map(|f| unsafe { memmap2::MmapMut::map_mut(f).unwrap() })
            .collect();

        // Get raw pointers for each shard
        let shard_ptrs: Vec<(usize, usize)> = shard_mmaps.iter()
            .map(|m| (m.as_ptr() as usize, m.len()))
            .collect();
        let shard_ptrs_ref = &shard_ptrs;

        let num_threads = rayon::current_num_threads();
        let chunk_size = (count + num_threads - 1) / num_threads;

        let start = Instant::now();
        rayon::scope(|s| {
            for thread_idx in 0..num_threads {
                let start_slot = thread_idx * chunk_size;
                let end_slot = ((thread_idx + 1) * chunk_size).min(count);

                s.spawn(move |_| {
                    for slot in start_slot..end_slot {
                        let doc = generate_doc(slot as u32);
                        let shard = slot % num_shards;
                        let slot_within_shard = slot / num_shards;
                        let offset = slot_within_shard * SLOT_SIZE;
                        let (ptr, len) = shard_ptrs_ref[shard];
                        if offset + SLOT_SIZE <= len {
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

        // Flush all shards
        for mmap in &shard_mmaps {
            mmap.flush().unwrap();
        }
        let elapsed = start.elapsed();
        let total_size: u64 = (0..num_shards)
            .map(|i| fs::metadata(shard_dir.join(format!("silo_{:02}.dat", i))).unwrap().len())
            .sum();

        let docs_per_sec = count as f64 / elapsed.as_secs_f64();
        let mb_per_sec = (count as f64 * DOC_SIZE as f64) / elapsed.as_secs_f64() / 1e6;
        println!("  Sharded (32 files, {} threads): {:.0} docs/s  {:.0} MB/s  {:.2}s  {:.2} GB total",
            num_threads, docs_per_sec, mb_per_sec, elapsed.as_secs_f64(), total_size as f64 / 1e9);
        println!("  vs single-file mmap (32 threads): 6.49M docs/s");
        let ratio = docs_per_sec / 6_490_000.0;
        println!("  Ratio: {:.2}x (>0.9 = no contention)", ratio);

        // Cleanup
        let _ = fs::remove_dir_all(&shard_dir);
    }
    println!();

    // =========================================================================
    // Experiment 2c: Ops log impact on read latency
    // =========================================================================
    println!("=== Experiment 2c: Ops Log Read Latency Impact ===");
    println!("Snapshot: 1M docs (mmap write+read). Ops log: LIFO scan for slot_id override.");
    println!();

    // Write snapshot via mmap
    let snap_path = dir.join("bench_ops_snapshot.dat");
    bench_mmap_write(&snap_path, count);

    // Ops log entry: [slot_id: u32][doc_len: u32][doc_bytes]
    const OP_HEADER: usize = 8; // slot_id(4) + doc_len(4)

    println!("  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "Ops", "p50 (μs)", "p95 (μs)", "p99 (μs)", "mean (μs)", "log size");
    println!("  {}", "-".repeat(65));

    for num_ops in [0usize, 10, 100, 1000, 10000] {
        // Build ops log in memory (simulating append-only file)
        let mut ops_log: Vec<u8> = Vec::new();
        // Track which slots have ops for realistic hit rate
        let mut rng_state: u64 = 0xCAFEBABE;
        for _ in 0..num_ops {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let slot = (rng_state >> 33) as u32 % count as u32;
            let doc = generate_doc(slot);
            ops_log.extend_from_slice(&slot.to_le_bytes());
            ops_log.extend_from_slice(&(doc.len() as u32).to_le_bytes());
            ops_log.extend_from_slice(&doc);
        }

        // Build reverse index of ops log entries for LIFO lookup
        // (offset, slot_id) pairs scanned from end
        let mut ops_entries: Vec<(usize, u32)> = Vec::new();
        {
            let mut pos = 0;
            while pos + OP_HEADER <= ops_log.len() {
                let slot = u32::from_le_bytes([ops_log[pos], ops_log[pos+1], ops_log[pos+2], ops_log[pos+3]]);
                let doc_len = u32::from_le_bytes([ops_log[pos+4], ops_log[pos+5], ops_log[pos+6], ops_log[pos+7]]) as usize;
                ops_entries.push((pos, slot));
                pos += OP_HEADER + doc_len;
            }
        }

        // Read benchmark: for each slot, LIFO scan ops log first, then fall back to snapshot
        // Use mmap for snapshot reads (production design)
        let snap_file = File::open(&snap_path).unwrap();
        let snap_mmap = unsafe { memmap2::Mmap::map(&snap_file).unwrap() };
        let mut rng2: u64 = 0xDEADBEEF;
        let slots: Vec<u32> = (0..read_count)
            .map(|_| {
                rng2 = rng2.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (rng2 >> 33) as u32 % count as u32
            })
            .collect();

        let mut latencies = Vec::with_capacity(read_count);

        for &target_slot in &slots {
            let start = Instant::now();

            // LIFO scan: search ops log from end for matching slot_id
            let mut found = false;
            for &(offset, slot) in ops_entries.iter().rev() {
                if slot == target_slot {
                    // Found in ops log — read doc from ops_log bytes
                    let doc_len = u32::from_le_bytes([
                        ops_log[offset+4], ops_log[offset+5],
                        ops_log[offset+6], ops_log[offset+7],
                    ]) as usize;
                    let doc = &ops_log[offset + OP_HEADER..offset + OP_HEADER + doc_len];
                    let stored = u32::from_le_bytes([doc[0], doc[1], doc[2], doc[3]]);
                    assert_eq!(stored, target_slot);
                    found = true;
                    break;
                }
            }

            if !found {
                // Fall back to snapshot via mmap (deterministic offset read)
                let offset = target_slot as usize * SLOT_SIZE;
                let slot_data = &snap_mmap[offset..offset + SLOT_SIZE];
                let doc_len = u32::from_le_bytes([slot_data[0], slot_data[1], slot_data[2], slot_data[3]]) as usize;
                if doc_len > 0 && doc_len + HEADER_SIZE <= SLOT_SIZE {
                    // Just access the bytes — mmap handles the read
                    let _doc = &slot_data[HEADER_SIZE..HEADER_SIZE + doc_len];
                }
            }

            latencies.push(start.elapsed());
        }

        latencies.sort();
        println!("  {:>8}  {:>10.1}  {:>10.1}  {:>10.1}  {:>10.1}  {:>10}",
            num_ops,
            percentile(&latencies, 50.0).as_nanos() as f64 / 1000.0,
            percentile(&latencies, 95.0).as_nanos() as f64 / 1000.0,
            percentile(&latencies, 99.0).as_nanos() as f64 / 1000.0,
            latencies.iter().map(|d| d.as_nanos()).sum::<u128>() as f64 / latencies.len() as f64 / 1000.0,
            if ops_log.len() > 1024 { format!("{} KB", ops_log.len() / 1024) } else { format!("{} B", ops_log.len()) },
        );
    }

    let _ = fs::remove_file(&snap_path);
    println!();

    // =========================================================================
    // Experiment 2e: mmap'd bitmap deserialization
    // =========================================================================
    println!("=== Experiment 2e: Bitmap mmap Deserialization ===");
    {
        use roaring::RoaringBitmap;

        // Create bitmaps of varying sizes (simulating filter/sort bitmaps at scale)
        let sizes = [1_000u32, 10_000, 100_000, 1_000_000, 10_000_000];

        println!("  {:>12}  {:>12}  {:>12}  {:>12}  {:>10}",
            "Bitmap size", "File bytes", "Vec+deser", "mmap+deser", "Speedup");
        println!("  {}", "-".repeat(65));

        for &n in &sizes {
            // Build a bitmap with n set bits (scattered for realism)
            let mut bm = RoaringBitmap::new();
            let mut rng: u64 = 0xBEEF;
            for _ in 0..n {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                bm.insert((rng >> 32) as u32);
            }

            // Serialize to file
            let bm_path = dir.join(format!("bench_bitmap_{}.bin", n));
            {
                let mut f = File::create(&bm_path).unwrap();
                bm.serialize_into(&mut f).unwrap();
                f.sync_all().unwrap();
            }
            let file_size = fs::metadata(&bm_path).unwrap().len();

            // Drop page cache hint (best effort — won't work on Windows but harmless)
            // On Windows, just reopen the file fresh each time.

            // Method 1: Read into Vec, then deserialize
            let vec_times: Vec<Duration> = (0..100).map(|_| {
                let start = Instant::now();
                let data = fs::read(&bm_path).unwrap();
                let _bm = RoaringBitmap::deserialize_from(data.as_slice()).unwrap();
                start.elapsed()
            }).collect();
            let vec_median = {
                let mut t = vec_times.clone();
                t.sort();
                t[t.len() / 2]
            };

            // Method 2: mmap, then deserialize from mmap'd slice
            let mmap_times: Vec<Duration> = (0..100).map(|_| {
                let start = Instant::now();
                let f = File::open(&bm_path).unwrap();
                let mmap = unsafe { memmap2::Mmap::map(&f).unwrap() };
                let _bm = RoaringBitmap::deserialize_from(&mmap[..]).unwrap();
                start.elapsed()
            }).collect();
            let mmap_median = {
                let mut t = mmap_times.clone();
                t.sort();
                t[t.len() / 2]
            };

            let speedup = vec_median.as_nanos() as f64 / mmap_median.as_nanos().max(1) as f64;
            println!("  {:>12}  {:>10} B  {:>10.0}μs  {:>10.0}μs  {:>9.2}x",
                format!("{}K", n / 1000),
                file_size,
                vec_median.as_nanos() as f64 / 1000.0,
                mmap_median.as_nanos() as f64 / 1000.0,
                speedup,
            );

            let _ = fs::remove_file(&bm_path);
        }
    }
    println!();

    // Summary
    println!("--- Projections at 107M scale ---");
    println!("  Disk: {:.1} GB (256B slots)", 107_000_000u64 as f64 * 256.0 / 1e9);
    println!("  Memory: 0 bytes (no index)");
    println!();
    println!("--- DocStore V2 Baseline ---");
    println!("  Write: 405K docs/s  |  Read: 10μs warm  |  Memory: ~1.6 GB index");
}
