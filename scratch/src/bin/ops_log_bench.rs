/// Microbench: mmap vs BufWriter for append-only ops log
///
/// Tests the two approaches for writing sequential ops:
/// A) BufWriter to a regular file (current OpsLog implementation)
/// B) mmap'd file with atomic bump allocator (like ParallelWriter)
///
/// Also tests read-back speed for both.

use std::hint::black_box;
use std::io::Write;
use std::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};

const NUM_OPS: u64 = 2_000_000;
const OP_SIZE: usize = 250; // typical Merge op size

fn main() {
    println!("=== Ops Log Append Benchmark ===\n");
    println!("  {} ops × {} bytes = {:.1} MB\n", NUM_OPS, OP_SIZE,
        NUM_OPS as f64 * OP_SIZE as f64 / 1e6);

    let op_data = vec![0xABu8; OP_SIZE];

    // ── A: BufWriter (current implementation) ──────────────────────────
    println!("--- A: BufWriter (64KB buffer) ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ops.log");

        let t = Instant::now();
        {
            let file = std::fs::OpenOptions::new()
                .create(true).append(true).open(&path).unwrap();
            let mut writer = std::io::BufWriter::with_capacity(65536, file);
            for _ in 0..NUM_OPS {
                writer.write_all(&op_data).unwrap();
            }
            writer.flush().unwrap();
        }
        let write_time = t.elapsed();
        let file_size = std::fs::metadata(&path).unwrap().len();
        println!("  Write: {:.3}s ({:.1}M ops/s, {:.1} MB/s)",
            write_time.as_secs_f64(),
            NUM_OPS as f64 / write_time.as_secs_f64() / 1e6,
            file_size as f64 / write_time.as_secs_f64() / 1e6);

        // Read back: mmap and scan
        let t = Instant::now();
        let file = std::fs::File::open(&path).unwrap();
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let mut pos = 0;
        let mut count = 0u64;
        while pos + OP_SIZE <= mmap.len() {
            black_box(&mmap[pos..pos + OP_SIZE]);
            pos += OP_SIZE;
            count += 1;
        }
        let read_time = t.elapsed();
        println!("  Read:  {:.3}s ({:.1}M ops/s) [{} ops]",
            read_time.as_secs_f64(),
            count as f64 / read_time.as_secs_f64() / 1e6,
            count);
    }

    // ── B: mmap'd file with cursor ─────────────────────────────────────
    println!("\n--- B: mmap (pre-allocated, atomic cursor) ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ops.mmap");

        let total_size = NUM_OPS as u64 * OP_SIZE as u64;
        let file = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).open(&path).unwrap();
        file.set_len(total_size).unwrap();
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
        let cursor = AtomicU64::new(0);

        let t = Instant::now();
        for _ in 0..NUM_OPS {
            let offset = cursor.fetch_add(OP_SIZE as u64, Ordering::Relaxed) as usize;
            let dst = &mmap[offset..offset + OP_SIZE] as *const [u8] as *mut [u8];
            unsafe { (*dst).copy_from_slice(&op_data); }
        }
        mmap.flush().unwrap();
        let write_time = t.elapsed();
        println!("  Write: {:.3}s ({:.1}M ops/s, {:.1} MB/s)",
            write_time.as_secs_f64(),
            NUM_OPS as f64 / write_time.as_secs_f64() / 1e6,
            total_size as f64 / write_time.as_secs_f64() / 1e6);

        // Read back
        let t = Instant::now();
        let used = cursor.load(Ordering::Relaxed) as usize;
        let mut pos = 0;
        let mut count = 0u64;
        while pos + OP_SIZE <= used {
            black_box(&mmap[pos..pos + OP_SIZE]);
            pos += OP_SIZE;
            count += 1;
        }
        let read_time = t.elapsed();
        println!("  Read:  {:.3}s ({:.1}M ops/s) [{} ops]",
            read_time.as_secs_f64(),
            count as f64 / read_time.as_secs_f64() / 1e6,
            count);
    }

    // ── C: mmap'd with CRC32 framing (realistic ops log) ──────────────
    println!("\n--- C: mmap with CRC32 framing (realistic) ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ops_crc.mmap");

        // Frame: [u32 key][u32 value_len][value bytes][u32 crc32]
        let frame_overhead = 4 + 4 + 4; // key + len + crc
        let frame_size = OP_SIZE + frame_overhead;
        let total_size = NUM_OPS as u64 * frame_size as u64;
        let file = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).open(&path).unwrap();
        file.set_len(total_size).unwrap();
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
        let cursor = AtomicU64::new(0);

        let t = Instant::now();
        for i in 0..NUM_OPS {
            let key = i as u32;
            let offset = cursor.fetch_add(frame_size as u64, Ordering::Relaxed) as usize;
            unsafe {
                let base = mmap.as_ptr().add(offset) as *mut u8;
                let d = std::slice::from_raw_parts_mut(base, frame_size);
                d[0..4].copy_from_slice(&key.to_le_bytes());
                d[4..8].copy_from_slice(&(OP_SIZE as u32).to_le_bytes());
                d[8..8 + OP_SIZE].copy_from_slice(&op_data);
                let crc = crc32fast::hash(&d[0..8 + OP_SIZE]);
                d[8 + OP_SIZE..frame_size].copy_from_slice(&crc.to_le_bytes());
            }
        }
        mmap.flush().unwrap();
        let write_time = t.elapsed();
        println!("  Write: {:.3}s ({:.1}M ops/s, {:.1} MB/s)",
            write_time.as_secs_f64(),
            NUM_OPS as f64 / write_time.as_secs_f64() / 1e6,
            total_size as f64 / write_time.as_secs_f64() / 1e6);

        // Read back with CRC validation
        let t = Instant::now();
        let used = cursor.load(Ordering::Relaxed) as usize;
        let mut pos = 0;
        let mut count = 0u64;
        let mut crc_ok = 0u64;
        while pos + 8 <= used {
            let key = u32::from_le_bytes(mmap[pos..pos+4].try_into().unwrap());
            let len = u32::from_le_bytes(mmap[pos+4..pos+8].try_into().unwrap()) as usize;
            if pos + 8 + len + 4 > used { break; }
            let data = &mmap[pos+8..pos+8+len];
            let stored_crc = u32::from_le_bytes(mmap[pos+8+len..pos+8+len+4].try_into().unwrap());
            let computed_crc = crc32fast::hash(&mmap[pos..pos+8+len]);
            if stored_crc == computed_crc { crc_ok += 1; }
            black_box((key, data));
            pos += 8 + len + 4;
            count += 1;
        }
        let read_time = t.elapsed();
        println!("  Read:  {:.3}s ({:.1}M ops/s, CRC valid: {}/{}) ",
            read_time.as_secs_f64(),
            count as f64 / read_time.as_secs_f64() / 1e6,
            crc_ok, count);
    }

    // ── D: mmap with 1MB thread-local regions (ParallelWriter approach) ──
    println!("\n--- D: mmap with 1MB thread-local regions (32 threads) ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ops_parallel.mmap");

        let frame_size = OP_SIZE + 12; // key(4) + len(4) + data + crc(4)
        let total_size = NUM_OPS as u64 * frame_size as u64 * 2; // 2x headroom
        let file = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).open(&path).unwrap();
        file.set_len(total_size).unwrap();
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
        let global_cursor = AtomicU64::new(0);
        let ops_written = AtomicU64::new(0);

        const REGION_SIZE: u64 = 1 << 20; // 1MB regions

        let t = Instant::now();
        let num_threads = 32usize;
        let ops_per_thread = NUM_OPS / num_threads as u64;

        std::thread::scope(|s| {
            for thread_id in 0..num_threads {
                let mmap_ptr = mmap.as_ptr() as usize; // Send-safe pointer
                let mmap_len = mmap.len();
                let global = &global_cursor;
                let counter = &ops_written;
                let op = &op_data;

                s.spawn(move || {
                    let mut cursor: usize = 0;
                    let mut region_end: usize = 0;

                    for i in 0..ops_per_thread {
                        let key = (thread_id as u64 * ops_per_thread + i) as u32;

                        // Allocate from thread-local region
                        if cursor + frame_size > region_end {
                            let start = global.fetch_add(REGION_SIZE, Ordering::Relaxed) as usize;
                            cursor = start;
                            region_end = start + REGION_SIZE as usize;
                        }

                        if cursor + frame_size > mmap_len { break; }

                        unsafe {
                            let base = (mmap_ptr as *mut u8).add(cursor);
                            let d = std::slice::from_raw_parts_mut(base, frame_size);
                            d[0..4].copy_from_slice(&key.to_le_bytes());
                            d[4..8].copy_from_slice(&(OP_SIZE as u32).to_le_bytes());
                            d[8..8 + OP_SIZE].copy_from_slice(op);
                            let crc = crc32fast::hash(&d[0..8 + OP_SIZE]);
                            d[8 + OP_SIZE..frame_size].copy_from_slice(&crc.to_le_bytes());
                        }

                        cursor += frame_size;
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });

        let write_time = t.elapsed();
        let total_written = ops_written.load(Ordering::Relaxed);
        let bytes_used = global_cursor.load(Ordering::Relaxed);
        println!("  Write: {:.3}s ({:.1}M ops/s, {:.1} MB/s) [{} ops]",
            write_time.as_secs_f64(),
            total_written as f64 / write_time.as_secs_f64() / 1e6,
            bytes_used as f64 / write_time.as_secs_f64() / 1e6,
            total_written);

        // Read back (sequential scan of used portion)
        let t = Instant::now();
        let used = bytes_used as usize;
        let mut pos = 0;
        let mut count = 0u64;
        let mut crc_ok = 0u64;
        while pos + 8 <= used {
            let len_bytes = mmap.get(pos+4..pos+8);
            if len_bytes.is_none() { break; }
            let len = u32::from_le_bytes(mmap[pos+4..pos+8].try_into().unwrap()) as usize;
            if len == 0 || len > OP_SIZE * 2 { pos += 1; continue; } // skip padding
            if pos + 8 + len + 4 > used { break; }
            let stored_crc = u32::from_le_bytes(mmap[pos+8+len..pos+8+len+4].try_into().unwrap());
            let computed_crc = crc32fast::hash(&mmap[pos..pos+8+len]);
            if stored_crc == computed_crc {
                crc_ok += 1;
                black_box(&mmap[pos+8..pos+8+len]);
                pos += 8 + len + 4;
            } else {
                pos += 1; // skip padding bytes between regions
            }
            count += 1;
        }
        let read_time = t.elapsed();
        println!("  Read:  {:.3}s ({:.1}M valid ops/s, CRC valid: {}/{})",
            read_time.as_secs_f64(),
            crc_ok as f64 / read_time.as_secs_f64() / 1e6,
            crc_ok, count);
    }

    // ── E: mmap with 64KB thread-local regions (32 threads) ──────────────
    println!("\n--- E: mmap with 64KB thread-local regions (32 threads) ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ops_64k.mmap");

        let frame_size = OP_SIZE + 12;
        let total_size = NUM_OPS as u64 * frame_size as u64 * 2;
        let file = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).open(&path).unwrap();
        file.set_len(total_size).unwrap();
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
        let global_cursor = AtomicU64::new(0);
        let ops_written = AtomicU64::new(0);

        const REGION_64K: u64 = 64 * 1024; // 64KB regions

        let t = Instant::now();
        let num_threads = 32usize;
        let ops_per_thread = NUM_OPS / num_threads as u64;

        std::thread::scope(|s| {
            for thread_id in 0..num_threads {
                let mmap_ptr = mmap.as_ptr() as usize;
                let mmap_len = mmap.len();
                let global = &global_cursor;
                let counter = &ops_written;
                let op = &op_data;

                s.spawn(move || {
                    let mut cursor: usize = 0;
                    let mut region_end: usize = 0;

                    for i in 0..ops_per_thread {
                        let key = (thread_id as u64 * ops_per_thread + i) as u32;

                        if cursor + frame_size > region_end {
                            let start = global.fetch_add(REGION_64K, Ordering::Relaxed) as usize;
                            cursor = start;
                            region_end = start + REGION_64K as usize;
                        }

                        if cursor + frame_size > mmap_len { break; }

                        unsafe {
                            let base = (mmap_ptr as *mut u8).add(cursor);
                            let d = std::slice::from_raw_parts_mut(base, frame_size);
                            d[0..4].copy_from_slice(&key.to_le_bytes());
                            d[4..8].copy_from_slice(&(OP_SIZE as u32).to_le_bytes());
                            d[8..8 + OP_SIZE].copy_from_slice(op);
                            let crc = crc32fast::hash(&d[0..8 + OP_SIZE]);
                            d[8 + OP_SIZE..frame_size].copy_from_slice(&crc.to_le_bytes());
                        }

                        cursor += frame_size;
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });

        let write_time = t.elapsed();
        let total_written = ops_written.load(Ordering::Relaxed);
        let bytes_used = global_cursor.load(Ordering::Relaxed);
        println!("  Write: {:.3}s ({:.1}M ops/s, {:.1} MB/s) [{} ops]",
            write_time.as_secs_f64(),
            total_written as f64 / write_time.as_secs_f64() / 1e6,
            bytes_used as f64 / write_time.as_secs_f64() / 1e6,
            total_written);

        // Waste calculation
        let ideal_bytes = total_written * frame_size as u64;
        let waste_pct = (bytes_used - ideal_bytes) as f64 / bytes_used as f64 * 100.0;
        println!("  Waste: {:.1}% ({:.1} MB used, {:.1} MB ideal)",
            waste_pct, bytes_used as f64 / 1e6, ideal_bytes as f64 / 1e6);
    }

    // ── F: Single-thread mmap sequential (steady-state simulation) ──────
    println!("\n--- F: mmap sequential single-thread (steady-state) ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ops_steady.mmap");

        let frame_size = OP_SIZE + 12;
        let steady_ops = 100_000u64; // simulate 100K ops (typical between compactions)
        let total_size = steady_ops * frame_size as u64 * 2;
        let file = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).open(&path).unwrap();
        file.set_len(total_size).unwrap();
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
        let mut cursor: usize = 0;

        let t = Instant::now();
        for i in 0..steady_ops {
            let key = i as u32;
            unsafe {
                let base = mmap.as_ptr().add(cursor) as *mut u8;
                let d = std::slice::from_raw_parts_mut(base, frame_size);
                d[0..4].copy_from_slice(&key.to_le_bytes());
                d[4..8].copy_from_slice(&(OP_SIZE as u32).to_le_bytes());
                d[8..8 + OP_SIZE].copy_from_slice(&op_data);
                let crc = crc32fast::hash(&d[0..8 + OP_SIZE]);
                d[8 + OP_SIZE..frame_size].copy_from_slice(&crc.to_le_bytes());
            }
            cursor += frame_size;
        }
        let write_time = t.elapsed();
        println!("  Write: {:.3}s ({:.1}M ops/s) [{} ops, steady-state sim]",
            write_time.as_secs_f64(),
            steady_ops as f64 / write_time.as_secs_f64() / 1e6,
            steady_ops);
        println!("  Waste: 0% (sequential, no regions)");
    }

    println!("\n=== Done ===");
}
