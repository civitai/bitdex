/// Tune DataSilo parallel write: region size sweep + cost breakdown.
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Pointer as usize for Send+Sync across threads.
/// Safety: we guarantee disjoint access regions via atomic bump allocator.
fn ptr_to_usize(p: *mut u8) -> usize { p as usize }
fn usize_to_ptr(u: usize) -> *mut u8 { u as *mut u8 }

fn main() {
    let num_entries = 10_000_000u32;
    let doc_size = 230usize;
    let doc_data: Vec<u8> = (0..doc_size).map(|i| (i % 256) as u8).collect();
    let threads = rayon::current_num_threads();

    println!("=== DataSilo Write Tuning ({} entries × {}B, {} threads) ===\n", num_entries, doc_size, threads);

    // Baseline: raw memcpy speed (no mmap, just memory)
    println!("--- Baseline: raw memcpy to Vec ---");
    let mut buf = vec![0u8; num_entries as usize * doc_size];
    let t = Instant::now();
    for i in 0..num_entries as usize {
        let offset = i * doc_size;
        buf[offset..offset + doc_size].copy_from_slice(&doc_data);
    }
    let memcpy_elapsed = t.elapsed();
    println!("  Sequential: {:.2}s ({:.1}M/s)",
        memcpy_elapsed.as_secs_f64(), num_entries as f64 / memcpy_elapsed.as_secs_f64() / 1e6);

    // Parallel memcpy to same Vec (via unsafe raw pointer)
    let t = Instant::now();
    let ptr = ptr_to_usize(buf.as_mut_ptr());
    let chunk = num_entries as usize / threads;
    {
        let ptr = ptr; // move into scope
        let doc_data = &doc_data;
        (0..threads).into_par_iter().for_each(move |tid| {
            let start = tid * chunk;
            let end = if tid == threads - 1 { num_entries as usize } else { start + chunk };
            for i in start..end {
                let offset = i * doc_size;
                unsafe {
                    std::ptr::copy_nonoverlapping(doc_data.as_ptr(), usize_to_ptr(ptr).add(offset), doc_size);
                }
            }
        });
    }
    let par_memcpy_elapsed = t.elapsed();
    println!("  Parallel:   {:.2}s ({:.1}M/s)\n",
        par_memcpy_elapsed.as_secs_f64(), num_entries as f64 / par_memcpy_elapsed.as_secs_f64() / 1e6);
    drop(buf);

    // Sweep region sizes
    println!("--- Region Size Sweep (parallel mmap) ---");
    let region_sizes: Vec<u64> = vec![
        4 * 1024,       // 4KB (1 page)
        16 * 1024,      // 16KB
        64 * 1024,      // 64KB
        256 * 1024,     // 256KB
        1024 * 1024,    // 1MB
        4 * 1024 * 1024,  // 4MB
        16 * 1024 * 1024, // 16MB
    ];

    for &region_size in &region_sizes {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = datasilo::DataSilo::open(
            dir.path(),
            datasilo::SiloConfig { buffer_ratio: 1.0 },
        ).unwrap();

        let total_bytes = (num_entries as u64 * doc_size as u64) * 6 / 5;
        let writer = silo.prepare_parallel_writer(num_entries - 1, total_bytes).unwrap();

        let chunk_size = (num_entries as usize / threads).max(1);
        let t = Instant::now();

        // Use custom region size via thread-local with manual region management
        // Extract raw values before closure to avoid capturing ParallelWriter
        let data_offset = std::sync::Arc::new(AtomicU64::new(0));
        let data_mmap_ptr = ptr_to_usize(writer.data_ptr());
        let data_mmap_len = writer.data_len();
        let index_mmap_ptr = ptr_to_usize(writer.index_ptr());
        let index_mmap_len = writer.index_len();
        let entries_written = std::sync::Arc::new(AtomicU64::new(0));

        (0..num_entries).into_par_iter().with_min_len(chunk_size).for_each_init(
            || {
                // Thread-local state: current region
                (0usize, 0usize) // (cursor, region_end)
            },
            |(cursor, region_end), i| {
                let len = doc_size;
                // Claim new region if needed
                if *cursor + len > *region_end {
                    let start = data_offset.fetch_add(region_size, Ordering::Relaxed) as usize;
                    *cursor = start;
                    *region_end = start + region_size as usize;
                }

                let offset = *cursor;
                *cursor += len;

                if offset + len <= data_mmap_len {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            doc_data.as_ptr(),
                            usize_to_ptr(data_mmap_ptr).add(offset),
                            len,
                        );
                    }
                }

                // Index entry
                let entry = datasilo::IndexEntry {
                    offset: offset as u64,
                    length: len as u32,
                    allocated: len as u32,
                };
                let idx_pos = i as usize * 16; // INDEX_ENTRY_SIZE
                if idx_pos + 16 <= index_mmap_len {
                    unsafe {
                        let bytes: [u8; 16] = std::mem::transmute(entry);
                        std::ptr::copy_nonoverlapping(
                            bytes.as_ptr(),
                            usize_to_ptr(index_mmap_ptr).add(idx_pos),
                            16,
                        );
                    }
                }

                entries_written.fetch_add(1, Ordering::Relaxed);
            },
        );

        let count = silo.finish_parallel_write(writer).unwrap();
        let elapsed = t.elapsed();
        let rate = count as f64 / elapsed.as_secs_f64();

        println!("  region={:>6}KB  {:.2}s  {:.1}M entries/sec",
            region_size / 1024, elapsed.as_secs_f64(), rate / 1e6);
    }

    // Cost breakdown: what fraction is data write vs index write vs atomic?
    println!("\n--- Cost Breakdown (1MB regions) ---");
    let dir = tempfile::tempdir().unwrap();
    let mut silo = datasilo::DataSilo::open(
        dir.path(), datasilo::SiloConfig { buffer_ratio: 1.0 },
    ).unwrap();
    let total_bytes = (num_entries as u64 * doc_size as u64) * 6 / 5;
    let writer = silo.prepare_parallel_writer(num_entries - 1, total_bytes).unwrap();
    let chunk_size = (num_entries as usize / threads).max(1);

    // Data write only (skip index)
    let t = Instant::now();
    let data_offset = std::sync::Arc::new(AtomicU64::new(0));
    let data_ptr = ptr_to_usize(writer.data_ptr());
    let data_len = writer.data_len();
    (0..num_entries).into_par_iter().with_min_len(chunk_size).for_each_init(
        || (0usize, 0usize),
        |(cursor, region_end), _i| {
            if *cursor + doc_size > *region_end {
                let start = data_offset.fetch_add(1 << 20, Ordering::Relaxed) as usize;
                *cursor = start;
                *region_end = start + (1 << 20);
            }
            let offset = *cursor;
            *cursor += doc_size;
            if offset + doc_size <= data_len {
                unsafe {
                    std::ptr::copy_nonoverlapping(doc_data.as_ptr(), usize_to_ptr(data_ptr).add(offset), doc_size);
                }
            }
        },
    );
    let data_only = t.elapsed();
    println!("  Data mmap only:  {:.2}s ({:.1}M/s)",
        data_only.as_secs_f64(), num_entries as f64 / data_only.as_secs_f64() / 1e6);

    // Index write only (no data)
    let t = Instant::now();
    let index_ptr = ptr_to_usize(writer.index_ptr());
    let index_len = writer.index_len();
    (0..num_entries).into_par_iter().with_min_len(chunk_size).for_each(|i| {
        let entry = datasilo::IndexEntry {
            offset: i as u64 * doc_size as u64,
            length: doc_size as u32,
            allocated: doc_size as u32,
        };
        let idx_pos = i as usize * 16;
        if idx_pos + 16 <= index_len {
            unsafe {
                let bytes: [u8; 16] = std::mem::transmute(entry);
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), usize_to_ptr(index_ptr).add(idx_pos), 16);
            }
        }
    });
    let index_only = t.elapsed();
    println!("  Index mmap only: {:.2}s ({:.1}M/s)",
        index_only.as_secs_f64(), num_entries as f64 / index_only.as_secs_f64() / 1e6);

    drop(silo);
}
