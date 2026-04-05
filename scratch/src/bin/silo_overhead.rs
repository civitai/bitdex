/// Precise overhead breakdown for parallel mmap writes.
/// Adds one cost at a time to isolate what's slow.
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

fn ptr_as_usize(p: *mut u8) -> usize { p as usize }

fn main() {
    let num: u32 = 10_000_000;
    let doc_size = 230usize;
    let doc_data: Vec<u8> = (0..doc_size).map(|i| (i % 256) as u8).collect();
    let threads = rayon::current_num_threads();
    let chunk = (num as usize / threads).max(1);
    const REGION: u64 = 1 << 20; // 1MB

    println!("=== Overhead Breakdown ({} entries × {}B, {} threads, 1MB regions) ===\n", num, doc_size, threads);

    // Allocate data + index mmaps
    let dir = tempfile::tempdir().unwrap();
    let total = (num as u64 * doc_size as u64) * 6 / 5;
    let index_size = (num as usize + 1) * 16;

    let data_file = std::fs::OpenOptions::new().create(true).read(true).write(true)
        .open(dir.path().join("data.bin")).unwrap();
    data_file.set_len(total).unwrap();
    let data_mmap = unsafe { memmap2::MmapMut::map_mut(&data_file).unwrap() };
    let data_ptr = ptr_as_usize(data_mmap.as_ptr() as *mut u8);
    let data_len = data_mmap.len();

    let idx_file = std::fs::OpenOptions::new().create(true).read(true).write(true)
        .open(dir.path().join("index.bin")).unwrap();
    idx_file.set_len(index_size as u64).unwrap();
    let idx_mmap = unsafe { memmap2::MmapMut::map_mut(&idx_file).unwrap() };
    let idx_ptr = ptr_as_usize(idx_mmap.as_ptr() as *mut u8);
    let idx_len = idx_mmap.len();

    // Test 1: Data write only (1MB regions, no index, no counter)
    let offset = AtomicU64::new(0);
    let t = Instant::now();
    (0..num).into_par_iter().with_min_len(chunk).for_each_init(
        || (0usize, 0usize),
        |(cursor, end), _| {
            if *cursor + doc_size > *end {
                let s = offset.fetch_add(REGION, Ordering::Relaxed) as usize;
                *cursor = s; *end = s + REGION as usize;
            }
            let o = *cursor; *cursor += doc_size;
            if o + doc_size <= data_len {
                unsafe { std::ptr::copy_nonoverlapping(doc_data.as_ptr(), (data_ptr as *mut u8).add(o), doc_size); }
            }
        },
    );
    println!("  1. Data only:                {:.3}s ({:.1}M/s)", t.elapsed().as_secs_f64(), num as f64 / t.elapsed().as_secs_f64() / 1e6);

    // Test 2: Data + index write (no counter)
    let offset = AtomicU64::new(0);
    let t = Instant::now();
    (0..num).into_par_iter().with_min_len(chunk).for_each_init(
        || (0usize, 0usize),
        |(cursor, end), i| {
            if *cursor + doc_size > *end {
                let s = offset.fetch_add(REGION, Ordering::Relaxed) as usize;
                *cursor = s; *end = s + REGION as usize;
            }
            let o = *cursor; *cursor += doc_size;
            if o + doc_size <= data_len {
                unsafe { std::ptr::copy_nonoverlapping(doc_data.as_ptr(), (data_ptr as *mut u8).add(o), doc_size); }
            }
            // Index write
            let idx_pos = i as usize * 16;
            if idx_pos + 16 <= idx_len {
                let entry: [u8; 16] = unsafe { std::mem::transmute((o as u64, doc_size as u32, doc_size as u32)) };
                unsafe { std::ptr::copy_nonoverlapping(entry.as_ptr(), (idx_ptr as *mut u8).add(idx_pos), 16); }
            }
        },
    );
    println!("  2. Data + index:             {:.3}s ({:.1}M/s)", t.elapsed().as_secs_f64(), num as f64 / t.elapsed().as_secs_f64() / 1e6);

    // Test 3: Data + index + atomic counter
    let offset = AtomicU64::new(0);
    let counter = AtomicU64::new(0);
    let t = Instant::now();
    (0..num).into_par_iter().with_min_len(chunk).for_each_init(
        || (0usize, 0usize),
        |(cursor, end), i| {
            if *cursor + doc_size > *end {
                let s = offset.fetch_add(REGION, Ordering::Relaxed) as usize;
                *cursor = s; *end = s + REGION as usize;
            }
            let o = *cursor; *cursor += doc_size;
            if o + doc_size <= data_len {
                unsafe { std::ptr::copy_nonoverlapping(doc_data.as_ptr(), (data_ptr as *mut u8).add(o), doc_size); }
            }
            let idx_pos = i as usize * 16;
            if idx_pos + 16 <= idx_len {
                let entry: [u8; 16] = unsafe { std::mem::transmute((o as u64, doc_size as u32, doc_size as u32)) };
                unsafe { std::ptr::copy_nonoverlapping(entry.as_ptr(), (idx_ptr as *mut u8).add(idx_pos), 16); }
            }
            counter.fetch_add(1, Ordering::Relaxed);
        },
    );
    println!("  3. Data + index + counter:   {:.3}s ({:.1}M/s)", t.elapsed().as_secs_f64(), num as f64 / t.elapsed().as_secs_f64() / 1e6);

    // Test 4: Same but with thread-local counter (no atomic per entry)
    let offset = AtomicU64::new(0);
    let t = Instant::now();
    let total_count: u64 = (0..num).into_par_iter().with_min_len(chunk).fold_with(
        (0usize, 0usize, 0u64),
        |(mut cursor, mut end, mut count), i| {
            if cursor + doc_size > end {
                let s = offset.fetch_add(REGION, Ordering::Relaxed) as usize;
                cursor = s; end = s + REGION as usize;
            }
            let o = cursor; cursor += doc_size;
            if o + doc_size <= data_len {
                unsafe { std::ptr::copy_nonoverlapping(doc_data.as_ptr(), (data_ptr as *mut u8).add(o), doc_size); }
            }
            let idx_pos = i as usize * 16;
            if idx_pos + 16 <= idx_len {
                let entry: [u8; 16] = unsafe { std::mem::transmute((o as u64, doc_size as u32, doc_size as u32)) };
                unsafe { std::ptr::copy_nonoverlapping(entry.as_ptr(), (idx_ptr as *mut u8).add(idx_pos), 16); }
            }
            count += 1;
            (cursor, end, count)
        },
    ).map(|(_, _, c)| c).sum();
    println!("  4. Data + index + local cnt: {:.3}s ({:.1}M/s) [count={}]", t.elapsed().as_secs_f64(), num as f64 / t.elapsed().as_secs_f64() / 1e6, total_count);

    // Test 5: Second run of test 1 (pages now hot)
    let offset = AtomicU64::new(0);
    let t = Instant::now();
    (0..num).into_par_iter().with_min_len(chunk).for_each_init(
        || (0usize, 0usize),
        |(cursor, end), _| {
            if *cursor + doc_size > *end {
                let s = offset.fetch_add(REGION, Ordering::Relaxed) as usize;
                *cursor = s; *end = s + REGION as usize;
            }
            let o = *cursor; *cursor += doc_size;
            if o + doc_size <= data_len {
                unsafe { std::ptr::copy_nonoverlapping(doc_data.as_ptr(), (data_ptr as *mut u8).add(o), doc_size); }
            }
        },
    );
    println!("  5. Data only (hot pages):    {:.3}s ({:.1}M/s)", t.elapsed().as_secs_f64(), num as f64 / t.elapsed().as_secs_f64() / 1e6);

    // Test 6: Second run of test 4 (hot pages)
    let offset = AtomicU64::new(0);
    let t = Instant::now();
    let _: u64 = (0..num).into_par_iter().with_min_len(chunk).fold_with(
        (0usize, 0usize, 0u64),
        |(mut cursor, mut end, mut count), i| {
            if cursor + doc_size > end {
                let s = offset.fetch_add(REGION, Ordering::Relaxed) as usize;
                cursor = s; end = s + REGION as usize;
            }
            let o = cursor; cursor += doc_size;
            if o + doc_size <= data_len {
                unsafe { std::ptr::copy_nonoverlapping(doc_data.as_ptr(), (data_ptr as *mut u8).add(o), doc_size); }
            }
            let idx_pos = i as usize * 16;
            if idx_pos + 16 <= idx_len {
                let entry: [u8; 16] = unsafe { std::mem::transmute((o as u64, doc_size as u32, doc_size as u32)) };
                unsafe { std::ptr::copy_nonoverlapping(entry.as_ptr(), (idx_ptr as *mut u8).add(idx_pos), 16); }
            }
            count += 1;
            (cursor, end, count)
        },
    ).map(|(_, _, c)| c).sum();
    println!("  6. Full (hot pages):         {:.3}s ({:.1}M/s)", t.elapsed().as_secs_f64(), num as f64 / t.elapsed().as_secs_f64() / 1e6);

    // Cleanup
    drop(data_mmap);
    drop(idx_mmap);
}
