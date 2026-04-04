/// Test pre-faulting strategies for mmap parallel writes.
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

fn p(p: *mut u8) -> usize { p as usize }

fn main() {
    let num: u32 = 10_000_000;
    let doc_size = 230usize;
    let doc_data: Vec<u8> = (0..doc_size).map(|i| (i % 256) as u8).collect();
    let threads = rayon::current_num_threads();
    let chunk = (num as usize / threads).max(1);
    const REGION: u64 = 1 << 20;

    println!("=== Pre-fault Strategies ({} entries × {}B, {} threads) ===\n", num, doc_size, threads);

    // Helper: run the parallel write and return time
    let run_write = |data_ptr: usize, data_len: usize, idx_ptr: usize, idx_len: usize| -> f64 {
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
        t.elapsed().as_secs_f64()
    };

    let total = (num as u64 * doc_size as u64) * 6 / 5;
    let index_size = (num as usize + 1) * 16;

    // Strategy 1: No pre-fault (baseline cold)
    {
        let dir = tempfile::tempdir().unwrap();
        let df = std::fs::OpenOptions::new().create(true).read(true).write(true)
            .open(dir.path().join("d")).unwrap();
        df.set_len(total).unwrap();
        let dm = unsafe { memmap2::MmapMut::map_mut(&df).unwrap() };
        let if_ = std::fs::OpenOptions::new().create(true).read(true).write(true)
            .open(dir.path().join("i")).unwrap();
        if_.set_len(index_size as u64).unwrap();
        let im = unsafe { memmap2::MmapMut::map_mut(&if_).unwrap() };

        let secs = run_write(p(dm.as_ptr() as *mut u8), dm.len(), p(im.as_ptr() as *mut u8), im.len());
        println!("  1. Cold (no prefault):       {:.3}s ({:.1}M/s)", secs, num as f64 / secs / 1e6);
    }

    // Strategy 2: Sequential memset (single thread zeros the file)
    {
        let dir = tempfile::tempdir().unwrap();
        let df = std::fs::OpenOptions::new().create(true).read(true).write(true)
            .open(dir.path().join("d")).unwrap();
        df.set_len(total).unwrap();
        let mut dm = unsafe { memmap2::MmapMut::map_mut(&df).unwrap() };
        let if_ = std::fs::OpenOptions::new().create(true).read(true).write(true)
            .open(dir.path().join("i")).unwrap();
        if_.set_len(index_size as u64).unwrap();
        let mut im = unsafe { memmap2::MmapMut::map_mut(&if_).unwrap() };

        let t = Instant::now();
        // Zero both mmaps to force page faults sequentially
        dm.fill(0);
        im.fill(0);
        let prefault_secs = t.elapsed().as_secs_f64();

        let secs = run_write(p(dm.as_ptr() as *mut u8), dm.len(), p(im.as_ptr() as *mut u8), im.len());
        println!("  2. Sequential memset:        {:.3}s prefault + {:.3}s write = {:.3}s total ({:.1}M/s effective)",
            prefault_secs, secs, prefault_secs + secs,
            num as f64 / (prefault_secs + secs) / 1e6);
    }

    // Strategy 3: Parallel memset (rayon threads zero the file)
    {
        let dir = tempfile::tempdir().unwrap();
        let df = std::fs::OpenOptions::new().create(true).read(true).write(true)
            .open(dir.path().join("d")).unwrap();
        df.set_len(total).unwrap();
        let dm = unsafe { memmap2::MmapMut::map_mut(&df).unwrap() };
        let if_ = std::fs::OpenOptions::new().create(true).read(true).write(true)
            .open(dir.path().join("i")).unwrap();
        if_.set_len(index_size as u64).unwrap();
        let im = unsafe { memmap2::MmapMut::map_mut(&if_).unwrap() };

        let t = Instant::now();
        // Parallel zero: each thread zeros a chunk
        let dp = p(dm.as_ptr() as *mut u8);
        let dl = dm.len();
        let ip = p(im.as_ptr() as *mut u8);
        let il = im.len();
        let par_chunk = dl / threads;
        (0..threads).into_par_iter().for_each(|tid| {
            let start = tid * par_chunk;
            let end = if tid == threads - 1 { dl } else { start + par_chunk };
            unsafe { std::ptr::write_bytes((dp as *mut u8).add(start), 0, end - start); }
        });
        let idx_chunk = il / threads;
        (0..threads).into_par_iter().for_each(|tid| {
            let start = tid * idx_chunk;
            let end = if tid == threads - 1 { il } else { start + idx_chunk };
            unsafe { std::ptr::write_bytes((ip as *mut u8).add(start), 0, end - start); }
        });
        let prefault_secs = t.elapsed().as_secs_f64();

        let secs = run_write(dp, dl, ip, il);
        println!("  3. Parallel memset:          {:.3}s prefault + {:.3}s write = {:.3}s total ({:.1}M/s effective)",
            prefault_secs, secs, prefault_secs + secs,
            num as f64 / (prefault_secs + secs) / 1e6);
    }

    // Strategy 4: Touch one byte per page (4KB stride)
    {
        let dir = tempfile::tempdir().unwrap();
        let df = std::fs::OpenOptions::new().create(true).read(true).write(true)
            .open(dir.path().join("d")).unwrap();
        df.set_len(total).unwrap();
        let dm = unsafe { memmap2::MmapMut::map_mut(&df).unwrap() };
        let if_ = std::fs::OpenOptions::new().create(true).read(true).write(true)
            .open(dir.path().join("i")).unwrap();
        if_.set_len(index_size as u64).unwrap();
        let im = unsafe { memmap2::MmapMut::map_mut(&if_).unwrap() };

        let t = Instant::now();
        let dp = p(dm.as_ptr() as *mut u8);
        let dl = dm.len();
        let ip = p(im.as_ptr() as *mut u8);
        let il = im.len();
        // Touch one byte per 4KB page — parallel
        let num_pages = dl / 4096;
        let pages_per_thread = num_pages / threads;
        (0..threads).into_par_iter().for_each(|tid| {
            let start_page = tid * pages_per_thread;
            let end_page = if tid == threads - 1 { num_pages } else { start_page + pages_per_thread };
            for page in start_page..end_page {
                unsafe { std::ptr::write_volatile((dp as *mut u8).add(page * 4096), 0); }
            }
        });
        // Touch index pages too
        let idx_pages = il / 4096;
        for page in 0..idx_pages {
            unsafe { std::ptr::write_volatile((ip as *mut u8).add(page * 4096), 0); }
        }
        let prefault_secs = t.elapsed().as_secs_f64();

        let secs = run_write(dp, dl, ip, il);
        println!("  4. Parallel page-touch:      {:.3}s prefault + {:.3}s write = {:.3}s total ({:.1}M/s effective)",
            prefault_secs, secs, prefault_secs + secs,
            num as f64 / (prefault_secs + secs) / 1e6);
    }
}
