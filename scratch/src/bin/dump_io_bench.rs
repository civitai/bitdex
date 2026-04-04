/// Benchmark the actual tags dump on real data (first 100M rows of tags.csv)
/// to measure I/O + parse + bitmap insert throughput on this machine.
///
/// This tests the REAL bottleneck: mmap read + CSV parse + bitmap insert
/// without any docstore writes.
use memmap2::Mmap;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::hint::black_box;
use std::time::Instant;

const MAX_TAG_ID: usize = 300_000;

fn main() {
    let csv_path = "C:/Dev/Repos/open-source/bitdex-v2/data/load_stage/tags.csv";

    println!("=== Real Tags CSV Benchmark ===\n");

    // Mmap the file
    let t = Instant::now();
    let file = std::fs::File::open(csv_path).expect("Failed to open tags.csv");
    let mmap = unsafe { Mmap::map(&file).expect("Failed to mmap") };
    let body = &mmap[..];
    println!("Mmap'd {:.1} GB in {:.1}ms", body.len() as f64 / 1e9, t.elapsed().as_secs_f64() * 1000.0);

    // Skip header
    let header_end = body.iter().position(|&b| b == b'\n').unwrap_or(0) + 1;
    let body = &body[header_end..];

    // Find column indices from header
    let header = &mmap[..header_end - 1];
    let header_str = std::str::from_utf8(header).unwrap_or("");
    let cols: Vec<&str> = header_str.split(',').collect();
    let image_col = cols.iter().position(|c| c.trim() == "imageId").unwrap_or(0);
    let tag_col = cols.iter().position(|c| c.trim() == "tagId").unwrap_or(1);
    println!("Columns: imageId={}, tagId={}", image_col, tag_col);

    // Test 1: Parse throughput (first 1GB only to avoid swap)
    let test_size = 1_000_000_000usize.min(body.len()); // 1GB
    let test_body = &body[..test_size];
    println!("\n--- Test 1: Parse first {:.0} MB ---", test_size as f64 / 1e6);

    let t = Instant::now();
    let mut count = 0u64;
    let mut line_start = 0;
    for i in 0..test_body.len() {
        if test_body[i] != b'\n' { continue; }
        let line = &test_body[line_start..i];
        line_start = i + 1;
        if line.is_empty() { continue; }
        if let Some((_, _)) = parse_two_cols(line, b',', image_col, tag_col) {
            count += 1;
        }
    }
    let elapsed = t.elapsed();
    println!("  {} rows in {:.1}ms ({:.1}M rows/sec, {:.1} GB/sec)",
        count, elapsed.as_secs_f64() * 1000.0,
        count as f64 / elapsed.as_secs_f64() / 1e6,
        test_size as f64 / elapsed.as_secs_f64() / 1e9);

    // Test 2: Parse + bitmap insert (first 1GB, single thread)
    println!("\n--- Test 2: Parse + Insert (1GB, single thread) ---");
    let t = Instant::now();
    let mut bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect();
    let mut count2 = 0u64;
    line_start = 0;
    for i in 0..test_body.len() {
        if test_body[i] != b'\n' { continue; }
        let line = &test_body[line_start..i];
        line_start = i + 1;
        if line.is_empty() { continue; }
        if let Some((slot, tag)) = parse_two_cols(line, b',', image_col, tag_col) {
            if (tag as usize) < MAX_TAG_ID {
                bitmaps[tag as usize].insert(slot);
            }
            count2 += 1;
        }
    }
    let elapsed2 = t.elapsed();
    println!("  {} rows in {:.1}ms ({:.1}M rows/sec)",
        count2, elapsed2.as_secs_f64() * 1000.0,
        count2 as f64 / elapsed2.as_secs_f64() / 1e6);
    drop(bitmaps);

    // Test 3: Parallel parse + insert (first 5GB)
    let test_5gb = 5_000_000_000usize.min(body.len());
    let test_body_5 = &body[..test_5gb];
    println!("\n--- Test 3: Parallel parse + insert (first {:.1} GB, {} threads) ---",
        test_5gb as f64 / 1e9, rayon::current_num_threads());

    let ranges = split_ranges(test_body_5, rayon::current_num_threads());
    let t = Instant::now();
    let total_rows = std::sync::atomic::AtomicU64::new(0);

    let results: Vec<Vec<RoaringBitmap>> = ranges
        .par_iter()
        .map(|&(start, end)| {
            let chunk = &test_body_5[start..end];
            let mut bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect();
            let mut count = 0u64;
            let mut line_start = 0;
            for i in 0..chunk.len() {
                if chunk[i] != b'\n' { continue; }
                let line = &chunk[line_start..i];
                line_start = i + 1;
                if line.is_empty() { continue; }
                if let Some((slot, tag)) = parse_two_cols(line, b',', image_col, tag_col) {
                    if (tag as usize) < MAX_TAG_ID {
                        bitmaps[tag as usize].insert(slot);
                    }
                    count += 1;
                }
            }
            total_rows.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            bitmaps
        })
        .collect();

    let parse_elapsed = t.elapsed();
    let rows = total_rows.load(std::sync::atomic::Ordering::Relaxed);
    println!("  Parse+insert: {} rows in {:.1}s ({:.1}M rows/sec)",
        rows, parse_elapsed.as_secs_f64(),
        rows as f64 / parse_elapsed.as_secs_f64() / 1e6);

    // Reduce
    let t2 = Instant::now();
    let merged = results
        .into_par_iter()
        .reduce(
            || (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect::<Vec<_>>(),
            |mut dst, src| {
                for (i, bm) in src.into_iter().enumerate() {
                    if !bm.is_empty() { dst[i] |= bm; }
                }
                dst
            },
        );
    let reduce_elapsed = t2.elapsed();
    let total_elapsed = parse_elapsed + reduce_elapsed;
    println!("  Reduce:       {:.1}s", reduce_elapsed.as_secs_f64());
    println!("  Total:        {:.1}s ({:.1}M rows/sec)",
        total_elapsed.as_secs_f64(),
        rows as f64 / total_elapsed.as_secs_f64() / 1e6);
    let non_empty = merged.iter().filter(|b| !b.is_empty()).count();
    println!("  {} non-empty bitmaps", non_empty);
    black_box(&merged);

    // Extrapolate to full file
    let full_rows = rows as f64 * (body.len() as f64 / test_5gb as f64);
    let full_time = full_rows / (rows as f64 / total_elapsed.as_secs_f64());
    println!("\n  Extrapolated full file ({:.1}B rows): {:.0}s ({:.1} min)",
        full_rows / 1e9, full_time, full_time / 60.0);
}

fn parse_two_cols(line: &[u8], delim: u8, col_a: usize, col_b: usize) -> Option<(u32, u32)> {
    let max_col = col_a.max(col_b);
    let mut col = 0;
    let mut start = 0;
    let mut vals = [0u32; 2];

    for i in 0..=line.len() {
        if i == line.len() || line[i] == delim {
            if col == col_a {
                vals[0] = fast_u32(&line[start..i]);
            } else if col == col_b {
                vals[1] = fast_u32(&line[start..i]);
            }
            col += 1;
            if col > max_col { break; }
            start = i + 1;
        }
    }
    if col > max_col { Some((vals[0], vals[1])) } else { None }
}

fn fast_u32(bytes: &[u8]) -> u32 {
    let mut r = 0u32;
    for &b in bytes {
        if b >= b'0' && b <= b'9' {
            r = r * 10 + (b - b'0') as u32;
        }
    }
    r
}

fn split_ranges(data: &[u8], n: usize) -> Vec<(usize, usize)> {
    let chunk = data.len() / n;
    let mut ranges = Vec::with_capacity(n);
    let mut start = 0;
    for i in 0..n {
        let mut end = if i == n - 1 { data.len() } else { (i + 1) * chunk };
        // Align to newline
        while end < data.len() && data[end] != b'\n' { end += 1; }
        if end < data.len() { end += 1; }
        ranges.push((start, end));
        start = end;
    }
    ranges
}
