/// Benchmark the bitmap inversion post-pass that writes per-slot tag arrays.
///
/// Simulates the exact algorithm used in dump_processor: for each shard range,
/// iterate all tag bitmaps, accumulate per-slot tag arrays, measure throughput.
///
/// Tests both sequential and parallel (rayon) versions.
use memmap2::Mmap;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const MAX_TAG_ID: usize = 300_000;
const SHARD_SIZE: u32 = 1_000_000;

fn main() {
    let csv_path = "C:/Dev/Repos/open-source/bitdex-v2/data/load_stage/tags.csv";

    println!("=== Post-Pass Bitmap Inversion Benchmark ===\n");

    // Step 1: Parse first 2GB of tags.csv into bitmaps (like the real pipeline)
    let test_bytes = 2_000_000_000usize;
    println!("Step 1: Building bitmaps from first {:.0} GB of tags.csv...", test_bytes as f64 / 1e9);

    let file = std::fs::File::open(csv_path).expect("Failed to open tags.csv");
    let mmap = unsafe { Mmap::map(&file).expect("Failed to mmap") };
    let body = &mmap[..test_bytes.min(mmap.len())];

    // Skip header
    let header_end = body.iter().position(|&b| b == b'\n').unwrap_or(0) + 1;
    let body = &body[header_end..];

    // Parse columns
    let header = &mmap[..header_end - 1];
    let header_str = std::str::from_utf8(header).unwrap_or("");
    let cols: Vec<&str> = header_str.split(',').collect();
    let image_col = cols.iter().position(|c| c.trim() == "imageId").unwrap_or(0);
    let tag_col = cols.iter().position(|c| c.trim() == "tagId").unwrap_or(1);

    // Build bitmaps (parallel, like real pipeline)
    let ranges = split_ranges(body, rayon::current_num_threads());
    let t = Instant::now();
    let total_rows = AtomicU64::new(0);

    let merged: Vec<RoaringBitmap> = ranges
        .par_iter()
        .map(|&(start, end)| {
            let chunk = &body[start..end];
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
            total_rows.fetch_add(count, Ordering::Relaxed);
            bitmaps
        })
        .reduce(
            || (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect::<Vec<_>>(),
            |mut dst, src| {
                for (i, bm) in src.into_iter().enumerate() {
                    if !bm.is_empty() { dst[i] |= bm; }
                }
                dst
            },
        );

    let rows = total_rows.load(Ordering::Relaxed);
    let parse_time = t.elapsed();
    let non_empty: Vec<usize> = merged.iter().enumerate()
        .filter(|(_, bm)| !bm.is_empty())
        .map(|(i, _)| i)
        .collect();
    println!("  {} rows parsed, {} distinct tags in {:.1}s ({:.1}M rows/sec)\n",
        rows, non_empty.len(), parse_time.as_secs_f64(),
        rows as f64 / parse_time.as_secs_f64() / 1e6);

    // Pre-compute tag ranges for fast shard skipping
    let tag_ranges: Vec<(usize, u32, u32)> = non_empty.iter()
        .filter_map(|&tag| {
            let bm = &merged[tag];
            Some((tag, bm.min()?, bm.max()?))
        })
        .collect();

    let max_slot = tag_ranges.iter().map(|&(_, _, max)| max).max().unwrap_or(0);
    let num_shards = (max_slot / SHARD_SIZE) + 1;
    println!("Max slot: {}, shards: {}\n", max_slot, num_shards);

    // Step 2: Sequential post-pass
    println!("--- Sequential Post-Pass ---");
    let t = Instant::now();
    let mut seq_docs = 0u64;
    let mut seq_tags = 0u64;

    for shard_idx in 0..num_shards {
        let shard_start = shard_idx * SHARD_SIZE;
        let shard_end = shard_start + SHARD_SIZE;

        let relevant: Vec<usize> = tag_ranges.iter()
            .filter(|&&(_, min, max)| max >= shard_start && min < shard_end)
            .map(|&(tag, _, _)| tag)
            .collect();
        if relevant.is_empty() { continue; }

        let mut counts = vec![0u32; SHARD_SIZE as usize];
        for &tag_id in &relevant {
            for slot in merged[tag_id].iter() {
                if slot < shard_start { continue; }
                if slot >= shard_end { break; }
                counts[(slot - shard_start) as usize] += 1;
            }
        }

        let total: u32 = counts.iter().sum();
        seq_tags += total as u64;
        seq_docs += counts.iter().filter(|&&c| c > 0).count() as u64;

        // Simulate the write (just count, don't actually write to disk)
        black_box(&counts);
    }
    let seq_time = t.elapsed();
    println!("  {} docs, {} tag entries in {:.1}s ({:.0} docs/sec)\n",
        seq_docs, seq_tags, seq_time.as_secs_f64(),
        seq_docs as f64 / seq_time.as_secs_f64());

    // Step 3: Parallel post-pass (rayon over shards)
    println!("--- Parallel Post-Pass ({} threads) ---", rayon::current_num_threads());
    let t = Instant::now();
    let par_docs = AtomicU64::new(0);
    let par_tags = AtomicU64::new(0);
    let merged_ref = &merged;
    let tag_ranges_ref = &tag_ranges;

    (0..num_shards).into_par_iter().for_each(|shard_idx| {
        let shard_start = shard_idx * SHARD_SIZE;
        let shard_end = shard_start + SHARD_SIZE;

        let relevant: Vec<usize> = tag_ranges_ref.iter()
            .filter(|&&(_, min, max)| max >= shard_start && min < shard_end)
            .map(|&(tag, _, _)| tag)
            .collect();
        if relevant.is_empty() { return; }

        let mut counts = vec![0u32; SHARD_SIZE as usize];
        for &tag_id in &relevant {
            for slot in merged_ref[tag_id].iter() {
                if slot < shard_start { continue; }
                if slot >= shard_end { break; }
                counts[(slot - shard_start) as usize] += 1;
            }
        }

        let total: u32 = counts.iter().sum();
        par_tags.fetch_add(total as u64, Ordering::Relaxed);
        par_docs.fetch_add(counts.iter().filter(|&&c| c > 0).count() as u64, Ordering::Relaxed);
        black_box(&counts);
    });
    let par_time = t.elapsed();
    let pd = par_docs.load(Ordering::Relaxed);
    let pt = par_tags.load(Ordering::Relaxed);
    println!("  {} docs, {} tag entries in {:.1}s ({:.0} docs/sec)",
        pd, pt, par_time.as_secs_f64(),
        pd as f64 / par_time.as_secs_f64());
    println!("  Speedup: {:.1}x over sequential\n", seq_time.as_secs_f64() / par_time.as_secs_f64());

    // Extrapolate to full file
    let scale = 4.5e9 / rows as f64;
    println!("=== Extrapolation to 4.5B rows (full tags.csv) ===");
    println!("  Parse+merge:     {:.0}s ({:.1} min)", parse_time.as_secs_f64() * scale, parse_time.as_secs_f64() * scale / 60.0);
    println!("  Sequential pass: {:.0}s ({:.1} min)", seq_time.as_secs_f64() * scale, seq_time.as_secs_f64() * scale / 60.0);
    println!("  Parallel pass:   {:.0}s ({:.1} min)", par_time.as_secs_f64() * scale, par_time.as_secs_f64() * scale / 60.0);
    println!("  Total (parallel): {:.0}s ({:.1} min)",
        (parse_time.as_secs_f64() + par_time.as_secs_f64()) * scale,
        (parse_time.as_secs_f64() + par_time.as_secs_f64()) * scale / 60.0);
}

fn parse_two_cols(line: &[u8], delim: u8, col_a: usize, col_b: usize) -> Option<(u32, u32)> {
    let max_col = col_a.max(col_b);
    let mut col = 0;
    let mut start = 0;
    let mut vals = [0u32; 2];
    for i in 0..=line.len() {
        if i == line.len() || line[i] == delim {
            if col == col_a { vals[0] = fast_u32(&line[start..i]); }
            else if col == col_b { vals[1] = fast_u32(&line[start..i]); }
            col += 1;
            if col > max_col { break; }
            start = i + 1;
        }
    }
    if col > max_col { Some((vals[0], vals[1])) } else { None }
}

fn fast_u32(bytes: &[u8]) -> u32 {
    let mut r = 0u32;
    for &b in bytes { if b >= b'0' && b <= b'9' { r = r * 10 + (b - b'0') as u32; } }
    r
}

fn split_ranges(data: &[u8], n: usize) -> Vec<(usize, usize)> {
    let chunk = data.len() / n;
    let mut ranges = Vec::with_capacity(n);
    let mut start = 0;
    for i in 0..n {
        let mut end = if i == n - 1 { data.len() } else { (i + 1) * chunk };
        while end < data.len() && data[end] != b'\n' { end += 1; }
        if end < data.len() { end += 1; }
        ranges.push((start, end));
        start = end;
    }
    ranges
}
