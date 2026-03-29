/// Microbenchmark: enrichment lookup table build strategies for posts.csv (22.8M rows).
///
/// Tests:
///   1. Current: DashMap (mmap + rayon + DashMap + convert to HashMap)
///   2. Per-thread HashMap + merge
///   3. Single-threaded pre-sized HashMap
///   4. Vec indexed by ID (if range allows)
///   5. Two-pass: count lines then pre-sized HashMap
///   6. Per-thread Vec + merge-sort + binary search
///
/// Run: cargo run --release --example bench_enrichment_load -- [path-to-posts.csv]
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let csv_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data/load_stage/posts.csv".to_string());

    eprintln!("=== Enrichment Load Benchmark ===");
    eprintln!("File: {csv_path}");

    let file = std::fs::File::open(&csv_path).expect("open csv");
    let mmap = unsafe { memmap2::Mmap::map(&file) }.expect("mmap");
    let raw = &mmap[..];
    eprintln!("File size: {:.1} MB", raw.len() as f64 / 1_048_576.0);

    // Posts.csv is headerless: id, publishedAtSecs, availability, modelVersionId
    let col_count = 4usize;

    // ---- Pre-work: split into line ranges ----
    let num_threads = rayon::current_num_threads();
    eprintln!("Rayon threads: {num_threads}");

    let ranges = split_ranges(raw, num_threads);

    // Count lines to know expected row count
    let t0 = Instant::now();
    let total_lines: usize = {
        use rayon::prelude::*;
        ranges
            .par_iter()
            .map(|&(s, e)| raw[s..e].iter().filter(|&&b| b == b'\n').count())
            .sum()
    };
    let count_ms = t0.elapsed().as_millis();
    eprintln!("Line count: {total_lines} ({count_ms} ms)\n");

    // ---- Find max key for Vec approach ----
    let t0 = Instant::now();
    let max_key = find_max_key(raw, &ranges);
    let mk_ms = t0.elapsed().as_millis();
    eprintln!("Max key: {max_key} ({mk_ms} ms)");
    eprintln!(
        "Vec<Option<Row>> at max_key+1 would need {:.1} GB (pointer array only)\n",
        (max_key as usize + 1) as f64 * std::mem::size_of::<Option<CompactRow>>() as f64
            / 1_073_741_824.0
    );

    // ---- Benchmark 1: DashMap (current approach) ----
    {
        eprintln!("--- 1. DashMap (current) ---");
        let t0 = Instant::now();
        let dm = bench_dashmap(raw, &ranges, col_count);
        let build_ms = t0.elapsed().as_millis();
        let t1 = Instant::now();
        let hm: HashMap<i64, CompactRow> = dm.into_iter().collect();
        let convert_ms = t1.elapsed().as_millis();
        eprintln!(
            "  Build: {build_ms} ms, Convert: {convert_ms} ms, Total: {} ms, Rows: {}",
            build_ms + convert_ms,
            hm.len()
        );
        sample_lookups(&hm, "HashMap");
    }

    // ---- Benchmark 2: Per-thread HashMap + merge ----
    {
        eprintln!("\n--- 2. Per-thread HashMap + merge ---");
        let t0 = Instant::now();
        let hm = bench_per_thread_hashmap(raw, &ranges, col_count, total_lines);
        let ms = t0.elapsed().as_millis();
        eprintln!("  Total: {ms} ms, Rows: {}", hm.len());
        sample_lookups(&hm, "HashMap");
    }

    // ---- Benchmark 3: Single-threaded pre-sized HashMap ----
    {
        eprintln!("\n--- 3. Single-thread pre-sized HashMap ---");
        let t0 = Instant::now();
        let hm = bench_single_thread(raw, col_count, total_lines);
        let ms = t0.elapsed().as_millis();
        eprintln!("  Total: {ms} ms, Rows: {}", hm.len());
        sample_lookups(&hm, "HashMap");
    }

    // ---- Benchmark 4: Vec indexed by ID ----
    if max_key < 50_000_000 {
        eprintln!("\n--- 4. Vec<Option<CompactRow>> indexed by ID ---");
        let t0 = Instant::now();
        let (vec, count) = bench_vec_indexed(raw, &ranges, col_count, max_key as usize);
        let ms = t0.elapsed().as_millis();
        eprintln!("  Total: {ms} ms, Rows: {count}, Vec len: {}", vec.len());
        // Lookup sample
        let keys = [26382515i64, 25088104, 24329721, 100000, 1];
        for k in keys {
            let t = Instant::now();
            let found = if (k as usize) < vec.len() {
                vec[k as usize].is_some()
            } else {
                false
            };
            let ns = t.elapsed().as_nanos();
            eprintln!("  Lookup {k}: found={found} ({ns} ns)");
        }
    } else {
        eprintln!("\n--- 4. Vec indexed: SKIPPED (max_key {max_key} too large) ---");
    }

    // ---- Benchmark 5: Two-pass: count then HashMap ----
    {
        eprintln!("\n--- 5. Two-pass count+fill HashMap ---");
        let t0 = Instant::now();
        let hm = bench_two_pass(raw, &ranges, col_count);
        let ms = t0.elapsed().as_millis();
        eprintln!("  Total: {ms} ms, Rows: {}", hm.len());
        sample_lookups(&hm, "HashMap");
    }

    // ---- Benchmark 6: DashMap with pre-capacity ----
    {
        eprintln!("\n--- 6. DashMap with capacity ---");
        let t0 = Instant::now();
        let dm = bench_dashmap_capacity(raw, &ranges, col_count, total_lines);
        let build_ms = t0.elapsed().as_millis();
        let t1 = Instant::now();
        let hm: HashMap<i64, CompactRow> = dm.into_iter().collect();
        let convert_ms = t1.elapsed().as_millis();
        eprintln!(
            "  Build: {build_ms} ms, Convert: {convert_ms} ms, Total: {} ms, Rows: {}",
            build_ms + convert_ms,
            hm.len()
        );
    }

    // ---- Benchmark 7: Per-thread Vec + sort for sorted lookup ----
    {
        eprintln!("\n--- 7. Sorted Vec + binary search ---");
        let t0 = Instant::now();
        let sorted = bench_sorted_vec(raw, &ranges, col_count);
        let ms = t0.elapsed().as_millis();
        eprintln!("  Total: {ms} ms, Rows: {}", sorted.len());
        // Lookup sample
        let keys = [26382515i64, 25088104, 24329721, 100000, 1];
        for k in keys {
            let t = Instant::now();
            let found = sorted.binary_search_by_key(&k, |(id, _)| *id).is_ok();
            let ns = t.elapsed().as_nanos();
            eprintln!("  Lookup {k}: found={found} ({ns} ns)");
        }
    }

    eprintln!("\n=== Done ===");
}

// ---- Compact row (mirrors LookupRow but without Arc<HashMap> overhead) ----

#[derive(Clone)]
struct CompactRow {
    values: [Option<Box<str>>; 4], // posts.csv has exactly 4 columns
}

impl CompactRow {
    fn from_fields(fields: &[&str], col_count: usize) -> Self {
        let mut values: [Option<Box<str>>; 4] = [None, None, None, None];
        for (i, &f) in fields.iter().enumerate().take(col_count.min(4)) {
            if !f.is_empty() {
                values[i] = Some(f.into());
            }
        }
        CompactRow { values }
    }
}

// ---- Helpers ----

fn split_ranges(raw: &[u8], n: usize) -> Vec<(usize, usize)> {
    let chunk_size = raw.len() / n;
    let mut ranges = Vec::with_capacity(n);
    let mut start = 0;
    for i in 0..n {
        let end = if i == n - 1 {
            raw.len()
        } else {
            let tentative = (start + chunk_size).min(raw.len());
            match raw[tentative..].iter().position(|&b| b == b'\n') {
                Some(offset) => tentative + offset + 1,
                None => raw.len(),
            }
        }
        .min(raw.len());
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }
    ranges
}

fn find_max_key(raw: &[u8], ranges: &[(usize, usize)]) -> i64 {
    use rayon::prelude::*;
    ranges
        .par_iter()
        .map(|&(s, e)| {
            let chunk = &raw[s..e];
            let mut max_k = 0i64;
            for line in chunk.split(|&b| b == b'\n') {
                let line = line.strip_suffix(&[b'\r']).unwrap_or(line);
                if line.is_empty() {
                    continue;
                }
                // Key is first field (up to first comma)
                let comma = line.iter().position(|&b| b == b',').unwrap_or(line.len());
                if let Ok(s) = std::str::from_utf8(&line[..comma]) {
                    if let Ok(k) = s.parse::<i64>() {
                        if k > max_k {
                            max_k = k;
                        }
                    }
                }
            }
            max_k
        })
        .max()
        .unwrap_or(0)
}

#[inline]
fn parse_line_fields(line: &[u8]) -> Option<(i64, Vec<&str>)> {
    let line = line.strip_suffix(&[b'\r']).unwrap_or(line);
    if line.is_empty() {
        return None;
    }
    let line_str = std::str::from_utf8(line).ok()?;
    // Fast path: no quotes in posts.csv — just split on comma
    let fields: Vec<&str> = line_str.split(',').collect();
    let key: i64 = fields.first()?.parse().ok()?;
    Some((key, fields))
}

fn sample_lookups(hm: &HashMap<i64, CompactRow>, label: &str) {
    let keys = [26382515i64, 25088104, 24329721, 100000, 1];
    for k in keys {
        let t = Instant::now();
        let found = hm.contains_key(&k);
        let ns = t.elapsed().as_nanos();
        eprintln!("  {label} lookup {k}: found={found} ({ns} ns)");
    }
}

// ---- Benchmark implementations ----

fn bench_dashmap(
    raw: &[u8],
    ranges: &[(usize, usize)],
    col_count: usize,
) -> dashmap::DashMap<i64, CompactRow> {
    use rayon::prelude::*;
    let dm: dashmap::DashMap<i64, CompactRow> = dashmap::DashMap::new();
    ranges.par_iter().for_each(|&(s, e)| {
        let chunk = &raw[s..e];
        for line in chunk.split(|&b| b == b'\n') {
            if let Some((key, fields)) = parse_line_fields(line) {
                dm.insert(key, CompactRow::from_fields(&fields, col_count));
            }
        }
    });
    dm
}

fn bench_dashmap_capacity(
    raw: &[u8],
    ranges: &[(usize, usize)],
    col_count: usize,
    expected_rows: usize,
) -> dashmap::DashMap<i64, CompactRow> {
    use rayon::prelude::*;
    let dm: dashmap::DashMap<i64, CompactRow> = dashmap::DashMap::with_capacity(expected_rows);
    ranges.par_iter().for_each(|&(s, e)| {
        let chunk = &raw[s..e];
        for line in chunk.split(|&b| b == b'\n') {
            if let Some((key, fields)) = parse_line_fields(line) {
                dm.insert(key, CompactRow::from_fields(&fields, col_count));
            }
        }
    });
    dm
}

fn bench_per_thread_hashmap(
    raw: &[u8],
    ranges: &[(usize, usize)],
    col_count: usize,
    expected_total: usize,
) -> HashMap<i64, CompactRow> {
    use rayon::prelude::*;
    let per_thread_est = expected_total / ranges.len() + 1024;

    // Each thread builds its own HashMap
    let maps: Vec<HashMap<i64, CompactRow>> = ranges
        .par_iter()
        .map(|&(s, e)| {
            let chunk = &raw[s..e];
            let mut local: HashMap<i64, CompactRow> = HashMap::with_capacity(per_thread_est);
            for line in chunk.split(|&b| b == b'\n') {
                if let Some((key, fields)) = parse_line_fields(line) {
                    local.insert(key, CompactRow::from_fields(&fields, col_count));
                }
            }
            local
        })
        .collect();

    // Merge: start from largest, extend with rest
    let mut maps = maps;
    // Find the largest map to use as base
    let max_idx = maps
        .iter()
        .enumerate()
        .max_by_key(|(_, m)| m.len())
        .map(|(i, _)| i)
        .unwrap_or(0);
    let mut merged = std::mem::take(&mut maps[max_idx]);
    merged.reserve(expected_total - merged.len());
    for (i, map) in maps.into_iter().enumerate() {
        if i == max_idx {
            continue; // already taken
        }
        merged.extend(map);
    }
    merged
}

fn bench_single_thread(
    raw: &[u8],
    col_count: usize,
    expected_total: usize,
) -> HashMap<i64, CompactRow> {
    let mut hm: HashMap<i64, CompactRow> = HashMap::with_capacity(expected_total);
    for line in raw.split(|&b| b == b'\n') {
        if let Some((key, fields)) = parse_line_fields(line) {
            hm.insert(key, CompactRow::from_fields(&fields, col_count));
        }
    }
    hm
}

fn bench_vec_indexed(
    raw: &[u8],
    ranges: &[(usize, usize)],
    col_count: usize,
    max_key: usize,
) -> (Vec<Option<CompactRow>>, usize) {
    use rayon::prelude::*;

    // Pre-allocate Vec with None
    let len = max_key + 1;
    let mut vec: Vec<Option<CompactRow>> = Vec::with_capacity(len);
    vec.resize_with(len, || None);

    // We can't safely parallel-write to Vec slots without unsafe.
    // Use a simple single-threaded approach here for correctness,
    // since the Vec allocation is the main cost.
    let count = AtomicUsize::new(0);

    // Actually, since each key maps to a unique index, we CAN safely
    // parallel-write with unsafe (no two threads write the same slot for unique keys).
    // But let's be conservative and use single-threaded fill.
    for line in raw.split(|&b| b == b'\n') {
        if let Some((key, fields)) = parse_line_fields(line) {
            let idx = key as usize;
            if idx < len {
                vec[idx] = Some(CompactRow::from_fields(&fields, col_count));
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    (vec, count.load(Ordering::Relaxed))
}

fn bench_two_pass(
    raw: &[u8],
    ranges: &[(usize, usize)],
    col_count: usize,
) -> HashMap<i64, CompactRow> {
    use rayon::prelude::*;

    // Pass 1: count rows (parallel)
    let count: usize = ranges
        .par_iter()
        .map(|&(s, e)| raw[s..e].iter().filter(|&&b| b == b'\n').count())
        .sum();

    // Pass 2: fill (single-threaded with exact capacity)
    let mut hm: HashMap<i64, CompactRow> = HashMap::with_capacity(count);
    for line in raw.split(|&b| b == b'\n') {
        if let Some((key, fields)) = parse_line_fields(line) {
            hm.insert(key, CompactRow::from_fields(&fields, col_count));
        }
    }
    hm
}

fn bench_sorted_vec(
    raw: &[u8],
    ranges: &[(usize, usize)],
    col_count: usize,
) -> Vec<(i64, CompactRow)> {
    use rayon::prelude::*;

    // Each thread builds a Vec
    let mut chunks: Vec<Vec<(i64, CompactRow)>> = ranges
        .par_iter()
        .map(|&(s, e)| {
            let chunk = &raw[s..e];
            let mut local: Vec<(i64, CompactRow)> = Vec::with_capacity(chunk.len() / 25);
            for line in chunk.split(|&b| b == b'\n') {
                if let Some((key, fields)) = parse_line_fields(line) {
                    local.push((key, CompactRow::from_fields(&fields, col_count)));
                }
            }
            local.sort_unstable_by_key(|(k, _)| *k);
            local
        })
        .collect();

    // Merge sorted chunks (k-way merge simplified as concat + sort)
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    let mut merged = Vec::with_capacity(total);
    for chunk in chunks {
        merged.extend(chunk);
    }
    merged.sort_unstable_by_key(|(k, _)| *k);
    merged
}
