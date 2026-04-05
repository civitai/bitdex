/// Bitmap merge strategy benchmark — 7 approaches, 1M rows, 32 threads
///
/// After the parse phase, 32 rayon threads each produce filter bitmap results.
/// Currently merged via rayon fold+reduce (~4.6s, 28% of wall time at 14.6M rows).
/// This bench finds the fastest path from "threads done parsing" to "final bitmaps ready."
///
/// Dataset: 1M rows, 8 fields (2 low, 3 medium, 3 high cardinality), 32 threads (~31K each).
/// Small enough for fast data gen (<10s), large enough to show relative differences.
///
/// Approaches (A-E use nested maps; F-G are flat-key variants):
///   A — Current: rayon fold+reduce (tree reduction) over nested HashMaps
///   B — Per-field parallel merge: collect per-field first, then par merge each field
///   C — Global sort: concat raw tuples, par_sort_unstable, build bitmaps once
///   D — K-way merge: 32 pre-sorted thread Vecs merged via min-heap into bitmaps
///   E — Global sort + fused serialize: C but serialize each bitmap immediately
///   F — Per-value parallel merge: sequential group by (field,val), then rayon par merge
///   G — Flat HashMap (u8,u64) key per thread: flat map per thread, then F-style merge
///
/// Run:
///   cargo run -p scratch --release --bin bitmap_merge_strategies

use ahash::AHashMap;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::collections::BinaryHeap;
use std::hint::black_box;
use std::time::Instant;

// ── Constants ─────────────────────────────────────────────────────────────────

const TOTAL_ROWS: usize = 1_000_000;
const NUM_THREADS: usize = 32;
const ROWS_PER_THREAD: usize = TOTAL_ROWS / NUM_THREADS; // ~31_250
const NUM_FIELDS: u8 = 8;
const ITERS: usize = 3;

// Field configs: (num_distinct_values, is_power_law)
const FIELD_CONFIGS: [(u64, bool); 8] = [
    (5,         false), // low-cardinality #1
    (5,         false), // low-cardinality #2
    (50_000,    true),  // medium-cardinality #1
    (50_000,    true),  // medium-cardinality #2
    (50_000,    true),  // medium-cardinality #3
    (2_000_000, false), // high-cardinality #1
    (2_000_000, false), // high-cardinality #2
    (2_000_000, false), // high-cardinality #3
];

// ── LCG ───────────────────────────────────────────────────────────────────────

#[inline(always)]
fn lcg64(x: u64) -> u64 {
    x.wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407)
}

// ── Data generation ───────────────────────────────────────────────────────────

/// Generate sorted tuples for one thread: Vec<(field_idx, value, slot)>
fn generate_thread_tuples(thread_idx: usize) -> Vec<(u8, u64, u32)> {
    let base_slot = (thread_idx * ROWS_PER_THREAD) as u32;
    let mut tuples = Vec::with_capacity(ROWS_PER_THREAD * NUM_FIELDS as usize);

    for row in 0..ROWS_PER_THREAD {
        let slot = base_slot + row as u32;
        let row_seed = lcg64(slot as u64 ^ (thread_idx as u64).wrapping_mul(0xDEAD_BEEF_CAFE_BABE));

        for (field_idx, &(num_values, power_law)) in FIELD_CONFIGS.iter().enumerate() {
            let field_seed = lcg64(row_seed ^ (field_idx as u64).wrapping_mul(0x1234_5678_9ABC_DEF0));
            let value = if power_law {
                let u = (field_seed % 65536) as f64 / 65536.0;
                ((1.0 - u * u) * num_values as f64) as u64
            } else {
                field_seed % num_values
            };
            tuples.push((field_idx as u8, value, slot));
        }
    }

    tuples.sort_unstable();
    tuples
}

/// Build nested HashMap<field, HashMap<value, RoaringBitmap>> from sorted tuples.
fn build_nested_map(tuples: &[(u8, u64, u32)]) -> AHashMap<u8, AHashMap<u64, RoaringBitmap>> {
    let mut map: AHashMap<u8, AHashMap<u64, RoaringBitmap>> = AHashMap::new();
    let mut i = 0;
    while i < tuples.len() {
        let (field, value, _) = tuples[i];
        let j = i + tuples[i..].partition_point(|&(f, v, _)| f == field && v == value);
        let bm = RoaringBitmap::from_sorted_iter(tuples[i..j].iter().map(|&(_, _, s)| s)).unwrap();
        map.entry(field).or_default().insert(value, bm);
        i = j;
    }
    map
}

/// Build flat HashMap<(field, value), RoaringBitmap> from sorted tuples (for G).
fn build_flat_map(tuples: &[(u8, u64, u32)]) -> AHashMap<(u8, u64), RoaringBitmap> {
    let mut map: AHashMap<(u8, u64), RoaringBitmap> = AHashMap::new();
    let mut i = 0;
    while i < tuples.len() {
        let (field, value, _) = tuples[i];
        let j = i + tuples[i..].partition_point(|&(f, v, _)| f == field && v == value);
        let bm = RoaringBitmap::from_sorted_iter(tuples[i..j].iter().map(|&(_, _, s)| s)).unwrap();
        map.insert((field, value), bm);
        i = j;
    }
    map
}

// ── Median helper ─────────────────────────────────────────────────────────────

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 0 { (v[n/2-1] + v[n/2]) / 2.0 } else { v[n/2] }
}

// ── Approach A: rayon fold+reduce ─────────────────────────────────────────────

fn approach_a(
    pool: &rayon::ThreadPool,
    thread_maps: &[AHashMap<u8, AHashMap<u64, RoaringBitmap>>],
) -> AHashMap<u8, AHashMap<u64, RoaringBitmap>> {
    // Clone inputs to simulate "consuming" them each iteration
    let owned: Vec<_> = thread_maps.iter().map(|m| {
        m.iter().map(|(&f, vals)| {
            (f, vals.iter().map(|(&k, bm)| (k, bm.clone())).collect::<AHashMap<_, _>>())
        }).collect::<AHashMap<_, _>>()
    }).collect();

    pool.install(|| {
        owned.into_par_iter().reduce(
            || AHashMap::new(),
            |mut acc, thread_result| {
                for (field, values) in thread_result {
                    let fm = acc.entry(field).or_default();
                    for (val, bm) in values {
                        fm.entry(val)
                          .and_modify(|e: &mut RoaringBitmap| *e |= &bm)
                          .or_insert(bm);
                    }
                }
                acc
            },
        )
    })
}

// ── Approach B: per-field parallel merge ─────────────────────────────────────

fn approach_b(
    pool: &rayon::ThreadPool,
    thread_maps: &[AHashMap<u8, AHashMap<u64, RoaringBitmap>>],
) -> AHashMap<u8, AHashMap<u64, RoaringBitmap>> {
    // Step 1: collect per-field from all threads (sequential)
    let mut per_field: AHashMap<u8, Vec<&AHashMap<u64, RoaringBitmap>>> = AHashMap::new();
    for tm in thread_maps {
        for (field, vals) in tm {
            per_field.entry(*field).or_default().push(vals);
        }
    }

    // Flatten into a Vec so rayon can own the data
    let work: Vec<(u8, Vec<&AHashMap<u64, RoaringBitmap>>)> = per_field.into_iter().collect();

    // Step 2: each field merged in parallel
    let pairs: Vec<(u8, AHashMap<u64, RoaringBitmap>)> = pool.install(|| {
        work.into_par_iter().map(|(field, thread_maps_for_field)| {
            let mut merged: AHashMap<u64, RoaringBitmap> = AHashMap::new();
            for map in thread_maps_for_field {
                for (val, bm) in map {
                    merged.entry(*val)
                          .and_modify(|e: &mut RoaringBitmap| *e |= bm)
                          .or_insert_with(|| bm.clone());
                }
            }
            (field, merged)
        }).collect()
    });
    pairs.into_iter().collect()
}

// ── Approach C: global sort + build bitmaps once ──────────────────────────────

fn approach_c(
    pool: &rayon::ThreadPool,
    thread_tuple_sets: &[Vec<(u8, u64, u32)>],
) -> AHashMap<u8, AHashMap<u64, RoaringBitmap>> {
    let total_len: usize = thread_tuple_sets.iter().map(|v| v.len()).sum();
    let mut all_tuples: Vec<(u8, u64, u32)> = Vec::with_capacity(total_len);
    for tuples in thread_tuple_sets {
        all_tuples.extend_from_slice(tuples);
    }

    let t_sort = Instant::now();
    pool.install(|| all_tuples.par_sort_unstable());
    let sort_ms = t_sort.elapsed().as_secs_f64() * 1000.0;

    let t_build = Instant::now();
    let mut result: AHashMap<u8, AHashMap<u64, RoaringBitmap>> = AHashMap::new();
    let mut i = 0;
    while i < all_tuples.len() {
        let (field, value, _) = all_tuples[i];
        let j = i + all_tuples[i..].partition_point(|&(f, v, _)| f == field && v == value);
        let bm = RoaringBitmap::from_sorted_iter(all_tuples[i..j].iter().map(|&(_, _, s)| s)).unwrap();
        result.entry(field).or_default().insert(value, bm);
        i = j;
    }
    let build_ms = t_build.elapsed().as_secs_f64() * 1000.0;
    println!("    [C] sort={:.1}ms  build={:.1}ms", sort_ms, build_ms);

    result
}

// ── Approach D: k-way merge of pre-sorted thread Vecs ─────────────────────────

#[derive(Eq, PartialEq)]
struct HeapEntry { tuple: (u8, u64, u32), thread_idx: usize, pos: usize }

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { other.tuple.cmp(&self.tuple) }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

fn approach_d(thread_tuple_sets: &[Vec<(u8, u64, u32)>]) -> AHashMap<u8, AHashMap<u64, RoaringBitmap>> {
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
    for (thread_idx, tuples) in thread_tuple_sets.iter().enumerate() {
        if !tuples.is_empty() {
            heap.push(HeapEntry { tuple: tuples[0], thread_idx, pos: 0 });
        }
    }

    let mut result: AHashMap<u8, AHashMap<u64, RoaringBitmap>> = AHashMap::new();
    let mut group: Vec<u32> = Vec::new();
    let mut cur_field: u8 = 0;
    let mut cur_value: u64 = 0;
    let mut first = true;

    while let Some(HeapEntry { tuple: (field, value, slot), thread_idx, pos }) = heap.pop() {
        let next_pos = pos + 1;
        if next_pos < thread_tuple_sets[thread_idx].len() {
            heap.push(HeapEntry { tuple: thread_tuple_sets[thread_idx][next_pos], thread_idx, pos: next_pos });
        }

        if !first && (field != cur_field || value != cur_value) {
            group.sort_unstable();
            let bm = RoaringBitmap::from_sorted_iter(group.drain(..)).unwrap();
            result.entry(cur_field).or_default().insert(cur_value, bm);
        }
        cur_field = field;
        cur_value = value;
        first = false;
        group.push(slot);
    }
    if !group.is_empty() {
        group.sort_unstable();
        let bm = RoaringBitmap::from_sorted_iter(group.drain(..)).unwrap();
        result.entry(cur_field).or_default().insert(cur_value, bm);
    }
    result
}

// ── Approach E: global sort + fused serialize ─────────────────────────────────

fn approach_e(pool: &rayon::ThreadPool, thread_tuple_sets: &[Vec<(u8, u64, u32)>]) -> usize {
    let total_len: usize = thread_tuple_sets.iter().map(|v| v.len()).sum();
    let mut all_tuples: Vec<(u8, u64, u32)> = Vec::with_capacity(total_len);
    for tuples in thread_tuple_sets { all_tuples.extend_from_slice(tuples); }

    let t_sort = Instant::now();
    pool.install(|| all_tuples.par_sort_unstable());
    let sort_ms = t_sort.elapsed().as_secs_f64() * 1000.0;

    let t_fused = Instant::now();
    let mut total_bytes = 0usize;
    let mut i = 0;
    while i < all_tuples.len() {
        let (field, value, _) = all_tuples[i];
        let j = i + all_tuples[i..].partition_point(|&(f, v, _)| f == field && v == value);
        let bm = RoaringBitmap::from_sorted_iter(all_tuples[i..j].iter().map(|&(_, _, s)| s)).unwrap();
        let mut buf = Vec::new();
        bm.serialize_into(&mut buf).unwrap();
        total_bytes += buf.len();
        black_box(&buf);
        i = j;
    }
    let fused_ms = t_fused.elapsed().as_secs_f64() * 1000.0;
    println!("    [E] sort={:.1}ms  build+ser={:.1}ms  bytes={:.1}MB", sort_ms, fused_ms, total_bytes as f64 / 1_048_576.0);

    total_bytes
}

// ── Approach F: sequential group-by (field,val), then par merge ───────────────

fn approach_f(
    pool: &rayon::ThreadPool,
    thread_maps: &[AHashMap<u8, AHashMap<u64, RoaringBitmap>>],
) -> Vec<(u8, u64, RoaringBitmap)> {
    // Step 1: sequential collect into flat group map
    let t_collect = Instant::now();
    let mut grouped: AHashMap<(u8, u64), Vec<&RoaringBitmap>> = AHashMap::new();
    for tm in thread_maps {
        for (&field, vals) in tm {
            for (&val, bm) in vals {
                grouped.entry((field, val)).or_default().push(bm);
            }
        }
    }
    let collect_ms = t_collect.elapsed().as_secs_f64() * 1000.0;
    let work_items: Vec<((u8, u64), Vec<&RoaringBitmap>)> = grouped.into_iter().collect();

    // Step 2: parallel merge — each (field, val) is an independent task
    let t_par = Instant::now();
    let merged: Vec<(u8, u64, RoaringBitmap)> = pool.install(|| {
        work_items.into_par_iter().map(|((field, val), bitmaps)| {
            let merged = bitmaps.into_iter().fold(RoaringBitmap::new(), |mut acc, bm| {
                acc |= bm;
                acc
            });
            (field, val, merged)
        }).collect()
    });
    let par_ms = t_par.elapsed().as_secs_f64() * 1000.0;
    println!("    [F] collect={:.1}ms  par_merge={:.1}ms  tasks={}", collect_ms, par_ms, merged.len());

    merged
}

// ── Approach G: flat (u8,u64) key per thread, then F-style merge ──────────────

fn approach_g(
    pool: &rayon::ThreadPool,
    thread_flat_maps: &[AHashMap<(u8, u64), RoaringBitmap>],
) -> Vec<((u8, u64), RoaringBitmap)> {
    // Step 1: sequential collect into grouped map — flat key, no nesting
    let t_collect = Instant::now();
    let mut grouped: AHashMap<(u8, u64), Vec<&RoaringBitmap>> = AHashMap::new();
    for tm in thread_flat_maps {
        for (key, bm) in tm {
            grouped.entry(*key).or_default().push(bm);
        }
    }
    let collect_ms = t_collect.elapsed().as_secs_f64() * 1000.0;
    let work_items: Vec<((u8, u64), Vec<&RoaringBitmap>)> = grouped.into_iter().collect();

    // Step 2: parallel merge
    let t_par = Instant::now();
    let merged: Vec<((u8, u64), RoaringBitmap)> = pool.install(|| {
        work_items.into_par_iter().map(|(key, bitmaps)| {
            let merged = bitmaps.into_iter().fold(RoaringBitmap::new(), |mut acc, bm| {
                acc |= bm;
                acc
            });
            (key, merged)
        }).collect()
    });
    let par_ms = t_par.elapsed().as_secs_f64() * 1000.0;
    println!("    [G] collect={:.1}ms  par_merge={:.1}ms  tasks={}", collect_ms, par_ms, merged.len());

    merged
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    println!("=== Bitmap Merge Strategy Benchmark ===");
    println!("  Total rows:    {}K", TOTAL_ROWS / 1_000);
    println!("  Threads:       {}", NUM_THREADS);
    println!("  Rows/thread:   {}K", ROWS_PER_THREAD / 1_000);
    println!("  Fields:        {} (2 low, 3 medium, 3 high cardinality)", NUM_FIELDS);
    println!("  Iterations:    {}", ITERS);
    println!();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build()
        .unwrap();

    // ── Generate thread tuples ────────────────────────────────────────────────
    println!("Generating {} threads x {}K rows...", NUM_THREADS, ROWS_PER_THREAD / 1_000);
    let t = Instant::now();
    let thread_tuple_sets: Vec<Vec<(u8, u64, u32)>> = pool.install(|| {
        (0..NUM_THREADS).into_par_iter().map(generate_thread_tuples).collect()
    });
    println!("  Done in {:.1}ms", t.elapsed().as_secs_f64() * 1000.0);

    // ── Build per-thread nested maps (for A/B/F) ──────────────────────────────
    println!("Building per-thread nested HashMaps (for A/B/F)...");
    let t = Instant::now();
    let thread_nested_maps: Vec<AHashMap<u8, AHashMap<u64, RoaringBitmap>>> = pool.install(|| {
        thread_tuple_sets.par_iter().map(|tuples| build_nested_map(tuples)).collect()
    });
    let nested_build_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!("  Done in {:.1}ms", nested_build_ms);

    // ── Build per-thread flat maps (for G) ────────────────────────────────────
    println!("Building per-thread flat HashMaps (for G)...");
    let t = Instant::now();
    let thread_flat_maps: Vec<AHashMap<(u8, u64), RoaringBitmap>> = pool.install(|| {
        thread_tuple_sets.par_iter().map(|tuples| build_flat_map(tuples)).collect()
    });
    let flat_build_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!("  Done in {:.1}ms", flat_build_ms);
    println!();

    // Stats
    {
        let mut field_value_counts: AHashMap<u8, usize> = AHashMap::new();
        for tm in &thread_nested_maps {
            for (&f, vals) in tm {
                *field_value_counts.entry(f).or_insert(0) += vals.len();
            }
        }
        let mut fields: Vec<u8> = field_value_counts.keys().copied().collect();
        fields.sort_unstable();
        for f in &fields {
            let card = FIELD_CONFIGS[*f as usize].0;
            println!("  field[{}] cardinality={:<10} thread-value pairs={}", f, card, field_value_counts[f]);
        }
        println!();
    }

    // ── Approach A ────────────────────────────────────────────────────────────
    println!("── Approach A: rayon fold+reduce (current) ─────────────────────────────────");
    let mut a_times = Vec::with_capacity(ITERS);
    for i in 0..ITERS {
        let t = Instant::now();
        let r = black_box(approach_a(&pool, &thread_nested_maps));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        a_times.push(ms);
        let total: usize = r.values().map(|v| v.len()).sum();
        println!("  iter {}: {:.1}ms  ({} bitmaps)", i+1, ms, total);
    }
    let a_med = median(a_times);
    println!("  MEDIAN: {:.1}ms\n", a_med);

    // ── Approach B ────────────────────────────────────────────────────────────
    println!("── Approach B: per-field parallel merge ─────────────────────────────────────");
    let mut b_times = Vec::with_capacity(ITERS);
    for i in 0..ITERS {
        let t = Instant::now();
        let r = black_box(approach_b(&pool, &thread_nested_maps));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        b_times.push(ms);
        let total: usize = r.values().map(|v| v.len()).sum();
        println!("  iter {}: {:.1}ms  ({} bitmaps)", i+1, ms, total);
    }
    let b_med = median(b_times);
    println!("  MEDIAN: {:.1}ms\n", b_med);

    // ── Approach C ────────────────────────────────────────────────────────────
    println!("── Approach C: global sort + build bitmaps once ─────────────────────────────");
    let mut c_times = Vec::with_capacity(ITERS);
    for i in 0..ITERS {
        let t = Instant::now();
        let r = black_box(approach_c(&pool, &thread_tuple_sets));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        c_times.push(ms);
        let total: usize = r.values().map(|v| v.len()).sum();
        println!("  iter {}: {:.1}ms  ({} bitmaps)", i+1, ms, total);
    }
    let c_med = median(c_times);
    println!("  MEDIAN: {:.1}ms\n", c_med);

    // ── Approach D ────────────────────────────────────────────────────────────
    println!("── Approach D: k-way merge (min-heap) ───────────────────────────────────────");
    let mut d_times = Vec::with_capacity(ITERS);
    for i in 0..ITERS {
        let t = Instant::now();
        let r = black_box(approach_d(&thread_tuple_sets));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        d_times.push(ms);
        let total: usize = r.values().map(|v| v.len()).sum();
        println!("  iter {}: {:.1}ms  ({} bitmaps)", i+1, ms, total);
    }
    let d_med = median(d_times);
    println!("  MEDIAN: {:.1}ms\n", d_med);

    // ── Approach E ────────────────────────────────────────────────────────────
    println!("── Approach E: global sort + fused serialize ────────────────────────────────");
    let mut e_times = Vec::with_capacity(ITERS);
    for i in 0..ITERS {
        let t = Instant::now();
        let bytes = black_box(approach_e(&pool, &thread_tuple_sets));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        e_times.push(ms);
        println!("  iter {}: {:.1}ms  ({:.1}MB)", i+1, ms, bytes as f64 / 1_048_576.0);
    }
    let e_med = median(e_times);
    println!("  MEDIAN: {:.1}ms\n", e_med);

    // ── Approach F ────────────────────────────────────────────────────────────
    println!("── Approach F: sequential group-by (field,val) + par merge ─────────────────");
    let mut f_times = Vec::with_capacity(ITERS);
    for i in 0..ITERS {
        let t = Instant::now();
        let r = black_box(approach_f(&pool, &thread_nested_maps));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        f_times.push(ms);
        println!("  iter {}: {:.1}ms  ({} bitmaps)", i+1, ms, r.len());
    }
    let f_med = median(f_times);
    println!("  MEDIAN: {:.1}ms\n", f_med);

    // ── Approach G ────────────────────────────────────────────────────────────
    println!("── Approach G: flat (u8,u64) key per thread + par merge ─────────────────────");
    let mut g_times = Vec::with_capacity(ITERS);
    for i in 0..ITERS {
        let t = Instant::now();
        let r = black_box(approach_g(&pool, &thread_flat_maps));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        g_times.push(ms);
        println!("  iter {}: {:.1}ms  ({} bitmaps)", i+1, ms, r.len());
    }
    let g_med = median(g_times);
    println!("  MEDIAN: {:.1}ms\n", g_med);

    // ── Summary table ─────────────────────────────────────────────────────────
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║  RESULTS — Median merge time, {}K rows, {} threads, {} iters             ║",
        TOTAL_ROWS / 1_000, NUM_THREADS, ITERS);
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");

    let mut rows: Vec<(&str, f64, &str)> = vec![
        ("A — rayon fold+reduce (current)",          a_med, "nested map, tree reduce"),
        ("B — per-field parallel merge",             b_med, "nested map, field-parallel OR"),
        ("C — global sort + build once",             c_med, "raw tuples, par_sort, from_sorted_iter"),
        ("D — k-way merge (min-heap)",               d_med, "raw tuples, streaming merge"),
        ("E — global sort + fused serialize",        e_med, "C + immediate serialize (no in-mem result)"),
        ("F — group-by(field,val) + par merge",      f_med, "nested map, per-value parallel OR"),
        ("G — flat (u8,u64) key + par merge",        g_med, "flat map, per-value parallel OR"),
    ];
    rows.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    for (rank, (name, ms, desc)) in rows.iter().enumerate() {
        let speedup = a_med / ms;
        let marker = if rank == 0 { " <<< WINNER" } else { "" };
        println!("║  {:>2}. {:<42} {:>7.1}ms  ({:.2}x vs A){}",
            rank + 1, name, ms, speedup, marker);
        println!("║      {}", desc);
        if rank < rows.len() - 1 { println!("║"); }
    }

    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  Per-thread bitmap build time:                                               ║");
    println!("║    nested (A/B/F): {:.1}ms    flat (G): {:.1}ms                        ║",
        nested_build_ms, flat_build_ms);
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  Apples-to-apples pipeline total (build + merge):                            ║");

    let ab_total = nested_build_ms + a_med.min(b_med).min(f_med).min(g_med - (g_med - flat_build_ms).min(0.0));
    // Separate compute for each
    let a_total  = nested_build_ms + a_med;
    let b_total  = nested_build_ms + b_med;
    let f_total  = nested_build_ms + f_med;
    let g_total  = flat_build_ms   + g_med;
    let c_total  = c_med;
    let d_total  = d_med;
    let e_total  = e_med;

    let mut pipeline_rows = vec![
        ("A", a_total), ("B", b_total), ("C (no pre-build)", c_total),
        ("D (no pre-build)", d_total), ("E (no pre-build)", e_total),
        ("F", f_total), ("G (flat build)", g_total),
    ];
    pipeline_rows.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    for (name, total) in &pipeline_rows {
        let speedup = a_total / total;
        println!("║    {:.<22} {:>7.1}ms total  ({:.2}x vs A pipeline)", name, total, speedup);
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    let _ = ab_total;
}
