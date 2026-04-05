/// filter_insert_bench.rs
///
/// Compares four strategies for building filter bitmaps during the dump pipeline
/// parse loop, at Civitai-realistic data shapes.
///
/// Approaches
/// ----------
/// A  HashMap<(field, value), RoaringBitmap> — per-row insert (current code)
/// B  Flat Vec<(field, value, slot)> → sort_unstable → from_sorted_iter
/// C  Flat Vec<(field, value, slot)> → sort_unstable → extend (via BTreeMap grouping)
/// D  HashMap<(field, value), Vec<u32>>   → sort each Vec → from_sorted_iter
///
/// Scenarios (simulate Civitai 14.6M-row image phase)
/// ---------------------------------------------------
/// 1  Low-cardinality   : 5  values,  2.92M slots/value   (nsfwLevel)
/// 2  Medium-cardinality: 50K values, power-law           (tagIds)
/// 3  High-cardinality  : 2M  values, ~7 slots/value      (userId)
/// 4  Mixed (8 fields)  : 2 low + 3 medium + 3 high, 14.6M rows

use ahash::AHashMap as HashMap;
use rand::Rng;
use rand::SeedableRng as _;
use roaring::RoaringBitmap;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

/// A single "emit" from the parse loop: (field_idx, value_key, slot)
type Row = (u8, u64, u32);

/// Build a low-cardinality scenario: `n_rows` rows, `n_values` distinct values,
/// slots assigned uniformly.
fn gen_low_card(n_rows: usize, n_values: u64, seed: u64) -> Vec<Row> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    (0..n_rows)
        .map(|i| (0u8, rng.gen_range(0..n_values), i as u32))
        .collect()
}

/// Power-law distribution over `n_values` distinct values.
/// The top ~1% of values hold ~50% of the slots (Zipf-like).
fn gen_power_law(n_rows: usize, n_values: u64, seed: u64) -> Vec<Row> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    // Simple Zipf approximation: sample bucket = floor(n_values / (uniform^2))
    // clamped to [0, n_values).
    (0..n_rows)
        .map(|i| {
            let u: f64 = rng.gen::<f64>().max(1e-6);
            let v = ((n_values as f64) * u * u) as u64;
            let v = v.min(n_values - 1);
            (0u8, v, i as u32)
        })
        .collect()
}

/// High-cardinality: uniform over 2M values.
fn gen_high_card(n_rows: usize, n_values: u64, seed: u64) -> Vec<Row> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    (0..n_rows)
        .map(|i| (0u8, rng.gen_range(0..n_values), i as u32))
        .collect()
}

/// Mixed: 8 fields, 14.6M rows.
/// Field 0-1: 5 values each (low)
/// Field 2-4: 50K values each (medium, power-law)
/// Field 5-7: 2M values each (high, uniform)
fn gen_mixed(n_rows: usize, seed: u64) -> Vec<Row> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut rows = Vec::with_capacity(n_rows * 8);
    for slot in 0..n_rows as u32 {
        // 2 low-card fields
        for f in 0u8..2 {
            let v: u64 = rng.gen_range(0..5);
            rows.push((f, v, slot));
        }
        // 3 medium-card power-law fields
        for f in 2u8..5 {
            let u: f64 = rng.gen::<f64>().max(1e-6);
            let v = ((50_000f64) * u * u) as u64;
            let v = v.min(49_999);
            rows.push((f, v, slot));
        }
        // 3 high-card uniform fields
        for f in 5u8..8 {
            let v: u64 = rng.gen_range(0..2_000_000);
            rows.push((f, v, slot));
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Approaches
// ---------------------------------------------------------------------------

/// Approach A: HashMap<(field, value), RoaringBitmap> — one insert per row
fn approach_a(rows: &[Row]) -> HashMap<(u8, u64), RoaringBitmap> {
    let mut map: HashMap<(u8, u64), RoaringBitmap> = HashMap::default();
    for &(field, value, slot) in rows {
        map.entry((field, value)).or_default().insert(slot);
    }
    map
}

/// Approach B: flat Vec → sort_unstable → from_sorted_iter
fn approach_b(rows: &[Row]) -> HashMap<(u8, u64), RoaringBitmap> {
    // Clone so we can sort in-place (simulates owning the buffer)
    let mut tuples: Vec<Row> = rows.to_vec();
    tuples.sort_unstable();

    let mut map: HashMap<(u8, u64), RoaringBitmap> = HashMap::default();
    if tuples.is_empty() {
        return map;
    }

    let mut i = 0;
    while i < tuples.len() {
        let (field, value, _) = tuples[i];
        let start = i;
        while i < tuples.len() && tuples[i].0 == field && tuples[i].1 == value {
            i += 1;
        }
        // slots are already sorted (sort_unstable on the full tuple)
        let bm = RoaringBitmap::from_sorted_iter(tuples[start..i].iter().map(|&(_, _, s)| s))
            .expect("slots must be sorted");
        map.insert((field, value), bm);
    }
    map
}

/// Approach C: flat Vec → sort_unstable → extend
/// Same as B but uses bitmap.extend() for construction.
fn approach_c(rows: &[Row]) -> HashMap<(u8, u64), RoaringBitmap> {
    let mut tuples: Vec<Row> = rows.to_vec();
    tuples.sort_unstable();

    let mut map: HashMap<(u8, u64), RoaringBitmap> = HashMap::default();
    if tuples.is_empty() {
        return map;
    }

    let mut i = 0;
    while i < tuples.len() {
        let (field, value, _) = tuples[i];
        let start = i;
        while i < tuples.len() && tuples[i].0 == field && tuples[i].1 == value {
            i += 1;
        }
        let bm = map.entry((field, value)).or_default();
        bm.extend(tuples[start..i].iter().map(|&(_, _, s)| s));
    }
    map
}

/// Approach D: HashMap<(field, value), Vec<u32>> → sort each Vec → from_sorted_iter
fn approach_d(rows: &[Row]) -> HashMap<(u8, u64), RoaringBitmap> {
    let mut collectors: HashMap<(u8, u64), Vec<u32>> = HashMap::default();
    for &(field, value, slot) in rows {
        collectors.entry((field, value)).or_default().push(slot);
    }
    let mut map: HashMap<(u8, u64), RoaringBitmap> = HashMap::default();
    for ((field, value), mut slots) in collectors {
        slots.sort_unstable();
        let bm = RoaringBitmap::from_sorted_iter(slots.into_iter())
            .expect("slots must be sorted");
        map.insert((field, value), bm);
    }
    map
}

// ---------------------------------------------------------------------------
// Correctness check
// ---------------------------------------------------------------------------

fn bitmap_fingerprint(map: &HashMap<(u8, u64), RoaringBitmap>) -> (usize, u64) {
    let total_bits: u64 = map.values().map(|bm| bm.len()).sum();
    (map.len(), total_bits)
}

fn assert_same_fingerprint(
    label: &str,
    reference: (usize, u64),
    got: (usize, u64),
) {
    assert_eq!(
        reference, got,
        "{label}: bitmap fingerprint mismatch (expected {reference:?}, got {got:?})"
    );
}

// ---------------------------------------------------------------------------
// Timing harness
// ---------------------------------------------------------------------------

struct BenchResult {
    scenario: &'static str,
    approach: &'static str,
    n_rows: usize,
    median_ms: f64,
    bitmap_count: usize,
    total_slots: u64,
}

fn median_ms(samples: &mut [f64]) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[samples.len() / 2]
}

fn run_bench<F>(
    scenario: &'static str,
    approach: &'static str,
    n_rows: usize,
    iters: usize,
    warmup: usize,
    f: F,
    reference_fp: Option<(usize, u64)>,
) -> BenchResult
where
    F: Fn() -> HashMap<(u8, u64), RoaringBitmap>,
{
    // Warmup
    for _ in 0..warmup {
        let _ = std::hint::black_box(f());
    }

    let mut samples = Vec::with_capacity(iters);
    let mut fp = (0usize, 0u64);
    for i in 0..iters {
        let t = Instant::now();
        let result = std::hint::black_box(f());
        let elapsed = t.elapsed().as_secs_f64() * 1000.0;
        if i == 0 {
            fp = bitmap_fingerprint(&result);
        }
        samples.push(elapsed);
    }

    if let Some(ref_fp) = reference_fp {
        assert_same_fingerprint(approach, ref_fp, fp);
    }

    BenchResult {
        scenario,
        approach,
        n_rows,
        median_ms: median_ms(&mut samples),
        bitmap_count: fp.0,
        total_slots: fp.1,
    }
}

// ---------------------------------------------------------------------------
// Memory estimation helpers
// ---------------------------------------------------------------------------

/// Rough memory estimate for Approach A result map (post-build, RoaringBitmaps).
/// We can't easily measure peak, but we can measure the flat Vec overhead vs
/// the HashMap overhead at collection time.
///
/// Instead we print pre-build allocation sizes as a proxy.
fn estimate_flat_vec_bytes(rows: &[Row]) -> usize {
    rows.len() * std::mem::size_of::<Row>() // (u8, u64, u32) = 13 bytes but aligned to 16
}

fn estimate_hashmap_overhead(n_entries: usize) -> usize {
    // ahash HashMap: each bucket is ~8 bytes overhead + entry.
    // Very rough: assume 1.5x load factor.
    n_entries * 24 // key(u8+u64=16) + value ptr + hash overhead
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    println!("filter_insert_bench — comparing bitmap build strategies");
    println!("=========================================================");
    println!();

    const ITERS: usize = 3;
    const WARMUP: usize = 1;

    // -----------------------------------------------------------------------
    // Scenario 1: Low cardinality — 14.6M rows, 5 values
    // -----------------------------------------------------------------------
    const N_LOW: usize = 14_600_000;
    let low_rows = gen_low_card(N_LOW, 5, 0xDEAD_BEEF);
    println!(
        "Scenario 1 — Low cardinality: {} rows, 5 values, ~{:.1}M slots/value",
        N_LOW,
        N_LOW as f64 / 5.0 / 1_000_000.0
    );
    let ref_a = approach_a(&low_rows);
    let ref_fp = bitmap_fingerprint(&ref_a);
    drop(ref_a);

    let mut results: Vec<BenchResult> = Vec::new();

    results.push(run_bench("Low-card", "A: HashMap insert", N_LOW, ITERS, WARMUP,
        || approach_a(&low_rows), Some(ref_fp)));
    results.push(run_bench("Low-card", "B: Vec+sort+from_sorted_iter", N_LOW, ITERS, WARMUP,
        || approach_b(&low_rows), Some(ref_fp)));
    results.push(run_bench("Low-card", "C: Vec+sort+extend", N_LOW, ITERS, WARMUP,
        || approach_c(&low_rows), Some(ref_fp)));
    results.push(run_bench("Low-card", "D: HashMap<Vec>+sort", N_LOW, ITERS, WARMUP,
        || approach_d(&low_rows), Some(ref_fp)));

    // -----------------------------------------------------------------------
    // Scenario 2: Medium cardinality — 14.6M rows, 50K values, power-law
    // -----------------------------------------------------------------------
    const N_MED: usize = 14_600_000;
    let med_rows = gen_power_law(N_MED, 50_000, 0xCAFE_BABE);
    println!(
        "\nScenario 2 — Medium cardinality: {} rows, 50K values (power-law)",
        N_MED
    );
    let ref_a = approach_a(&med_rows);
    let ref_fp_med = bitmap_fingerprint(&ref_a);
    drop(ref_a);

    results.push(run_bench("Med-card", "A: HashMap insert", N_MED, ITERS, WARMUP,
        || approach_a(&med_rows), Some(ref_fp_med)));
    results.push(run_bench("Med-card", "B: Vec+sort+from_sorted_iter", N_MED, ITERS, WARMUP,
        || approach_b(&med_rows), Some(ref_fp_med)));
    results.push(run_bench("Med-card", "C: Vec+sort+extend", N_MED, ITERS, WARMUP,
        || approach_c(&med_rows), Some(ref_fp_med)));
    results.push(run_bench("Med-card", "D: HashMap<Vec>+sort", N_MED, ITERS, WARMUP,
        || approach_d(&med_rows), Some(ref_fp_med)));

    // -----------------------------------------------------------------------
    // Scenario 3: High cardinality — 14.6M rows, 2M values, uniform
    // -----------------------------------------------------------------------
    const N_HIGH: usize = 14_600_000;
    let high_rows = gen_high_card(N_HIGH, 2_000_000, 0xFEED_FACE);
    println!(
        "\nScenario 3 — High cardinality: {} rows, 2M values (~7 slots/value avg)",
        N_HIGH
    );
    let ref_a = approach_a(&high_rows);
    let ref_fp_high = bitmap_fingerprint(&ref_a);
    drop(ref_a);

    results.push(run_bench("High-card", "A: HashMap insert", N_HIGH, ITERS, WARMUP,
        || approach_a(&high_rows), Some(ref_fp_high)));
    results.push(run_bench("High-card", "B: Vec+sort+from_sorted_iter", N_HIGH, ITERS, WARMUP,
        || approach_b(&high_rows), Some(ref_fp_high)));
    results.push(run_bench("High-card", "C: Vec+sort+extend", N_HIGH, ITERS, WARMUP,
        || approach_c(&high_rows), Some(ref_fp_high)));
    results.push(run_bench("High-card", "D: HashMap<Vec>+sort", N_HIGH, ITERS, WARMUP,
        || approach_d(&high_rows), Some(ref_fp_high)));

    // -----------------------------------------------------------------------
    // Scenario 4: Mixed — 8 fields × 14.6M rows
    // -----------------------------------------------------------------------
    const N_MIXED: usize = 14_600_000;
    let mixed_rows = gen_mixed(N_MIXED, 0xABCD_1234);
    println!(
        "\nScenario 4 — Mixed (8 fields): {} base rows, {} total tuples",
        N_MIXED,
        mixed_rows.len()
    );
    let ref_a = approach_a(&mixed_rows);
    let ref_fp_mixed = bitmap_fingerprint(&ref_a);
    drop(ref_a);

    results.push(run_bench("Mixed-8f", "A: HashMap insert", N_MIXED, ITERS, WARMUP,
        || approach_a(&mixed_rows), Some(ref_fp_mixed)));
    results.push(run_bench("Mixed-8f", "B: Vec+sort+from_sorted_iter", N_MIXED, ITERS, WARMUP,
        || approach_b(&mixed_rows), Some(ref_fp_mixed)));
    results.push(run_bench("Mixed-8f", "C: Vec+sort+extend", N_MIXED, ITERS, WARMUP,
        || approach_c(&mixed_rows), Some(ref_fp_mixed)));
    results.push(run_bench("Mixed-8f", "D: HashMap<Vec>+sort", N_MIXED, ITERS, WARMUP,
        || approach_d(&mixed_rows), Some(ref_fp_mixed)));

    // -----------------------------------------------------------------------
    // Results table
    // -----------------------------------------------------------------------
    println!();
    println!("RESULTS");
    println!("=======");
    println!(
        "{:<12}  {:<32}  {:>9}  {:>10}  {:>12}  {:>12}",
        "Scenario", "Approach", "Rows", "Median ms", "Bitmaps", "Total slots"
    );
    println!("{}", "-".repeat(97));

    let mut last_scenario = "";
    for r in &results {
        if r.scenario != last_scenario {
            if last_scenario != "" {
                println!();
            }
            last_scenario = r.scenario;
        }
        println!(
            "{:<12}  {:<32}  {:>9}  {:>10.1}  {:>12}  {:>12}",
            r.scenario,
            r.approach,
            r.n_rows,
            r.median_ms,
            r.bitmap_count,
            r.total_slots,
        );
    }

    println!();

    // -----------------------------------------------------------------------
    // Per-scenario winner summary
    // -----------------------------------------------------------------------
    println!("WINNER SUMMARY (by scenario)");
    println!("============================");
    let scenarios = ["Low-card", "Med-card", "High-card", "Mixed-8f"];
    for &sc in &scenarios {
        let sc_results: Vec<&BenchResult> = results.iter().filter(|r| r.scenario == sc).collect();
        if sc_results.is_empty() {
            continue;
        }
        let fastest = sc_results.iter().min_by(|a, b| {
            a.median_ms.partial_cmp(&b.median_ms).unwrap()
        }).unwrap();
        let slowest_ms = sc_results.iter().map(|r| r.median_ms).fold(f64::NEG_INFINITY, f64::max);
        let speedup = slowest_ms / fastest.median_ms;
        println!(
            "{:<12}  winner: {:<32}  {:.1}ms  (max speedup vs slowest: {:.2}x)",
            sc, fastest.approach, fastest.median_ms, speedup
        );
    }

    // -----------------------------------------------------------------------
    // Speedup of each approach vs Approach A (current baseline)
    // -----------------------------------------------------------------------
    println!();
    println!("SPEEDUP VS APPROACH A (current baseline)");
    println!("=========================================");
    for &sc in &scenarios {
        let sc_results: Vec<&BenchResult> = results.iter().filter(|r| r.scenario == sc).collect();
        let baseline = sc_results.iter().find(|r| r.approach == "A: HashMap insert");
        if let Some(base) = baseline {
            println!("  {}:", sc);
            for r in &sc_results {
                let ratio = base.median_ms / r.median_ms;
                let indicator = if ratio > 1.05 { "FASTER" } else if ratio < 0.95 { "SLOWER" } else { "same" };
                println!(
                    "    {:<32}  {:.2}x  {}",
                    r.approach, ratio, indicator
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Memory overhead estimate
    // -----------------------------------------------------------------------
    println!();
    println!("MEMORY OVERHEAD ESTIMATE (collection phase only, before bitmap build)");
    println!("======================================================================");
    let mixed_n = mixed_rows.len();
    let flat_vec_bytes = estimate_flat_vec_bytes(&mixed_rows);
    let approx_entries_d = 2 * N_MIXED  // 2 low fields × ~5 = 10 entries
        + 3 * 50_000                    // 3 med fields
        + 3 * 2_000_000;               // 3 high fields (worst case)
    let hashmap_overhead_d = estimate_hashmap_overhead(approx_entries_d);

    println!(
        "  Mixed scenario ({} tuples):",
        mixed_n
    );
    println!(
        "    Approach B/C flat Vec:          {:>8} MB  ({} bytes/tuple)",
        flat_vec_bytes / 1_048_576,
        std::mem::size_of::<Row>()
    );
    println!(
        "    Approach D HashMap<Vec>:        {:>8} MB  (key overhead + vec ptrs, ~{} entries)",
        hashmap_overhead_d / 1_048_576,
        approx_entries_d
    );
    println!(
        "    Note: B/C also allocate {} MB for the sort buffer (in-place on the owned vec)",
        flat_vec_bytes / 1_048_576,
    );
    println!();
    println!("  Row tuple size: {} bytes (u8 field_idx, u64 value, u32 slot)",
        std::mem::size_of::<Row>());
    println!("  Alignment: {} bytes", std::mem::align_of::<Row>());

    println!();
    println!("Done.");
}
