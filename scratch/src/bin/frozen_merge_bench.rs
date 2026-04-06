/// Frozen bitmap merge experiment — Approach A vs B
///
/// Context: dump pipeline builds per-thread bitmaps then ORs them together.
/// The merge + apply_bitmap_maps cost ~7s total at 107M scale.
///
/// Approach A (current): per-thread RoaringBitmap → par_iter reduce (OR) → serialize frozen
/// Approach B (new):     per-thread RoaringBitmap → serialize frozen per thread →
///                       ops-log buffer → sequential frozen OR compaction → final frozen
///
/// Benchmarks at 4, 8, 16, 32 threads with realistic 14.6M slot IDs scattered
/// across 0..15_000_000, ~60% fill rate for a single filter bitmap, plus 32
/// sort bit-layer bitmaps.
///
/// Run:
///   cargo run -p scratch --release --bin frozen_merge_bench

use rayon::prelude::*;
use roaring::{FrozenRoaringBitmap, RoaringBitmap};
use std::hint::black_box;
use std::time::Instant;

// ── Simulation constants ──────────────────────────────────────────────────────

/// Total slot range (0..MAX_SLOT). Realistic for civitai at 15M image IDs.
const MAX_SLOT: u32 = 15_000_000;

/// Total rows being processed.
/// We use 2M here for reasonable bench runtime; at 14.6M the build phase
/// dominates at ~38s/iter. Merge/compact costs scale with bitmap density,
/// not row count, so 2M rows at 60% fill still exercises the same code paths.
const TOTAL_ROWS: u32 = 2_000_000;

/// Fraction of rows where the filter predicate matches (nsfwLevel=1).
const FILTER_HIT_RATE: f64 = 0.60;

/// Number of sort bit-layers (u32 → 32 bits).
const SORT_BITS: usize = 32;

/// Number of benchmark iterations to average over.
const ITERS: usize = 5;

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    println!("=== Frozen Bitmap Merge Benchmark ===");
    println!("  Total rows:     {}M", TOTAL_ROWS / 1_000_000);
    println!("  Slot range:     0..{}", MAX_SLOT);
    println!("  Filter hit rate: {:.0}%", FILTER_HIT_RATE * 100.0);
    println!("  Sort bit-layers: {}", SORT_BITS);
    println!("  Iterations:     {}", ITERS);
    println!();

    // Pre-generate the slot data once — scattered using a simple LCG so it's
    // fast to generate but not trivially compressible.
    println!("Generating {} slot IDs...", TOTAL_ROWS);
    let t = Instant::now();
    let slot_ids: Vec<u32> = (0..TOTAL_ROWS)
        .map(|i| {
            // LCG scatter across slot range
            let lcg = (i as u64).wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (lcg >> 32) as u32 % MAX_SLOT
        })
        .collect();
    println!("  Done in {:.1}ms\n", t.elapsed().as_millis());

    // Run at multiple thread counts.
    for &num_threads in &[4usize, 8, 16, 32] {
        run_comparison(&slot_ids, num_threads);
        println!();
    }
}

// ── Per-thread-count comparison ───────────────────────────────────────────────

fn run_comparison(slot_ids: &[u32], num_threads: usize) {
    println!("══ {} threads ══════════════════════════════════════════════════", num_threads);

    // Build the rayon pool with the exact thread count we want.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    // Chunk data across threads.
    let chunk_size = slot_ids.len() / num_threads;
    let chunks: Vec<&[u32]> = (0..num_threads)
        .map(|t| {
            let start = t * chunk_size;
            let end = if t == num_threads - 1 { slot_ids.len() } else { start + chunk_size };
            &slot_ids[start..end]
        })
        .collect();

    // ── Approach A ───────────────────────────────────────────────────────────
    println!("  Approach A — in-memory merge (current)");
    let mut a_build_ms = Vec::with_capacity(ITERS);
    let mut a_merge_ms = Vec::with_capacity(ITERS);
    let mut a_serial_ms = Vec::with_capacity(ITERS);
    let mut a_total_ms = Vec::with_capacity(ITERS);

    for _ in 0..ITERS {
        let t_total = Instant::now();

        // Phase 1: per-thread bitmap build
        let t_build = Instant::now();
        let thread_results: Vec<(RoaringBitmap, Vec<RoaringBitmap>)> = pool.install(|| {
            chunks
                .par_iter()
                .enumerate()
                .map(|(tid, chunk)| build_thread_bitmaps(tid, chunk))
                .collect()
        });
        a_build_ms.push(t_build.elapsed().as_secs_f64() * 1000.0);

        // Phase 2: reduce (OR all per-thread bitmaps together)
        let t_merge = Instant::now();
        let (merged_filter, merged_sorts) = pool.install(|| {
            thread_results
                .into_par_iter()
                .reduce(
                    || (RoaringBitmap::new(), vec![RoaringBitmap::new(); SORT_BITS]),
                    |mut dst, src| {
                        dst.0 |= src.0;
                        for (i, bm) in src.1.into_iter().enumerate() {
                            dst.1[i] |= bm;
                        }
                        dst
                    },
                )
        });
        a_merge_ms.push(t_merge.elapsed().as_secs_f64() * 1000.0);

        // Phase 3: serialize to frozen bytes (what the dump pipeline writes to disk)
        let t_serial = Instant::now();
        let filter_bytes = serialize_frozen(&merged_filter);
        let sort_bytes: Vec<Vec<u8>> = merged_sorts.iter().map(serialize_frozen).collect();
        a_serial_ms.push(t_serial.elapsed().as_secs_f64() * 1000.0);

        a_total_ms.push(t_total.elapsed().as_secs_f64() * 1000.0);
        black_box((&filter_bytes, &sort_bytes));
    }

    print_stats("    build", &a_build_ms);
    print_stats("    merge", &a_merge_ms);
    print_stats("    serialize", &a_serial_ms);
    print_stats("    TOTAL", &a_total_ms);

    // Report approximate heap peak for approach A:
    // Each thread holds (1 filter + 32 sort) RoaringBitmaps concurrently.
    // Rough estimate: MAX_SLOT / 8 bytes worst-case per bitmap, but roaring compresses.
    // Use the serialized sizes as a proxy for data content.
    println!();

    // ── Approach B ───────────────────────────────────────────────────────────
    println!("  Approach B — frozen merge via ops log");
    let mut b_build_ms = Vec::with_capacity(ITERS);
    let mut b_freeze_ms = Vec::with_capacity(ITERS);
    let mut b_compact_ms = Vec::with_capacity(ITERS);
    let mut b_total_ms = Vec::with_capacity(ITERS);
    let mut b_ops_log_bytes: usize = 0;

    for iter in 0..ITERS {
        let t_total = Instant::now();

        // Phase 1: per-thread bitmap build (same as A)
        let t_build = Instant::now();
        let thread_results: Vec<(RoaringBitmap, Vec<RoaringBitmap>)> = pool.install(|| {
            chunks
                .par_iter()
                .enumerate()
                .map(|(tid, chunk)| build_thread_bitmaps(tid, chunk))
                .collect()
        });
        b_build_ms.push(t_build.elapsed().as_secs_f64() * 1000.0);

        // Phase 2: each thread serializes its partial bitmaps to frozen bytes
        // and writes them into the ops-log buffer.
        // In the real pipeline these would be written to an ops log file per field.
        // Here we use Vec<Vec<u8>> to simulate the ops log in memory.
        let t_freeze = Instant::now();
        // ops_log[i] = list of frozen blobs from each thread for bitmap slot i
        // Slot 0 = filter bitmap, slots 1..=32 = sort bit-layers
        let total_slots = 1 + SORT_BITS;
        let mut ops_log: Vec<Vec<Vec<u8>>> = vec![Vec::with_capacity(num_threads); total_slots];

        for (filter_bm, sort_bms) in &thread_results {
            // Filter bitmap
            if !filter_bm.is_empty() {
                ops_log[0].push(serialize_frozen_aligned(filter_bm));
            }
            // Sort bit-layers
            for (bit, sort_bm) in sort_bms.iter().enumerate() {
                if !sort_bm.is_empty() {
                    ops_log[1 + bit].push(serialize_frozen_aligned(sort_bm));
                }
            }
        }
        b_freeze_ms.push(t_freeze.elapsed().as_secs_f64() * 1000.0);

        if iter == 0 {
            b_ops_log_bytes = ops_log.iter().flat_map(|v| v.iter()).map(|b| b.len()).sum();
        }

        // Phase 3: compaction — iterate frozen blobs per slot, OR them together
        let t_compact = Instant::now();
        let final_frozen: Vec<Vec<u8>> = ops_log
            .iter()
            .map(|blobs| compact_frozen_blobs(blobs))
            .collect();
        b_compact_ms.push(t_compact.elapsed().as_secs_f64() * 1000.0);

        b_total_ms.push(t_total.elapsed().as_secs_f64() * 1000.0);
        black_box(&final_frozen);
    }

    print_stats("    build", &b_build_ms);
    print_stats("    freeze (serialize per thread)", &b_freeze_ms);
    print_stats("    compact (frozen OR)", &b_compact_ms);
    print_stats("    TOTAL", &b_total_ms);

    println!();

    // ── Approach B2: frozen OR (FrozenBitmap | FrozenBitmap path) ─────────────
    println!("  Approach B2 — frozen merge (FrozenBitmap | FrozenBitmap)");
    let mut b2_build_ms = Vec::with_capacity(ITERS);
    let mut b2_freeze_ms = Vec::with_capacity(ITERS);
    let mut b2_compact_ms = Vec::with_capacity(ITERS);
    let mut b2_total_ms = Vec::with_capacity(ITERS);

    for _ in 0..ITERS {
        let t_total = Instant::now();

        // Phase 1: per-thread bitmap build (identical to A and B)
        let t_build = Instant::now();
        let thread_results: Vec<(RoaringBitmap, Vec<RoaringBitmap>)> = pool.install(|| {
            chunks
                .par_iter()
                .enumerate()
                .map(|(tid, chunk)| build_thread_bitmaps(tid, chunk))
                .collect()
        });
        b2_build_ms.push(t_build.elapsed().as_secs_f64() * 1000.0);

        // Phase 2: serialize to frozen (same as B)
        let t_freeze = Instant::now();
        let total_slots = 1 + SORT_BITS;
        let mut ops_log: Vec<Vec<Vec<u8>>> = vec![Vec::with_capacity(num_threads); total_slots];
        for (filter_bm, sort_bms) in &thread_results {
            if !filter_bm.is_empty() {
                ops_log[0].push(serialize_frozen_aligned(filter_bm));
            }
            for (bit, sort_bm) in sort_bms.iter().enumerate() {
                if !sort_bm.is_empty() {
                    ops_log[1 + bit].push(serialize_frozen_aligned(sort_bm));
                }
            }
        }
        b2_freeze_ms.push(t_freeze.elapsed().as_secs_f64() * 1000.0);

        // Phase 3: frozen OR compaction
        let t_compact = Instant::now();
        let final_frozen: Vec<Vec<u8>> = ops_log
            .iter()
            .map(|blobs| compact_frozen_blobs_frozen_or(blobs))
            .collect();
        b2_compact_ms.push(t_compact.elapsed().as_secs_f64() * 1000.0);

        b2_total_ms.push(t_total.elapsed().as_secs_f64() * 1000.0);
        black_box(&final_frozen);
    }

    print_stats("    build", &b2_build_ms);
    print_stats("    freeze (serialize per thread)", &b2_freeze_ms);
    print_stats("    compact (FrozenBitmap OR)", &b2_compact_ms);
    print_stats("    TOTAL", &b2_total_ms);

    println!();

    // ── Summary ──────────────────────────────────────────────────────────────
    let a_avg = avg(&a_total_ms);
    let b_avg = avg(&b_total_ms);
    let b2_avg = avg(&b2_total_ms);
    let _speedup_unused = a_avg / b_avg; // suppress unused warning

    println!("  ┌─────────────────────────────────────────────┐");
    println!("  │  {} threads — summary", num_threads);
    println!("  │  Approach A  (owned OR + serialize):  {:>7.1}ms  [merge only: {:.1}ms]",
        a_avg, avg(&a_merge_ms));
    println!("  │  Approach B  (decode OR):             {:>7.1}ms  [compact:    {:.1}ms]",
        b_avg, avg(&b_compact_ms));
    println!("  │  Approach B2 (frozen OR):             {:>7.1}ms  [compact:    {:.1}ms]",
        b2_avg, avg(&b2_compact_ms));

    let b_vs_a = a_avg / b_avg;
    let b2_vs_a = a_avg / b2_avg;
    if b_vs_a >= 1.0 {
        println!("  │  B  vs A: {:.2}x FASTER", b_vs_a);
    } else {
        println!("  │  B  vs A: {:.2}x SLOWER", 1.0 / b_vs_a);
    }
    if b2_vs_a >= 1.0 {
        println!("  │  B2 vs A: {:.2}x FASTER", b2_vs_a);
    } else {
        println!("  │  B2 vs A: {:.2}x SLOWER", 1.0 / b2_vs_a);
    }
    println!("  │  Ops log size: {:.1} MB", b_ops_log_bytes as f64 / 1e6);
    println!("  │  A heap:  {} threads × {} bitmaps live during reduce",
        num_threads, 1 + SORT_BITS);
    println!("  │  B heap:  build same; compact reads frozen blobs sequentially");
    println!("  └─────────────────────────────────────────────┘");
}

// ── Core routines ─────────────────────────────────────────────────────────────

/// Simulate what each thread does during the dump pipeline:
/// - filter bitmap: insert slot if `tid * row_index` hashes to < FILTER_HIT_RATE
/// - sort bit-layers: decompose a synthetic sort value into 32 bit-layers
fn build_thread_bitmaps(tid: usize, slots: &[u32]) -> (RoaringBitmap, Vec<RoaringBitmap>) {
    let mut filter = RoaringBitmap::new();
    let mut sort_layers: Vec<RoaringBitmap> = (0..SORT_BITS).map(|_| RoaringBitmap::new()).collect();

    for (i, &slot) in slots.iter().enumerate() {
        // Filter: roughly FILTER_HIT_RATE of rows match
        // Use a fast deterministic hash so each thread's data is independent
        let h = (slot as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(tid as u64 * 0xbf58_476d_1ce4_e5b9);
        if (h & 0xFFFF) < (FILTER_HIT_RATE * 65536.0) as u64 {
            filter.insert(slot);
        }

        // Sort bit-layers: synthetic sort value (simulates a unix timestamp decomposed into bits)
        // Each row gets a sort_val derived from its position so the distribution is spread.
        let sort_val: u32 = ((slot as u64).wrapping_mul(0x6c62_272e_07bb_0142).wrapping_add(i as u64)) as u32;
        for bit in 0..SORT_BITS {
            if (sort_val >> bit) & 1 == 1 {
                sort_layers[bit].insert(slot);
            }
        }
    }

    (filter, sort_layers)
}

/// Serialize a RoaringBitmap to frozen bytes in a plain Vec<u8>.
/// The vec is not guaranteed 32-byte aligned (for approach A timing only).
fn serialize_frozen(bm: &RoaringBitmap) -> Vec<u8> {
    if bm.is_empty() {
        return Vec::new();
    }
    let size = bm.frozen_serialized_size();
    let mut buf = vec![0u8; size];
    bm.serialize_frozen_into(&mut buf).unwrap();
    buf
}

/// Serialize a RoaringBitmap to frozen bytes.
/// Stored as plain bytes — alignment is provided fresh on decode.
fn serialize_frozen_aligned(bm: &RoaringBitmap) -> Vec<u8> {
    serialize_frozen(bm)
}

/// Decode a frozen blob into an owned RoaringBitmap.
///
/// `FrozenRoaringBitmap::view()` requires 32-byte aligned input, but our ops-log
/// Vec<u8> entries are not guaranteed to be aligned. We copy into a fresh heap
/// allocation that we aligned manually. This is O(blob_size) — same cost as
/// `to_owned()` on the frozen view — so it's correct and predictable.
fn decode_frozen_blob(blob: &[u8]) -> Option<RoaringBitmap> {
    if blob.is_empty() {
        return None;
    }
    // Allocate with 31 bytes of headroom so we can guarantee a 32-byte aligned start.
    let size = blob.len();
    let mut aligned: Vec<u8> = vec![0u8; size + 31];
    let offset = aligned.as_ptr() as usize % 32;
    let start = if offset == 0 { 0 } else { 32 - offset };
    aligned[start..start + size].copy_from_slice(blob);

    // view() borrows from `aligned`, which we keep alive
    let frozen = FrozenRoaringBitmap::view(&aligned[start..start + size]).ok()?;
    Some(frozen.to_owned())
}

/// Compact a list of frozen blobs by decoding to owned RoaringBitmap and ORing.
/// This is the "compaction pass" for Approach B (via owned OR).
fn compact_frozen_blobs(blobs: &[Vec<u8>]) -> Vec<u8> {
    if blobs.is_empty() {
        return Vec::new();
    }

    let mut merged = RoaringBitmap::new();
    for blob in blobs {
        if let Some(bm) = decode_frozen_blob(blob) {
            merged |= bm;
        }
    }

    serialize_frozen(&merged)
}

/// Compact a list of frozen blobs using `&FrozenRoaringBitmap | &FrozenRoaringBitmap`.
///
/// This uses the frozen-to-frozen OR path, which walks the container lists without
/// ever fully materializing an intermediate RoaringBitmap — the result is a new
/// owned RoaringBitmap, but the inputs are consumed as frozen views.
///
/// Each step: frozen | frozen → owned; then owned | frozen → owned for subsequent steps.
fn compact_frozen_blobs_frozen_or(blobs: &[Vec<u8>]) -> Vec<u8> {
    if blobs.is_empty() {
        return Vec::new();
    }

    // Build aligned buffers so view() works.
    // Tuple: (buffer, start_offset, payload_size) — view slice is buf[start..start+size].
    let aligned: Vec<(Vec<u8>, usize, usize)> = blobs
        .iter()
        .filter_map(|b| {
            if b.is_empty() { return None; }
            let size = b.len();
            let mut buf = vec![0u8; size + 31];
            let offset = buf.as_ptr() as usize % 32;
            let start = if offset == 0 { 0 } else { 32 - offset };
            buf[start..start + size].copy_from_slice(b);
            Some((buf, start, size))
        })
        .collect();

    if aligned.is_empty() {
        return Vec::new();
    }

    // First pair: FrozenBitmap | FrozenBitmap → RoaringBitmap
    let (buf0, s0, sz0) = &aligned[0];
    let f0 = FrozenRoaringBitmap::view(&buf0[*s0..*s0 + *sz0]).unwrap();
    let mut merged: RoaringBitmap = if aligned.len() == 1 {
        f0.to_owned()
    } else {
        let (buf1, s1, sz1) = &aligned[1];
        let f1 = FrozenRoaringBitmap::view(&buf1[*s1..*s1 + *sz1]).unwrap();
        // Uses BitOr<&FrozenRoaringBitmap> for &FrozenRoaringBitmap
        &f0 | &f1
    };

    // Remaining: RoaringBitmap | &FrozenRoaringBitmap
    for (buf, start, size) in &aligned[2..] {
        let frozen = FrozenRoaringBitmap::view(&buf[*start..*start + *size]).unwrap();
        // Uses BitOr<&FrozenRoaringBitmap> for &RoaringBitmap
        merged = &merged | &frozen;
    }

    serialize_frozen(&merged)
}

// ── Statistics helpers ────────────────────────────────────────────────────────

fn avg(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}

fn min(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::INFINITY, f64::min)
}

fn max(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
}

fn stddev(v: &[f64]) -> f64 {
    let a = avg(v);
    let variance = v.iter().map(|x| (x - a).powi(2)).sum::<f64>() / v.len() as f64;
    variance.sqrt()
}

fn print_stats(label: &str, v: &[f64]) {
    println!(
        "    {:<32}  avg={:>7.1}ms  min={:>7.1}ms  max={:>7.1}ms  σ={:.1}ms",
        label,
        avg(v),
        min(v),
        max(v),
        stddev(v)
    );
}
