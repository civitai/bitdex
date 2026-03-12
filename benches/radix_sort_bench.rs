//! Microbenchmarks for the Radix Sort Trie optimization.
//!
//! Validates 5 concepts before implementing in production code:
//! 1. Formation: cost to bucket slots by sort-value prefix
//! 2. Partial bifurcate: sort on small bucket vs full bitmap
//! 3. Deep pagination: cumulative rank skip vs fetch+drop
//! 4. Adaptive splitting: handling clustered distributions
//! 5. Live maintenance: insert/remove overhead for radix vs flat

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use rand::Rng;
use roaring::RoaringBitmap;

use bitdex_v2::config::SortFieldConfig;
use bitdex_v2::sort::SortField;

// ── Data Generation ─────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Distribution {
    /// Uniform u32 — models reactionCount. Best case for 8-bit radix.
    Uniform,
    /// Clustered timestamps (2021-2025 unix seconds). Adversarial: top 8 bits nearly identical.
    Clustered,
    /// 90% in one bucket (top 8 bits = 0x80), 10% spread. Tests adaptive splitting.
    Skewed,
}

fn make_sort_config() -> SortFieldConfig {
    SortFieldConfig {
        name: "bench_sort".to_string(),
        source_type: "uint32".to_string(),
        encoding: "linear".to_string(),
        bits: 32,
    }
}

/// Generate a SortField with `n` slots and a specific value distribution.
/// Returns (SortField, Vec<(slot, sort_value)>).
fn generate_sort_field(n: u32, dist: Distribution) -> (SortField, Vec<(u32, u32)>) {
    let mut sf = SortField::new(make_sort_config());
    let mut rng = rand::thread_rng();
    let mut entries = Vec::with_capacity(n as usize);

    for slot in 0..n {
        let value = match dist {
            Distribution::Uniform => rng.gen::<u32>(),
            Distribution::Clustered => {
                // Unix seconds 2021-01-01 to 2025-01-01
                // Top 8 bits: 0x5F..0x67 (very narrow range)
                1_609_459_200u32 + rng.gen_range(0..126_230_400)
            }
            Distribution::Skewed => {
                if rng.gen_range(0u32..10) < 9 {
                    0x8000_0000 | rng.gen_range(0..0x00FF_FFFFu32)
                } else {
                    rng.gen::<u32>()
                }
            }
        };
        sf.insert(slot, value);
        entries.push((slot, value));
    }
    sf.merge_all();
    (sf, entries)
}

/// Build a RoaringBitmap from a range of slot IDs.
fn bitmap_from_range(start: u32, end: u32) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    for slot in start..end {
        bm.insert(slot);
    }
    bm
}

/// Bucket slots by top 8 bits of sort value into a fixed-size array.
fn bucket_slots_array(
    sf: &SortField,
    candidates: &RoaringBitmap,
) -> [Option<RoaringBitmap>; 256] {
    let mut buckets: [Option<RoaringBitmap>; 256] = std::array::from_fn(|_| None);
    for slot in candidates.iter() {
        let val = sf.reconstruct_value(slot);
        let prefix = (val >> 24) as u8;
        buckets[prefix as usize]
            .get_or_insert_with(RoaringBitmap::new)
            .insert(slot);
    }
    buckets
}

/// Bucket using pre-computed values (no reconstruct_value calls).
fn bucket_slots_precomputed(
    entries: &[(u32, u32)],
) -> [Option<RoaringBitmap>; 256] {
    let mut buckets: [Option<RoaringBitmap>; 256] = std::array::from_fn(|_| None);
    for &(slot, value) in entries {
        let prefix = (value >> 24) as u8;
        buckets[prefix as usize]
            .get_or_insert_with(RoaringBitmap::new)
            .insert(slot);
    }
    buckets
}

// ── Group 1: Radix Bucketing Formation ──────────────────────────────────────

fn bench_radix_formation(c: &mut Criterion) {
    let mut group = c.benchmark_group("radix_formation");

    for &size in &[4_000u32, 16_000, 64_000] {
        let (sf, entries) = generate_sort_field(size, Distribution::Uniform);
        let candidates = bitmap_from_range(0, size);

        // Baseline: clone a flat bitmap
        group.bench_with_input(
            BenchmarkId::new("flat_clone", size),
            &candidates,
            |b, bm| b.iter(|| bm.clone()),
        );

        // Radix: bucket using reconstruct_value (the real cost during formation)
        group.bench_with_input(
            BenchmarkId::new("radix_reconstruct", size),
            &(&sf, &candidates),
            |b, &(sf, bm)| b.iter(|| bucket_slots_array(sf, bm)),
        );

        // Radix: bucket using pre-computed values (formation from sorted_slots + value_fn)
        group.bench_with_input(
            BenchmarkId::new("radix_precomputed", size),
            &entries,
            |b, entries| b.iter(|| bucket_slots_precomputed(entries)),
        );
    }

    // Report approximate memory for context
    {
        let (sf, _) = generate_sort_field(64_000, Distribution::Uniform);
        let candidates = bitmap_from_range(0, 64_000);
        let flat_bytes = candidates.serialized_size();
        let buckets = bucket_slots_array(&sf, &candidates);
        let radix_bytes: usize = buckets
            .iter()
            .filter_map(|b| b.as_ref())
            .map(|bm| bm.serialized_size())
            .sum::<usize>()
            + 256 * std::mem::size_of::<Option<RoaringBitmap>>();
        eprintln!(
            "\n[Memory] 64K slots: flat={flat_bytes} bytes, radix={radix_bytes} bytes, ratio={:.2}x",
            radix_bytes as f64 / flat_bytes as f64
        );
    }

    group.finish();
}

// ── Group 2: Partial Bifurcate (Small Bitmap Sort) ──────────────────────────

fn bench_partial_bifurcate(c: &mut Criterion) {
    let mut group = c.benchmark_group("partial_bifurcate");

    // Full 64K bitmap — current approach
    let (sf_64k, _) = generate_sort_field(64_000, Distribution::Uniform);
    let candidates_64k = bitmap_from_range(0, 64_000);

    group.bench_function("full_64k_top20_desc", |b| {
        b.iter(|| sf_64k.top_n(&candidates_64k, 20, true, None))
    });

    group.bench_function("full_64k_top20_asc", |b| {
        b.iter(|| sf_64k.top_n(&candidates_64k, 20, false, None))
    });

    // Typical bucket: ~250 items (64K / 256 buckets, uniform distribution)
    let (sf_250, _) = generate_sort_field(250, Distribution::Uniform);
    let candidates_250 = bitmap_from_range(0, 250);

    group.bench_function("bucket_250_top20_desc", |b| {
        b.iter(|| sf_250.top_n(&candidates_250, 20, true, None))
    });

    // Small bucket: 39 items
    let (sf_39, _) = generate_sort_field(39, Distribution::Uniform);
    let candidates_39 = bitmap_from_range(0, 39);

    group.bench_function("bucket_39_top20_desc", |b| {
        b.iter(|| sf_39.top_n(&candidates_39, 20, true, None))
    });

    // Tiny: 20 items (limit == candidates, bifurcate short-circuits)
    let (sf_20, _) = generate_sort_field(20, Distribution::Uniform);
    let candidates_20 = bitmap_from_range(0, 20);

    group.bench_function("bucket_20_top20_desc", |b| {
        b.iter(|| sf_20.top_n(&candidates_20, 20, true, None))
    });

    // Measure reconstruct_value in isolation
    group.bench_function("reconstruct_value_single", |b| {
        b.iter(|| sf_64k.reconstruct_value(1000))
    });

    // Measure reconstruct_value x20 (the order_results cost for 20 slots)
    group.bench_function("reconstruct_value_x20", |b| {
        b.iter(|| {
            let mut sum = 0u32;
            for slot in 0..20u32 {
                sum = sum.wrapping_add(sf_64k.reconstruct_value(slot));
            }
            sum
        })
    });

    group.finish();
}

// ── Group 3: Deep Pagination (Cumulative Rank Skip) ─────────────────────────

fn bench_deep_pagination(c: &mut Criterion) {
    let mut group = c.benchmark_group("deep_pagination");

    let (sf, _entries) = generate_sort_field(64_000, Distribution::Uniform);
    let candidates = bitmap_from_range(0, 64_000);

    // Pre-build radix buckets with cumulative counts
    let buckets = bucket_slots_array(&sf, &candidates);
    // Build sorted bucket list (desc order: prefix 255 → 0)
    let mut bucket_list: Vec<(u8, u32, RoaringBitmap)> = buckets
        .into_iter()
        .enumerate()
        .filter_map(|(i, opt)| opt.map(|bm| (i as u8, bm.len() as u32, bm)))
        .collect();
    bucket_list.sort_by(|a, b| b.0.cmp(&a.0)); // desc by prefix

    // Pre-compute cumulative counts
    let mut cumulative: Vec<u64> = Vec::with_capacity(bucket_list.len());
    let mut running = 0u64;
    for (_, count, _) in &bucket_list {
        running += *count as u64;
        cumulative.push(running);
    }

    let limit = 20usize;

    for &offset in &[100usize, 1_000, 4_000, 10_000, 32_000] {
        let fetch = offset + limit;

        // Current approach: fetch offset+limit, drop offset
        if fetch <= 64_000 {
            group.bench_with_input(
                BenchmarkId::new("current_fetch_drop", offset),
                &fetch,
                |b, &fetch| {
                    b.iter(|| {
                        let all = sf.top_n(&candidates, fetch, true, None);
                        all.into_iter().skip(offset).take(limit).collect::<Vec<_>>()
                    })
                },
            );
        }

        // Radix approach: skip buckets via cumulative counts, sort only target bucket
        group.bench_with_input(
            BenchmarkId::new("radix_skip", offset),
            &offset,
            |b, &offset| {
                b.iter(|| {
                    // Find which bucket contains the offset
                    let mut target_idx = 0;
                    for (i, &cum) in cumulative.iter().enumerate() {
                        if cum as usize > offset {
                            target_idx = i;
                            break;
                        }
                    }
                    let prev_cum = if target_idx > 0 {
                        cumulative[target_idx - 1] as usize
                    } else {
                        0
                    };
                    let within_offset = offset - prev_cum;
                    let within_fetch = within_offset + limit;
                    let bucket_bm = &bucket_list[target_idx].2;
                    let items = sf.top_n(bucket_bm, within_fetch, true, None);
                    items
                        .into_iter()
                        .skip(within_offset)
                        .take(limit)
                        .collect::<Vec<_>>()
                })
            },
        );
    }

    group.finish();
}

// ── Group 4: Adaptive Splitting (Clustered Data) ────────────────────────────

fn bench_adaptive_splitting(c: &mut Criterion) {
    let mut group = c.benchmark_group("adaptive_splitting");

    // Clustered timestamps — adversarial for static 8-bit radix
    let (sf, _entries) = generate_sort_field(64_000, Distribution::Clustered);
    let candidates = bitmap_from_range(0, 64_000);

    // Baseline: full 32-layer traversal on 64K items
    group.bench_function("baseline_32layer_clustered", |b| {
        b.iter(|| sf.top_n(&candidates, 20, true, None))
    });

    // Static 8-bit radix: most items in 1-3 buckets, largest bucket nearly full
    let buckets_8bit = bucket_slots_array(&sf, &candidates);
    // Find the highest non-empty bucket (for desc sort)
    let best_8bit = buckets_8bit
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, opt)| opt.as_ref().map(|bm| (i, bm)))
        .unwrap();
    eprintln!(
        "\n[Clustered] 8-bit: best bucket prefix=0x{:02X}, size={}",
        best_8bit.0,
        best_8bit.1.len()
    );

    group.bench_function("static_8bit_best_bucket", |b| {
        b.iter(|| sf.top_n(best_8bit.1, 20, true, None))
    });

    // Adaptive 16-bit: split the largest 8-bit bucket further on bits 16-23
    let mut sub_buckets_16bit: [Option<RoaringBitmap>; 256] = std::array::from_fn(|_| None);
    for slot in best_8bit.1.iter() {
        let val = sf.reconstruct_value(slot);
        let sub_prefix = ((val >> 16) & 0xFF) as u8;
        sub_buckets_16bit[sub_prefix as usize]
            .get_or_insert_with(RoaringBitmap::new)
            .insert(slot);
    }
    let best_16bit = sub_buckets_16bit
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, opt)| opt.as_ref().map(|bm| (i, bm)))
        .unwrap();
    eprintln!(
        "[Clustered] 16-bit: best sub-bucket prefix=0x{:02X}, size={}",
        best_16bit.0,
        best_16bit.1.len()
    );

    group.bench_function("adaptive_16bit_best_sub", |b| {
        b.iter(|| sf.top_n(best_16bit.1, 20, true, None))
    });

    // Also test uniform distribution for comparison (best case)
    let (sf_uniform, _) = generate_sort_field(64_000, Distribution::Uniform);
    let candidates_uniform = bitmap_from_range(0, 64_000);
    let buckets_uniform = bucket_slots_array(&sf_uniform, &candidates_uniform);
    let best_uniform = buckets_uniform
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, opt)| opt.as_ref().map(|bm| (i, bm)))
        .unwrap();
    eprintln!(
        "[Uniform] 8-bit: best bucket prefix=0x{:02X}, size={}",
        best_uniform.0,
        best_uniform.1.len()
    );

    group.bench_function("uniform_8bit_best_bucket", |b| {
        b.iter(|| sf_uniform.top_n(best_uniform.1, 20, true, None))
    });

    // Skewed distribution (90% in one bucket)
    let (sf_skewed, _) = generate_sort_field(64_000, Distribution::Skewed);
    let candidates_skewed = bitmap_from_range(0, 64_000);

    group.bench_function("baseline_32layer_skewed", |b| {
        b.iter(|| sf_skewed.top_n(&candidates_skewed, 20, true, None))
    });

    let buckets_skewed = bucket_slots_array(&sf_skewed, &candidates_skewed);
    let best_skewed = buckets_skewed
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, opt)| opt.as_ref().map(|bm| (i, bm)))
        .unwrap();
    eprintln!(
        "[Skewed] 8-bit: best bucket prefix=0x{:02X}, size={}",
        best_skewed.0,
        best_skewed.1.len()
    );

    group.bench_function("skewed_8bit_best_bucket", |b| {
        b.iter(|| sf_skewed.top_n(best_skewed.1, 20, true, None))
    });

    group.finish();
}

// ── Group 5: Live Maintenance ───────────────────────────────────────────────

fn bench_live_maintenance(c: &mut Criterion) {
    let mut group = c.benchmark_group("live_maintenance");

    let (sf, entries) = generate_sort_field(64_000, Distribution::Uniform);

    // Pre-build flat bitmap
    let flat = bitmap_from_range(0, 64_000);

    // Pre-build radix buckets
    let radix = bucket_slots_precomputed(&entries);

    // Prepare 1000 new slots with random values
    let mut rng = rand::thread_rng();
    let new_slots: Vec<(u32, u32)> = (64_000..65_000u32)
        .map(|slot| (slot, rng.gen::<u32>()))
        .collect();

    // Insert 1000 into flat
    group.bench_function("flat_insert_1000", |b| {
        b.iter_batched(
            || flat.clone(),
            |mut bm| {
                for &(slot, _) in &new_slots {
                    bm.insert(slot);
                }
                bm
            },
            BatchSize::SmallInput,
        )
    });

    // Insert 1000 into radix (extract prefix + index into array)
    group.bench_function("radix_insert_1000", |b| {
        b.iter_batched(
            || radix.clone(),
            |mut buckets| {
                for &(slot, value) in &new_slots {
                    let prefix = (value >> 24) as u8;
                    buckets[prefix as usize]
                        .get_or_insert_with(RoaringBitmap::new)
                        .insert(slot);
                }
                buckets
            },
            BatchSize::SmallInput,
        )
    });

    // Remove 1000 from flat
    group.bench_function("flat_remove_1000", |b| {
        b.iter_batched(
            || {
                let mut bm = flat.clone();
                for &(slot, _) in &new_slots {
                    bm.insert(slot);
                }
                bm
            },
            |mut bm| {
                for &(slot, _) in &new_slots {
                    bm.remove(slot);
                }
                bm
            },
            BatchSize::SmallInput,
        )
    });

    // Remove 1000 from radix (known prefix — best case)
    group.bench_function("radix_remove_1000_known_prefix", |b| {
        b.iter_batched(
            || {
                let mut buckets = radix.clone();
                for &(slot, value) in &new_slots {
                    let prefix = (value >> 24) as u8;
                    buckets[prefix as usize]
                        .get_or_insert_with(RoaringBitmap::new)
                        .insert(slot);
                }
                buckets
            },
            |mut buckets| {
                for &(slot, value) in &new_slots {
                    let prefix = (value >> 24) as u8;
                    if let Some(ref mut bm) = buckets[prefix as usize] {
                        bm.remove(slot);
                    }
                }
                buckets
            },
            BatchSize::SmallInput,
        )
    });

    // Remove 1000 from radix (scan all buckets — worst case, no prefix lookup)
    group.bench_function("radix_remove_1000_scan_buckets", |b| {
        b.iter_batched(
            || {
                let mut buckets = radix.clone();
                for &(slot, value) in &new_slots {
                    let prefix = (value >> 24) as u8;
                    buckets[prefix as usize]
                        .get_or_insert_with(RoaringBitmap::new)
                        .insert(slot);
                }
                buckets
            },
            |mut buckets| {
                for &(slot, _) in &new_slots {
                    for bucket in buckets.iter_mut().flatten() {
                        if bucket.contains(slot) {
                            bucket.remove(slot);
                            break;
                        }
                    }
                }
                buckets
            },
            BatchSize::SmallInput,
        )
    });

    // Also measure just the overhead of reconstruct_value (needed for formation)
    group.bench_function("reconstruct_1000_values", |b| {
        b.iter(|| {
            let mut sum = 0u32;
            for slot in 0..1000u32 {
                sum = sum.wrapping_add(sf.reconstruct_value(slot));
            }
            sum
        })
    });

    group.finish();
}

// ── Group 6: 4-Bit vs 8-Bit Radix Tradeoff ─────────────────────────────────

/// Bucket by top 4 bits (16 buckets) instead of 8 bits (256 buckets).
fn bucket_slots_4bit(entries: &[(u32, u32)]) -> [Option<RoaringBitmap>; 16] {
    let mut buckets: [Option<RoaringBitmap>; 16] = std::array::from_fn(|_| None);
    for &(slot, value) in entries {
        let prefix = (value >> 28) as u8; // top 4 bits
        buckets[prefix as usize]
            .get_or_insert_with(RoaringBitmap::new)
            .insert(slot);
    }
    buckets
}

fn bench_4bit_vs_8bit(c: &mut Criterion) {
    let mut group = c.benchmark_group("4bit_vs_8bit");

    for &size in &[4_000u32, 16_000, 64_000] {
        let (sf, entries) = generate_sort_field(size, Distribution::Uniform);
        let candidates = bitmap_from_range(0, size);

        // Formation: 4-bit
        group.bench_with_input(
            BenchmarkId::new("formation_4bit", size),
            &entries,
            |b, entries| b.iter(|| bucket_slots_4bit(entries)),
        );

        // Formation: 8-bit
        group.bench_with_input(
            BenchmarkId::new("formation_8bit", size),
            &entries,
            |b, entries| b.iter(|| bucket_slots_precomputed(entries)),
        );

        // Memory comparison
        let buckets_4 = bucket_slots_4bit(&entries);
        let buckets_8 = bucket_slots_precomputed(&entries);
        let mem_4: usize = buckets_4.iter().filter_map(|b| b.as_ref()).map(|bm| bm.serialized_size()).sum::<usize>()
            + 16 * std::mem::size_of::<Option<RoaringBitmap>>();
        let mem_8: usize = buckets_8.iter().filter_map(|b| b.as_ref()).map(|bm| bm.serialized_size()).sum::<usize>()
            + 256 * std::mem::size_of::<Option<RoaringBitmap>>();
        let pop_4 = buckets_4.iter().filter(|b| b.is_some()).count();
        let pop_8 = buckets_8.iter().filter(|b| b.is_some()).count();
        eprintln!(
            "\n[{size}] 4-bit: {pop_4} buckets, {:.1} KB | 8-bit: {pop_8} buckets, {:.1} KB | ratio: {:.1}x",
            mem_4 as f64 / 1024.0, mem_8 as f64 / 1024.0, mem_8 as f64 / mem_4 as f64
        );

        // Deep pagination: 4-bit vs 8-bit at offset=4000
        if size >= 16_000 {
            let offset = 4000usize;
            let limit = 20usize;

            // 4-bit deep pagination
            let mut bucket_list_4: Vec<(u8, u32, &RoaringBitmap)> = buckets_4
                .iter()
                .enumerate()
                .filter_map(|(i, opt)| opt.as_ref().map(|bm| (i as u8, bm.len() as u32, bm)))
                .collect();
            bucket_list_4.sort_by(|a, b| b.0.cmp(&a.0));
            let mut cum_4: Vec<u64> = Vec::new();
            let mut run = 0u64;
            for (_, count, _) in &bucket_list_4 {
                run += *count as u64;
                cum_4.push(run);
            }

            group.bench_with_input(
                BenchmarkId::new("deep_4000_4bit", size),
                &offset,
                |b, &offset| {
                    b.iter(|| {
                        let mut target = 0;
                        for (i, &c) in cum_4.iter().enumerate() {
                            if c as usize > offset { target = i; break; }
                        }
                        let prev = if target > 0 { cum_4[target-1] as usize } else { 0 };
                        let within = offset - prev;
                        let items = sf.top_n(bucket_list_4[target].2, within + limit, true, None);
                        items.into_iter().skip(within).take(limit).collect::<Vec<_>>()
                    })
                },
            );

            // 8-bit deep pagination
            let mut bucket_list_8: Vec<(u8, u32, &RoaringBitmap)> = buckets_8
                .iter()
                .enumerate()
                .filter_map(|(i, opt)| opt.as_ref().map(|bm| (i as u8, bm.len() as u32, bm)))
                .collect();
            bucket_list_8.sort_by(|a, b| b.0.cmp(&a.0));
            let mut cum_8: Vec<u64> = Vec::new();
            let mut run8 = 0u64;
            for (_, count, _) in &bucket_list_8 {
                run8 += *count as u64;
                cum_8.push(run8);
            }

            group.bench_with_input(
                BenchmarkId::new("deep_4000_8bit", size),
                &offset,
                |b, &offset| {
                    b.iter(|| {
                        let mut target = 0;
                        for (i, &c) in cum_8.iter().enumerate() {
                            if c as usize > offset { target = i; break; }
                        }
                        let prev = if target > 0 { cum_8[target-1] as usize } else { 0 };
                        let within = offset - prev;
                        let items = sf.top_n(bucket_list_8[target].2, within + limit, true, None);
                        items.into_iter().skip(within).take(limit).collect::<Vec<_>>()
                    })
                },
            );
        }
    }

    group.finish();
}

// ── Group 7: Scale-Aware Bifurcate ──────────────────────────────────────────

fn bench_scale_aware_bifurcate(c: &mut Criterion) {
    let mut group = c.benchmark_group("scale_aware_bifurcate");
    // Longer measurement time for these expensive benchmarks
    group.measurement_time(std::time::Duration::from_secs(10));

    // Create a large SortField (1M slots) to simulate production-scale bit layers.
    // At 105M, each bit layer is a ~105M-wide bitmap. At 1M, we get a taste of
    // the scaling behavior without blowing up bench memory.
    let sizes = [100_000u32, 500_000, 1_000_000];

    for &total_slots in &sizes {
        let (sf, _) = generate_sort_field(total_slots, Distribution::Uniform);

        // Candidate sets of various sizes within the large SortField
        for &candidate_size in &[20u32, 250, 4_000, 64_000] {
            if candidate_size > total_slots { continue; }

            // Build candidate bitmap: first N slots
            let candidates = bitmap_from_range(0, candidate_size);

            group.bench_with_input(
                BenchmarkId::new(
                    format!("bifurcate_{}total_{}cand", total_slots, candidate_size),
                    candidate_size,
                ),
                &(&sf, &candidates),
                |b, &(sf, cands)| {
                    b.iter(|| sf.top_n(cands, 20, true, None))
                },
            );
        }

        // Also measure simple_sort cost (reconstruct all + sort) at this scale
        // This simulates what radix bucket pagination would do
        for &candidate_size in &[20u32, 250, 4_000] {
            if candidate_size > total_slots { continue; }
            let candidates = bitmap_from_range(0, candidate_size);

            group.bench_with_input(
                BenchmarkId::new(
                    format!("reconstruct_sort_{}total_{}cand", total_slots, candidate_size),
                    candidate_size,
                ),
                &(&sf, &candidates),
                |b, &(sf, cands)| {
                    b.iter(|| {
                        // Reconstruct values for all candidates
                        let mut entries: Vec<(u32, u32)> = cands
                            .iter()
                            .map(|slot| (slot, sf.reconstruct_value(slot)))
                            .collect();
                        // Sort descending
                        entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
                        // Take top 20
                        entries.truncate(20);
                        entries
                    })
                },
            );
        }
    }

    group.finish();
}

// ── Group 8: Sorted Vec Alternative (GPT Approach) ──────────────────────────

fn bench_sorted_vec(c: &mut Criterion) {
    let mut group = c.benchmark_group("sorted_vec");

    for &size in &[4_000u32, 16_000, 64_000] {
        let (_sf, entries) = generate_sort_field(size, Distribution::Uniform);

        // Build sorted Vec<u64> of packed (sort_value, slot_id)
        let mut sorted: Vec<u64> = entries
            .iter()
            .map(|&(slot, val)| ((val as u64) << 32) | slot as u64)
            .collect();
        sorted.sort_unstable_by(|a, b| b.cmp(a)); // desc

        // Formation: build the sorted vec
        group.bench_with_input(
            BenchmarkId::new("formation", size),
            &entries,
            |b, entries| {
                b.iter(|| {
                    let mut v: Vec<u64> = entries
                        .iter()
                        .map(|&(slot, val)| ((val as u64) << 32) | slot as u64)
                        .collect();
                    v.sort_unstable_by(|a, b| b.cmp(a));
                    v
                })
            },
        );

        // Pagination: cursor lookup via binary search + take 20
        // Simulate cursor at position 100
        let cursor_packed = sorted.get(100).copied().unwrap_or(0);
        group.bench_with_input(
            BenchmarkId::new("paginate_cursor_100", size),
            &(&sorted, cursor_packed),
            |b, &(sorted, cursor)| {
                b.iter(|| {
                    // Binary search for cursor position (desc order: search reversed)
                    let pos = sorted.partition_point(|&x| x > cursor);
                    sorted[pos..].iter().take(20).copied().collect::<Vec<_>>()
                })
            },
        );

        // Deep pagination: cursor at position 4000
        if size > 4000 {
            let cursor_4000 = sorted.get(4000).copied().unwrap_or(0);
            group.bench_with_input(
                BenchmarkId::new("paginate_cursor_4000", size),
                &(&sorted, cursor_4000),
                |b, &(sorted, cursor)| {
                    b.iter(|| {
                        let pos = sorted.partition_point(|&x| x > cursor);
                        sorted[pos..].iter().take(20).copied().collect::<Vec<_>>()
                    })
                },
            );
        }

        // Insert 1000 (binary search + Vec::insert — O(n) shift)
        let mut rng = rand::thread_rng();
        let new_items: Vec<u64> = (0..1000u32)
            .map(|i| {
                let slot = size + i;
                let val: u32 = rng.gen();
                ((val as u64) << 32) | slot as u64
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("insert_1000", size),
            &(&sorted, &new_items),
            |b, &(sorted, items)| {
                b.iter_batched(
                    || sorted.clone(),
                    |mut v| {
                        for &item in items.iter() {
                            let pos = v.partition_point(|&x| x > item);
                            v.insert(pos, item);
                        }
                        v
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        // Remove 1000 (need to find item first)
        let remove_items: Vec<u64> = sorted.iter().take(1000).copied().collect();
        group.bench_with_input(
            BenchmarkId::new("remove_1000", size),
            &(&sorted, &remove_items),
            |b, &(sorted, items)| {
                b.iter_batched(
                    || sorted.clone(),
                    |mut v| {
                        for &item in items.iter() {
                            let pos = v.partition_point(|&x| x > item);
                            if pos < v.len() && v[pos] == item {
                                v.remove(pos);
                            }
                        }
                        v
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        // Memory
        let mem = size as usize * 8;
        eprintln!(
            "\n[Sorted Vec {size}] memory: {} bytes ({:.1} KB), 5000 entries: {:.1} MB",
            mem, mem as f64 / 1024.0, mem as f64 * 5000.0 / 1024.0 / 1024.0
        );
    }

    group.finish();
}

// ── Criterion Main ──────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_radix_formation,
    bench_partial_bifurcate,
    bench_deep_pagination,
    bench_adaptive_splitting,
    bench_live_maintenance,
    bench_4bit_vs_8bit,
    bench_scale_aware_bifurcate,
    bench_sorted_vec,
);
criterion_main!(benches);
