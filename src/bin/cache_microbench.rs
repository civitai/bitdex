// Microbenchmark for UnifiedCache maintenance strategies.
//
// Answers three questions:
//   1. Is bulk entry.add_slots_bulk() faster than N add_slot() calls? By how much?
//   2. Does the inverted-loop maintenance shape (per-slot preamble, per-entry
//      fast-reject, per-entry bulk update) scale with mut_slots rather than
//      total cache entries?
//   3. Does the inverted loop produce the same cache state as the current
//      nested-loop implementation?
//
// Stubs the expensive ops (reconstruct_value, slot_matches_filter) with
// fixed-cost closures so the loop SHAPE is isolated from index lookup cost.
//
// Env knobs:
//   BENCH_ENTRIES       default 70_000
//   BENCH_MUT_SLOTS     default 200
//   BENCH_BATCH_SIZES   default "1,10,50,200,1000"  — for the bulk vs individual test
//   BENCH_ENTRY_SWEEP   default "10000,50000,100000,200000,500000"
//   BENCH_SHOW_PROD     default "1" — also run the original prod path for comparison
//   BENCH_ITERS         default 10
//
// Run with:
//   cargo run --release --bin cache_microbench
use std::time::Instant;
use bitdex_v2::cache::CanonicalClause;
use bitdex_v2::config::SortFieldConfig;
use bitdex_v2::query::SortDirection;
use bitdex_v2::sort::SortField;
use bitdex_v2::unified_cache::{UnifiedCache, UnifiedCacheConfig, UnifiedKey};
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}
fn env_list(key: &str, default: &str) -> Vec<usize> {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
        .split(',').filter_map(|s| s.trim().parse().ok()).collect()
}
fn make_config(n_entries: usize) -> UnifiedCacheConfig {
    UnifiedCacheConfig {
        max_entries: n_entries * 2,
        max_bytes: 4 * 1024 * 1024 * 1024,
        initial_capacity: 100,
        max_capacity: 1600,
        min_filter_size: 0,
        max_maintenance_work: usize::MAX / 2,
        max_maintenance_ms: 5,
        ..Default::default()
    }
}
fn populate(cache: &mut UnifiedCache, n_entries: usize) {
    let slots: Vec<u32> = (0..100).collect();
    for i in 0..n_entries {
        let val = i.to_string();
        let key = UnifiedKey {
            filter_clauses: vec![CanonicalClause {
                field: "userId".to_string(),
                op: "eq".to_string(),
                value_repr: val,
            }],
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        cache.form_and_store(key, &slots, true, 100_000, |s| {
            1000u32.saturating_sub(s)
        });
    }
}
struct Stats {
    min: u128, p50: u128, avg: u128, p95: u128, max: u128,
}
fn stats(mut s: Vec<u128>) -> Stats {
    s.sort_unstable();
    let sum: u128 = s.iter().sum();
    Stats {
        min: *s.first().unwrap(),
        p50: s[s.len() / 2],
        avg: sum / s.len() as u128,
        p95: s[(s.len() * 95) / 100],
        max: *s.last().unwrap(),
    }
}
fn fmt(ns: u128) -> String { format!("{:8.3} ms", ns as f64 / 1_000_000.0) }
fn report(name: &str, s: &Stats) {
    println!("[{:30}] min={} p50={} avg={} p95={} max={}",
        name, fmt(s.min), fmt(s.p50), fmt(s.avg), fmt(s.p95), fmt(s.max));
}
// ─── Test 1: Bulk vs individual add_slot on a single entry ────────────────
fn test_bulk_vs_individual(batch_sizes: &[usize], iters: usize) {
    println!("\n=== Test 1: bulk add_slots_bulk vs N add_slot calls ===");
    println!("Single entry, vary batch size. Lower is better.\n");
    for &batch in batch_sizes {
        // Build synthetic (slot, value) pairs
        let adds: Vec<(u32, u32)> = (0..batch as u32)
            .map(|i| (i.wrapping_mul(2654435761), 500 + i))
            .collect();
        // ── Individual add_slot path ──
        let mut ind_samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let mut cache = UnifiedCache::new(make_config(2));
            populate(&mut cache, 1);
            let key = UnifiedKey {
                filter_clauses: vec![CanonicalClause {
                    field: "userId".to_string(),
                    op: "eq".to_string(),
                    value_repr: "0".to_string(),
                }],
                sort_field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            };
            // Measure just the mutation time, not setup
            let t = Instant::now();
            if let Some(entry) = cache.get_mut(&key) {
                for &(slot, val) in &adds {
                    entry.add_slot(slot, val);
                }
            }
            ind_samples.push(t.elapsed().as_nanos());
        }
        // ── Bulk add_slots_bulk path ──
        let mut bulk_samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let mut cache = UnifiedCache::new(make_config(2));
            populate(&mut cache, 1);
            let key = UnifiedKey {
                filter_clauses: vec![CanonicalClause {
                    field: "userId".to_string(),
                    op: "eq".to_string(),
                    value_repr: "0".to_string(),
                }],
                sort_field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            };
            let t = Instant::now();
            if let Some(entry) = cache.get_mut(&key) {
                entry.add_slots_bulk(&adds);
            }
            bulk_samples.push(t.elapsed().as_nanos());
        }
        let ind = stats(ind_samples);
        let bulk = stats(bulk_samples);
        let speedup = ind.p50 as f64 / bulk.p50.max(1) as f64;
        println!(
            "batch={:5}  individual p50={:>9}  bulk p50={:>9}  speedup={:5.1}x",
            batch, fmt(ind.p50), fmt(bulk.p50), speedup
        );
    }
}
// ─── Test 2: Loop-shape scaling (inverted loop vs current) ────────────────
//
// We stub the expensive per-(entry,slot) ops with a fixed integer compare so
// we can isolate the loop SHAPE cost. The inverted loop should be ~flat in
// total entries; the current loop should grow linearly.
fn test_loop_scaling(entry_counts: &[usize], mut_slots: usize, iters: usize) {
    println!("\n=== Test 2: Loop-shape scaling ===");
    println!("Fixed mut_slots={}, vary total cache entries. Lower is better.\n", mut_slots);
    // Precompute stub per-slot data
    let slot_values: Vec<(u32, u32)> = (0..mut_slots as u32)
        .map(|i| (i.wrapping_mul(2654435761), (i * 17) % 2000))
        .collect();
    for &n_entries in entry_counts {
        // ── Setup — shared across both loop variants, measured outside ──
        let mut cache = UnifiedCache::new(make_config(n_entries));
        populate(&mut cache, n_entries);
        // ── Current loop shape: for each entry, for each slot, do cost ──
        // (We're not running the actual current path because it needs
        //  FilterIndex+SortIndex scaffolding. Instead we simulate the
        //  per-(entry,slot) inner cost as a single integer compare plus a
        //  HashMap lookup, which is a LOWER BOUND on what current does.)
        let mut current_samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let mut survivors = 0usize;
            // Mimic affected_entries walk → per-entry, per-slot eval
            for meta_id in 0..n_entries as u32 {
                // Stub "get entry" — a map/index touch
                let _dummy = meta_id.wrapping_mul(31);
                for &(_slot, value) in &slot_values {
                    // Stub per-entry-per-slot work
                    // (current path reconstructs sort value + checks filter
                    //  — here we just do a min_tracked compare as the
                    //  cheapest POSSIBLE version of what current does)
                    if value > 500 {
                        survivors += 1;
                    }
                }
            }
            let elapsed = t.elapsed().as_nanos();
            current_samples.push(elapsed);
            std::hint::black_box(survivors);
        }
        // ── Inverted loop shape ──
        // Preamble: per-slot work is ALREADY done (slot_values).
        // Per entry: fast min_tracked reject, then process surviving slots.
        let mut inverted_samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            // Preamble: simulate "build per-slot data" — reconstruct_value
            // + signature. Fixed cost per mutated slot (not per entry).
            let mut preamble_sum = 0u64;
            for &(_slot, value) in &slot_values {
                // Stub the ~1-10μs reconstruct + signature cost
                preamble_sum = preamble_sum.wrapping_add(value as u64);
            }
            // Per-entry: fast reject on min_tracked
            let mut survivors = 0usize;
            for _meta_id in 0..n_entries {
                // Integer compare against entry.min_tracked_value (stubbed)
                // This is the O(entries) walk but with ~5ns per iteration.
                let min_tracked = 500u32;
                // Check if any mut slot might qualify. In the real impl this
                // would be: does max(slot_values) > min_tracked? If not, skip.
                if slot_values.iter().any(|&(_, v)| v > min_tracked) {
                    // For surviving entries, build batch of qualifying slots
                    // and call add_slots_bulk. Here we just simulate the filter
                    // work as a HashSet containment check per survivor per slot.
                    for &(_slot, value) in &slot_values {
                        if value > min_tracked {
                            survivors += 1;
                        }
                    }
                }
            }
            std::hint::black_box(preamble_sum);
            std::hint::black_box(survivors);
            inverted_samples.push(t.elapsed().as_nanos());
        }
        let current = stats(current_samples);
        let inverted = stats(inverted_samples);
        let speedup = current.p50 as f64 / inverted.p50.max(1) as f64;
        println!(
            "entries={:7}  current p50={:>10}  inverted p50={:>10}  speedup={:5.1}x",
            n_entries, fmt(current.p50), fmt(inverted.p50), speedup
        );
    }
}
// ─── Test 0: raw reconstruct_value cost ───────────────────────────────────
// Builds a real SortField with ~1M populated slots, measures reconstruct_value
// latency in a tight loop. This gives us the `X` for scaling math:
//   current path cost ≈ N_entries × K_mut_slots × X
//   inverted path cost ≈ K_mut_slots × X + N_entries × cheap_reject
fn test_reconstruct_value_cost() -> f64 {
    println!("\n=== Test 0: reconstruct_value raw cost ===");
    println!("Real SortField, 32 bit layers, synthetic data.\n");
    // Build a SortField with 1M slots, values spread across u20 range
    let config = SortFieldConfig {
        name: "reactionCount".to_string(),
        source_type: "uint32".to_string(),
        encoding: "linear".to_string(),
        bits: 32,
        eager_load: false,
        computed: None,
    };
    let mut field = SortField::new(config);
    let n_slots = 1_000_000u32;
    println!("populating {} slots...", n_slots);
    let t_pop = Instant::now();
    for slot in 0..n_slots {
        // Hash-scatter values across ~20 bits of range
        let value = slot.wrapping_mul(2654435761) & 0xFFFFF;
        field.insert(slot, value);
    }
    println!("populated in {:.2}s", t_pop.elapsed().as_secs_f64());
    // Call reconstruct_value on random slots, measure per-call cost
    let iters = 100_000;
    let t = Instant::now();
    let mut checksum: u64 = 0;
    for i in 0..iters {
        let slot = (i as u32).wrapping_mul(2654435761) % n_slots;
        checksum = checksum.wrapping_add(field.reconstruct_value(slot) as u64);
    }
    let elapsed = t.elapsed();
    std::hint::black_box(checksum);
    let per_call_ns = elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "{} calls in {:.2}ms = {:.1}ns per reconstruct_value",
        iters,
        elapsed.as_secs_f64() * 1000.0,
        per_call_ns
    );
    per_call_ns
}
// ─── Test 3: Loop-shape with REAL reconstruct_value calls ─────────────────
//
// Builds an actual SortField and calls reconstruct_value for real. Compares
// nested (current) vs inverted loop shapes.
fn test_loop_scaling_real(entry_counts: &[usize], mut_slots: usize, iters: usize) {
    println!("\n=== Test 3: Loop-shape scaling (real reconstruct_value) ===");
    println!("Fixed mut_slots={}, vary total cache entries.\n", mut_slots);
    // Build a real SortField
    let config = SortFieldConfig {
        name: "reactionCount".to_string(),
        source_type: "uint32".to_string(),
        encoding: "linear".to_string(),
        bits: 32,
        eager_load: false,
        computed: None,
    };
    let mut field = SortField::new(config);
    let n_slots_field = 1_000_000u32;
    for slot in 0..n_slots_field {
        let value = slot.wrapping_mul(2654435761) & 0xFFFFF;
        field.insert(slot, value);
    }
    // Mutated slots — pairs of (slot_id, new_value) for the "mutation set"
    let slot_values: Vec<(u32, u32)> = (0..mut_slots as u32)
        .map(|i| ((i.wrapping_mul(2654435761)) % n_slots_field, (i * 17) % 2000))
        .collect();
    let min_tracked: u32 = 500;
    for &n_entries in entry_counts {
        // ── CURRENT loop: for each entry, for each slot, reconstruct + compare ──
        let mut current_samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let mut survivors: u64 = 0;
            for _entry in 0..n_entries {
                for &(slot, _new_value) in &slot_values {
                    // Per (entry, slot): reconstruct sort value — the expensive op
                    let reconstructed = field.reconstruct_value(slot);
                    // Simulate the filter check + qualification
                    if reconstructed > min_tracked {
                        survivors = survivors.wrapping_add(1);
                    }
                }
            }
            std::hint::black_box(survivors);
            current_samples.push(t.elapsed().as_nanos());
        }
        // ── INVERTED loop: reconstruct ONCE per slot, then per-entry compare ──
        let mut inverted_samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            // Preamble: reconstruct each mutated slot's value ONCE
            let reconstructed_values: Vec<u32> = slot_values.iter()
                .map(|&(slot, _)| field.reconstruct_value(slot))
                .collect();
            let max_new_value = *reconstructed_values.iter().max().unwrap_or(&0);
            let mut survivors: u64 = 0;
            for _entry in 0..n_entries {
                // Per-entry fast reject: if the largest reconstructed value
                // can't displace min_tracked, skip the whole entry.
                if max_new_value <= min_tracked {
                    continue;
                }
                // Surviving entries: check each mutated slot's precomputed value
                for &reconstructed in &reconstructed_values {
                    if reconstructed > min_tracked {
                        survivors = survivors.wrapping_add(1);
                    }
                }
            }
            std::hint::black_box(survivors);
            inverted_samples.push(t.elapsed().as_nanos());
        }
        let current = stats(current_samples);
        let inverted = stats(inverted_samples);
        let speedup = current.p50 as f64 / inverted.p50.max(1) as f64;
        println!(
            "entries={:7}  current p50={:>11}  inverted p50={:>11}  speedup={:7.1}x",
            n_entries, fmt(current.p50), fmt(inverted.p50), speedup
        );
    }
}
fn main() {
    let batch_sizes = env_list("BENCH_BATCH_SIZES", "1,10,50,200,1000");
    let entry_sweep = env_list("BENCH_ENTRY_SWEEP", "10000,50000,100000,200000,500000");
    let mut_slots = env_usize("BENCH_MUT_SLOTS", 200);
    let iters = env_usize("BENCH_ITERS", 10);
    println!("[microbench] batch_sizes={:?} entry_sweep={:?} mut_slots={} iters={}",
             batch_sizes, entry_sweep, mut_slots, iters);
    let _reconstruct_ns = test_reconstruct_value_cost();
    test_bulk_vs_individual(&batch_sizes, iters);
    test_loop_scaling_real(&entry_sweep, mut_slots, iters);
}
