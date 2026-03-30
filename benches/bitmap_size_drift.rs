//! Experiment 7: Frozen bitmap size drift after mutations.
//!
//! Measures how frozen serialized size changes after realistic mutations
//! (set N random bits, clear M random bits) across multiple rounds.
//! This determines the buffer ratio needed for in-place updates in DataSilo<K>.
//!
//! Run: cargo bench --bench bitmap_size_drift --features data-silo

use roaring::RoaringBitmap;
use std::time::Instant;

const MAX_VALUE: u32 = 107_000_000;

fn make_bitmap(seed: u64, density: f64) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    let mut rng = seed;
    let threshold = (density * u32::MAX as f64) as u32;
    for i in 0..MAX_VALUE {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        if (rng >> 32) as u32 <= threshold {
            bm.insert(i);
        }
    }
    bm
}

fn mutate_bitmap(bm: &mut RoaringBitmap, sets: usize, clears: usize, seed: u64) {
    let mut rng = seed;
    for _ in 0..sets {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        bm.insert((rng >> 32) as u32 % MAX_VALUE);
    }
    for _ in 0..clears {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        bm.remove((rng >> 32) as u32 % MAX_VALUE);
    }
}

fn main() {
    println!("=== Experiment 7: Frozen Bitmap Size Drift After Mutations ===");
    println!("Max value: {}M", MAX_VALUE / 1_000_000);
    println!();

    // Test various bitmap sizes/densities
    let configs: Vec<(&str, f64, u64)> = vec![
        // (label, density, approx frozen size target)
        ("~1KB (very sparse)", 0.00001, 0xA001),
        ("~100KB (sparse)", 0.001, 0xA002),
        ("~1MB (medium)", 0.01, 0xA003),
        ("~10MB (dense)", 0.10, 0xA004),
        ("~13MB (very dense, like nsfwLevel)", 0.80, 0xA005),
    ];

    let mutation_rounds = [1usize, 5, 10, 50];
    let sets_per_round = 100;
    let clears_per_round = 50;

    println!("  Mutations per round: +{} bits, -{} bits", sets_per_round, clears_per_round);
    println!();

    // Header
    println!("  {:>35}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "Bitmap", "Original", "Rd 1", "Rd 5", "Rd 10", "Rd 50", "Max Drift%");
    println!("  {}", "-".repeat(100));

    for (label, density, seed) in &configs {
        print!("  Generating {}...", label);
        let t0 = Instant::now();
        let original = make_bitmap(*seed, *density);
        println!(" {:.1}s ({} bits)", t0.elapsed().as_secs_f64(), original.len());

        let orig_frozen_size = original.frozen_serialized_size();
        let mut sizes_by_round: Vec<(usize, usize)> = Vec::new();

        // Run mutation rounds
        let mut bm = original.clone();
        let mut max_drift_pct: f64 = 0.0;

        for round in 1..=*mutation_rounds.last().unwrap() {
            mutate_bitmap(&mut bm, sets_per_round, clears_per_round, *seed + round as u64 * 1000);
            let new_size = bm.frozen_serialized_size();

            if mutation_rounds.contains(&round) {
                sizes_by_round.push((round, new_size));
            }

            let drift_pct = ((new_size as f64 - orig_frozen_size as f64) / orig_frozen_size as f64).abs() * 100.0;
            if drift_pct > max_drift_pct {
                max_drift_pct = drift_pct;
            }
        }

        // Format sizes
        let fmt = |bytes: usize| -> String {
            if bytes < 1024 { format!("{} B", bytes) }
            else if bytes < 1_048_576 { format!("{:.1} KB", bytes as f64 / 1024.0) }
            else { format!("{:.2} MB", bytes as f64 / 1_048_576.0) }
        };

        let rd1 = sizes_by_round.iter().find(|(r, _)| *r == 1).map(|(_, s)| *s).unwrap_or(0);
        let rd5 = sizes_by_round.iter().find(|(r, _)| *r == 5).map(|(_, s)| *s).unwrap_or(0);
        let rd10 = sizes_by_round.iter().find(|(r, _)| *r == 10).map(|(_, s)| *s).unwrap_or(0);
        let rd50 = sizes_by_round.iter().find(|(r, _)| *r == 50).map(|(_, s)| *s).unwrap_or(0);

        println!("  {:>35}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>9.4}%",
            label,
            fmt(orig_frozen_size),
            fmt(rd1),
            fmt(rd5),
            fmt(rd10),
            fmt(rd50),
            max_drift_pct,
        );
    }

    println!();

    // Detailed analysis: size change per mutation for different container types
    println!("=== Detailed: Per-Mutation Size Change ===");
    println!();

    for (label, density, seed) in &configs {
        let original = make_bitmap(*seed, *density);
        let orig_size = original.frozen_serialized_size();

        // Single set-bit mutation
        let mut bm_set = original.clone();
        // Insert a value in a NEW container (high bits different from any existing)
        bm_set.insert(MAX_VALUE - 1);
        let set_new_container = bm_set.frozen_serialized_size() as i64 - orig_size as i64;

        // Insert a value in an EXISTING container
        let mut bm_set2 = original.clone();
        if let Some(existing) = original.iter().next() {
            let container_base = (existing >> 16) << 16;
            bm_set2.insert(container_base + 1); // Same container, different bit
        }
        let set_existing = bm_set2.frozen_serialized_size() as i64 - orig_size as i64;

        // Clear a bit
        let mut bm_clear = original.clone();
        if let Some(v) = original.iter().next() {
            bm_clear.remove(v);
        }
        let clear_existing = bm_clear.frozen_serialized_size() as i64 - orig_size as i64;

        println!("  {:>35}: set(new container)={:+} bytes, set(existing)={:+} bytes, clear={:+} bytes",
            label, set_new_container, set_existing, clear_existing);
    }

    println!();
    println!("=== Recommendations ===");
    println!("  Buffer ratio should accommodate worst-case drift over compaction interval.");
    println!("  At 1K ops threshold (~7 min), 50 rounds of +100/-50 mutations is generous.");
    println!("  Watch the max drift % — that's your minimum buffer ratio.");
}
