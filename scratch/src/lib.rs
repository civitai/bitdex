// Scratch crate for throwaway microbenchmarks and experiments.
//
// Agents: put your hypothesis-testing benchmarks here as #[test] functions
// or as [[bin]] entries. They won't be compiled during `cargo test` on the
// main crate — only when you explicitly run `cargo test -p scratch`.
//
// When you're done with an experiment, delete the file. This crate is disposable.

#[cfg(test)]
mod bench_filter_pipeline {
    use roaring::RoaringBitmap;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Instant;

    // ---- File format parsers (fpack/sort file format) ----

    const BITMAP_ROOT: &str = "C:/Dev/Repos/open-source/bitdex-v2/data/indexes/civitai/bitmaps";

    fn filter_path(field: &str) -> std::path::PathBuf {
        Path::new(BITMAP_ROOT).join("filter").join(field)
    }

    fn sort_path(field: &str) -> std::path::PathBuf {
        Path::new(BITMAP_ROOT).join("sort").join(format!("{field}.sort"))
    }

    fn alive_path() -> std::path::PathBuf {
        Path::new(BITMAP_ROOT).join("system").join("alive.roar")
    }

    fn load_roar(path: &Path) -> RoaringBitmap {
        let data = std::fs::read(path).expect("failed to read .roar file");
        RoaringBitmap::deserialize_from(data.as_slice())
            .expect("alive bitmap deserialize failed")
    }

    fn load_fpack(path: &Path) -> Vec<(u64, RoaringBitmap)> {
        let data = std::fs::read(path).expect("failed to read .fpack file");
        assert!(data.len() >= 4, "fpack header truncated");
        let num_entries = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header_size = 4 + num_entries * 16;
        assert!(data.len() >= header_size, "fpack index truncated");
        let data_start = header_size;
        let mut result = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let idx = 4 + i * 16;
            let value = u64::from_le_bytes([
                data[idx], data[idx+1], data[idx+2], data[idx+3],
                data[idx+4], data[idx+5], data[idx+6], data[idx+7],
            ]);
            let offset = u32::from_le_bytes([
                data[idx+8], data[idx+9], data[idx+10], data[idx+11],
            ]) as usize;
            let length = u32::from_le_bytes([
                data[idx+12], data[idx+13], data[idx+14], data[idx+15],
            ]) as usize;
            let start = data_start + offset;
            let end = start + length;
            assert!(end <= data.len(), "fpack bitmap data truncated");
            let bm = RoaringBitmap::deserialize_from(&data[start..end])
                .expect("fpack bitmap deserialize failed");
            result.push((value, bm));
        }
        result
    }

    /// Load all fpack files for a field directory
    fn load_field(field: &str) -> HashMap<u64, RoaringBitmap> {
        let dir = filter_path(field);
        let mut result = HashMap::new();
        if !dir.exists() {
            return result;
        }
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.extension().map_or(true, |ext| ext != "fpack") {
                continue;
            }
            for (value, bm) in load_fpack(&path) {
                result.insert(value, bm);
            }
        }
        result
    }

    fn load_sort_layers(path: &Path, num_layers: usize) -> Vec<RoaringBitmap> {
        let data = std::fs::read(path).expect("failed to read .sort file");
        assert!(!data.is_empty(), "empty .sort file");
        let stored_layers = data[0] as usize;
        let header_size = 1 + stored_layers * 9;
        assert!(data.len() >= header_size, "sort file header truncated");
        let data_start = header_size;
        let mut layers = vec![RoaringBitmap::new(); num_layers];
        for i in 0..stored_layers {
            let idx = 1 + i * 9;
            let bit_pos = data[idx] as usize;
            let off = u32::from_le_bytes([
                data[idx + 1], data[idx + 2], data[idx + 3], data[idx + 4],
            ]) as usize;
            let length = u32::from_le_bytes([
                data[idx + 5], data[idx + 6], data[idx + 7], data[idx + 8],
            ]) as usize;
            if bit_pos < num_layers {
                let start = data_start + off;
                let end = start + length;
                assert!(end <= data.len(), "sort layer data truncated");
                layers[bit_pos] = RoaringBitmap::deserialize_from(&data[start..end])
                    .expect("sort layer deserialize failed");
            }
        }
        layers
    }

    // ---- Sort operations (mirror sort.rs) ----

    fn reconstruct_value(layers: &[RoaringBitmap], slot: u32) -> u32 {
        let mut value = 0u32;
        for (bit, layer) in layers.iter().enumerate() {
            if layer.contains(slot) {
                value |= 1 << bit;
            }
        }
        value
    }

    fn bifurcate(
        layers: &[RoaringBitmap],
        candidates: &RoaringBitmap,
        limit: usize,
        descending: bool,
    ) -> RoaringBitmap {
        let total = candidates.len() as usize;
        if total <= limit {
            return candidates.clone();
        }
        let num_bits = layers.len();
        let mut result = RoaringBitmap::new();
        let mut remaining = candidates.clone();
        let mut remaining_limit = limit;
        for bit in (0..num_bits).rev() {
            if remaining_limit == 0 || remaining.is_empty() {
                break;
            }
            let layer = &layers[bit];
            let preferred = if descending {
                &remaining & layer
            } else {
                &remaining - layer
            };
            let preferred_count = preferred.len() as usize;
            if preferred_count == 0 {
                continue;
            } else if preferred_count >= remaining_limit {
                remaining = preferred;
            } else {
                result |= &preferred;
                remaining -= &preferred;
                remaining_limit -= preferred_count;
            }
        }
        if remaining_limit > 0 && !remaining.is_empty() {
            let mut taken = 0;
            for slot in remaining.iter() {
                if taken >= remaining_limit {
                    break;
                }
                result.insert(slot);
                taken += 1;
            }
        }
        result
    }

    fn top_n(
        layers: &[RoaringBitmap],
        candidates: &RoaringBitmap,
        limit: usize,
        descending: bool,
    ) -> Vec<u32> {
        if candidates.is_empty() || limit == 0 {
            return Vec::new();
        }
        let top_n_bitmap = bifurcate(layers, candidates, limit, descending);
        let mut entries: Vec<(u32, u32)> = top_n_bitmap
            .iter()
            .map(|slot| (slot, reconstruct_value(layers, slot)))
            .collect();
        if descending {
            entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        } else {
            entries.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        }
        entries.into_iter().map(|(slot, _)| slot).collect()
    }

    // ---- Bench harness ----

    const WARMUP: usize = 3;
    const ITERS: usize = 5;

    fn bench<T, F: FnMut() -> T>(warmup: usize, iters: usize, mut f: F) -> (f64, Vec<f64>, T) {
        let mut last = None;
        for _ in 0..warmup {
            last = Some(std::hint::black_box(f()));
        }
        let mut timings = Vec::with_capacity(iters);
        for _ in 0..iters {
            let start = Instant::now();
            last = Some(f());
            let elapsed = start.elapsed();
            std::hint::black_box(last.as_ref().unwrap());
            timings.push(elapsed.as_secs_f64() * 1000.0);
        }
        timings.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = timings[timings.len() / 2];
        (median, timings, last.unwrap())
    }

    fn fmt_timings(t: &[f64]) -> String {
        t.iter().map(|v| format!("{:.2}", v)).collect::<Vec<_>>().join(", ")
    }

    fn bitmap_size_mb(bm: &RoaringBitmap) -> f64 {
        bm.serialized_size() as f64 / 1048576.0
    }

    // ---- The benchmark test ----

    #[test]
    fn bench_filter_pipeline_profiling() {
        println!("\n=== Filter Pipeline Profiling (Real Data, 105M records) ===\n");

        // ---- Load data from disk ----
        println!("--- Loading bitmap data from disk ---\n");

        let t = Instant::now();
        let alive = load_roar(&alive_path());
        println!("Alive bitmap: {} slots, {:.2} MB ({:.0}ms)",
            alive.len(), bitmap_size_mb(&alive), t.elapsed().as_secs_f64() * 1000.0);

        // Load filter fields
        let t = Instant::now();
        let nsfw_bitmaps = load_field("nsfwLevel");
        println!("nsfwLevel: {} values ({:.0}ms)", nsfw_bitmaps.len(), t.elapsed().as_secs_f64() * 1000.0);
        for (&v, bm) in &nsfw_bitmaps {
            println!("  val={}: {} slots, {:.2} MB", v, bm.len(), bitmap_size_mb(bm));
        }

        let t = Instant::now();
        let published_bitmaps = load_field("isPublished");
        println!("isPublished: {} values ({:.0}ms)", published_bitmaps.len(), t.elapsed().as_secs_f64() * 1000.0);
        for (&v, bm) in &published_bitmaps {
            println!("  val={}: {} slots, {:.2} MB", v, bm.len(), bitmap_size_mb(bm));
        }

        let t = Instant::now();
        let type_bitmaps = load_field("type");
        println!("type: {} values ({:.0}ms)", type_bitmaps.len(), t.elapsed().as_secs_f64() * 1000.0);
        for (&v, bm) in &type_bitmaps {
            println!("  val={}: {} slots, {:.2} MB", v, bm.len(), bitmap_size_mb(bm));
        }

        let t = Instant::now();
        let onsite_bitmaps = load_field("onSite");
        println!("onSite: {} values ({:.0}ms)", onsite_bitmaps.len(), t.elapsed().as_secs_f64() * 1000.0);
        for (&v, bm) in &onsite_bitmaps {
            println!("  val={}: {} slots, {:.2} MB", v, bm.len(), bitmap_size_mb(bm));
        }

        let t = Instant::now();
        let hasmeta_bitmaps = load_field("hasMeta");
        println!("hasMeta: {} values ({:.0}ms)", hasmeta_bitmaps.len(), t.elapsed().as_secs_f64() * 1000.0);
        for (&v, bm) in &hasmeta_bitmaps {
            println!("  val={}: {} slots, {:.2} MB", v, bm.len(), bitmap_size_mb(bm));
        }

        // Load sort field
        let sort_path = sort_path("reactionCount");
        let t = Instant::now();
        let sort_layers = load_sort_layers(&sort_path, 32);
        let sort_load_ms = t.elapsed().as_secs_f64() * 1000.0;
        let sort_bytes: usize = sort_layers.iter().map(|l| l.serialized_size()).sum();
        println!("\nreactionCount sort: 32 layers, {:.2} MB ({:.0}ms)",
            sort_bytes as f64 / 1048576.0, sort_load_ms);

        // ======================================================================
        // Bench 1: Individual fused() cost
        // ======================================================================
        println!("\n=== Bench 1: fused() cost per field ===\n");
        println!("  This simulates what evaluate_clause does: vb.fused() clones the bitmap.\n");

        // Wrap in Arc to simulate VersionedBitmap behavior (clean diff = base.clone())
        let test_cases: Vec<(&str, u64, &RoaringBitmap)> = vec![
            // Pick the largest nsfwLevel value (usually val=1)
            ("nsfwLevel", 1, nsfw_bitmaps.get(&1).unwrap_or_else(|| nsfw_bitmaps.values().next().unwrap())),
            ("isPublished", 1, published_bitmaps.get(&1).unwrap_or_else(|| published_bitmaps.values().next().unwrap())),
            ("type", *type_bitmaps.keys().next().unwrap(), type_bitmaps.values().next().unwrap()),
        ];

        for &(field, val, bm) in &test_cases {
            // Test 1a: Clone the bitmap (what fused() does when diff is empty)
            let (median, times, _) = bench(WARMUP, ITERS, || {
                let cloned: RoaringBitmap = bm.clone();
                cloned
            });
            println!("  {} val={}: clone={:.3}ms, bitmap_size={:.2} MB, {} slots [{}]",
                field, val, median, bitmap_size_mb(bm), bm.len(), fmt_timings(&times));

            // Test 1b: Arc::clone (what we'd get if we could avoid fused())
            let arc_bm = Arc::new(bm.clone());
            let (median_arc, times_arc, _) = bench(WARMUP, ITERS, || {
                Arc::clone(&arc_bm)
            });
            println!("  {} val={}: Arc::clone={:.6}ms [{}]",
                field, val, median_arc, fmt_timings(&times_arc));
        }

        // ======================================================================
        // Bench 2: compute_filters simulation — step by step
        // ======================================================================
        println!("\n=== Bench 2: compute_filters step-by-step ===\n");
        println!("  Simulating: In(nsfwLevel,[1,2]) AND Eq(isPublished,true) AND In(type,[X]) AND Eq(onSite,true) AND Eq(hasMeta,true)\n");

        // Get the bitmaps we'll use
        let nsfw_1 = nsfw_bitmaps.get(&1).expect("nsfwLevel=1");
        let nsfw_2 = nsfw_bitmaps.get(&2).unwrap_or(nsfw_1);
        let published_true = published_bitmaps.get(&1).expect("isPublished=1");
        let type_val = type_bitmaps.values().max_by_key(|bm| bm.len()).unwrap();
        let type_key = *type_bitmaps.iter().max_by_key(|(_, bm)| bm.len()).unwrap().0;
        let onsite_true = onsite_bitmaps.get(&1).unwrap_or_else(|| onsite_bitmaps.values().next().unwrap());
        let hasmeta_true = hasmeta_bitmaps.get(&1).unwrap_or_else(|| hasmeta_bitmaps.values().next().unwrap());

        // Step 1: In(nsfwLevel, [1, 2]) — two fused() + OR
        let (step1_ms, step1_times, step1_result) = bench(WARMUP, ITERS, || {
            let bm1 = nsfw_1.clone(); // fused()
            let bm2 = nsfw_2.clone(); // fused()
            bm1 | bm2
        });
        println!("  Step 1: In(nsfwLevel,[1,2]) — 2x clone + OR = {:.3}ms (result: {}M bits) [{}]",
            step1_ms, step1_result.len() / 1_000_000, fmt_timings(&step1_times));

        // Step 1 broken down: individual clones vs OR
        let (clone1_ms, _, _) = bench(WARMUP, ITERS, || { nsfw_1.clone() });
        let (clone2_ms, _, _) = bench(WARMUP, ITERS, || { nsfw_2.clone() });
        let nsfw_1_cloned = nsfw_1.clone();
        let nsfw_2_cloned = nsfw_2.clone();
        let (or_ms, _, _) = bench(WARMUP, ITERS, || { &nsfw_1_cloned | &nsfw_2_cloned });
        println!("    breakdown: clone1={:.3}ms, clone2={:.3}ms, OR={:.3}ms", clone1_ms, clone2_ms, or_ms);

        // Step 2: AND isPublished=true
        let step1_clone = step1_result.clone();
        let (step2_ms, step2_times, step2_result) = bench(WARMUP, ITERS, || {
            let published = published_true.clone(); // fused()
            &step1_clone & &published
        });
        println!("  Step 2: AND Eq(isPublished,true) — clone + AND = {:.3}ms (result: {}M bits) [{}]",
            step2_ms, step2_result.len() / 1_000_000, fmt_timings(&step2_times));

        // Step 2 breakdown
        let (pub_clone_ms, _, _) = bench(WARMUP, ITERS, || { published_true.clone() });
        let pub_cloned = published_true.clone();
        let (pub_and_ms, _, _) = bench(WARMUP, ITERS, || { &step1_clone & &pub_cloned });
        println!("    breakdown: clone={:.3}ms, AND={:.3}ms", pub_clone_ms, pub_and_ms);

        // Step 3: AND type
        let step2_clone = step2_result.clone();
        let (step3_ms, step3_times, step3_result) = bench(WARMUP, ITERS, || {
            let t = type_val.clone(); // fused()
            &step2_clone & &t
        });
        println!("  Step 3: AND In(type,[{}]) — clone + AND = {:.3}ms (result: {}M bits) [{}]",
            type_key, step3_ms, step3_result.len() / 1_000_000, fmt_timings(&step3_times));

        // Step 4: AND onSite=true
        let step3_clone = step3_result.clone();
        let (step4_ms, step4_times, step4_result) = bench(WARMUP, ITERS, || {
            let os = onsite_true.clone(); // fused()
            &step3_clone & &os
        });
        println!("  Step 4: AND Eq(onSite,true) — clone + AND = {:.3}ms (result: {}M bits) [{}]",
            step4_ms, step4_result.len() / 1_000_000, fmt_timings(&step4_times));

        // Step 5: AND hasMeta=true
        let step4_clone = step4_result.clone();
        let (step5_ms, step5_times, step5_result) = bench(WARMUP, ITERS, || {
            let hm = hasmeta_true.clone(); // fused()
            &step4_clone & &hm
        });
        println!("  Step 5: AND Eq(hasMeta,true) — clone + AND = {:.3}ms (result: {}M bits) [{}]",
            step5_ms, step5_result.len() / 1_000_000, fmt_timings(&step5_times));

        // Full filter pipeline
        let (full_filter_ms, full_filter_times, full_filter_result) = bench(WARMUP, ITERS, || {
            // Simulate exactly what compute_filters + evaluate_clause does
            let bm1 = nsfw_1.clone();
            let bm2 = nsfw_2.clone();
            let in_result = bm1 | bm2;

            let published = published_true.clone();
            let result = &in_result & &published;

            let t = type_val.clone();
            let result = &result & &t;

            let os = onsite_true.clone();
            let result = &result & &os;

            let hm = hasmeta_true.clone();
            let result = &result & &hm;

            result
        });
        println!("\n  Full 5-clause filter: {:.3}ms (result: {}M bits) [{}]",
            full_filter_ms, full_filter_result.len() / 1_000_000, fmt_timings(&full_filter_times));
        println!("  Sum of steps: {:.3}ms", step1_ms + step2_ms + step3_ms + step4_ms + step5_ms);

        // Alternative: avoid clone, use references (AND only, no fused())
        let (noref_ms, noref_times, noref_result) = bench(WARMUP, ITERS, || {
            let in_result = nsfw_1 | nsfw_2;  // OR requires owned or refs

            let result = &in_result & published_true;
            let result = &result & type_val;
            let result = &result & onsite_true;
            let result = &result & hasmeta_true;
            result
        });
        println!("  No-clone path (ref AND): {:.3}ms (result: {}M bits) [{}]",
            noref_ms, noref_result.len() / 1_000_000, fmt_timings(&noref_times));
        println!("  Speedup from avoiding clones: {:.1}x",
            full_filter_ms / noref_ms);

        // ======================================================================
        // Bench 3: Full pipeline (filter + sort)
        // ======================================================================
        println!("\n=== Bench 3: Full pipeline (filter + sort) ===\n");

        let filter_result = full_filter_result.clone();
        println!("  Filter result: {} slots ({:.1}M)", filter_result.len(), filter_result.len() as f64 / 1_000_000.0);

        // Sort only
        let filter_clone = filter_result.clone();
        let (sort_only_ms, sort_only_times, _sort_result) = bench(WARMUP, ITERS, || {
            top_n(&sort_layers, &filter_clone, 4000, true)
        });
        println!("  Sort only (top_n 4000 desc): {:.1}ms [{}]",
            sort_only_ms, fmt_timings(&sort_only_times));

        // Filter + sort combined
        let (combined_ms, combined_times, _) = bench(WARMUP, ITERS, || {
            // Filter
            let bm1 = nsfw_1.clone();
            let bm2 = nsfw_2.clone();
            let in_result = bm1 | bm2;
            let result = &in_result & published_true;
            let result = &result & type_val;
            let result = &result & onsite_true;
            let result = &result & hasmeta_true;
            // Sort
            top_n(&sort_layers, &result, 4000, true)
        });
        println!("  Filter + sort combined: {:.1}ms [{}]",
            combined_ms, fmt_timings(&combined_times));
        println!("  Expected (filter + sort): {:.1}ms",
            full_filter_ms + sort_only_ms);
        println!("  Overhead: {:.1}ms ({:.1}x)",
            combined_ms - full_filter_ms - sort_only_ms,
            combined_ms / (full_filter_ms + sort_only_ms));

        // ======================================================================
        // Bench 4: Candidate set size vs sort time
        // ======================================================================
        println!("\n=== Bench 4: Candidate set size vs sort time ===\n");

        // Create candidate sets of different sizes by intersecting with different filters
        let candidate_sets: Vec<(&str, RoaringBitmap)> = {
            let mut sets = Vec::new();

            // ~1M: intersect many filters
            let small = {
                let bm1 = nsfw_1.clone();
                let r = &bm1 & published_true;
                let r = &r & type_val;
                let r = &r & onsite_true;
                let r = &r & hasmeta_true;
                // further shrink if needed
                let mut shrunk = RoaringBitmap::new();
                let target = 1_000_000usize;
                let mut count = 0;
                for slot in r.iter() {
                    if count >= target { break; }
                    shrunk.insert(slot);
                    count += 1;
                }
                shrunk
            };
            sets.push(("~1M (truncated)", small));

            // ~5M
            let med = {
                let mut shrunk = RoaringBitmap::new();
                let target = 5_000_000usize;
                let mut count = 0;
                for slot in alive.iter() {
                    if count >= target { break; }
                    shrunk.insert(slot);
                    count += 1;
                }
                shrunk
            };
            sets.push(("~5M (truncated alive)", med));

            // ~10M
            let med2 = {
                let mut shrunk = RoaringBitmap::new();
                let target = 10_000_000usize;
                let mut count = 0;
                for slot in alive.iter() {
                    if count >= target { break; }
                    shrunk.insert(slot);
                    count += 1;
                }
                shrunk
            };
            sets.push(("~10M (truncated alive)", med2));

            // ~21M: nsfwLevel=1
            sets.push(("~21M (nsfwLevel=1)", nsfw_1.clone()));

            // ~26M: nsfwLevel IN [1,2]
            let nsfw_12 = nsfw_1 | nsfw_2;
            sets.push(("~26M (nsfw IN [1,2])", nsfw_12));

            // Full alive (~105M)
            sets.push(("~105M (alive)", alive.clone()));

            sets
        };

        for (label, candidates) in &candidate_sets {
            let (ms, times, _) = bench(WARMUP, ITERS, || {
                top_n(&sort_layers, candidates, 4000, true)
            });
            println!("  {} ({} slots): {:.1}ms [{}]",
                label, candidates.len(), ms, fmt_timings(&times));
        }

        // ======================================================================
        // Bench 5: Bifurcation vs full top_n breakdown
        // ======================================================================
        println!("\n=== Bench 5: Bifurcation vs reconstruct+sort breakdown ===\n");

        let big_candidates = nsfw_1.clone();
        let (bif_ms, bif_times, bif_result) = bench(WARMUP, ITERS, || {
            bifurcate(&sort_layers, &big_candidates, 4000, true)
        });
        println!("  Bifurcate only (21M -> 4000): {:.1}ms [{}]",
            bif_ms, fmt_timings(&bif_times));

        let bif_clone = bif_result.clone();
        let (recon_ms, recon_times, _) = bench(WARMUP, ITERS, || {
            let mut entries: Vec<(u32, u32)> = bif_clone
                .iter()
                .map(|slot| (slot, reconstruct_value(&sort_layers, slot)))
                .collect();
            entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
            entries.into_iter().map(|(slot, _)| slot).collect::<Vec<u32>>()
        });
        println!("  Reconstruct+sort (4000 slots): {:.3}ms [{}]",
            recon_ms, fmt_timings(&recon_times));

        // ======================================================================
        // Bench 6: Clone cost at scale — how much does 105M bitmap clone cost?
        // ======================================================================
        println!("\n=== Bench 6: Clone cost at different bitmap sizes ===\n");

        let sizes: Vec<(&str, &RoaringBitmap)> = vec![
            ("alive (~105M)", &alive),
            ("nsfwLevel=1 (~21M)", nsfw_1),
            ("isPublished=1", published_true),
            ("type (largest)", type_val),
            ("onSite=1", onsite_true),
            ("hasMeta=1", hasmeta_true),
        ];

        for &(label, bm) in &sizes {
            let (ms, times, _) = bench(WARMUP, ITERS, || {
                let cloned: RoaringBitmap = bm.clone();
                cloned
            });
            println!("  {} — {} slots, {:.2} MB — clone: {:.3}ms [{}]",
                label, bm.len(), bitmap_size_mb(bm), ms, fmt_timings(&times));
        }

        println!("\n=== Done ===\n");
    }
}

#[cfg(test)]
mod bench_sort_opt {
    use roaring::RoaringBitmap;
    use std::path::Path;
    use std::time::Instant;

    // ---- File format parsers (fpack/sort file format, standalone) ----

    /// Load sort layers from a .sort file.
    /// Format: [u8 num_layers][N x (u8 bit_pos, u32 offset, u32 len)][packed roaring data]
    fn load_sort_layers(path: &Path, num_layers: usize) -> Vec<RoaringBitmap> {
        let data = std::fs::read(path).expect("failed to read .sort file");
        assert!(!data.is_empty(), "empty .sort file");

        let stored_layers = data[0] as usize;
        let header_size = 1 + stored_layers * 9;
        assert!(data.len() >= header_size, "sort file header truncated");

        let data_start = header_size;
        let mut layers = vec![RoaringBitmap::new(); num_layers];

        for i in 0..stored_layers {
            let idx = 1 + i * 9;
            let bit_pos = data[idx] as usize;
            let offset = u32::from_le_bytes([
                data[idx + 1], data[idx + 2], data[idx + 3], data[idx + 4],
            ]) as usize;
            let length = u32::from_le_bytes([
                data[idx + 5], data[idx + 6], data[idx + 7], data[idx + 8],
            ]) as usize;

            if bit_pos < num_layers {
                let start = data_start + offset;
                let end = start + length;
                assert!(end <= data.len(), "sort layer data truncated");
                layers[bit_pos] = RoaringBitmap::deserialize_from(&data[start..end])
                    .expect("sort layer deserialize failed");
            }
        }

        layers
    }

    /// Load filter bitmap values from an .fpack file.
    /// Format: [u32 num_entries][N x (u64 value, u32 offset, u32 len)][packed roaring data]
    fn load_fpack(path: &Path) -> Vec<(u64, RoaringBitmap)> {
        let data = std::fs::read(path).expect("failed to read .fpack file");
        assert!(data.len() >= 4, "fpack header truncated");

        let num_entries = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header_size = 4 + num_entries * 16;
        assert!(data.len() >= header_size, "fpack index truncated");

        let data_start = header_size;
        let mut result = Vec::with_capacity(num_entries);

        for i in 0..num_entries {
            let idx = 4 + i * 16;
            let value = u64::from_le_bytes([
                data[idx], data[idx+1], data[idx+2], data[idx+3],
                data[idx+4], data[idx+5], data[idx+6], data[idx+7],
            ]);
            let offset = u32::from_le_bytes([
                data[idx+8], data[idx+9], data[idx+10], data[idx+11],
            ]) as usize;
            let length = u32::from_le_bytes([
                data[idx+12], data[idx+13], data[idx+14], data[idx+15],
            ]) as usize;

            let start = data_start + offset;
            let end = start + length;
            assert!(end <= data.len(), "fpack bitmap data truncated");

            let bm = RoaringBitmap::deserialize_from(&data[start..end])
                .expect("fpack bitmap deserialize failed");
            result.push((value, bm));
        }

        result
    }

    /// Load alive bitmap from .roar file.
    fn load_roar(path: &Path) -> RoaringBitmap {
        let data = std::fs::read(path).expect("failed to read .roar file");
        RoaringBitmap::deserialize_from(data.as_slice())
            .expect("alive bitmap deserialize failed")
    }

    // ---- Sort operations (mirrors sort.rs) ----

    /// Reconstruct the u32 value for a slot from bit layers.
    fn reconstruct_value(layers: &[RoaringBitmap], slot: u32) -> u32 {
        let mut value = 0u32;
        for (bit, layer) in layers.iter().enumerate() {
            if layer.contains(slot) {
                value |= 1 << bit;
            }
        }
        value
    }

    /// MSB-to-LSB bifurcation — the current algorithm.
    fn bifurcate(
        layers: &[RoaringBitmap],
        candidates: &RoaringBitmap,
        limit: usize,
        descending: bool,
    ) -> RoaringBitmap {
        let total = candidates.len() as usize;
        if total <= limit {
            return candidates.clone();
        }

        let num_bits = layers.len();
        let mut result = RoaringBitmap::new();
        let mut remaining = candidates.clone();
        let mut remaining_limit = limit;

        for bit in (0..num_bits).rev() {
            if remaining_limit == 0 || remaining.is_empty() {
                break;
            }

            let layer = &layers[bit];

            let preferred = if descending {
                &remaining & layer
            } else {
                &remaining - layer
            };

            let preferred_count = preferred.len() as usize;

            if preferred_count == 0 {
                continue;
            } else if preferred_count >= remaining_limit {
                remaining = preferred;
            } else {
                result |= &preferred;
                remaining -= &preferred;
                remaining_limit -= preferred_count;
            }
        }

        if remaining_limit > 0 && !remaining.is_empty() {
            let mut taken = 0;
            for slot in remaining.iter() {
                if taken >= remaining_limit {
                    break;
                }
                result.insert(slot);
                taken += 1;
            }
        }

        result
    }

    /// Full top_n: bifurcate then reconstruct+sort the result.
    fn top_n(
        layers: &[RoaringBitmap],
        candidates: &RoaringBitmap,
        limit: usize,
        descending: bool,
    ) -> Vec<u32> {
        if candidates.is_empty() || limit == 0 {
            return Vec::new();
        }

        let top_n_bitmap = bifurcate(layers, candidates, limit, descending);

        // Reconstruct values only for the result set
        let mut entries: Vec<(u32, u32)> = top_n_bitmap
            .iter()
            .map(|slot| (slot, reconstruct_value(layers, slot)))
            .collect();

        if descending {
            entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        } else {
            entries.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        }

        entries.into_iter().map(|(slot, _)| slot).collect()
    }

    // ---- Benchmark harness ----

    struct BenchResult {
        median_ms: f64,
        all_ms: Vec<f64>,
    }

    fn bench_fn<F: FnMut() -> Vec<u32>>(
        warmup: usize,
        iterations: usize,
        mut f: F,
    ) -> BenchResult {
        // Warmup
        for _ in 0..warmup {
            let _ = std::hint::black_box(f());
        }

        // Measure
        let mut timings = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            let result = f();
            let elapsed = start.elapsed();
            std::hint::black_box(&result);
            timings.push(elapsed.as_secs_f64() * 1000.0);
        }

        timings.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median_ms = timings[timings.len() / 2];

        BenchResult {
            median_ms,
            all_ms: timings,
        }
    }

    // ---- Build optimization structures ----

    /// Build a bitmap of all slots where value > 0 (any bit layer set).
    fn build_non_zero_bitmap(layers: &[RoaringBitmap]) -> RoaringBitmap {
        let mut non_zero = RoaringBitmap::new();
        for layer in layers {
            non_zero |= layer;
        }
        non_zero
    }

    /// Build 256 radix buckets by top 8 bits (value >> 24).
    /// Returns (buckets, distribution counts for reporting).
    fn build_radix_buckets(
        layers: &[RoaringBitmap],
        universe: &RoaringBitmap,
    ) -> (Vec<RoaringBitmap>, Vec<(u8, u64)>) {
        // For each of the top 8 bits (bits 24-31), we have a layer bitmap.
        // We can compute the bucket for each slot by checking bits 24-31.
        // But iterating 105M slots is slow. Instead, use bitmap set operations.
        //
        // bucket = (value >> 24) & 0xFF = bits 24..31
        // We build each bucket by AND/ANDNOT combinations of bits 24-31.
        // With 8 bits that's 256 combinations — too many AND chains.
        //
        // Faster approach: iterate only the non-zero slots (which for sparse
        // fields is tiny), reconstruct just the top byte, and insert.
        // For dense fields, we have to iterate everything.

        let mut buckets: Vec<RoaringBitmap> = (0..256).map(|_| RoaringBitmap::new()).collect();

        // Build a non-zero bitmap for the top 8 bits only (bits 24-31)
        let mut has_top_bits = RoaringBitmap::new();
        for bit in 24..layers.len().min(32) {
            has_top_bits |= &layers[bit];
        }
        // Intersect with universe
        let top_slots = &has_top_bits & universe;

        // Slots with no top-8 bits set go to bucket 0
        let zero_top = universe - &has_top_bits;
        buckets[0] = zero_top;

        // For slots with any top bit set, reconstruct the bucket index
        for slot in top_slots.iter() {
            let mut bucket: u8 = 0;
            for bit in 24..layers.len().min(32) {
                if layers[bit].contains(slot) {
                    bucket |= 1 << (bit - 24);
                }
            }
            buckets[bucket as usize].insert(slot);
        }

        // Collect distribution
        let mut dist: Vec<(u8, u64)> = buckets
            .iter()
            .enumerate()
            .filter(|(_, b)| !b.is_empty())
            .map(|(i, b)| (i as u8, b.len()))
            .collect();
        dist.sort_by(|a, b| b.0.cmp(&a.0)); // highest bucket first

        (buckets, dist)
    }

    /// Radix bucket top-N: scan from highest bucket down, bifurcate within winning bucket.
    fn radix_top_n(
        layers: &[RoaringBitmap],
        candidates: &RoaringBitmap,
        buckets: &[RoaringBitmap],
        limit: usize,
        descending: bool,
    ) -> Vec<u32> {
        if !descending {
            // For ascending, scan from bucket 0 up
            return radix_top_n_asc(layers, candidates, buckets, limit);
        }

        // Descending: scan buckets 255→0
        let mut collected = RoaringBitmap::new();
        let mut remaining_limit = limit;

        for bucket_idx in (0..256).rev() {
            if remaining_limit == 0 {
                break;
            }

            let bucket_candidates = &buckets[bucket_idx] & candidates;
            let count = bucket_candidates.len() as usize;

            if count == 0 {
                continue;
            }

            if count <= remaining_limit {
                // All slots in this bucket are winners
                collected |= &bucket_candidates;
                remaining_limit -= count;
            } else {
                // This bucket has more than we need — bifurcate within it
                // Use only bits 0-23 (the lower bits within this bucket)
                let partial = bifurcate(layers, &bucket_candidates, remaining_limit, true);
                collected |= &partial;
                remaining_limit = 0;
            }
        }

        // Reconstruct values and sort
        let mut entries: Vec<(u32, u32)> = collected
            .iter()
            .map(|slot| (slot, reconstruct_value(layers, slot)))
            .collect();
        entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        entries.into_iter().map(|(slot, _)| slot).collect()
    }

    fn radix_top_n_asc(
        layers: &[RoaringBitmap],
        candidates: &RoaringBitmap,
        buckets: &[RoaringBitmap],
        limit: usize,
    ) -> Vec<u32> {
        let mut collected = RoaringBitmap::new();
        let mut remaining_limit = limit;

        for bucket_idx in 0..256 {
            if remaining_limit == 0 {
                break;
            }

            let bucket_candidates = &buckets[bucket_idx] & candidates;
            let count = bucket_candidates.len() as usize;

            if count == 0 {
                continue;
            }

            if count <= remaining_limit {
                collected |= &bucket_candidates;
                remaining_limit -= count;
            } else {
                let partial = bifurcate(layers, &bucket_candidates, remaining_limit, false);
                collected |= &partial;
                remaining_limit = 0;
            }
        }

        let mut entries: Vec<(u32, u32)> = collected
            .iter()
            .map(|slot| (slot, reconstruct_value(layers, slot)))
            .collect();
        entries.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        entries.into_iter().map(|(slot, _)| slot).collect()
    }

    // ---- Data paths ----

    const BITMAP_ROOT: &str = "C:/Dev/Repos/open-source/bitdex-v2/data/indexes/civitai/bitmaps";

    fn sort_path(field: &str) -> std::path::PathBuf {
        Path::new(BITMAP_ROOT).join("sort").join(format!("{field}.sort"))
    }

    fn alive_path() -> std::path::PathBuf {
        Path::new(BITMAP_ROOT).join("system").join("alive.roar")
    }

    fn nsfw_fpack_path() -> std::path::PathBuf {
        Path::new(BITMAP_ROOT).join("filter").join("nsfwLevel").join("00.fpack")
    }

    // ---- The benchmark test ----

    #[test]
    fn bench_sort_optimizations() {
        const LIMIT: usize = 4000;
        const WARMUP: usize = 3;
        const ITERS: usize = 7;

        println!("\n=== Loading bitmap data from disk ===\n");

        // Load alive bitmap
        let t = Instant::now();
        let alive = load_roar(&alive_path());
        println!("Alive bitmap: {} slots ({:.1}ms)", alive.len(), t.elapsed().as_secs_f64() * 1000.0);

        // Load nsfwLevel=1 as candidate set (~21M)
        let t = Instant::now();
        let nsfw_entries = load_fpack(&nsfw_fpack_path());
        let mut candidates = RoaringBitmap::new();
        for (value, bm) in &nsfw_entries {
            if *value == 1 {
                candidates = bm.clone();
                break;
            }
        }
        if candidates.is_empty() {
            // Fallback: use first available nsfwLevel
            println!("nsfwLevel=1 not found, available values:");
            for (v, bm) in &nsfw_entries {
                println!("  nsfwLevel={}: {} slots", v, bm.len());
            }
            candidates = nsfw_entries[0].1.clone();
        }
        println!("Candidates (nsfwLevel=1): {} slots ({:.1}ms)", candidates.len(), t.elapsed().as_secs_f64() * 1000.0);

        // Load sort fields
        let fields: Vec<(&str, &str)> = vec![
            ("reactionCount", "sparse, ~90% zeros"),
            ("sortAt", "dense"),
            ("commentCount", "very sparse"),
        ];

        println!("\n=== Building optimization structures ===\n");

        for &(field_name, description) in &fields {
            let path = sort_path(field_name);
            if !path.exists() {
                println!("SKIP {field_name}: file not found at {}", path.display());
                continue;
            }

            let t = Instant::now();
            let layers = load_sort_layers(&path, 32);
            let load_ms = t.elapsed().as_secs_f64() * 1000.0;
            let total_bytes: usize = layers.iter().map(|l| l.serialized_size()).sum();
            println!("{field_name}: loaded 32 layers, {:.1} MB in memory ({:.0}ms)",
                total_bytes as f64 / 1048576.0, load_ms);

            // Build non-zero bitmap
            let t = Instant::now();
            let non_zero = build_non_zero_bitmap(&layers);
            let nz_build_ms = t.elapsed().as_secs_f64() * 1000.0;
            let nz_in_candidates = (&non_zero & &candidates).len();
            let zero_pct = 100.0 * (1.0 - non_zero.len() as f64 / alive.len() as f64);
            println!("  Non-zero bitmap: {} slots ({:.1}% zeros globally), {} in candidates ({:.0}ms build)",
                non_zero.len(), zero_pct, nz_in_candidates, nz_build_ms);

            // Build radix buckets
            let t = Instant::now();
            let (buckets, dist) = build_radix_buckets(&layers, &alive);
            let bucket_build_ms = t.elapsed().as_secs_f64() * 1000.0;
            println!("  Radix buckets: {} non-empty buckets ({:.0}ms build)", dist.len(), bucket_build_ms);
            // Show top 5 buckets
            let show = dist.len().min(5);
            for i in 0..show {
                let (idx, count) = dist[i];
                println!("    bucket {}: {} slots", idx, count);
            }

            println!();

            // --- Bench A: Baseline bifurcation ---
            let baseline = bench_fn(WARMUP, ITERS, || {
                top_n(&layers, &candidates, LIMIT, true)
            });
            println!("  {field_name} ({description}):");
            println!("    Baseline bifurcate:     {:.1}ms ({}M candidates -> {} results) [all: {}]",
                baseline.median_ms,
                candidates.len() / 1_000_000,
                LIMIT,
                fmt_timings(&baseline.all_ms));

            // --- Bench B: Non-zero optimization ---
            let nz_bm = non_zero.clone();
            let nonzero_result = bench_fn(WARMUP, ITERS, || {
                let nz_candidates = &candidates & &nz_bm;
                if (nz_candidates.len() as usize) >= LIMIT {
                    top_n(&layers, &nz_candidates, LIMIT, true)
                } else {
                    // Fallback to full candidates if non-zero set is too small
                    top_n(&layers, &candidates, LIMIT, true)
                }
            });
            println!("    Non-zero pre-filter:    {:.1}ms ({:.1}M nz candidates) [all: {}]",
                nonzero_result.median_ms,
                nz_in_candidates as f64 / 1_000_000.0,
                fmt_timings(&nonzero_result.all_ms));

            // --- Bench C: Radix buckets ---
            let buckets_ref = &buckets;
            let radix_result = bench_fn(WARMUP, ITERS, || {
                radix_top_n(&layers, &candidates, buckets_ref, LIMIT, true)
            });
            println!("    Radix buckets only:     {:.1}ms [all: {}]",
                radix_result.median_ms,
                fmt_timings(&radix_result.all_ms));

            // --- Bench D: Combined non-zero + radix ---
            let nz_bm2 = non_zero.clone();
            let combined_result = bench_fn(WARMUP, ITERS, || {
                let nz_candidates = &candidates & &nz_bm2;
                if (nz_candidates.len() as usize) >= LIMIT {
                    radix_top_n(&layers, &nz_candidates, buckets_ref, LIMIT, true)
                } else {
                    top_n(&layers, &candidates, LIMIT, true)
                }
            });
            println!("    Non-zero + radix:       {:.1}ms [all: {}]",
                combined_result.median_ms,
                fmt_timings(&combined_result.all_ms));

            // Speedup summary
            let nz_speedup = baseline.median_ms / nonzero_result.median_ms;
            let radix_speedup = baseline.median_ms / radix_result.median_ms;
            let combined_speedup = baseline.median_ms / combined_result.median_ms;
            println!("    Speedups: non-zero={:.1}x, radix={:.1}x, combined={:.1}x",
                nz_speedup, radix_speedup, combined_speedup);

            // Verify correctness: all approaches should return the same top results
            let baseline_result = top_n(&layers, &candidates, LIMIT, true);
            let nz_candidates_check = &candidates & &non_zero;
            let nz_result_check = if (nz_candidates_check.len() as usize) >= LIMIT {
                top_n(&layers, &nz_candidates_check, LIMIT, true)
            } else {
                top_n(&layers, &candidates, LIMIT, true)
            };
            let radix_result_check = radix_top_n(&layers, &candidates, &buckets, LIMIT, true);

            // Top values should match (slot ordering may differ for ties)
            let baseline_top_val = reconstruct_value(&layers, baseline_result[0]);
            let nz_top_val = reconstruct_value(&layers, nz_result_check[0]);
            let radix_top_val = reconstruct_value(&layers, radix_result_check[0]);
            println!("    Correctness: baseline_top={}, nz_top={}, radix_top={} {}",
                baseline_top_val, nz_top_val, radix_top_val,
                if baseline_top_val == nz_top_val && baseline_top_val == radix_top_val {
                    "OK"
                } else {
                    "MISMATCH"
                });

            // Also check last value
            let baseline_last_val = reconstruct_value(&layers, baseline_result[LIMIT - 1]);
            let nz_last_val = reconstruct_value(&layers, nz_result_check[LIMIT - 1]);
            let radix_last_val = reconstruct_value(&layers, radix_result_check[LIMIT - 1]);
            println!("    Correctness: baseline_4000th={}, nz_4000th={}, radix_4000th={}",
                baseline_last_val, nz_last_val, radix_last_val);

            println!();
        }

        println!("=== Done ===");
    }

    fn fmt_timings(timings: &[f64]) -> String {
        timings
            .iter()
            .map(|t| format!("{:.1}", t))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

// ============================================================================
// Lazy bitmap loading benchmarks
// ============================================================================
#[cfg(test)]
mod bench_lazy_load {
    use memmap2::Mmap;
    use rayon::prelude::*;
    use roaring::RoaringBitmap;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    const BITMAP_ROOT: &str = "C:/Dev/Repos/open-source/bitdex-v2/data/indexes/civitai/bitmaps";

    fn filter_dir(field: &str) -> PathBuf {
        Path::new(BITMAP_ROOT).join("filter").join(field)
    }

    fn list_fpack_files(field: &str) -> Vec<PathBuf> {
        let dir = filter_dir(field);
        let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .expect("read dir")
            .filter_map(|e| {
                let p = e.ok()?.path();
                if p.extension().map_or(true, |ext| ext != "fpack") {
                    None
                } else {
                    Some(p)
                }
            })
            .collect();
        files.sort();
        files
    }

    // ---------- Baseline: sequential load (fpack parsing) ----------

    fn parse_fpack(data: &[u8]) -> Vec<(u64, RoaringBitmap)> {
        let num_entries = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header_size = 4 + num_entries * 16;
        let data_start = header_size;
        let mut result = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let idx = 4 + i * 16;
            let value = u64::from_le_bytes(data[idx..idx + 8].try_into().unwrap());
            let offset = u32::from_le_bytes(data[idx + 8..idx + 12].try_into().unwrap()) as usize;
            let length = u32::from_le_bytes(data[idx + 12..idx + 16].try_into().unwrap()) as usize;
            let start = data_start + offset;
            let end = start + length;
            let bm = RoaringBitmap::deserialize_from(&data[start..end])
                .expect("deserialize failed");
            result.push((value, bm));
        }
        result
    }

    /// Baseline: sequential fs::read + sequential deserialize
    fn load_field_baseline(files: &[PathBuf]) -> HashMap<u64, RoaringBitmap> {
        let mut result = HashMap::new();
        for path in files {
            let data = std::fs::read(path).expect("read");
            for (value, bm) in parse_fpack(&data) {
                result.insert(value, bm);
            }
        }
        result
    }

    /// Opt A: parallel file reading + deserialization (rayon par_iter over files)
    fn load_field_parallel_files(files: &[PathBuf]) -> HashMap<u64, RoaringBitmap> {
        let chunks: Vec<Vec<(u64, RoaringBitmap)>> = files
            .par_iter()
            .map(|path| {
                let data = std::fs::read(path).expect("read");
                parse_fpack(&data)
            })
            .collect();
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        let mut result = HashMap::with_capacity(total);
        for chunk in chunks {
            for (value, bm) in chunk {
                result.insert(value, bm);
            }
        }
        result
    }

    /// Opt B: parallel files + parallel deserialization within each file
    fn parse_fpack_parallel(data: &[u8]) -> Vec<(u64, RoaringBitmap)> {
        let num_entries = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header_size = 4 + num_entries * 16;
        let data_start = header_size;

        // Parse headers first (cheap)
        let headers: Vec<(u64, usize, usize)> = (0..num_entries)
            .map(|i| {
                let idx = 4 + i * 16;
                let value = u64::from_le_bytes(data[idx..idx + 8].try_into().unwrap());
                let offset =
                    u32::from_le_bytes(data[idx + 8..idx + 12].try_into().unwrap()) as usize;
                let length =
                    u32::from_le_bytes(data[idx + 12..idx + 16].try_into().unwrap()) as usize;
                (value, data_start + offset, data_start + offset + length)
            })
            .collect();

        // Parallel deserialize
        headers
            .par_iter()
            .map(|&(value, start, end)| {
                let bm = RoaringBitmap::deserialize_from(&data[start..end])
                    .expect("deserialize failed");
                (value, bm)
            })
            .collect()
    }

    fn load_field_parallel_all(files: &[PathBuf]) -> HashMap<u64, RoaringBitmap> {
        let chunks: Vec<Vec<(u64, RoaringBitmap)>> = files
            .par_iter()
            .map(|path| {
                let data = std::fs::read(path).expect("read");
                parse_fpack_parallel(&data)
            })
            .collect();
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        let mut result = HashMap::with_capacity(total);
        for chunk in chunks {
            for (value, bm) in chunk {
                result.insert(value, bm);
            }
        }
        result
    }

    /// Opt C: mmap + parallel files
    fn load_field_mmap_parallel(files: &[PathBuf]) -> HashMap<u64, RoaringBitmap> {
        let chunks: Vec<Vec<(u64, RoaringBitmap)>> = files
            .par_iter()
            .map(|path| {
                let file = std::fs::File::open(path).expect("open");
                let mmap = unsafe { Mmap::map(&file).expect("mmap") };
                parse_fpack(&mmap)
            })
            .collect();
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        let mut result = HashMap::with_capacity(total);
        for chunk in chunks {
            for (value, bm) in chunk {
                result.insert(value, bm);
            }
        }
        result
    }

    /// Opt D: mmap + parallel files + parallel deserialization
    fn load_field_mmap_parallel_all(files: &[PathBuf]) -> HashMap<u64, RoaringBitmap> {
        let chunks: Vec<Vec<(u64, RoaringBitmap)>> = files
            .par_iter()
            .map(|path| {
                let file = std::fs::File::open(path).expect("open");
                let mmap = unsafe { Mmap::map(&file).expect("mmap") };
                parse_fpack_parallel(&mmap)
            })
            .collect();
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        let mut result = HashMap::with_capacity(total);
        for chunk in chunks {
            for (value, bm) in chunk {
                result.insert(value, bm);
            }
        }
        result
    }

    /// Opt E: parallel files with fold+reduce to build HashMap in parallel
    fn load_field_parallel_fold(files: &[PathBuf]) -> HashMap<u64, RoaringBitmap> {
        files
            .par_iter()
            .flat_map_iter(|path| {
                let data = std::fs::read(path).expect("read");
                parse_fpack(&data).into_iter()
            })
            .fold(HashMap::new, |mut map, (value, bm)| {
                map.insert(value, bm);
                map
            })
            .reduce(HashMap::new, |mut a, b| {
                a.extend(b);
                a
            })
    }

    fn bench_fn<F: FnMut() -> HashMap<u64, RoaringBitmap>>(
        name: &str,
        iters: usize,
        mut f: F,
    ) -> f64 {
        // Warmup (1 iter)
        let warmup = f();
        let count = warmup.len();
        drop(warmup);

        let mut timings = Vec::with_capacity(iters);
        for _ in 0..iters {
            let start = Instant::now();
            let result = f();
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(result.len(), count, "value count mismatch");
            std::hint::black_box(&result);
            drop(result);
            timings.push(elapsed);
        }
        timings.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = timings[timings.len() / 2];
        let min = timings[0];
        let max = timings[timings.len() - 1];
        println!(
            "  {:<35} median={:>8.1}ms  min={:>8.1}ms  max={:>8.1}ms  ({} values)  [{}]",
            name,
            median,
            min,
            max,
            count,
            timings
                .iter()
                .map(|t| format!("{:.1}", t))
                .collect::<Vec<_>>()
                .join(", ")
        );
        median
    }

    // ---- Breakdown benchmark: I/O vs deserialization ----

    fn bench_io_vs_deser(field: &str, files: &[PathBuf]) {
        println!("\n  --- I/O vs Deserialization breakdown for {} ---", field);

        // Measure pure I/O (read all files, don't deserialize)
        let start = Instant::now();
        let mut total_bytes = 0usize;
        let mut total_entries = 0usize;
        let file_data: Vec<Vec<u8>> = files
            .iter()
            .map(|p| {
                let d = std::fs::read(p).expect("read");
                total_bytes += d.len();
                let n = u32::from_le_bytes([d[0], d[1], d[2], d[3]]) as usize;
                total_entries += n;
                d
            })
            .collect();
        let io_ms = start.elapsed().as_secs_f64() * 1000.0;
        println!(
            "    I/O only (sequential): {:.1}ms for {} files, {:.1} MB, {} entries",
            io_ms,
            files.len(),
            total_bytes as f64 / 1048576.0,
            total_entries
        );

        // Measure pure deserialization (data already in memory)
        let start = Instant::now();
        let mut deser_count = 0usize;
        for data in &file_data {
            let entries = parse_fpack(data);
            deser_count += entries.len();
            std::hint::black_box(&entries);
        }
        let deser_ms = start.elapsed().as_secs_f64() * 1000.0;
        println!(
            "    Deserialization only (sequential): {:.1}ms for {} bitmaps",
            deser_ms, deser_count
        );

        // Parallel I/O
        let start = Instant::now();
        let _: Vec<Vec<u8>> = files
            .par_iter()
            .map(|p| std::fs::read(p).expect("read"))
            .collect();
        let par_io_ms = start.elapsed().as_secs_f64() * 1000.0;
        println!("    I/O only (parallel): {:.1}ms", par_io_ms);

        // Parallel deserialization (data already in memory)
        let start = Instant::now();
        let _: Vec<Vec<(u64, RoaringBitmap)>> =
            file_data.par_iter().map(|d| parse_fpack(d)).collect();
        let par_deser_ms = start.elapsed().as_secs_f64() * 1000.0;
        println!(
            "    Deserialization only (parallel): {:.1}ms",
            par_deser_ms
        );

        // HashMap insertion cost
        let parsed: Vec<Vec<(u64, RoaringBitmap)>> =
            file_data.iter().map(|d| parse_fpack(d)).collect();
        let total: usize = parsed.iter().map(|c| c.len()).sum();
        let start = Instant::now();
        let mut map = HashMap::with_capacity(total);
        for chunk in parsed {
            for (v, bm) in chunk {
                map.insert(v, bm);
            }
        }
        let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
        println!(
            "    HashMap insert only: {:.1}ms for {} entries",
            insert_ms, total
        );
        drop(map);

        println!(
            "    Summary: I/O={:.0}% deser={:.0}% insert={:.0}%",
            io_ms / (io_ms + deser_ms + insert_ms) * 100.0,
            deser_ms / (io_ms + deser_ms + insert_ms) * 100.0,
            insert_ms / (io_ms + deser_ms + insert_ms) * 100.0,
        );
    }

    #[test]
    fn bench_lazy_load_userId() {
        println!(
            "\n=== Lazy Load Benchmark: userId (749K values, 256 fpack files, ~289 MB) ===\n"
        );

        let files = list_fpack_files("userId");
        println!("  Files: {}", files.len());

        // Breakdown first
        bench_io_vs_deser("userId", &files);

        println!("\n  --- Full load strategies ---\n");
        let iters = 3;

        let baseline = bench_fn("baseline (sequential)", iters, || {
            load_field_baseline(&files)
        });
        let par_files = bench_fn("parallel files", iters, || {
            load_field_parallel_files(&files)
        });
        let par_all = bench_fn("parallel files+deser", iters, || {
            load_field_parallel_all(&files)
        });
        let mmap_par = bench_fn("mmap + parallel files", iters, || {
            load_field_mmap_parallel(&files)
        });
        let mmap_par_all = bench_fn("mmap + parallel files+deser", iters, || {
            load_field_mmap_parallel_all(&files)
        });
        let par_fold = bench_fn("parallel fold", iters, || {
            load_field_parallel_fold(&files)
        });

        println!(
            "\n  --- Speedups vs baseline ({:.1}ms) ---\n",
            baseline
        );
        println!(
            "    parallel files:          {:.2}x",
            baseline / par_files
        );
        println!(
            "    parallel files+deser:    {:.2}x",
            baseline / par_all
        );
        println!(
            "    mmap + parallel:         {:.2}x",
            baseline / mmap_par
        );
        println!(
            "    mmap + parallel+deser:   {:.2}x",
            baseline / mmap_par_all
        );
        println!(
            "    parallel fold:           {:.2}x",
            baseline / par_fold
        );

        println!("\n=== Done ===\n");
    }

    #[test]
    fn bench_lazy_load_postedToId() {
        println!(
            "\n=== Lazy Load Benchmark: postedToId (928K values, ~87 MB) ===\n"
        );
        let files = list_fpack_files("postedToId");
        println!("  Files: {}", files.len());
        bench_io_vs_deser("postedToId", &files);

        println!("\n  --- Full load strategies ---\n");
        let iters = 3;

        let baseline = bench_fn("baseline (sequential)", iters, || {
            load_field_baseline(&files)
        });
        let par_files = bench_fn("parallel files", iters, || {
            load_field_parallel_files(&files)
        });
        let mmap_par = bench_fn("mmap + parallel files", iters, || {
            load_field_mmap_parallel(&files)
        });

        println!(
            "\n  --- Speedups vs baseline ({:.1}ms) ---\n",
            baseline
        );
        println!(
            "    parallel files:   {:.2}x",
            baseline / par_files
        );
        println!(
            "    mmap + parallel:  {:.2}x",
            baseline / mmap_par
        );
    }

    #[test]
    fn bench_lazy_load_nsfwLevel() {
        println!(
            "\n=== Lazy Load Benchmark: nsfwLevel (7 values, ~72 MB) ===\n"
        );
        let files = list_fpack_files("nsfwLevel");
        println!("  Files: {}", files.len());
        bench_io_vs_deser("nsfwLevel", &files);

        println!("\n  --- Full load strategies ---\n");
        let iters = 5;

        let baseline = bench_fn("baseline (sequential)", iters, || {
            load_field_baseline(&files)
        });
        let par_files = bench_fn("parallel files", iters, || {
            load_field_parallel_files(&files)
        });
        let mmap_par = bench_fn("mmap + parallel files", iters, || {
            load_field_mmap_parallel(&files)
        });

        println!(
            "\n  --- Speedups vs baseline ({:.1}ms) ---\n",
            baseline
        );
        println!(
            "    parallel files:   {:.2}x",
            baseline / par_files
        );
        println!(
            "    mmap + parallel:  {:.2}x",
            baseline / mmap_par
        );
    }
}
