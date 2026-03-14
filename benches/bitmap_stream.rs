//! Bitmap stream benchmarks: validates table-at-a-time bulk loading patterns.
//!
//! Compares tag-ordered (one bitmap, many slots) vs image-ordered (one image, many bitmaps)
//! streaming patterns, both through the coalescer channel and via direct bitmap manipulation.

use std::collections::HashMap;
use std::thread;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::distributions::{Distribution, Uniform};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use roaring::RoaringBitmap;

use bitdex_v2::concurrent_engine::{ConcurrentEngine, InnerEngine};
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::filter::FilterFieldType;

const NUM_DOCS: u32 = 1_000_000;
const NUM_TAGS: usize = 1000;
const NUM_IMAGES_B2: usize = 100_000;

/// Build a Civitai-like config for benchmarking.
fn bench_config() -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "nsfwLevel".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            },
            FilterFieldConfig {
                name: "userId".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            },
            FilterFieldConfig {
                name: "type".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            },
            FilterFieldConfig {
                name: "tagIds".into(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            },
        ],
        sort_fields: vec![
            SortFieldConfig {
                name: "sortAt".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
                eager_load: false,
            },
            SortFieldConfig {
                name: "reactionCount".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
                eager_load: false,
            },
        ],
        channel_capacity: 2_000_000,
        ..Config::default()
    }
}

/// Pre-populate an InnerEngine with 1M alive docs with scalar fields set.
/// Returns the engine with filters/sorts/alive all populated.
fn prepopulate_inner(config: &Config) -> InnerEngine {
    let mut filters = bitdex_v2::filter::FilterIndex::new();
    let mut sorts = bitdex_v2::sort::SortIndex::new();
    for fc in &config.filter_fields {
        filters.add_field(fc.clone());
    }
    for sc in &config.sort_fields {
        sorts.add_field(sc.clone());
    }

    let mut rng = StdRng::seed_from_u64(42);

    // Build alive bitmap
    let mut alive = RoaringBitmap::new();
    alive.insert_range(0..NUM_DOCS);

    // Build scalar filter bitmaps
    let nsfw_levels = [1u64, 2, 4, 8, 16, 32];
    let image_types = [1u64, 2, 3]; // image, video, audio

    // Pre-build bitmaps for scalar filters
    let mut nsfw_bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();
    let mut type_bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();
    let mut user_bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();

    // Sort values: build per-bit bitmaps
    let mut sort_at_bits: Vec<RoaringBitmap> = (0..32).map(|_| RoaringBitmap::new()).collect();
    let mut reaction_bits: Vec<RoaringBitmap> = (0..32).map(|_| RoaringBitmap::new()).collect();

    for slot in 0..NUM_DOCS {
        let nsfw = nsfw_levels[rng.gen_range(0..nsfw_levels.len())];
        nsfw_bitmaps.entry(nsfw).or_default().insert(slot);

        let img_type = image_types[rng.gen_range(0..image_types.len())];
        type_bitmaps.entry(img_type).or_default().insert(slot);

        let user_id = rng.gen_range(1u64..100_000);
        user_bitmaps.entry(user_id).or_default().insert(slot);

        let sort_at: u32 = rng.gen_range(1_600_000_000u32..1_700_000_000);
        for bit in 0..32 {
            if (sort_at >> bit) & 1 == 1 {
                sort_at_bits[bit].insert(slot);
            }
        }

        let reactions: u32 = rng.gen_range(0..10_000);
        for bit in 0..32 {
            if (reactions >> bit) & 1 == 1 {
                reaction_bits[bit].insert(slot);
            }
        }
    }

    // Apply to filter index
    for (val, bm) in &nsfw_bitmaps {
        if let Some(field) = filters.get_field_mut("nsfwLevel") {
            field.or_bitmap(*val, bm);
        }
    }
    for (val, bm) in &type_bitmaps {
        if let Some(field) = filters.get_field_mut("type") {
            field.or_bitmap(*val, bm);
        }
    }
    for (val, bm) in &user_bitmaps {
        if let Some(field) = filters.get_field_mut("userId") {
            field.or_bitmap(*val, bm);
        }
    }

    // Apply sort layers
    for (bit, bm) in sort_at_bits.iter().enumerate() {
        if let Some(field) = sorts.get_field_mut("sortAt") {
            field.or_layer(bit, bm);
        }
    }
    for (bit, bm) in reaction_bits.iter().enumerate() {
        if let Some(field) = sorts.get_field_mut("reactionCount") {
            field.or_layer(bit, bm);
        }
    }

    // Build slot allocator
    let slots = bitdex_v2::slot::SlotAllocator::from_state(
        NUM_DOCS,
        alive,
        RoaringBitmap::new(),
    );

    InnerEngine { slots, filters, sorts }
}

/// Generate tag data: 1000 tags, each with a random set of slots.
/// Returns Vec<(tag_id, sorted_slots)>.
fn generate_tag_data(rng: &mut StdRng) -> Vec<(u64, Vec<u32>)> {
    let mut tags = Vec::with_capacity(NUM_TAGS);
    for tag_id in 0..NUM_TAGS as u64 {
        // Realistic distribution: mean ~1000, range 10-50000
        let size = rng.gen_range(10u32..50_000).min(NUM_DOCS);
        let slot_dist = Uniform::new(0u32, NUM_DOCS);
        let mut slots: Vec<u32> = (0..size).map(|_| slot_dist.sample(rng)).collect();
        slots.sort_unstable();
        slots.dedup();
        tags.push((tag_id, slots));
    }
    tags
}

/// Generate image data for B2: 100K images, each with scalar filter + sort field values.
/// Returns Vec<(slot, nsfw, user_id, img_type, sort_at, reactions)>.
fn generate_image_data(rng: &mut StdRng) -> Vec<(u32, u64, u64, u64, u32, u32)> {
    let nsfw_levels = [1u64, 2, 4, 8, 16, 32];
    let image_types = [1u64, 2, 3];
    (0..NUM_IMAGES_B2)
        .map(|i| {
            let slot = i as u32;
            let nsfw = nsfw_levels[rng.gen_range(0..nsfw_levels.len())];
            let user_id = rng.gen_range(1u64..100_000);
            let img_type = image_types[rng.gen_range(0..image_types.len())];
            let sort_at: u32 = rng.gen_range(1_600_000_000u32..1_700_000_000);
            let reactions: u32 = rng.gen_range(0..10_000);
            (slot, nsfw, user_id, img_type, sort_at, reactions)
        })
        .collect()
}

/// Count total tag-slot insertions for throughput measurement.
fn total_tag_insertions(tags: &[(u64, Vec<u32>)]) -> u64 {
    tags.iter().map(|(_, slots)| slots.len() as u64).sum()
}

// ==========================================================================
// B3: Tag-ordered via direct bitmap insertion (loading mode)
// ==========================================================================

fn bench_b3_tag_direct(c: &mut Criterion) {
    let config = bench_config();
    let base_inner = prepopulate_inner(&config);
    let mut rng = StdRng::seed_from_u64(123);
    let tags = generate_tag_data(&mut rng);
    let total_insertions = total_tag_insertions(&tags);

    let mut group = c.benchmark_group("b3_tag_direct_bitmap");
    group.throughput(Throughput::Elements(total_insertions));
    group.sample_size(10);

    group.bench_function("1000_tags_or_bitmap", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut inner = base_inner.clone();

                let start = Instant::now();
                for (tag_id, slots) in &tags {
                    // Build a RoaringBitmap from the sorted slots
                    let bm = RoaringBitmap::from_sorted_iter(slots.iter().copied()).unwrap();
                    if let Some(field) = inner.filters.get_field_mut("tagIds") {
                        field.or_bitmap(*tag_id, &bm);
                    }
                }
                total += start.elapsed();
            }
            total
        });
    });

    // Also benchmark creating the bitmaps from slots (the full pipeline)
    group.bench_function("1000_tags_from_sorted_iter", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut inner = base_inner.clone();

                let start = Instant::now();
                for (tag_id, slots) in &tags {
                    // This is what the real pipeline does: collect sorted slot IDs
                    // from the Postgres query and bulk-OR into the bitmap
                    let bm = RoaringBitmap::from_sorted_iter(slots.iter().copied()).unwrap();
                    if let Some(field) = inner.filters.get_field_mut("tagIds") {
                        field.or_bitmap(*tag_id, &bm);
                    }
                }
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ==========================================================================
// B2: Image-ordered stream (current pattern) — direct bitmap manipulation
// ==========================================================================

fn bench_b2_image_ordered_direct(c: &mut Criterion) {
    let config = bench_config();
    let base_inner = prepopulate_inner(&config);
    let mut rng = StdRng::seed_from_u64(456);
    let images = generate_image_data(&mut rng);

    // Count total bitmap ops: per image = 3 filter inserts + 2 sort fields * ~16 avg set bits
    // Roughly: 100K * (3 + 2*16) = 3.5M ops
    let total_ops = images.len() as u64;

    let mut group = c.benchmark_group("b2_image_ordered_direct");
    group.throughput(Throughput::Elements(total_ops));
    group.sample_size(10);

    group.bench_function("100k_images_per_slot", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut inner = base_inner.clone();

                let start = Instant::now();
                for &(slot, nsfw, user_id, img_type, sort_at, reactions) in &images {
                    // Filter inserts — one at a time per image (current pattern)
                    if let Some(field) = inner.filters.get_field_mut("nsfwLevel") {
                        field.insert(nsfw, slot);
                    }
                    if let Some(field) = inner.filters.get_field_mut("userId") {
                        field.insert(user_id, slot);
                    }
                    if let Some(field) = inner.filters.get_field_mut("type") {
                        field.insert(img_type, slot);
                    }

                    // Sort field decomposition — set each bit layer
                    for bit in 0..32u32 {
                        if (sort_at >> bit) & 1 == 1 {
                            if let Some(field) = inner.sorts.get_field_mut("sortAt") {
                                field.set_layer_bulk(bit as usize, std::iter::once(slot));
                            }
                        }
                        if (reactions >> bit) & 1 == 1 {
                            if let Some(field) = inner.sorts.get_field_mut("reactionCount") {
                                field.set_layer_bulk(bit as usize, std::iter::once(slot));
                            }
                        }
                    }

                    // Mark alive
                    inner.slots.alive_or_bitmap(&{
                        let mut bm = RoaringBitmap::new();
                        bm.insert(slot);
                        bm
                    });
                }
                total += start.elapsed();
            }
            total
        });
    });

    // Compare: image-ordered but batched (collect per-value, then OR)
    group.bench_function("100k_images_batched", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut inner = base_inner.clone();

                let start = Instant::now();

                // Phase 1: Decompose into per-bitmap accumulators
                let mut filter_maps: HashMap<(&str, u64), RoaringBitmap> = HashMap::new();
                let mut sort_maps: HashMap<(&str, usize), RoaringBitmap> = HashMap::new();
                let mut alive = RoaringBitmap::new();

                for &(slot, nsfw, user_id, img_type, sort_at, reactions) in &images {
                    filter_maps.entry(("nsfwLevel", nsfw)).or_default().insert(slot);
                    filter_maps.entry(("userId", user_id)).or_default().insert(slot);
                    filter_maps.entry(("type", img_type)).or_default().insert(slot);

                    for bit in 0..32u32 {
                        if (sort_at >> bit) & 1 == 1 {
                            sort_maps.entry(("sortAt", bit as usize)).or_default().insert(slot);
                        }
                        if (reactions >> bit) & 1 == 1 {
                            sort_maps.entry(("reactionCount", bit as usize)).or_default().insert(slot);
                        }
                    }

                    alive.insert(slot);
                }

                // Phase 2: Apply bulk
                for ((field_name, value), bm) in &filter_maps {
                    if let Some(field) = inner.filters.get_field_mut(field_name) {
                        field.or_bitmap(*value, bm);
                    }
                }
                for ((field_name, bit), bm) in &sort_maps {
                    if let Some(field) = inner.sorts.get_field_mut(field_name) {
                        field.or_layer(*bit, bm);
                    }
                }
                inner.slots.alive_or_bitmap(&alive);

                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ==========================================================================
// B3b: Tag-ordered with parallel decomposition (simulates multi-threaded PG stream)
// ==========================================================================

fn bench_b3_tag_parallel(c: &mut Criterion) {
    let config = bench_config();
    let base_inner = prepopulate_inner(&config);
    let mut rng = StdRng::seed_from_u64(123);
    let tags = generate_tag_data(&mut rng);
    let total_insertions = total_tag_insertions(&tags);

    let mut group = c.benchmark_group("b3_tag_parallel");
    group.throughput(Throughput::Elements(total_insertions));
    group.sample_size(10);

    for num_threads in [1, 2, 4] {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &nt| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let mut inner = base_inner.clone();
                        let chunk_size = (tags.len() + nt - 1) / nt;

                        let start = Instant::now();

                        // Phase 1: Build bitmaps in parallel threads
                        let thread_bitmaps: Vec<Vec<(u64, RoaringBitmap)>> =
                            thread::scope(|s| {
                                let handles: Vec<_> = (0..nt)
                                    .map(|t| {
                                        let tag_start = t * chunk_size;
                                        let tag_end = (tag_start + chunk_size).min(tags.len());
                                        let tag_slice = &tags[tag_start..tag_end];
                                        s.spawn(move || {
                                            tag_slice
                                                .iter()
                                                .map(|(tag_id, slots)| {
                                                    let bm = RoaringBitmap::from_sorted_iter(
                                                        slots.iter().copied(),
                                                    )
                                                    .unwrap();
                                                    (*tag_id, bm)
                                                })
                                                .collect::<Vec<_>>()
                                        })
                                    })
                                    .collect();
                                handles.into_iter().map(|h| h.join().unwrap()).collect()
                            });

                        // Phase 2: Apply sequentially (single writer)
                        for chunk in &thread_bitmaps {
                            for (tag_id, bm) in chunk {
                                if let Some(field) = inner.filters.get_field_mut("tagIds") {
                                    field.or_bitmap(*tag_id, bm);
                                }
                            }
                        }

                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

// ==========================================================================
// B4: Concurrent streams (tags + scalars simultaneously)
// ==========================================================================

fn bench_b4_concurrent_streams(c: &mut Criterion) {
    let config = bench_config();
    let base_inner = prepopulate_inner(&config);
    let mut rng = StdRng::seed_from_u64(789);
    let tags = generate_tag_data(&mut rng);
    let images = generate_image_data(&mut rng);
    let total_insertions = total_tag_insertions(&tags) + images.len() as u64;

    let mut group = c.benchmark_group("b4_concurrent_streams");
    group.throughput(Throughput::Elements(total_insertions));
    group.sample_size(10);

    // Sequential baseline: tags then images
    group.bench_function("sequential", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut inner = base_inner.clone();

                let start = Instant::now();

                // Tags
                for (tag_id, slots) in &tags {
                    let bm = RoaringBitmap::from_sorted_iter(slots.iter().copied()).unwrap();
                    if let Some(field) = inner.filters.get_field_mut("tagIds") {
                        field.or_bitmap(*tag_id, &bm);
                    }
                }

                // Images (batched)
                let mut filter_maps: HashMap<(&str, u64), RoaringBitmap> = HashMap::new();
                let mut sort_maps: HashMap<(&str, usize), RoaringBitmap> = HashMap::new();
                let mut alive = RoaringBitmap::new();

                for &(slot, nsfw, user_id, img_type, sort_at, reactions) in &images {
                    filter_maps.entry(("nsfwLevel", nsfw)).or_default().insert(slot);
                    filter_maps.entry(("userId", user_id)).or_default().insert(slot);
                    filter_maps.entry(("type", img_type)).or_default().insert(slot);

                    for bit in 0..32u32 {
                        if (sort_at >> bit) & 1 == 1 {
                            sort_maps.entry(("sortAt", bit as usize)).or_default().insert(slot);
                        }
                        if (reactions >> bit) & 1 == 1 {
                            sort_maps.entry(("reactionCount", bit as usize)).or_default().insert(slot);
                        }
                    }
                    alive.insert(slot);
                }

                for ((field_name, value), bm) in &filter_maps {
                    if let Some(field) = inner.filters.get_field_mut(field_name) {
                        field.or_bitmap(*value, bm);
                    }
                }
                for ((field_name, bit), bm) in &sort_maps {
                    if let Some(field) = inner.sorts.get_field_mut(field_name) {
                        field.or_layer(*bit, bm);
                    }
                }
                inner.slots.alive_or_bitmap(&alive);

                total += start.elapsed();
            }
            total
        });
    });

    // Concurrent: tags + image decomposition in parallel, then sequential apply
    group.bench_function("concurrent_decompose", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut inner = base_inner.clone();

                let start = Instant::now();

                // Decompose in parallel
                let (tag_bitmaps, image_results) = thread::scope(|s| {
                    let tag_handle = s.spawn(|| {
                        tags.iter()
                            .map(|(tag_id, slots)| {
                                let bm =
                                    RoaringBitmap::from_sorted_iter(slots.iter().copied()).unwrap();
                                (*tag_id, bm)
                            })
                            .collect::<Vec<_>>()
                    });

                    let image_handle = s.spawn(|| {
                        let mut filter_maps: HashMap<(&str, u64), RoaringBitmap> = HashMap::new();
                        let mut sort_maps: HashMap<(&str, usize), RoaringBitmap> = HashMap::new();
                        let mut alive = RoaringBitmap::new();

                        for &(slot, nsfw, user_id, img_type, sort_at, reactions) in &images {
                            filter_maps.entry(("nsfwLevel", nsfw)).or_default().insert(slot);
                            filter_maps.entry(("userId", user_id)).or_default().insert(slot);
                            filter_maps.entry(("type", img_type)).or_default().insert(slot);

                            for bit in 0..32u32 {
                                if (sort_at >> bit) & 1 == 1 {
                                    sort_maps
                                        .entry(("sortAt", bit as usize))
                                        .or_default()
                                        .insert(slot);
                                }
                                if (reactions >> bit) & 1 == 1 {
                                    sort_maps
                                        .entry(("reactionCount", bit as usize))
                                        .or_default()
                                        .insert(slot);
                                }
                            }
                            alive.insert(slot);
                        }

                        (filter_maps, sort_maps, alive)
                    });

                    (tag_handle.join().unwrap(), image_handle.join().unwrap())
                });

                // Apply sequentially (single writer owns the InnerEngine)
                for (tag_id, bm) in &tag_bitmaps {
                    if let Some(field) = inner.filters.get_field_mut("tagIds") {
                        field.or_bitmap(*tag_id, bm);
                    }
                }

                let (filter_maps, sort_maps, alive) = &image_results;
                for ((field_name, value), bm) in filter_maps {
                    if let Some(field) = inner.filters.get_field_mut(field_name) {
                        field.or_bitmap(*value, bm);
                    }
                }
                for ((field_name, bit), bm) in sort_maps {
                    if let Some(field) = inner.sorts.get_field_mut(field_name) {
                        field.or_layer(*bit, bm);
                    }
                }
                inner.slots.alive_or_bitmap(alive);

                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ==========================================================================
// Scaling: measure tag insertion rate at different scales
// ==========================================================================

fn bench_tag_insertion_rate(c: &mut Criterion) {
    let config = bench_config();

    let mut group = c.benchmark_group("tag_insertion_rate");
    group.sample_size(10);

    // Test with varying tag counts to see how HashMap growth affects perf
    for num_tags in [100, 1000, 5000, 10000, 31000] {
        let mut rng = StdRng::seed_from_u64(42);
        let mut tags = Vec::with_capacity(num_tags);
        for tag_id in 0..num_tags as u64 {
            let size = rng.gen_range(10u32..50_000).min(NUM_DOCS);
            let slot_dist = Uniform::new(0u32, NUM_DOCS);
            let mut slots: Vec<u32> = (0..size).map(|_| slot_dist.sample(&mut rng)).collect();
            slots.sort_unstable();
            slots.dedup();
            tags.push((tag_id, slots));
        }
        let total_insertions: u64 = tags.iter().map(|(_, s)| s.len() as u64).sum();

        group.throughput(Throughput::Elements(total_insertions));
        group.bench_with_input(
            BenchmarkId::new("tags", num_tags),
            &tags,
            |b, tags| {
                // Build a fresh base with no tags
                let base_inner = prepopulate_inner(&config);

                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let mut inner = base_inner.clone();

                        let start = Instant::now();
                        for (tag_id, slots) in tags {
                            let bm =
                                RoaringBitmap::from_sorted_iter(slots.iter().copied()).unwrap();
                            if let Some(field) = inner.filters.get_field_mut("tagIds") {
                                field.or_bitmap(*tag_id, &bm);
                            }
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

// ==========================================================================
// ConcurrentEngine channel benchmark (B1 full path)
// ==========================================================================

fn bench_b1_full_concurrent_engine(c: &mut Criterion) {
    let config = bench_config();
    let mut rng = StdRng::seed_from_u64(123);
    let tags = generate_tag_data(&mut rng);
    let total_insertions = total_tag_insertions(&tags);

    // For B1 through ConcurrentEngine, we use put_bulk with pre-built Documents
    // that have tagIds set. This exercises the real pipeline.
    let mut group = c.benchmark_group("b1_full_put_bulk");
    group.throughput(Throughput::Elements(total_insertions));
    group.sample_size(10);

    group.bench_function("put_bulk_loading", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let engine = ConcurrentEngine::new(config.clone()).unwrap();
                let mut staging = engine.clone_staging();

                // Pre-build alive bitmap for all 1M slots
                let mut alive = RoaringBitmap::new();
                alive.insert_range(0..NUM_DOCS);
                staging.slots.alive_or_bitmap(&alive);

                let start = Instant::now();
                // Tag-ordered: OR each tag's bitmap directly
                for (tag_id, slots) in &tags {
                    let bm = RoaringBitmap::from_sorted_iter(slots.iter().copied()).unwrap();
                    if let Some(field) = staging.filters.get_field_mut("tagIds") {
                        field.or_bitmap(*tag_id, &bm);
                    }
                }

                engine.publish_staging(staging);
                total += start.elapsed();

                drop(engine);
            }
            total
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_b3_tag_direct,
    bench_b2_image_ordered_direct,
    bench_b3_tag_parallel,
    bench_b4_concurrent_streams,
    bench_tag_insertion_rate,
    bench_b1_full_concurrent_engine,
);
criterion_main!(benches);
