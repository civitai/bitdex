//! Microbenchmarks for docstore → bitmap rebuild pipeline.
//!
//! Measures individual stages of the rebuild pipeline to identify bottlenecks:
//! 1. Raw shard I/O: read + zstd decompress
//! 2. Decode: msgpack → StoredDoc
//! 3. Bitmap extraction: StoredDoc → filter/sort bitmaps
//! 4. Full pipeline: read → decode → extract → merge
//!
//! Usage:
//!   cargo run --release --bin rebuild_bench -- --data-dir ./data --index civitai [--shards 1000]

use ahash::AHashMap as HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rayon::prelude::*;
use roaring::RoaringBitmap;

use bitdex_v2::shard_store_doc::{DocStoreV3, PackedValue, StoredDoc};
use bitdex_v2::mutation::{value_to_bitmap_key, value_to_sort_u32};

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

struct BenchConfig {
    data_dir: PathBuf,
    index_name: String,
    max_shards: Option<u32>,
    full_build: bool,
    add_field: Option<String>,
}

fn parse_args() -> BenchConfig {
    let args: Vec<String> = std::env::args().collect();
    let mut data_dir = PathBuf::from("./data");
    let mut index_name = "civitai".to_string();
    let mut max_shards: Option<u32> = None;
    let mut full_build = false;
    let mut add_field: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => { data_dir = PathBuf::from(&args[i + 1]); i += 2; }
            "--index" => { index_name = args[i + 1].clone(); i += 2; }
            "--shards" => { max_shards = Some(args[i + 1].parse().unwrap()); i += 2; }
            "--full" => { full_build = true; i += 1; }
            "--add-field" => { add_field = Some(args[i + 1].clone()); i += 2; }
            _ => { i += 1; }
        }
    }

    BenchConfig { data_dir, index_name, max_shards, full_build, add_field }
}

/// Count total shards by scanning the shard directory.
#[allow(dead_code)]
fn count_shards(docs_path: &Path) -> u32 {
    let shards_dir = docs_path.join("shards");
    let mut count = 0u32;
    if let Ok(entries) = std::fs::read_dir(&shards_dir) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if let Ok(sub_entries) = std::fs::read_dir(entry.path()) {
                    count += sub_entries
                        .filter(|e| e.as_ref().map(|e| {
                            e.path().extension().map(|ext| ext == "bin").unwrap_or(false)
                        }).unwrap_or(false))
                        .count() as u32;
                }
            }
        }
    }
    count
}

/// Find the maximum shard ID by scanning shard files.
fn find_max_shard(docs_path: &Path) -> u32 {
    let shards_dir = docs_path.join("shards");
    let mut max_id = 0u32;
    if let Ok(entries) = std::fs::read_dir(&shards_dir) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if let Ok(sub_entries) = std::fs::read_dir(entry.path()) {
                    for sub in sub_entries.flatten() {
                        if let Some(stem) = sub.path().file_stem() {
                            if let Ok(id) = stem.to_string_lossy().parse::<u32>() {
                                max_id = max_id.max(id);
                            }
                        }
                    }
                }
            }
        }
    }
    max_id
}

/// Stage 1: Raw shard I/O — read files + zstd decompress, no decode.
fn bench_raw_io(docs_path: &Path, num_shards: u32) -> (f64, u64, u64) {
    eprintln!("\n=== Stage 1: Raw shard I/O (read + zstd decompress) ===");
    let bytes_read = AtomicU64::new(0);
    let bytes_decompressed = AtomicU64::new(0);
    let shards_read = AtomicU64::new(0);

    let t0 = Instant::now();

    (0..num_shards).into_par_iter().for_each(|shard_id| {
        let dir_byte = ((shard_id >> 8) & 0xFF) as u8;
        let path = docs_path
            .join("shards")
            .join(format!("{:02x}", dir_byte))
            .join(format!("{:06}.bin", shard_id));

        match std::fs::read(&path) {
            Ok(data) => {
                bytes_read.fetch_add(data.len() as u64, Ordering::Relaxed);
                // Decompress to measure decompression throughput
                // ShardStore format — count bytes as decompressed (no separate compression layer)
                bytes_decompressed.fetch_add(data.len() as u64, Ordering::Relaxed);
                shards_read.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {}
        }
    });

    let elapsed = t0.elapsed().as_secs_f64();
    let total_read = bytes_read.load(Ordering::Relaxed);
    let total_decompressed = bytes_decompressed.load(Ordering::Relaxed);
    let total_shards = shards_read.load(Ordering::Relaxed);

    eprintln!("  Shards read:       {}", total_shards);
    eprintln!("  Compressed bytes:  {:.2} GB", total_read as f64 / 1e9);
    eprintln!("  Decompressed:      {:.2} GB", total_decompressed as f64 / 1e9);
    eprintln!("  Time:              {:.2}s", elapsed);
    eprintln!("  Read throughput:   {:.0} MB/s (compressed)", total_read as f64 / elapsed / 1e6);
    eprintln!("  Decomp throughput: {:.0} MB/s (decompressed)", total_decompressed as f64 / elapsed / 1e6);

    (elapsed, total_read, total_decompressed)
}

/// Stage 2: Read + decode to StoredDoc.
fn bench_decode(docs_path: &Path, num_shards: u32) -> (f64, u64) {
    eprintln!("\n=== Stage 2: Read + Decode (→ StoredDoc) ===");
    let docs_decoded = AtomicU64::new(0);

    let reader = DocStoreV3::open(docs_path).expect("open docstore");

    let t0 = Instant::now();

    (0..num_shards).into_par_iter().for_each(|shard_id| {
        match reader.get_shard(shard_id) {
            Ok(docs) => {
                docs_decoded.fetch_add(docs.len() as u64, Ordering::Relaxed);
            }
            Err(_) => {}
        }
    });

    let elapsed = t0.elapsed().as_secs_f64();
    let total_docs = docs_decoded.load(Ordering::Relaxed);

    eprintln!("  Docs decoded:      {}", total_docs);
    eprintln!("  Time:              {:.2}s", elapsed);
    eprintln!("  Throughput:        {:.0} docs/s", total_docs as f64 / elapsed);
    eprintln!("  Per-doc avg:       {:.2} µs", elapsed * 1e6 / total_docs as f64);

    (elapsed, total_docs)
}

/// Stage 3: Full rebuild pipeline — read + decode + extract filter/sort bitmaps + merge.
fn bench_full_rebuild(
    docs_path: &Path,
    num_shards: u32,
    filter_names: &[&str],
    sort_names: &[&str],
    sort_bits: &[usize],
) -> (f64, u64) {
    eprintln!("\n=== Stage 3: Full Rebuild Pipeline ===");
    eprintln!("  Filter fields: {:?}", filter_names);
    eprintln!("  Sort fields:   {:?}", sort_names);

    let reader = DocStoreV3::open(docs_path).expect("open docstore");

    type FilterMap = HashMap<(usize, u64), RoaringBitmap>;
    struct Accum {
        sort_layers: Vec<Vec<RoaringBitmap>>,
        filter_map: FilterMap,
        alive: RoaringBitmap,
        count: u64,
    }

    let make_accum = || Accum {
        sort_layers: sort_bits.iter().map(|&b| {
            (0..b).map(|_| RoaringBitmap::new()).collect()
        }).collect(),
        filter_map: FilterMap::new(),
        alive: RoaringBitmap::new(),
        count: 0,
    };

    let chunk_size = 500u32;
    let num_chunks = (num_shards + chunk_size - 1) / chunk_size;

    let t0 = Instant::now();

    let merged = (0..num_chunks)
        .into_par_iter()
        .fold(make_accum, |mut acc, chunk_idx| {
            let shard_start = chunk_idx * chunk_size;
            let shard_end = std::cmp::min(shard_start + chunk_size, num_shards);

            for shard_id in shard_start..shard_end {
                let docs = match reader.get_shard(shard_id) {
                    Ok(d) => d,
                    Err(_) => continue,
                };

                for (slot_id, doc) in &docs {
                    acc.alive.insert(*slot_id);

                    // Filter bitmap extraction
                    for (fi, &fname) in filter_names.iter().enumerate() {
                        if let Some(fv) = doc.fields.get(fname) {
                            match fv {
                                bitdex_v2::mutation::FieldValue::Single(v) => {
                                    if let Some(key) = value_to_bitmap_key(v) {
                                        acc.filter_map
                                            .entry((fi, key))
                                            .or_insert_with(RoaringBitmap::new)
                                            .insert(*slot_id);
                                    }
                                }
                                bitdex_v2::mutation::FieldValue::Multi(vals) => {
                                    for v in vals {
                                        if let Some(key) = value_to_bitmap_key(v) {
                                            acc.filter_map
                                                .entry((fi, key))
                                                .or_insert_with(RoaringBitmap::new)
                                                .insert(*slot_id);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Sort bitmap extraction
                    for (si, &sname) in sort_names.iter().enumerate() {
                        if let Some(fv) = doc.fields.get(sname) {
                            if let bitdex_v2::mutation::FieldValue::Single(ref v) = fv {
                                if let Some(value) = value_to_sort_u32(v) {
                                    let num_bits = sort_bits[si];
                                    for bit in 0..num_bits {
                                        if (value >> bit) & 1 == 1 {
                                            acc.sort_layers[si][bit].insert(*slot_id);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    acc.count += 1;
                }
            }
            acc
        })
        .reduce(make_accum, |mut a, b| {
            for (si, b_layers) in b.sort_layers.into_iter().enumerate() {
                for (bit, bm) in b_layers.into_iter().enumerate() {
                    a.sort_layers[si][bit] |= bm;
                }
            }
            for (key, bm) in b.filter_map {
                a.filter_map.entry(key)
                    .and_modify(|existing| *existing |= &bm)
                    .or_insert(bm);
            }
            a.alive |= b.alive;
            a.count += b.count;
            a
        });

    let elapsed = t0.elapsed().as_secs_f64();

    let total_filter_bitmaps: usize = merged.filter_map.len();
    let total_sort_layers: usize = merged.sort_layers.iter()
        .map(|layers| layers.iter().filter(|bm| !bm.is_empty()).count())
        .sum();

    eprintln!("  Docs processed:    {}", merged.count);
    eprintln!("  Alive bitmap:      {} bits", merged.alive.len());
    eprintln!("  Filter bitmaps:    {} distinct (field,value) pairs", total_filter_bitmaps);
    eprintln!("  Sort layers:       {} non-empty layers", total_sort_layers);
    eprintln!("  Time:              {:.2}s", elapsed);
    eprintln!("  Throughput:        {:.0} docs/s", merged.count as f64 / elapsed);

    (elapsed, merged.count)
}

/// Stage 4: Rebuild a single field — measures per-field rebuild cost.
fn bench_single_field_rebuild(
    docs_path: &Path,
    num_shards: u32,
    field_name: &str,
    is_sort: bool,
    bits: usize,
) -> (f64, u64) {
    eprintln!("\n=== Stage 4: Single Field Rebuild — {} ({}) ===",
        field_name, if is_sort { "sort" } else { "filter" });

    let reader = DocStoreV3::open(docs_path).expect("open docstore");
    let _docs_processed = AtomicU64::new(0);

    let chunk_size = 500u32;
    let num_chunks = (num_shards + chunk_size - 1) / chunk_size;

    let t0 = Instant::now();

    if is_sort {
        // Sort field rebuild
        struct SortAccum {
            layers: Vec<RoaringBitmap>,
            count: u64,
        }

        let make_accum = || SortAccum {
            layers: (0..bits).map(|_| RoaringBitmap::new()).collect(),
            count: 0,
        };

        let merged = (0..num_chunks)
            .into_par_iter()
            .fold(make_accum, |mut acc, chunk_idx| {
                let shard_start = chunk_idx * chunk_size;
                let shard_end = std::cmp::min(shard_start + chunk_size, num_shards);

                for shard_id in shard_start..shard_end {
                    let docs = match reader.get_shard(shard_id) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    for (slot_id, doc) in &docs {
                        if let Some(fv) = doc.fields.get(field_name) {
                            if let bitdex_v2::mutation::FieldValue::Single(ref v) = fv {
                                if let Some(value) = value_to_sort_u32(v) {
                                    for bit in 0..bits {
                                        if (value >> bit) & 1 == 1 {
                                            acc.layers[bit].insert(*slot_id);
                                        }
                                    }
                                }
                            }
                        }
                        acc.count += 1;
                    }
                }
                acc
            })
            .reduce(make_accum, |mut a, b| {
                for (bit, bm) in b.layers.into_iter().enumerate() {
                    a.layers[bit] |= bm;
                }
                a.count += b.count;
                a
            });

        let elapsed = t0.elapsed().as_secs_f64();
        let non_empty = merged.layers.iter().filter(|l| !l.is_empty()).count();
        eprintln!("  Docs:    {}", merged.count);
        eprintln!("  Layers:  {}/{} non-empty", non_empty, bits);
        eprintln!("  Time:    {:.2}s", elapsed);
        eprintln!("  Rate:    {:.0} docs/s", merged.count as f64 / elapsed);
        (elapsed, merged.count)
    } else {
        // Filter field rebuild
        type FMap = HashMap<u64, RoaringBitmap>;
        struct FilterAccum {
            map: FMap,
            count: u64,
        }

        let make_accum = || FilterAccum {
            map: FMap::new(),
            count: 0,
        };

        let merged = (0..num_chunks)
            .into_par_iter()
            .fold(make_accum, |mut acc, chunk_idx| {
                let shard_start = chunk_idx * chunk_size;
                let shard_end = std::cmp::min(shard_start + chunk_size, num_shards);

                for shard_id in shard_start..shard_end {
                    let docs = match reader.get_shard(shard_id) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    for (slot_id, doc) in &docs {
                        if let Some(fv) = doc.fields.get(field_name) {
                            match fv {
                                bitdex_v2::mutation::FieldValue::Single(v) => {
                                    if let Some(key) = value_to_bitmap_key(v) {
                                        acc.map.entry(key)
                                            .or_insert_with(RoaringBitmap::new)
                                            .insert(*slot_id);
                                    }
                                }
                                bitdex_v2::mutation::FieldValue::Multi(vals) => {
                                    for v in vals {
                                        if let Some(key) = value_to_bitmap_key(v) {
                                            acc.map.entry(key)
                                                .or_insert_with(RoaringBitmap::new)
                                                .insert(*slot_id);
                                        }
                                    }
                                }
                            }
                        }
                        acc.count += 1;
                    }
                }
                acc
            })
            .reduce(make_accum, |mut a, b| {
                for (key, bm) in b.map {
                    a.map.entry(key)
                        .and_modify(|existing| *existing |= &bm)
                        .or_insert(bm);
                }
                a.count += b.count;
                a
            });

        let elapsed = t0.elapsed().as_secs_f64();
        eprintln!("  Docs:        {}", merged.count);
        eprintln!("  Distinct:    {} values", merged.map.len());
        eprintln!("  Time:        {:.2}s", elapsed);
        eprintln!("  Rate:        {:.0} docs/s", merged.count as f64 / elapsed);
        (elapsed, merged.count)
    }
}

/// Stage 5: Split-phase — pre-read all shards into memory, then benchmark
/// bitmap construction with zero I/O. This isolates CPU cost of bitmap ops.
fn bench_bitmap_only(
    docs_path: &Path,
    num_shards: u32,
    filter_names: &[&str],
    sort_names: &[&str],
    sort_bits: &[usize],
) -> (f64, f64, u64) {
    eprintln!("\n=== Stage 5: Split-Phase (pre-read → bitmap-only) ===");

    let reader = DocStoreV3::open(docs_path).expect("open docstore");

    // Phase A: Read all shards into memory (decoded StoredDocs)
    let t_read = Instant::now();
    let all_docs: Vec<Vec<(u32, StoredDoc)>> = (0..num_shards)
        .into_par_iter()
        .filter_map(|shard_id| {
            reader.get_shard(shard_id).ok().filter(|d| !d.is_empty())
        })
        .collect();
    let read_time = t_read.elapsed().as_secs_f64();

    let total_docs: u64 = all_docs.iter().map(|s| s.len() as u64).sum();
    eprintln!("  Read phase:        {:.2}s ({} docs, {:.0} docs/s)",
        read_time, total_docs, total_docs as f64 / read_time);

    // Phase B: Build bitmaps from in-memory docs (no I/O)
    type FilterMap = HashMap<(usize, u64), RoaringBitmap>;
    struct Accum {
        sort_layers: Vec<Vec<RoaringBitmap>>,
        filter_map: FilterMap,
        alive: RoaringBitmap,
        count: u64,
    }

    let make_accum = || Accum {
        sort_layers: sort_bits.iter().map(|&b| {
            (0..b).map(|_| RoaringBitmap::new()).collect()
        }).collect(),
        filter_map: FilterMap::new(),
        alive: RoaringBitmap::new(),
        count: 0,
    };

    let t_bitmap = Instant::now();

    let merged = all_docs
        .par_iter()
        .fold(make_accum, |mut acc, shard_docs| {
            for (slot_id, doc) in shard_docs {
                acc.alive.insert(*slot_id);

                for (fi, &fname) in filter_names.iter().enumerate() {
                    if let Some(fv) = doc.fields.get(fname) {
                        match fv {
                            bitdex_v2::mutation::FieldValue::Single(v) => {
                                if let Some(key) = value_to_bitmap_key(v) {
                                    acc.filter_map
                                        .entry((fi, key))
                                        .or_insert_with(RoaringBitmap::new)
                                        .insert(*slot_id);
                                }
                            }
                            bitdex_v2::mutation::FieldValue::Multi(vals) => {
                                for v in vals {
                                    if let Some(key) = value_to_bitmap_key(v) {
                                        acc.filter_map
                                            .entry((fi, key))
                                            .or_insert_with(RoaringBitmap::new)
                                            .insert(*slot_id);
                                    }
                                }
                            }
                        }
                    }
                }

                for (si, &sname) in sort_names.iter().enumerate() {
                    if let Some(fv) = doc.fields.get(sname) {
                        if let bitdex_v2::mutation::FieldValue::Single(ref v) = fv {
                            if let Some(value) = value_to_sort_u32(v) {
                                let num_bits = sort_bits[si];
                                for bit in 0..num_bits {
                                    if (value >> bit) & 1 == 1 {
                                        acc.sort_layers[si][bit].insert(*slot_id);
                                    }
                                }
                            }
                        }
                    }
                }

                acc.count += 1;
            }
            acc
        })
        .reduce(make_accum, |mut a, b| {
            for (si, b_layers) in b.sort_layers.into_iter().enumerate() {
                for (bit, bm) in b_layers.into_iter().enumerate() {
                    a.sort_layers[si][bit] |= bm;
                }
            }
            for (key, bm) in b.filter_map {
                a.filter_map.entry(key)
                    .and_modify(|existing| *existing |= &bm)
                    .or_insert(bm);
            }
            a.alive |= b.alive;
            a.count += b.count;
            a
        });

    let bitmap_time = t_bitmap.elapsed().as_secs_f64();

    eprintln!("  Bitmap phase:      {:.2}s ({:.0} docs/s)",
        bitmap_time, merged.count as f64 / bitmap_time);
    eprintln!("  Filter bitmaps:    {} distinct pairs", merged.filter_map.len());
    eprintln!("  Total:             {:.2}s", read_time + bitmap_time);

    (read_time, bitmap_time, merged.count)
}

/// Stage 6: Raw bytes → bitmap extraction WITHOUT full StoredDoc decode.
/// Decodes msgpack pairs directly, only extracting fields we need.
fn bench_selective_decode(
    docs_path: &Path,
    num_shards: u32,
    target_fields: &[&str],
) -> (f64, u64) {
    eprintln!("\n=== Stage 6: Selective Decode (skip full StoredDoc) ===");
    eprintln!("  Target fields: {:?}", target_fields);

    let reader = DocStoreV3::open(docs_path).expect("open docstore");
    let _field_to_idx = &reader;

    // We'll read raw shard bytes and decode only needed fields
    let _docs_processed = AtomicU64::new(0);

    let t0 = Instant::now();

    // For now, just measure the difference by reading shards and only looking up target fields
    // This shows the cost of HashMap::get vs iterating all fields
    let chunk_size = 500u32;
    let num_chunks = (num_shards + chunk_size - 1) / chunk_size;

    let total: u64 = (0..num_chunks)
        .into_par_iter()
        .map(|chunk_idx| {
            let shard_start = chunk_idx * chunk_size;
            let shard_end = std::cmp::min(shard_start + chunk_size, num_shards);
            let mut count = 0u64;

            for shard_id in shard_start..shard_end {
                let docs = match reader.get_shard(shard_id) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                for (_slot_id, doc) in &docs {
                    // Only access target fields — simulates selective decode
                    for &fname in target_fields {
                        let _ = doc.fields.get(fname);
                    }
                    count += 1;
                }
            }
            count
        })
        .sum();

    let elapsed = t0.elapsed().as_secs_f64();
    eprintln!("  Docs:    {}", total);
    eprintln!("  Time:    {:.2}s", elapsed);
    eprintln!("  Rate:    {:.0} docs/s", total as f64 / elapsed);

    (elapsed, total)
}

/// Stage 7: Zero-alloc packed rebuild — decode to Vec<(u16, PackedValue)> directly,
/// use field dictionary u16 indices instead of String HashMap lookups.
/// This is the "what if we skip StoredDoc entirely" benchmark.
fn bench_packed_rebuild(
    docs_path: &Path,
    num_shards: u32,
    filter_names: &[&str],
    sort_names: &[&str],
    sort_bits: &[usize],
) -> (f64, u64) {
    eprintln!("\n=== Stage 7: Packed Rebuild (skip StoredDoc) ===");

    let reader = DocStoreV3::open(docs_path).expect("open docstore");

    // Build u16 index → (role, position) lookup table from field dictionary
    // role: 0 = filter, 1 = sort, 2 = both
    let field_dict = reader.field_to_idx();
    let mut filter_idx_map: HashMap<u16, usize> = HashMap::new(); // dict_idx → filter position
    let mut sort_idx_map: HashMap<u16, (usize, usize)> = HashMap::new(); // dict_idx → (sort position, bits)

    for (fi, &fname) in filter_names.iter().enumerate() {
        if let Some(&idx) = field_dict.get(fname) {
            filter_idx_map.insert(idx, fi);
        }
    }
    for (si, &sname) in sort_names.iter().enumerate() {
        if let Some(&idx) = field_dict.get(sname) {
            sort_idx_map.insert(idx, (si, sort_bits[si]));
        }
    }

    eprintln!("  Filter fields mapped: {}/{}", filter_idx_map.len(), filter_names.len());
    eprintln!("  Sort fields mapped:   {}/{}", sort_idx_map.len(), sort_names.len());

    type FilterMap = HashMap<(usize, u64), RoaringBitmap>;
    struct Accum {
        sort_layers: Vec<Vec<RoaringBitmap>>,
        filter_map: FilterMap,
        alive: RoaringBitmap,
        count: u64,
    }

    let make_accum = || Accum {
        sort_layers: sort_bits.iter().map(|&b| {
            (0..b).map(|_| RoaringBitmap::new()).collect()
        }).collect(),
        filter_map: FilterMap::new(),
        alive: RoaringBitmap::new(),
        count: 0,
    };

    let chunk_size = 500u32;
    let num_chunks = (num_shards + chunk_size - 1) / chunk_size;

    let t0 = Instant::now();

    let merged = (0..num_chunks)
        .into_par_iter()
        .fold(make_accum, |mut acc, chunk_idx| {
            let shard_start = chunk_idx * chunk_size;
            let shard_end = std::cmp::min(shard_start + chunk_size, num_shards);

            for shard_id in shard_start..shard_end {
                let packed_docs = match reader.get_shard_packed(shard_id) {
                    Ok(d) => d,
                    Err(_) => continue,
                };

                for (slot_id, pairs) in &packed_docs {
                    acc.alive.insert(*slot_id);

                    for (field_idx, pv) in pairs {
                        // Filter extraction — direct u16 lookup, no String
                        if let Some(&fi) = filter_idx_map.get(field_idx) {
                            match pv {
                                PackedValue::I(v) => {
                                    acc.filter_map
                                        .entry((fi, *v as u64))
                                        .or_insert_with(RoaringBitmap::new)
                                        .insert(*slot_id);
                                }
                                PackedValue::B(b) => {
                                    acc.filter_map
                                        .entry((fi, if *b { 1 } else { 0 }))
                                        .or_insert_with(RoaringBitmap::new)
                                        .insert(*slot_id);
                                }
                                PackedValue::Mi(vals) => {
                                    for v in vals {
                                        acc.filter_map
                                            .entry((fi, *v as u64))
                                            .or_insert_with(RoaringBitmap::new)
                                            .insert(*slot_id);
                                    }
                                }
                                _ => {}
                            }
                        }

                        // Sort extraction — direct u16 lookup
                        if let Some(&(si, bits)) = sort_idx_map.get(field_idx) {
                            if let PackedValue::I(v) = pv {
                                let value = (*v).max(0) as u32;
                                for bit in 0..bits {
                                    if (value >> bit) & 1 == 1 {
                                        acc.sort_layers[si][bit].insert(*slot_id);
                                    }
                                }
                            }
                        }
                    }

                    acc.count += 1;
                }
            }
            acc
        })
        .reduce(make_accum, |mut a, b| {
            for (si, b_layers) in b.sort_layers.into_iter().enumerate() {
                for (bit, bm) in b_layers.into_iter().enumerate() {
                    a.sort_layers[si][bit] |= bm;
                }
            }
            for (key, bm) in b.filter_map {
                a.filter_map.entry(key)
                    .and_modify(|existing| *existing |= &bm)
                    .or_insert(bm);
            }
            a.alive |= b.alive;
            a.count += b.count;
            a
        });

    let elapsed = t0.elapsed().as_secs_f64();

    eprintln!("  Docs processed:    {}", merged.count);
    eprintln!("  Filter bitmaps:    {} distinct pairs", merged.filter_map.len());
    eprintln!("  Time:              {:.2}s", elapsed);
    eprintln!("  Throughput:        {:.0} docs/s", merged.count as f64 / elapsed);

    (elapsed, merged.count)
}

/// Full-scale build: creates a ConcurrentEngine, calls build_all_from_docstore,
/// monitors memory throughout. This is the "boot in build-index mode" scenario.
fn run_full_build(data_dir: &Path, index_name: &str) {
    use bitdex_v2::concurrent_engine::{ConcurrentEngine, get_rss_bytes};

    let index_dir = data_dir.join("indexes").join(index_name);
    let config_path = bitdex_v2::server::find_index_config(&index_dir)
        .unwrap_or_else(|| { eprintln!("No config found in {}", index_dir.display()); std::process::exit(1); });
    let docs_path = index_dir.join("docs");

    eprintln!("\n=== FULL BUILD: build_all_from_docstore ===");
    eprintln!("Index: {}", index_name);
    eprintln!("Docs:  {}", docs_path.display());

    let index_def = bitdex_v2::server::IndexDefinition::from_file(&config_path)
        .unwrap_or_else(|e| { eprintln!("Failed to parse config: {e}"); std::process::exit(1); });
    let mut config = index_def.config;

    // Set bitmap_path so save_and_unload() can persist to disk
    let bitmap_path = index_dir.join("bitmaps");
    config.storage.bitmap_path = Some(bitmap_path.clone());
    eprintln!("Bitmaps: {}", bitmap_path.display());

    let rss_start = get_rss_bytes();
    eprintln!("RSS before engine: {:.2} MB", rss_start as f64 / 1e6);

    // Create engine with docstore path + bitmap path for persistence
    let engine = ConcurrentEngine::new_with_path(config, &docs_path)
        .expect("create engine");

    let rss_after_engine = get_rss_bytes();
    eprintln!("RSS after engine init: {:.2} MB", rss_after_engine as f64 / 1e6);

    let progress = std::sync::Arc::new(AtomicU64::new(0));

    // Memory monitoring callback — prints every 5 seconds
    let memory_cb: Box<dyn Fn(u64, f64, u64) + Send + Sync> = Box::new(|docs, elapsed, rss| {
        if elapsed > 0.0 {
            eprintln!("  [{:>6.1}s] {:>10} docs ({:>7.0} docs/s)  RSS={:.2} GB",
                elapsed, docs, docs as f64 / elapsed, rss as f64 / 1e9);
        }
    });

    eprintln!("Starting build...");
    let _t0 = Instant::now();

    let (total_docs, elapsed) = engine.build_all_from_docstore(
        progress.clone(),
        Some(memory_cb),
    ).expect("build_all_from_docstore");

    let rss_after_build = get_rss_bytes();
    let bitmap_rss = rss_after_build.saturating_sub(rss_start);

    eprintln!("\n--- BUILD PHASE COMPLETE ---");
    eprintln!("  Docs:              {}", total_docs);
    eprintln!("  Time:              {:.1}s ({:.1} min)", elapsed, elapsed / 60.0);
    eprintln!("  Throughput:        {:.0} docs/s", total_docs as f64 / elapsed);
    eprintln!("  RSS after build:   {:.2} GB", rss_after_build as f64 / 1e9);
    eprintln!("  RSS delta (bitmaps): {:.2} GB", bitmap_rss as f64 / 1e9);

    // Phase 2: Persist bitmaps to disk and unload from memory
    eprintln!("\n--- PERSIST PHASE ---");
    eprintln!("Saving bitmaps to {} ...", bitmap_path.display());
    let persist_t0 = Instant::now();

    engine.save_and_unload()
        .expect("save_and_unload");

    let persist_elapsed = persist_t0.elapsed().as_secs_f64();
    let rss_after_persist = get_rss_bytes();

    eprintln!("  Persist time:      {:.1}s", persist_elapsed);
    eprintln!("  RSS after unload:  {:.2} GB", rss_after_persist as f64 / 1e9);
    eprintln!("  Memory freed:      {:.2} GB", (rss_after_build.saturating_sub(rss_after_persist)) as f64 / 1e9);

    let total_time = elapsed + persist_elapsed;

    eprintln!("\n========================================");
    eprintln!("  FULL BUILD + PERSIST COMPLETE");
    eprintln!("========================================");
    eprintln!("  Docs:              {}", total_docs);
    eprintln!("  Build time:        {:.1}s ({:.1} min)", elapsed, elapsed / 60.0);
    eprintln!("  Persist time:      {:.1}s", persist_elapsed);
    eprintln!("  Total time:        {:.1}s ({:.1} min)", total_time, total_time / 60.0);
    eprintln!("  Throughput (e2e):  {:.0} docs/s", total_docs as f64 / total_time);
    eprintln!("  RSS start:         {:.2} GB", rss_start as f64 / 1e9);
    eprintln!("  RSS peak (build):  {:.2} GB", rss_after_build as f64 / 1e9);
    eprintln!("  RSS final (unloaded): {:.2} GB", rss_after_persist as f64 / 1e9);
    eprintln!("  Bytes/doc (build): {:.0}", bitmap_rss as f64 / total_docs as f64);
}

/// --add-field mode: build a full engine from docstore, then hot-add a single field.
/// This benchmarks the add_fields_from_docstore() path that will back the HTTP endpoint.
///
/// Strategy: load the config, remove the target field, build the engine without it,
/// then add it back via add_fields_from_docstore and measure the cost.
fn run_add_field(data_dir: &Path, index_name: &str, field_name: &str) {
    use bitdex_v2::concurrent_engine::{ConcurrentEngine, get_rss_bytes};
    use bitdex_v2::config::{FilterFieldConfig, SortFieldConfig};

    let index_dir = data_dir.join("indexes").join(index_name);
    let config_path = index_dir.join("config.json");
    let docs_path = index_dir.join("docs");

    eprintln!("\n=== ADD-FIELD BENCHMARK: '{}' ===", field_name);
    eprintln!("Index: {}", index_name);

    #[derive(serde::Deserialize)]
    struct IndexDef {
        config: bitdex_v2::config::Config,
    }
    let config_json = std::fs::read_to_string(&config_path).expect("read config.json");
    let index_def: IndexDef = serde_json::from_str(&config_json).expect("parse config.json");
    let mut config = index_def.config;

    // Find and remove the target field from config (so we can add it back)
    let removed_filter: Option<FilterFieldConfig> = {
        let pos = config.filter_fields.iter().position(|f| f.name == field_name);
        pos.map(|i| config.filter_fields.remove(i))
    };
    let removed_sort: Option<SortFieldConfig> = {
        let pos = config.sort_fields.iter().position(|f| f.name == field_name);
        pos.map(|i| config.sort_fields.remove(i))
    };

    if removed_filter.is_none() && removed_sort.is_none() {
        eprintln!("ERROR: Field '{}' not found in config (neither filter nor sort)", field_name);
        std::process::exit(1);
    }

    eprintln!("  Removed from config: filter={}, sort={}",
        removed_filter.is_some(), removed_sort.is_some());
    eprintln!("  Will build engine without '{}', then hot-add it", field_name);

    // Build engine without the target field
    let bitmap_path = index_dir.join("bitmaps");
    config.storage.bitmap_path = Some(bitmap_path.clone());

    let _rss_before = get_rss_bytes();

    let engine = ConcurrentEngine::new_with_path(config, &docs_path)
        .expect("create engine");

    // Full build without the target field
    eprintln!("\n--- Phase 1: Full build (without '{}') ---", field_name);
    let progress = std::sync::Arc::new(AtomicU64::new(0));
    let _t_build = Instant::now();
    let (total_docs, build_elapsed) = engine.build_all_from_docstore(
        progress.clone(),
        None,
    ).expect("build_all_from_docstore");

    let rss_after_build = get_rss_bytes();
    eprintln!("  Build: {} docs in {:.1}s ({:.0} docs/s)",
        total_docs, build_elapsed, total_docs as f64 / build_elapsed);
    eprintln!("  RSS after build: {:.2} GB", rss_after_build as f64 / 1e9);

    // Now hot-add the field
    eprintln!("\n--- Phase 2: Hot-add '{}' ---", field_name);
    let rss_before_add = get_rss_bytes();
    progress.store(0, Ordering::Relaxed);
    let t_add = Instant::now();

    let new_filters = removed_filter.map(|f| vec![f]).unwrap_or_default();
    let new_sorts = removed_sort.map(|f| vec![f]).unwrap_or_default();

    let (slots, fields) = engine.add_fields_from_docstore(
        new_filters,
        new_sorts,
        progress,
    ).expect("add_fields_from_docstore");

    let add_elapsed = t_add.elapsed().as_secs_f64();
    let rss_after_add = get_rss_bytes();
    let rss_delta = rss_after_add.saturating_sub(rss_before_add);

    eprintln!("  Slots scanned:     {}", slots);
    eprintln!("  Fields added:      {:?}", fields);
    eprintln!("  Time:              {:.1}s", add_elapsed);
    eprintln!("  Throughput:        {:.0} docs/s", slots as f64 / add_elapsed);
    eprintln!("  RSS delta:         {:.2} MB", rss_delta as f64 / 1e6);
    eprintln!("  RSS total:         {:.2} GB", rss_after_add as f64 / 1e9);

    // Optional: persist
    eprintln!("\n--- Phase 3: Persist ---");
    let t_persist = Instant::now();
    engine.save_and_unload().expect("save_and_unload");
    let persist_elapsed = t_persist.elapsed().as_secs_f64();
    let rss_after_persist = get_rss_bytes();
    eprintln!("  Persist time:      {:.1}s", persist_elapsed);
    eprintln!("  RSS after unload:  {:.2} GB", rss_after_persist as f64 / 1e9);

    eprintln!("\n========================================");
    eprintln!("  ADD-FIELD BENCHMARK COMPLETE");
    eprintln!("========================================");
    eprintln!("  Field:             {}", field_name);
    eprintln!("  Full build:        {:.1}s (without field)", build_elapsed);
    eprintln!("  Hot-add:           {:.1}s ({:.0} docs/s)", add_elapsed, slots as f64 / add_elapsed);
    eprintln!("  Persist:           {:.1}s", persist_elapsed);
    eprintln!("  Add + persist:     {:.1}s", add_elapsed + persist_elapsed);
    eprintln!("  Add overhead:      {:.1}% of full build", add_elapsed / build_elapsed * 100.0);
}

fn main() {
    let config = parse_args();
    let index_dir = config.data_dir.join("indexes").join(&config.index_name);
    let docs_path = index_dir.join("docs");

    // --add-field mode: benchmark hot-adding a single field
    if let Some(ref field_name) = config.add_field {
        run_add_field(&config.data_dir, &config.index_name, field_name);
        return;
    }

    // --full mode: run the engine-level build_all_from_docstore
    if config.full_build {
        run_full_build(&config.data_dir, &config.index_name);
        return;
    }

    eprintln!("Rebuild Benchmark — index: {}", config.index_name);
    eprintln!("Docs path: {}", docs_path.display());

    // Count shards
    let t0 = Instant::now();
    let max_shard = find_max_shard(&docs_path);
    let total_shards = max_shard + 1;
    eprintln!("Found max shard ID {} ({} total) in {:.1}s",
        max_shard, total_shards, t0.elapsed().as_secs_f64());

    let num_shards = config.max_shards.unwrap_or(total_shards).min(total_shards);
    eprintln!("Benchmarking {} shards (~{} docs)",
        num_shards, num_shards as u64 * 512);

    let num_threads = rayon::current_num_threads();
    eprintln!("Rayon threads: {}", num_threads);

    // Stage 1: Raw I/O
    let (io_time, compressed_bytes, decompressed_bytes) = bench_raw_io(&docs_path, num_shards);

    // Stage 2: Read + Decode
    let (decode_time, total_docs) = bench_decode(&docs_path, num_shards);

    // Stage 3: Full rebuild (all filter + sort fields)
    let filter_names: Vec<&str> = vec![
        "nsfwLevel", "userId", "postId", "postedToId", "type", "baseModel",
        "availability", "blockedFor", "remixOfId", "hasMeta", "onSite", "poi", "minor",
        "tagIds", "modelVersionIds", "modelVersionIdsManual", "toolIds", "techniqueIds",
    ];
    let sort_names: Vec<&str> = vec!["reactionCount", "sortAt", "commentCount", "collectedCount", "id"];
    let sort_bits: Vec<usize> = vec![32, 32, 32, 32, 32];

    let (full_time, full_docs) = bench_full_rebuild(
        &docs_path, num_shards,
        &filter_names, &sort_names, &sort_bits,
    );

    // Stage 4: Single field rebuilds (interesting ones)
    eprintln!("\n--- Per-field rebuild times ---");

    let (nsfw_time, _) = bench_single_field_rebuild(&docs_path, num_shards, "nsfwLevel", false, 0);
    let (tags_time, _) = bench_single_field_rebuild(&docs_path, num_shards, "tagIds", false, 0);
    let (sort_time, _) = bench_single_field_rebuild(&docs_path, num_shards, "sortAt", true, 32);
    let (reaction_time, _) = bench_single_field_rebuild(&docs_path, num_shards, "reactionCount", true, 32);

    // Stage 5: Split-phase (isolate I/O from CPU)
    let (split_read, split_bitmap, split_docs) = bench_bitmap_only(
        &docs_path, num_shards,
        &filter_names, &sort_names, &sort_bits,
    );

    // Stage 6: Selective decode (only target fields)
    let (selective_1_time, _) = bench_selective_decode(
        &docs_path, num_shards, &["nsfwLevel"],
    );
    let (selective_all_time, _) = bench_selective_decode(
        &docs_path, num_shards,
        &["nsfwLevel", "userId", "tagIds", "reactionCount", "sortAt"],
    );

    // Stage 7: Packed rebuild (skip StoredDoc entirely)
    let (packed_time, packed_docs) = bench_packed_rebuild(
        &docs_path, num_shards,
        &filter_names, &sort_names, &sort_bits,
    );

    // Summary
    eprintln!("\n========================================");
    eprintln!("  SUMMARY ({} docs, {} shards, {} threads)", total_docs, num_shards, num_threads);
    eprintln!("========================================");
    eprintln!("  Raw I/O:          {:.2}s  ({:.0} MB/s compressed, {:.0} MB/s decompressed)",
        io_time,
        compressed_bytes as f64 / io_time / 1e6,
        decompressed_bytes as f64 / io_time / 1e6);
    eprintln!("  Read + Decode:    {:.2}s  ({:.0} docs/s)",
        decode_time, total_docs as f64 / decode_time);
    eprintln!("  Full Rebuild:     {:.2}s  ({:.0} docs/s) [current: StoredDoc path]",
        full_time, full_docs as f64 / full_time);
    eprintln!("  Packed Rebuild:   {:.2}s  ({:.0} docs/s) [new: skip StoredDoc]",
        packed_time, packed_docs as f64 / packed_time);
    if packed_time < full_time {
        eprintln!("  >>> Packed is {:.1}x FASTER than current <<<",
            full_time / packed_time);
    }
    eprintln!("  ---");
    eprintln!("  Split-phase read: {:.2}s  ({:.0} docs/s)",
        split_read, split_docs as f64 / split_read);
    eprintln!("  Split-phase bmap: {:.2}s  ({:.0} docs/s)",
        split_bitmap, split_docs as f64 / split_bitmap);
    eprintln!("  ---");
    eprintln!("  nsfwLevel only:   {:.2}s", nsfw_time);
    eprintln!("  tagIds only:      {:.2}s", tags_time);
    eprintln!("  sortAt only:      {:.2}s", sort_time);
    eprintln!("  reactionCount:    {:.2}s", reaction_time);
    eprintln!("  ---");
    eprintln!("  Selective (1):    {:.2}s  (decode + 1 field lookup)", selective_1_time);
    eprintln!("  Selective (5):    {:.2}s  (decode + 5 field lookups)", selective_all_time);
    eprintln!("  ---");
    eprintln!("  Decode overhead:  {:.1}x vs raw I/O", decode_time / io_time);
    eprintln!("  Bitmap overhead:  {:.1}x vs decode-only", full_time / decode_time);
    eprintln!("  I/O vs CPU split: {:.0}% I/O, {:.0}% bitmap",
        split_read / (split_read + split_bitmap) * 100.0,
        split_bitmap / (split_read + split_bitmap) * 100.0);
    eprintln!("  ---");
    eprintln!("  105M extrapolation:");
    eprintln!("    Current:  {:.0}s ({:.1} min)", total_docs as f64 / (full_docs as f64 / full_time) * (105e6 / total_docs as f64),
        total_docs as f64 / (full_docs as f64 / full_time) * (105e6 / total_docs as f64) / 60.0);
    eprintln!("    Packed:   {:.0}s ({:.1} min)", total_docs as f64 / (packed_docs as f64 / packed_time) * (105e6 / total_docs as f64),
        total_docs as f64 / (packed_docs as f64 / packed_time) * (105e6 / total_docs as f64) / 60.0);
}
