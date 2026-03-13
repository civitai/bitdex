//! Columnar loader microbenchmark.
//!
//! Validates the "columnar temp storage + final row assembly" strategy for
//! BitDex's table-at-a-time bulk loader. Each Postgres table stream writes to
//! column-oriented temp files; a final pass assembles complete documents.
//!
//! Workloads:
//!   W1: Scalar column write (Image stream) — fixed-width + url blob/index
//!   W2: Tag pair append (Tag stream) — sequential (imageId, tagId) pairs
//!   W3: Tag pair sort + adjacency index build
//!   W4: Final row assembly — read all columns, serialize msgpack, zstd compress, write shards
//!   W5: End-to-end pipeline (W1+W2 parallel → W3 → W4)

use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::Rng;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NUM_DOCS: u32 = 1_000_000;
const SHARD_SIZE: u32 = 512;
const AVG_TAGS_PER_DOC: u32 = 35;
const NUM_UNIQUE_TAGS: u32 = 1000;
const BUF_SIZE: usize = 8 * 1024 * 1024; // 8MB buffers

// ---------------------------------------------------------------------------
// Column file structures
// ---------------------------------------------------------------------------

/// Fixed-width scalar entry for image table stream.
/// 20 bytes per doc, indexed by docId.
#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
struct ImageScalarEntry {
    present_mask: u8,
    nsfw_level: u8,
    user_id: u64,
    image_type: u8,
    sort_at: u64,
    poi: u8,
    minor: u8,
}

const SCALAR_ENTRY_SIZE: usize = std::mem::size_of::<ImageScalarEntry>();

/// URL index entry: (offset, len) into the blob file.
#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
struct UrlIndexEntry {
    offset: u64,
    len: u32,
}

const URL_INDEX_SIZE: usize = std::mem::size_of::<UrlIndexEntry>();

/// Tag pair: (image_id, tag_id), 8 bytes.
#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
struct TagPair {
    image_id: u32,
    tag_id: u32,
}

const TAG_PAIR_SIZE: usize = std::mem::size_of::<TagPair>();

/// Adjacency index entry: (start, count) into sorted tag_values.
#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
struct AdjEntry {
    start: u32,
    count: u32,
}

const ADJ_ENTRY_SIZE: usize = std::mem::size_of::<AdjEntry>();

// ---------------------------------------------------------------------------
// Unsafe helpers for packed struct I/O
// ---------------------------------------------------------------------------

unsafe fn as_bytes<T: Sized>(val: &T) -> &[u8] {
    std::slice::from_raw_parts(val as *const T as *const u8, std::mem::size_of::<T>())
}

unsafe fn from_bytes<T: Sized + Copy>(bytes: &[u8]) -> T {
    assert!(bytes.len() >= std::mem::size_of::<T>());
    std::ptr::read_unaligned(bytes.as_ptr() as *const T)
}

// ---------------------------------------------------------------------------
// W1: Scalar column write
// ---------------------------------------------------------------------------

struct ScalarColumnFiles {
    scalar_path: PathBuf,
    url_blob_path: PathBuf,
    url_index_path: PathBuf,
}

fn w1_write_scalars(dir: &Path, num_docs: u32) -> ScalarColumnFiles {
    let scalar_path = dir.join("image_scalars.col");
    let url_blob_path = dir.join("url_data.col");
    let url_index_path = dir.join("url_index.col");

    // Scalar file: fixed-width, direct indexed
    let scalar_file = std::fs::File::create(&scalar_path).unwrap();
    let mut scalar_w = BufWriter::with_capacity(BUF_SIZE, scalar_file);

    // URL blob: append-only
    let url_blob_file = std::fs::File::create(&url_blob_path).unwrap();
    let mut url_blob_w = BufWriter::with_capacity(BUF_SIZE, url_blob_file);

    // URL index: fixed-width, indexed by docId
    let url_index_file = std::fs::File::create(&url_index_path).unwrap();
    let mut url_index_w = BufWriter::with_capacity(BUF_SIZE, url_index_file);

    let mut url_offset: u64 = 0;

    for doc_id in 0..num_docs {
        let entry = ImageScalarEntry {
            present_mask: 0xFF, // all fields present
            nsfw_level: (doc_id % 32) as u8,
            user_id: (doc_id as u64).wrapping_mul(7).wrapping_add(13),
            image_type: (doc_id % 4) as u8,
            sort_at: 1_700_000_000u64 + doc_id as u64,
            poi: if doc_id % 5 == 0 { 1 } else { 0 },
            minor: if doc_id % 20 == 0 { 1 } else { 0 },
        };
        unsafe {
            scalar_w.write_all(as_bytes(&entry)).unwrap();
        }

        // URL string
        let url = format!(
            "xG1nkqKTMzGDvpLrqFT7WA/{:08x}/width=400/image.jpeg",
            doc_id
        );
        let url_bytes = url.as_bytes();
        url_blob_w.write_all(url_bytes).unwrap();

        let idx_entry = UrlIndexEntry {
            offset: url_offset,
            len: url_bytes.len() as u32,
        };
        unsafe {
            url_index_w.write_all(as_bytes(&idx_entry)).unwrap();
        }
        url_offset += url_bytes.len() as u64;
    }

    scalar_w.flush().unwrap();
    url_blob_w.flush().unwrap();
    url_index_w.flush().unwrap();

    ScalarColumnFiles {
        scalar_path,
        url_blob_path,
        url_index_path,
    }
}

// ---------------------------------------------------------------------------
// W2: Tag pair append
// ---------------------------------------------------------------------------

fn w2_write_tag_pairs(dir: &Path, num_docs: u32) -> PathBuf {
    let pairs_path = dir.join("tags_pairs.col");
    let pairs_file = std::fs::File::create(&pairs_path).unwrap();
    let mut w = BufWriter::with_capacity(BUF_SIZE, pairs_file);

    let mut rng = rand::thread_rng();

    // Generate pairs: for each doc, pick ~AVG_TAGS_PER_DOC random tags
    for doc_id in 0..num_docs {
        // Vary count: 15-55 tags per doc (avg ~35)
        let n_tags = 15 + rng.gen_range(0..41u32);
        for _ in 0..n_tags {
            let tag_id = rng.gen_range(0..NUM_UNIQUE_TAGS);
            let pair = TagPair {
                image_id: doc_id,
                tag_id,
            };
            unsafe {
                w.write_all(as_bytes(&pair)).unwrap();
            }
        }
    }

    w.flush().unwrap();
    pairs_path
}

// ---------------------------------------------------------------------------
// W3: Tag pair sort + adjacency index
// ---------------------------------------------------------------------------

struct SortedTags {
    sorted_pairs_path: PathBuf,
    adj_index_path: PathBuf,
    total_pairs: usize,
}

fn w3_sort_tags(dir: &Path, pairs_path: &Path, num_docs: u32) -> SortedTags {
    // Read all pairs into memory
    let raw = std::fs::read(pairs_path).unwrap();
    let total_pairs = raw.len() / TAG_PAIR_SIZE;

    let mut pairs: Vec<TagPair> = Vec::with_capacity(total_pairs);
    for i in 0..total_pairs {
        let offset = i * TAG_PAIR_SIZE;
        unsafe {
            pairs.push(from_bytes(&raw[offset..offset + TAG_PAIR_SIZE]));
        }
    }

    // Sort by image_id
    pairs.sort_unstable_by_key(|p| p.image_id);

    // Write sorted pairs
    let sorted_path = dir.join("tags_sorted.col");
    {
        let f = std::fs::File::create(&sorted_path).unwrap();
        let mut w = BufWriter::with_capacity(BUF_SIZE, f);
        for p in &pairs {
            unsafe {
                w.write_all(as_bytes(p)).unwrap();
            }
        }
        w.flush().unwrap();
    }

    // Build adjacency index
    let adj_path = dir.join("tags_adj.col");
    {
        let f = std::fs::File::create(&adj_path).unwrap();
        let mut w = BufWriter::with_capacity(BUF_SIZE, f);

        // Initialize all entries to (0, 0)
        let empty = AdjEntry { start: 0, count: 0 };
        let empty_bytes = unsafe { as_bytes(&empty) };
        for _ in 0..num_docs {
            w.write_all(empty_bytes).unwrap();
        }
        w.flush().unwrap();
    }

    // Now fill in the adjacency entries
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&adj_path)
            .unwrap();

        let mut cur_id = pairs[0].image_id;
        let mut cur_start: u32 = 0;
        let mut cur_count: u32 = 0;

        for (i, p) in pairs.iter().enumerate() {
            if p.image_id != cur_id {
                // Write entry for cur_id
                let entry = AdjEntry {
                    start: cur_start,
                    count: cur_count,
                };
                f.seek(SeekFrom::Start(cur_id as u64 * ADJ_ENTRY_SIZE as u64))
                    .unwrap();
                unsafe {
                    f.write_all(as_bytes(&entry)).unwrap();
                }
                cur_id = p.image_id;
                cur_start = i as u32;
                cur_count = 0;
            }
            cur_count += 1;
        }
        // Write last entry
        let entry = AdjEntry {
            start: cur_start,
            count: cur_count,
        };
        f.seek(SeekFrom::Start(cur_id as u64 * ADJ_ENTRY_SIZE as u64))
            .unwrap();
        unsafe {
            f.write_all(as_bytes(&entry)).unwrap();
        }
    }

    SortedTags {
        sorted_pairs_path: sorted_path,
        adj_index_path: adj_path,
        total_pairs,
    }
}

// ---------------------------------------------------------------------------
// W4: Final row assembly
// ---------------------------------------------------------------------------

fn w4_assemble(
    dir: &Path,
    scalars: &ScalarColumnFiles,
    sorted_tags: &SortedTags,
    num_docs: u32,
) {
    // Memory-map style: read all column files into memory
    let scalar_data = std::fs::read(&scalars.scalar_path).unwrap();
    let url_blob = std::fs::read(&scalars.url_blob_path).unwrap();
    let url_index_data = std::fs::read(&scalars.url_index_path).unwrap();
    let sorted_pairs_data = std::fs::read(&sorted_tags.sorted_pairs_path).unwrap();
    let adj_data = std::fs::read(&sorted_tags.adj_index_path).unwrap();

    let shards_dir = dir.join("shards");
    std::fs::create_dir_all(&shards_dir).unwrap();

    let mut shard_buf: Vec<Vec<u8>> = Vec::with_capacity(SHARD_SIZE as usize);
    let mut shard_id: u32 = 0;

    for doc_id in 0..num_docs {
        // Read scalar
        let s_offset = doc_id as usize * SCALAR_ENTRY_SIZE;
        let scalar: ImageScalarEntry =
            unsafe { from_bytes(&scalar_data[s_offset..s_offset + SCALAR_ENTRY_SIZE]) };

        // Read URL
        let u_offset = doc_id as usize * URL_INDEX_SIZE;
        let url_idx: UrlIndexEntry =
            unsafe { from_bytes(&url_index_data[u_offset..u_offset + URL_INDEX_SIZE]) };
        let url = &url_blob[url_idx.offset as usize..(url_idx.offset + url_idx.len as u64) as usize];

        // Read tags via adjacency index
        let a_offset = doc_id as usize * ADJ_ENTRY_SIZE;
        let adj: AdjEntry = unsafe { from_bytes(&adj_data[a_offset..a_offset + ADJ_ENTRY_SIZE]) };

        let mut tag_ids: Vec<u32> = Vec::with_capacity(adj.count as usize);
        for t in 0..adj.count {
            let p_offset = (adj.start + t) as usize * TAG_PAIR_SIZE;
            let pair: TagPair =
                unsafe { from_bytes(&sorted_pairs_data[p_offset..p_offset + TAG_PAIR_SIZE]) };
            tag_ids.push(pair.tag_id);
        }

        // Assemble document as msgpack via (field_index, value) pairs
        let mut fields: Vec<(u16, serde_json::Value)> = Vec::with_capacity(9);
        fields.push((0, serde_json::Value::from(doc_id as i64))); // id
        fields.push((1, serde_json::Value::from(scalar.nsfw_level))); // nsfwLevel
        fields.push((2, serde_json::Value::from(scalar.user_id))); // userId
        fields.push((3, serde_json::Value::from(scalar.image_type))); // type
        fields.push((4, serde_json::Value::from(scalar.sort_at))); // sortAt
        fields.push((5, serde_json::Value::from(scalar.poi))); // poi
        fields.push((6, serde_json::Value::from(scalar.minor))); // minor
        fields.push((
            7,
            serde_json::Value::from(std::str::from_utf8(url).unwrap_or("")),
        )); // url
        fields.push((
            8,
            serde_json::Value::Array(tag_ids.iter().map(|&t| serde_json::Value::from(t)).collect()),
        )); // tagIds

        let encoded = rmp_serde::to_vec(&fields).unwrap();
        shard_buf.push(encoded);

        // Flush shard when full
        if shard_buf.len() == SHARD_SIZE as usize {
            flush_shard(&shards_dir, shard_id, &shard_buf);
            shard_buf.clear();
            shard_id += 1;
        }
    }

    // Flush remaining
    if !shard_buf.is_empty() {
        flush_shard(&shards_dir, shard_id, &shard_buf);
    }
}

fn flush_shard(dir: &Path, shard_id: u32, docs: &[Vec<u8>]) {
    // Concatenate all doc bytes, then zstd compress the batch
    let total_size: usize = docs.iter().map(|d| d.len()).sum();
    let mut raw = Vec::with_capacity(total_size + docs.len() * 8);

    // Write a simple header: [num_docs: u32] [offsets: u32 * num_docs] [data]
    let num = docs.len() as u32;
    raw.extend_from_slice(&num.to_le_bytes());

    // Calculate offsets
    let mut offset: u32 = 0;
    for d in docs {
        raw.extend_from_slice(&offset.to_le_bytes());
        raw.extend_from_slice(&(d.len() as u32).to_le_bytes());
        offset += d.len() as u32;
    }
    for d in docs {
        raw.extend_from_slice(d);
    }

    let compressed = zstd::encode_all(raw.as_slice(), 1).unwrap();

    let path = dir.join(format!("shard_{:06}.bin", shard_id));
    std::fs::write(&path, &compressed).unwrap();
}

// ---------------------------------------------------------------------------
// Benchmark functions
// ---------------------------------------------------------------------------

fn bench_w1_scalar_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("W1_scalar_column_write");
    group.throughput(Throughput::Elements(NUM_DOCS as u64));
    group.sample_size(10);

    group.bench_function("1M_docs", |b| {
        let dir = tempfile::tempdir().unwrap();
        b.iter(|| {
            w1_write_scalars(dir.path(), NUM_DOCS);
        });
    });

    group.finish();
}

fn bench_w2_tag_pair_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("W2_tag_pair_append");
    // ~35M pairs at 1M docs
    group.throughput(Throughput::Elements((NUM_DOCS as u64) * AVG_TAGS_PER_DOC as u64));
    group.sample_size(10);

    group.bench_function("35M_pairs", |b| {
        let dir = tempfile::tempdir().unwrap();
        b.iter(|| {
            w2_write_tag_pairs(dir.path(), NUM_DOCS);
        });
    });

    group.finish();
}

fn bench_w3_tag_sort(c: &mut Criterion) {
    // Pre-generate the pairs file
    let dir = tempfile::tempdir().unwrap();
    let pairs_path = w2_write_tag_pairs(dir.path(), NUM_DOCS);
    let raw_size = std::fs::metadata(&pairs_path).unwrap().len();
    let total_pairs = raw_size as u64 / TAG_PAIR_SIZE as u64;

    let mut group = c.benchmark_group("W3_tag_pair_sort");
    group.throughput(Throughput::Elements(total_pairs));
    group.sample_size(10);

    group.bench_function("sort_and_index", |b| {
        b.iter(|| {
            w3_sort_tags(dir.path(), &pairs_path, NUM_DOCS);
        });
    });

    group.finish();
}

fn bench_w4_assembly(c: &mut Criterion) {
    // Pre-generate all column files
    let dir = tempfile::tempdir().unwrap();
    let scalars = w1_write_scalars(dir.path(), NUM_DOCS);
    let pairs_path = w2_write_tag_pairs(dir.path(), NUM_DOCS);
    let sorted_tags = w3_sort_tags(dir.path(), &pairs_path, NUM_DOCS);

    let mut group = c.benchmark_group("W4_final_assembly");
    group.throughput(Throughput::Elements(NUM_DOCS as u64));
    group.sample_size(10);

    group.bench_function("assemble_1M", |b| {
        b.iter(|| {
            let out_dir = dir.path().join("output");
            std::fs::create_dir_all(&out_dir).ok();
            w4_assemble(&out_dir, &scalars, &sorted_tags, NUM_DOCS);
            // Clean up output for next iteration
            std::fs::remove_dir_all(&out_dir).ok();
        });
    });

    group.finish();
}

fn bench_w5_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("W5_end_to_end");
    group.throughput(Throughput::Elements(NUM_DOCS as u64));
    group.sample_size(10);

    group.bench_function("full_pipeline_1M", |b| {
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();

            // Phase 1: W1 + W2 (would be parallel in production; sequential here for measurement)
            let scalars = w1_write_scalars(dir.path(), NUM_DOCS);
            let pairs_path = w2_write_tag_pairs(dir.path(), NUM_DOCS);

            // Phase 2: W3 sort
            let sorted_tags = w3_sort_tags(dir.path(), &pairs_path, NUM_DOCS);

            // Phase 3: W4 assembly
            let out_dir = dir.path().join("output");
            std::fs::create_dir_all(&out_dir).unwrap();
            w4_assemble(&out_dir, &scalars, &sorted_tags, NUM_DOCS);
        });
    });

    group.finish();
}

/// Non-Criterion timing run that prints detailed per-phase breakdown and disk usage.
/// Called as a separate benchmark group with a single iteration.
fn bench_w5_detailed(c: &mut Criterion) {
    let mut group = c.benchmark_group("W5_detailed_breakdown");
    group.sample_size(10);

    group.bench_function("breakdown", |b| {
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();

            let t0 = Instant::now();
            let scalars = w1_write_scalars(dir.path(), NUM_DOCS);
            let t1 = Instant::now();
            let pairs_path = w2_write_tag_pairs(dir.path(), NUM_DOCS);
            let t2 = Instant::now();
            let sorted_tags = w3_sort_tags(dir.path(), &pairs_path, NUM_DOCS);
            let t3 = Instant::now();
            let out_dir = dir.path().join("output");
            std::fs::create_dir_all(&out_dir).unwrap();
            w4_assemble(&out_dir, &scalars, &sorted_tags, NUM_DOCS);
            let t4 = Instant::now();

            // Only print on first iteration (Criterion runs warmup + samples)
            // We use eprintln so it shows in benchmark output
            let w1_ms = t1.duration_since(t0).as_millis();
            let w2_ms = t2.duration_since(t1).as_millis();
            let w3_ms = t3.duration_since(t2).as_millis();
            let w4_ms = t4.duration_since(t3).as_millis();
            let total_ms = t4.duration_since(t0).as_millis();

            // Disk usage
            let scalar_size = std::fs::metadata(&scalars.scalar_path).map(|m| m.len()).unwrap_or(0);
            let url_blob_size = std::fs::metadata(&scalars.url_blob_path).map(|m| m.len()).unwrap_or(0);
            let url_index_size = std::fs::metadata(&scalars.url_index_path).map(|m| m.len()).unwrap_or(0);
            let pairs_size = std::fs::metadata(&pairs_path).map(|m| m.len()).unwrap_or(0);
            let sorted_size = std::fs::metadata(&sorted_tags.sorted_pairs_path).map(|m| m.len()).unwrap_or(0);
            let adj_size = std::fs::metadata(&sorted_tags.adj_index_path).map(|m| m.len()).unwrap_or(0);

            eprintln!("\n=== Columnar Loader Breakdown (1M docs) ===");
            eprintln!("W1 scalar write:    {:>6} ms  ({:.0}K docs/s)", w1_ms, NUM_DOCS as f64 / w1_ms as f64);
            eprintln!("W2 tag pair append: {:>6} ms  ({:.0}K pairs/s, {} pairs)", w2_ms, sorted_tags.total_pairs as f64 / w2_ms as f64, sorted_tags.total_pairs);
            eprintln!("W3 tag sort+index:  {:>6} ms  ({:.0}K pairs/s)", w3_ms, sorted_tags.total_pairs as f64 / w3_ms as f64);
            eprintln!("W4 assembly:        {:>6} ms  ({:.0}K docs/s)", w4_ms, NUM_DOCS as f64 / w4_ms as f64);
            eprintln!("Total:              {:>6} ms  ({:.0}K docs/s end-to-end)", total_ms, NUM_DOCS as f64 / total_ms as f64);
            eprintln!("");
            eprintln!("=== Temp Disk Usage (1M docs) ===");
            eprintln!("Scalar column:    {:>8.1} MB", scalar_size as f64 / 1_048_576.0);
            eprintln!("URL blob:         {:>8.1} MB", url_blob_size as f64 / 1_048_576.0);
            eprintln!("URL index:        {:>8.1} MB", url_index_size as f64 / 1_048_576.0);
            eprintln!("Tag pairs (raw):  {:>8.1} MB", pairs_size as f64 / 1_048_576.0);
            eprintln!("Tag pairs (sort): {:>8.1} MB", sorted_size as f64 / 1_048_576.0);
            eprintln!("Adjacency index:  {:>8.1} MB", adj_size as f64 / 1_048_576.0);
            let total_temp = scalar_size + url_blob_size + url_index_size + pairs_size + sorted_size + adj_size;
            eprintln!("Total temp:       {:>8.1} MB", total_temp as f64 / 1_048_576.0);
            eprintln!("");
            eprintln!("=== Extrapolation to 105M ===");
            eprintln!("Scalar column:    {:>8.1} GB", scalar_size as f64 * 105.0 / 1_073_741_824.0);
            eprintln!("URL blob:         {:>8.1} GB", url_blob_size as f64 * 105.0 / 1_073_741_824.0);
            eprintln!("URL index:        {:>8.1} GB", url_index_size as f64 * 105.0 / 1_073_741_824.0);
            eprintln!("Tag pairs:        {:>8.1} GB (need external sort)", pairs_size as f64 * 105.0 / 1_073_741_824.0);
            eprintln!("Total temp:       {:>8.1} GB", total_temp as f64 * 105.0 / 1_073_741_824.0);
            let est_total_s = total_ms as f64 * 105.0 / 1000.0;
            eprintln!("Est. wall time:   {:>8.1} min (linear extrapolation)", est_total_s / 60.0);
            eprintln!("  Note: tag sort at 105M (~3.7B pairs, ~29.6GB) requires external sort");
            eprintln!("  Note: assembly is CPU-bound — parallelizable across shard ranges");
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_w1_scalar_write,
    bench_w2_tag_pair_append,
    bench_w3_tag_sort,
    bench_w4_assembly,
    bench_w5_end_to_end,
    bench_w5_detailed,
);
criterion_main!(benches);
