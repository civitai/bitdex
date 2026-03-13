//! Slot loader microbenchmark.
//!
//! Validates the "pre-allocated fixed-size slot" strategy for bulk loading.
//! Each document gets a 512-byte slot in a memory-mapped file. Multiple table
//! streams write to different field offsets concurrently. A finalization pass
//! reads populated slots, serializes to msgpack, and compresses with zstd.
//!
//! Workloads:
//!   W1: Sequential scalar writes (Image table stream)
//!   W2: Random tag writes (TagsOnImageNew stream)
//!   W3: Concurrent W1 + W2
//!   W4: Finalization (read + msgpack + zstd compress)
//!   W5: End-to-end pipeline

use std::fs::{self, File};
use std::io::Write as IoWrite;
use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use memmap2::MmapMut;
use rand::distributions::Uniform;
use rand::prelude::*;
use serde::Serialize;
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Slot layout — 512 bytes, packed, raw byte offsets
// ---------------------------------------------------------------------------
//
// Offset  Size  Field
// ------  ----  -----
//   0       8   present_mask (u64 LE)
//   8       8   image_id (u64 LE)
//  16       1   nsfw_level (u8)
//  17       8   user_id (u64 LE)
//  25       1   image_type (u8)
//  26       8   sort_at (u64 LE)
//  34       1   poi (u8)
//  35       1   minor (u8)
//  36      80   url ([u8; 80], first byte = length)
// 116      40   hash ([u8; 40], first byte = length)
// 156       1   tag_count (u8)
// 157     192   tag_ids ([u32; 48] LE)
// 349       1   mv_count (u8)
// 350      32   model_version_ids ([u32; 8] LE)
// 382       1   tool_count (u8)
// 383      32   tool_ids ([u32; 8] LE)
// 415       1   technique_count (u8)
// 416      16   technique_ids ([u32; 4] LE)
// 432      80   _padding
// ---     ---
// 512     total

const SLOT_SIZE: usize = 512;
const DOC_COUNT: usize = 1_000_000;
const SHARD_SIZE: usize = 512; // docs per final shard

// Field offsets
const OFF_PRESENT: usize = 0;
const OFF_IMAGE_ID: usize = 8;
const OFF_NSFW: usize = 16;
const OFF_USER_ID: usize = 17;
const OFF_IMAGE_TYPE: usize = 25;
const OFF_SORT_AT: usize = 26;
const OFF_POI: usize = 34;
const OFF_MINOR: usize = 35;
const OFF_URL: usize = 36;
const OFF_HASH: usize = 116;
const OFF_TAG_COUNT: usize = 156;
const OFF_TAG_IDS: usize = 157;
const OFF_MV_COUNT: usize = 349;
const OFF_MV_IDS: usize = 350;
const OFF_TOOL_COUNT: usize = 382;
const OFF_TOOL_IDS: usize = 383;
const OFF_TECH_COUNT: usize = 415;
const OFF_TECH_IDS: usize = 416;

// Present mask bits
const MASK_IMAGE_ID: u64 = 1 << 0;
const MASK_NSFW: u64 = 1 << 1;
const MASK_USER_ID: u64 = 1 << 2;
const MASK_IMAGE_TYPE: u64 = 1 << 3;
const MASK_SORT_AT: u64 = 1 << 4;
const MASK_POI: u64 = 1 << 5;
const MASK_MINOR: u64 = 1 << 6;
const MASK_URL: u64 = 1 << 7;
const MASK_HASH: u64 = 1 << 8;
const MASK_TAGS: u64 = 1 << 9;
const MASK_MV: u64 = 1 << 10;
const MASK_TOOLS: u64 = 1 << 11;
const MASK_TECHNIQUES: u64 = 1 << 12;

const SCALAR_MASK: u64 = MASK_IMAGE_ID
    | MASK_NSFW
    | MASK_USER_ID
    | MASK_IMAGE_TYPE
    | MASK_SORT_AT
    | MASK_POI
    | MASK_MINOR
    | MASK_URL
    | MASK_HASH;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_mmap(path: &Path, num_docs: usize) -> MmapMut {
    let file_size = num_docs * SLOT_SIZE;
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("create mmap file");
    file.set_len(file_size as u64).expect("set file length");
    unsafe { MmapMut::map_mut(&file).expect("mmap") }
}

#[inline]
fn write_u64_le(mmap: &mut MmapMut, offset: usize, val: u64) {
    mmap[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
}

#[inline]
fn read_u64_le(mmap: &MmapMut, offset: usize) -> u64 {
    u64::from_le_bytes(mmap[offset..offset + 8].try_into().unwrap())
}

#[inline]
fn write_u32_le(mmap: &mut MmapMut, offset: usize, val: u32) {
    mmap[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline]
fn read_u32_le(mmap: &MmapMut, offset: usize) -> u32 {
    u32::from_le_bytes(mmap[offset..offset + 4].try_into().unwrap())
}

/// Write a length-prefixed inline string (first byte = len, rest = UTF-8 bytes).
#[inline]
fn write_inline_str(mmap: &mut MmapMut, offset: usize, max_len: usize, s: &[u8]) {
    let len = s.len().min(max_len - 1);
    mmap[offset] = len as u8;
    mmap[offset + 1..offset + 1 + len].copy_from_slice(&s[..len]);
}

/// Generate tag assignments: returns Vec<(tag_id, Vec<doc_id>)>.
/// 1000 unique tags, each assigned to ~1000 random docs (with overlap).
fn generate_tag_assignments(rng: &mut StdRng) -> Vec<(u32, Vec<u32>)> {
    let doc_dist = Uniform::new(0u32, DOC_COUNT as u32);
    let count_dist = Uniform::new(800u32, 1200);
    (0..1000u32)
        .map(|tag_id| {
            let n = count_dist.sample(rng) as usize;
            let docs: Vec<u32> = (0..n).map(|_| doc_dist.sample(rng)).collect();
            (tag_id, docs)
        })
        .collect()
}

/// Per-doc tag additions: returns Vec<(doc_id, Vec<tag_id>)> for more realistic load.
/// Average ~35 tags per doc, max 48.
fn generate_per_doc_tags(rng: &mut StdRng) -> Vec<Vec<u32>> {
    let tag_dist = Uniform::new(0u32, 1000);
    let count_dist = Uniform::new(20u32, 48);
    (0..DOC_COUNT)
        .map(|_| {
            let n = count_dist.sample(rng) as usize;
            (0..n).map(|_| tag_dist.sample(rng)).collect()
        })
        .collect()
}

// Serializable doc for finalization
#[derive(Serialize, Deserialize)]
struct FinalDoc {
    #[serde(skip_serializing_if = "Option::is_none")]
    image_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nsfw_level: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image_type: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    poi: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    minor: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tag_ids: Vec<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    model_version_ids: Vec<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tool_ids: Vec<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    technique_ids: Vec<u32>,
}

fn read_slot_to_doc(mmap: &MmapMut, doc_id: usize) -> FinalDoc {
    let base = doc_id * SLOT_SIZE;
    let mask = read_u64_le(mmap, base + OFF_PRESENT);

    let image_id = if mask & MASK_IMAGE_ID != 0 {
        Some(read_u64_le(mmap, base + OFF_IMAGE_ID))
    } else {
        None
    };
    let nsfw_level = if mask & MASK_NSFW != 0 {
        Some(mmap[base + OFF_NSFW])
    } else {
        None
    };
    let user_id = if mask & MASK_USER_ID != 0 {
        Some(read_u64_le(mmap, base + OFF_USER_ID))
    } else {
        None
    };
    let image_type = if mask & MASK_IMAGE_TYPE != 0 {
        Some(mmap[base + OFF_IMAGE_TYPE])
    } else {
        None
    };
    let sort_at = if mask & MASK_SORT_AT != 0 {
        Some(read_u64_le(mmap, base + OFF_SORT_AT))
    } else {
        None
    };
    let poi = if mask & MASK_POI != 0 {
        Some(mmap[base + OFF_POI] != 0)
    } else {
        None
    };
    let minor = if mask & MASK_MINOR != 0 {
        Some(mmap[base + OFF_MINOR] != 0)
    } else {
        None
    };
    let url = if mask & MASK_URL != 0 {
        let len = mmap[base + OFF_URL] as usize;
        if len > 0 {
            Some(
                String::from_utf8_lossy(&mmap[base + OFF_URL + 1..base + OFF_URL + 1 + len])
                    .into_owned(),
            )
        } else {
            None
        }
    } else {
        None
    };
    let hash = if mask & MASK_HASH != 0 {
        let len = mmap[base + OFF_HASH] as usize;
        if len > 0 {
            Some(
                String::from_utf8_lossy(&mmap[base + OFF_HASH + 1..base + OFF_HASH + 1 + len])
                    .into_owned(),
            )
        } else {
            None
        }
    } else {
        None
    };

    let tag_count = if mask & MASK_TAGS != 0 {
        mmap[base + OFF_TAG_COUNT] as usize
    } else {
        0
    };
    let tag_ids: Vec<u32> = (0..tag_count)
        .map(|i| read_u32_le(mmap, base + OFF_TAG_IDS + i * 4))
        .collect();

    let mv_count = if mask & MASK_MV != 0 {
        mmap[base + OFF_MV_COUNT] as usize
    } else {
        0
    };
    let model_version_ids: Vec<u32> = (0..mv_count)
        .map(|i| read_u32_le(mmap, base + OFF_MV_IDS + i * 4))
        .collect();

    let tool_count = if mask & MASK_TOOLS != 0 {
        mmap[base + OFF_TOOL_COUNT] as usize
    } else {
        0
    };
    let tool_ids: Vec<u32> = (0..tool_count)
        .map(|i| read_u32_le(mmap, base + OFF_TOOL_IDS + i * 4))
        .collect();

    let tech_count = if mask & MASK_TECHNIQUES != 0 {
        mmap[base + OFF_TECH_COUNT] as usize
    } else {
        0
    };
    let technique_ids: Vec<u32> = (0..tech_count)
        .map(|i| read_u32_le(mmap, base + OFF_TECH_IDS + i * 4))
        .collect();

    FinalDoc {
        image_id,
        nsfw_level,
        user_id,
        image_type,
        sort_at,
        poi,
        minor,
        url,
        hash,
        tag_ids,
        model_version_ids,
        tool_ids,
        technique_ids,
    }
}

// ---------------------------------------------------------------------------
// W1: Image stream — sequential scalar writes
// ---------------------------------------------------------------------------

fn w1_image_stream(mmap: &mut MmapMut) {
    let fake_url = b"https://image.civitai.com/xG1nkqKTMzGDvpLrqFT7WA/abcd1234-5678-90ab-cdef/w=400";
    let fake_hash = b"UKJ8_4%2t7WBayj[j[fQ~qRjayj[";

    for doc_id in 0..DOC_COUNT {
        let base = doc_id * SLOT_SIZE;

        // Write present mask (scalars only for this stream)
        write_u64_le(mmap, base + OFF_PRESENT, SCALAR_MASK);

        // Scalars
        write_u64_le(mmap, base + OFF_IMAGE_ID, doc_id as u64);
        mmap[base + OFF_NSFW] = ((doc_id % 32) + 1) as u8;
        write_u64_le(mmap, base + OFF_USER_ID, (doc_id * 7 + 13) as u64);
        mmap[base + OFF_IMAGE_TYPE] = (doc_id % 3) as u8;
        write_u64_le(mmap, base + OFF_SORT_AT, 1700000000u64 + doc_id as u64);
        mmap[base + OFF_POI] = (doc_id % 20 == 0) as u8;
        mmap[base + OFF_MINOR] = (doc_id % 100 == 0) as u8;

        // Inline strings
        write_inline_str(mmap, base + OFF_URL, 80, fake_url);
        write_inline_str(mmap, base + OFF_HASH, 40, fake_hash);
    }
}

// ---------------------------------------------------------------------------
// W2: Tag stream — random slot writes
// ---------------------------------------------------------------------------

fn w2_tag_stream(mmap: &mut MmapMut, tags: &[(u32, Vec<u32>)]) -> usize {
    let mut total_insertions = 0usize;

    for (tag_id, doc_ids) in tags {
        for &doc_id in doc_ids {
            let base = doc_id as usize * SLOT_SIZE;

            // Read current tag count
            let count = mmap[base + OFF_TAG_COUNT] as usize;
            if count >= 48 {
                continue; // slot full
            }

            // Write the tag
            write_u32_le(mmap, base + OFF_TAG_IDS + count * 4, *tag_id);
            mmap[base + OFF_TAG_COUNT] = (count + 1) as u8;

            // Update present mask to include tags
            let mask = read_u64_le(mmap, base + OFF_PRESENT);
            if mask & MASK_TAGS == 0 {
                write_u64_le(mmap, base + OFF_PRESENT, mask | MASK_TAGS);
            }

            total_insertions += 1;
        }
    }

    total_insertions
}

// ---------------------------------------------------------------------------
// W2b: Per-doc tag writes (for concurrent test — each doc written once)
// ---------------------------------------------------------------------------

fn w2b_per_doc_tags(mmap: &mut MmapMut, per_doc_tags: &[Vec<u32>]) -> usize {
    let mut total = 0usize;
    for (doc_id, tags) in per_doc_tags.iter().enumerate() {
        let base = doc_id * SLOT_SIZE;
        let n = tags.len().min(48);
        mmap[base + OFF_TAG_COUNT] = n as u8;
        for (i, &tag_id) in tags[..n].iter().enumerate() {
            write_u32_le(mmap, base + OFF_TAG_IDS + i * 4, tag_id);
        }
        // Update present mask
        let mask = read_u64_le(mmap, base + OFF_PRESENT);
        write_u64_le(mmap, base + OFF_PRESENT, mask | MASK_TAGS);
        total += n;
    }
    total
}

// ---------------------------------------------------------------------------
// W4: Finalization pass
// ---------------------------------------------------------------------------

fn w4_finalize(mmap: &MmapMut, output_dir: &Path) -> (usize, usize) {
    let num_shards = (DOC_COUNT + SHARD_SIZE - 1) / SHARD_SIZE;
    let mut total_docs = 0usize;
    let mut total_bytes = 0usize;

    for shard_idx in 0..num_shards {
        let start = shard_idx * SHARD_SIZE;
        let end = (start + SHARD_SIZE).min(DOC_COUNT);

        // Serialize all docs in this shard to msgpack
        let mut shard_buf = Vec::with_capacity(SHARD_SIZE * 256);
        for doc_id in start..end {
            let doc = read_slot_to_doc(mmap, doc_id);
            let encoded = rmp_serde::to_vec(&doc).expect("msgpack encode");
            // Length-prefix each doc
            shard_buf.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            shard_buf.extend_from_slice(&encoded);
            total_docs += 1;
        }

        // Compress the shard
        let compressed = zstd::encode_all(shard_buf.as_slice(), 1).expect("zstd compress");
        total_bytes += compressed.len();

        // Write to disk
        let shard_path = output_dir.join(format!("shard_{:06}.zst", shard_idx));
        let mut f = File::create(shard_path).expect("create shard file");
        f.write_all(&compressed).expect("write shard");
    }

    (total_docs, total_bytes)
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_w1_image_stream(c: &mut Criterion) {
    let dir = tempdir().expect("tempdir");
    let slot_path = dir.path().join("slots.bin");

    let mut group = c.benchmark_group("slot_loader");
    group.throughput(Throughput::Elements(DOC_COUNT as u64));
    group.sample_size(10);

    group.bench_function("W1_image_stream_sequential", |b| {
        b.iter(|| {
            let mut mmap = create_mmap(&slot_path, DOC_COUNT);
            w1_image_stream(&mut mmap);
            mmap.flush().ok();
        });
    });

    group.finish();
}

fn bench_w2_tag_stream(c: &mut Criterion) {
    let dir = tempdir().expect("tempdir");
    let slot_path = dir.path().join("slots.bin");

    let mut rng = StdRng::seed_from_u64(42);
    let tags = generate_tag_assignments(&mut rng);
    let total_tag_insertions: usize = tags.iter().map(|(_, docs)| docs.len()).sum();

    let mut group = c.benchmark_group("slot_loader");
    group.throughput(Throughput::Elements(total_tag_insertions as u64));
    group.sample_size(10);

    group.bench_function("W2_tag_stream_random", |b| {
        b.iter(|| {
            let mut mmap = create_mmap(&slot_path, DOC_COUNT);
            w2_tag_stream(&mut mmap, &tags);
        });
    });

    group.finish();
}

fn bench_w3_concurrent(c: &mut Criterion) {
    let dir = tempdir().expect("tempdir");
    let slot_path = dir.path().join("slots.bin");

    let mut rng = StdRng::seed_from_u64(42);
    let per_doc_tags = generate_per_doc_tags(&mut rng);

    let mut group = c.benchmark_group("slot_loader");
    group.throughput(Throughput::Elements(DOC_COUNT as u64));
    group.sample_size(10);

    // W1 alone (baseline)
    group.bench_function("W1_alone_baseline", |b| {
        b.iter(|| {
            let mut mmap = create_mmap(&slot_path, DOC_COUNT);
            w1_image_stream(&mut mmap);
        });
    });

    // W3: Concurrent W1 + W2 using two threads writing to non-overlapping field offsets.
    // Since MmapMut isn't Sync, we use unsafe raw pointer sharing (benchmark only).
    group.bench_function("W3_concurrent_image_plus_tags", |b| {
        b.iter(|| {
            let mut mmap = create_mmap(&slot_path, DOC_COUNT);
            let ptr = mmap.as_mut_ptr();
            let len = mmap.len();

            // SAFETY: Two threads write to non-overlapping byte ranges within each slot.
            // W1 writes offsets 0-155, W2 writes offsets 156-348.
            // present_mask (offset 0) is technically shared, but W1 writes it first
            // and W2 only ORs in MASK_TAGS — for the benchmark we accept this race.
            let raw = ptr as usize;

            std::thread::scope(|s| {
                // Thread 1: image scalars
                s.spawn(|| {
                    let mmap_ptr = raw as *mut u8;
                    let slice = unsafe { std::slice::from_raw_parts_mut(mmap_ptr, len) };
                    // Re-implement w1 on raw slice
                    let fake_url = b"https://image.civitai.com/xG1nkqKTMzGDvpLrqFT7WA/abcd1234-5678-90ab-cdef/w=400";
                    let fake_hash = b"UKJ8_4%2t7WBayj[j[fQ~qRjayj[";
                    for doc_id in 0..DOC_COUNT {
                        let base = doc_id * SLOT_SIZE;
                        slice[base + OFF_PRESENT..base + OFF_PRESENT + 8]
                            .copy_from_slice(&SCALAR_MASK.to_le_bytes());
                        slice[base + OFF_IMAGE_ID..base + OFF_IMAGE_ID + 8]
                            .copy_from_slice(&(doc_id as u64).to_le_bytes());
                        slice[base + OFF_NSFW] = ((doc_id % 32) + 1) as u8;
                        slice[base + OFF_USER_ID..base + OFF_USER_ID + 8]
                            .copy_from_slice(&((doc_id * 7 + 13) as u64).to_le_bytes());
                        slice[base + OFF_IMAGE_TYPE] = (doc_id % 3) as u8;
                        slice[base + OFF_SORT_AT..base + OFF_SORT_AT + 8]
                            .copy_from_slice(&(1700000000u64 + doc_id as u64).to_le_bytes());
                        slice[base + OFF_POI] = (doc_id % 20 == 0) as u8;
                        slice[base + OFF_MINOR] = (doc_id % 100 == 0) as u8;
                        // url
                        let url_len = fake_url.len().min(79);
                        slice[base + OFF_URL] = url_len as u8;
                        slice[base + OFF_URL + 1..base + OFF_URL + 1 + url_len]
                            .copy_from_slice(&fake_url[..url_len]);
                        // hash
                        let hash_len = fake_hash.len().min(39);
                        slice[base + OFF_HASH] = hash_len as u8;
                        slice[base + OFF_HASH + 1..base + OFF_HASH + 1 + hash_len]
                            .copy_from_slice(&fake_hash[..hash_len]);
                    }
                });

                // Thread 2: tags (per-doc, non-overlapping with scalars)
                s.spawn(|| {
                    let mmap_ptr = raw as *mut u8;
                    let slice = unsafe { std::slice::from_raw_parts_mut(mmap_ptr, len) };
                    for (doc_id, tags) in per_doc_tags.iter().enumerate() {
                        let base = doc_id * SLOT_SIZE;
                        let n = tags.len().min(48);
                        slice[base + OFF_TAG_COUNT] = n as u8;
                        for (i, &tag_id) in tags[..n].iter().enumerate() {
                            let off = base + OFF_TAG_IDS + i * 4;
                            slice[off..off + 4].copy_from_slice(&tag_id.to_le_bytes());
                        }
                        // OR in tag mask — read-modify-write race with W1 on present_mask
                        // For bench purposes this is acceptable
                        let mut mask_bytes = [0u8; 8];
                        mask_bytes.copy_from_slice(&slice[base + OFF_PRESENT..base + OFF_PRESENT + 8]);
                        let mask = u64::from_le_bytes(mask_bytes) | MASK_TAGS;
                        slice[base + OFF_PRESENT..base + OFF_PRESENT + 8]
                            .copy_from_slice(&mask.to_le_bytes());
                    }
                });
            });

            mmap.flush().ok();
        });
    });

    group.finish();
}

fn bench_w4_finalize(c: &mut Criterion) {
    let dir = tempdir().expect("tempdir");
    let slot_path = dir.path().join("slots.bin");
    let output_dir = dir.path().join("shards");
    fs::create_dir_all(&output_dir).expect("create output dir");

    // Pre-populate the mmap with realistic data
    let mut mmap = create_mmap(&slot_path, DOC_COUNT);
    w1_image_stream(&mut mmap);

    let mut rng = StdRng::seed_from_u64(42);
    let per_doc_tags = generate_per_doc_tags(&mut rng);
    w2b_per_doc_tags(&mut mmap, &per_doc_tags);
    mmap.flush().ok();

    let mut group = c.benchmark_group("slot_loader");
    group.throughput(Throughput::Elements(DOC_COUNT as u64));
    group.sample_size(10);

    group.bench_function("W4_finalize_msgpack_zstd", |b| {
        b.iter(|| {
            // Clean output dir
            for entry in fs::read_dir(&output_dir).unwrap() {
                fs::remove_file(entry.unwrap().path()).ok();
            }
            let (docs, bytes) = w4_finalize(&mmap, &output_dir);
            assert_eq!(docs, DOC_COUNT);
            bytes
        });
    });

    group.finish();
}

fn bench_w5_end_to_end(c: &mut Criterion) {
    let dir = tempdir().expect("tempdir");
    let slot_path = dir.path().join("slots.bin");
    let output_dir = dir.path().join("shards");
    fs::create_dir_all(&output_dir).expect("create output dir");

    let mut rng = StdRng::seed_from_u64(42);
    let per_doc_tags = generate_per_doc_tags(&mut rng);

    let mut group = c.benchmark_group("slot_loader");
    group.throughput(Throughput::Elements(DOC_COUNT as u64));
    group.sample_size(10);

    group.bench_function("W5_end_to_end_pipeline", |b| {
        b.iter(|| {
            // Create fresh mmap
            let mut mmap = create_mmap(&slot_path, DOC_COUNT);
            let ptr = mmap.as_mut_ptr();
            let len = mmap.len();
            let raw = ptr as usize;

            // Phase 1: Concurrent scalar + tag writes
            let fake_url = b"https://image.civitai.com/xG1nkqKTMzGDvpLrqFT7WA/abcd1234-5678-90ab-cdef/w=400";
            let fake_hash = b"UKJ8_4%2t7WBayj[j[fQ~qRjayj[";

            std::thread::scope(|s| {
                s.spawn(|| {
                    let mmap_ptr = raw as *mut u8;
                    let slice = unsafe { std::slice::from_raw_parts_mut(mmap_ptr, len) };
                    for doc_id in 0..DOC_COUNT {
                        let base = doc_id * SLOT_SIZE;
                        slice[base + OFF_PRESENT..base + OFF_PRESENT + 8]
                            .copy_from_slice(&SCALAR_MASK.to_le_bytes());
                        slice[base + OFF_IMAGE_ID..base + OFF_IMAGE_ID + 8]
                            .copy_from_slice(&(doc_id as u64).to_le_bytes());
                        slice[base + OFF_NSFW] = ((doc_id % 32) + 1) as u8;
                        slice[base + OFF_USER_ID..base + OFF_USER_ID + 8]
                            .copy_from_slice(&((doc_id * 7 + 13) as u64).to_le_bytes());
                        slice[base + OFF_IMAGE_TYPE] = (doc_id % 3) as u8;
                        slice[base + OFF_SORT_AT..base + OFF_SORT_AT + 8]
                            .copy_from_slice(&(1700000000u64 + doc_id as u64).to_le_bytes());
                        slice[base + OFF_POI] = (doc_id % 20 == 0) as u8;
                        slice[base + OFF_MINOR] = (doc_id % 100 == 0) as u8;
                        let url_len = fake_url.len().min(79);
                        slice[base + OFF_URL] = url_len as u8;
                        slice[base + OFF_URL + 1..base + OFF_URL + 1 + url_len]
                            .copy_from_slice(&fake_url[..url_len]);
                        let hash_len = fake_hash.len().min(39);
                        slice[base + OFF_HASH] = hash_len as u8;
                        slice[base + OFF_HASH + 1..base + OFF_HASH + 1 + hash_len]
                            .copy_from_slice(&fake_hash[..hash_len]);
                    }
                });

                s.spawn(|| {
                    let mmap_ptr = raw as *mut u8;
                    let slice = unsafe { std::slice::from_raw_parts_mut(mmap_ptr, len) };
                    for (doc_id, tags) in per_doc_tags.iter().enumerate() {
                        let base = doc_id * SLOT_SIZE;
                        let n = tags.len().min(48);
                        slice[base + OFF_TAG_COUNT] = n as u8;
                        for (i, &tag_id) in tags[..n].iter().enumerate() {
                            let off = base + OFF_TAG_IDS + i * 4;
                            slice[off..off + 4].copy_from_slice(&tag_id.to_le_bytes());
                        }
                        let mut mask_bytes = [0u8; 8];
                        mask_bytes.copy_from_slice(&slice[base + OFF_PRESENT..base + OFF_PRESENT + 8]);
                        let mask = u64::from_le_bytes(mask_bytes) | MASK_TAGS;
                        slice[base + OFF_PRESENT..base + OFF_PRESENT + 8]
                            .copy_from_slice(&mask.to_le_bytes());
                    }
                });
            });

            // Phase 2: Finalization
            for entry in fs::read_dir(&output_dir).unwrap() {
                fs::remove_file(entry.unwrap().path()).ok();
            }
            let (docs, bytes) = w4_finalize(&mmap, &output_dir);
            assert_eq!(docs, DOC_COUNT);
            bytes
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_w1_image_stream,
    bench_w2_tag_stream,
    bench_w3_concurrent,
    bench_w4_finalize,
    bench_w5_end_to_end,
);
criterion_main!(benches);
