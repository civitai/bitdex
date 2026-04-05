/// Microbench: Phase 2 merge strategies for DataSilo
///
/// Compares two approaches for adding tags to existing docs:
/// A) Read-merge-write via mmap: read existing doc, merge tags, write merged doc back
/// B) Append-only ops log via mmap: just append merge ops, compact later
///
/// Simulates: 10M docs from phase 1, then phase 2 adds ~30 tags per doc (avg)
/// to test real-world merge performance at scale.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use datasilo::{DataSilo, SiloConfig};
use memmap2::MmapMut;

const NUM_DOCS: u32 = 2_000_000; // 2M docs (quick bench)
const AVG_TAGS_PER_DOC: usize = 30;
const DOC_SIZE: usize = 230; // typical BitDex doc
const TAG_MERGE_SIZE: usize = 250; // Mi([30 tags]) encoded

fn main() {
    println!("=== Merge Strategy Benchmark ===\n");
    println!("  {} docs, ~{} tags/doc\n", NUM_DOCS, AVG_TAGS_PER_DOC);

    // ── Strategy A: Read-Merge-Write via mmap ─────────────────────────
    // Phase 1: write initial docs
    // Phase 2: for each slot, read existing bytes, decode, merge tags, encode, write new
    println!("--- Strategy A: Read-Merge-Write (mmap) ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig {
            buffer_ratio: 1.3,
            min_entry_size: 256,
        }).unwrap();

        // Phase 1: bulk write via ParallelWriter
        let estimated_bytes = NUM_DOCS as u64 * 400;
        let pw = silo.prepare_parallel_writer(NUM_DOCS, estimated_bytes).unwrap();

        let t1 = Instant::now();
        // Simulate writing initial docs (just fill with DOC_SIZE bytes)
        let doc_bytes = vec![0xABu8; DOC_SIZE];
        for slot in 0..NUM_DOCS {
            pw.write(slot, &doc_bytes);
        }
        let phase1_write = t1.elapsed();
        let count = silo.finish_parallel_write(pw).unwrap();
        println!("  Phase 1 write: {:.3}s ({:.1}M docs/s)",
            phase1_write.as_secs_f64(),
            count as f64 / phase1_write.as_secs_f64() / 1e6);

        // Phase 2: read-merge-write
        // For each slot: read existing doc bytes, "merge" tags, write back via ops log
        let tag_bytes = vec![0xCDu8; TAG_MERGE_SIZE];
        let t2 = Instant::now();
        let mut merged_count = 0u64;
        // Batch to avoid per-op lock overhead
        let batch_size = 10_000;
        let mut batch: Vec<(u32, Vec<u8>)> = Vec::with_capacity(batch_size);

        for slot in 0..NUM_DOCS {
            // Read existing doc
            let existing = silo.get(slot);
            let _ = black_box(existing);

            // "Merge" — in reality we'd decode, add tags, re-encode
            // Simulate by creating merged bytes (existing + tags)
            let mut merged = Vec::with_capacity(DOC_SIZE + TAG_MERGE_SIZE);
            if let Some(data) = silo.get(slot) {
                merged.extend_from_slice(data);
            }
            merged.extend_from_slice(&tag_bytes);

            batch.push((slot, merged));
            if batch.len() >= batch_size {
                silo.append_ops_batch(&batch).unwrap();
                merged_count += batch.len() as u64;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            silo.append_ops_batch(&batch).unwrap();
            merged_count += batch.len() as u64;
        }
        let phase2_rmw = t2.elapsed();
        println!("  Phase 2 read-merge-write: {:.3}s ({:.1}M ops/s)",
            phase2_rmw.as_secs_f64(),
            merged_count as f64 / phase2_rmw.as_secs_f64() / 1e6);

        // Compact
        let t3 = Instant::now();
        let compacted = silo.compact().unwrap();
        let compact_time = t3.elapsed();
        println!("  Compact: {:.3}s ({} ops)", compact_time.as_secs_f64(), compacted);
        println!("  Total A: {:.3}s\n", (phase1_write + phase2_rmw + compact_time).as_secs_f64());
    }

    // ── Strategy B: Append-only ops log (write-only, compact later) ───
    // Phase 1: write initial docs as merge ops
    // Phase 2: write tag merge ops (append-only, no reading)
    // Then compact once at the end
    println!("--- Strategy B: Append-Only Ops (mmap'd ops log) ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig {
            buffer_ratio: 1.3,
            min_entry_size: 256,
        }).unwrap();

        // Phase 1: write initial docs as ops (no ParallelWriter)
        let doc_bytes = vec![0xABu8; DOC_SIZE];
        let t1 = Instant::now();
        let batch_size = 10_000;
        let mut batch: Vec<(u32, Vec<u8>)> = Vec::with_capacity(batch_size);
        for slot in 0..NUM_DOCS {
            batch.push((slot, doc_bytes.clone()));
            if batch.len() >= batch_size {
                silo.append_ops_batch(&batch).unwrap();
                batch.clear();
            }
        }
        if !batch.is_empty() {
            silo.append_ops_batch(&batch).unwrap();
        }
        let phase1_ops = t1.elapsed();
        println!("  Phase 1 ops write: {:.3}s ({:.1}M ops/s)",
            phase1_ops.as_secs_f64(),
            NUM_DOCS as f64 / phase1_ops.as_secs_f64() / 1e6);

        // Phase 2: append tag ops (write-only, no reading)
        let tag_bytes = vec![0xCDu8; TAG_MERGE_SIZE];
        let t2 = Instant::now();
        let mut batch: Vec<(u32, Vec<u8>)> = Vec::with_capacity(batch_size);
        for slot in 0..NUM_DOCS {
            batch.push((slot, tag_bytes.clone()));
            if batch.len() >= batch_size {
                silo.append_ops_batch(&batch).unwrap();
                batch.clear();
            }
        }
        if !batch.is_empty() {
            silo.append_ops_batch(&batch).unwrap();
        }
        let phase2_ops = t2.elapsed();
        println!("  Phase 2 ops append: {:.3}s ({:.1}M ops/s)",
            phase2_ops.as_secs_f64(),
            NUM_DOCS as f64 / phase2_ops.as_secs_f64() / 1e6);

        // Compact (must merge phase 1 + phase 2 ops per slot)
        let t3 = Instant::now();
        let compacted = silo.compact().unwrap();
        let compact_time = t3.elapsed();
        println!("  Compact: {:.3}s ({} ops)", compact_time.as_secs_f64(), compacted);
        println!("  Total B: {:.3}s\n", (phase1_ops + phase2_ops + compact_time).as_secs_f64());
    }

    // ── Strategy C: ParallelWriter phase 1 + ops phase 2 (hybrid) ─────
    println!("--- Strategy C: ParallelWriter phase 1 + Ops phase 2 (hybrid) ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig {
            buffer_ratio: 1.3,
            min_entry_size: 256,
        }).unwrap();

        // Phase 1: ParallelWriter (fast bulk)
        let estimated_bytes = NUM_DOCS as u64 * 400;
        let pw = silo.prepare_parallel_writer(NUM_DOCS, estimated_bytes).unwrap();
        let doc_bytes = vec![0xABu8; DOC_SIZE];

        let t1 = Instant::now();
        for slot in 0..NUM_DOCS {
            pw.write(slot, &doc_bytes);
        }
        let phase1_write = t1.elapsed();
        let count = silo.finish_parallel_write(pw).unwrap();
        println!("  Phase 1 ParallelWriter: {:.3}s ({:.1}M docs/s)",
            phase1_write.as_secs_f64(),
            count as f64 / phase1_write.as_secs_f64() / 1e6);

        // Phase 2: append tag ops (write-only, no reading)
        let tag_bytes = vec![0xCDu8; TAG_MERGE_SIZE];
        let t2 = Instant::now();
        let batch_size = 10_000;
        let mut batch: Vec<(u32, Vec<u8>)> = Vec::with_capacity(batch_size);
        for slot in 0..NUM_DOCS {
            batch.push((slot, tag_bytes.clone()));
            if batch.len() >= batch_size {
                silo.append_ops_batch(&batch).unwrap();
                batch.clear();
            }
        }
        if !batch.is_empty() {
            silo.append_ops_batch(&batch).unwrap();
        }
        let phase2_ops = t2.elapsed();
        println!("  Phase 2 ops append: {:.3}s ({:.1}M ops/s)",
            phase2_ops.as_secs_f64(),
            NUM_DOCS as f64 / phase2_ops.as_secs_f64() / 1e6);

        // Compact
        let t3 = Instant::now();
        let compacted = silo.compact().unwrap();
        let compact_time = t3.elapsed();
        println!("  Compact: {:.3}s ({} ops)", compact_time.as_secs_f64(), compacted);
        println!("  Total C: {:.3}s\n", (phase1_write + phase2_ops + compact_time).as_secs_f64());
    }

    // ── Strategy D: Hybrid with disk-only ops (no HashMap) ──────────────
    println!("--- Strategy D: ParallelWriter phase 1 + Disk-Only Ops phase 2 ---");
    {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DataSilo::open(dir.path(), SiloConfig {
            buffer_ratio: 1.3,
            min_entry_size: 256,
        }).unwrap();

        // Phase 1: ParallelWriter (fast bulk)
        let estimated_bytes = NUM_DOCS as u64 * 400;
        let pw = silo.prepare_parallel_writer(NUM_DOCS, estimated_bytes).unwrap();
        let doc_bytes = vec![0xABu8; DOC_SIZE];

        let t1 = Instant::now();
        for slot in 0..NUM_DOCS {
            pw.write(slot, &doc_bytes);
        }
        let phase1_write = t1.elapsed();
        let count = silo.finish_parallel_write(pw).unwrap();
        println!("  Phase 1 ParallelWriter: {:.3}s ({:.1}M docs/s)",
            phase1_write.as_secs_f64(),
            count as f64 / phase1_write.as_secs_f64() / 1e6);

        // Phase 2: disk-only ops (NO HashMap overhead)
        let tag_bytes = vec![0xCDu8; TAG_MERGE_SIZE];
        let t2 = Instant::now();
        let batch_size = 10_000;
        let mut batch: Vec<(u32, Vec<u8>)> = Vec::with_capacity(batch_size);
        for slot in 0..NUM_DOCS {
            batch.push((slot, tag_bytes.clone()));
            if batch.len() >= batch_size {
                silo.append_ops_disk_only(&batch).unwrap();
                batch.clear();
            }
        }
        if !batch.is_empty() {
            silo.append_ops_disk_only(&batch).unwrap();
        }
        let phase2_ops = t2.elapsed();
        println!("  Phase 2 disk-only ops: {:.3}s ({:.1}M ops/s)",
            phase2_ops.as_secs_f64(),
            NUM_DOCS as f64 / phase2_ops.as_secs_f64() / 1e6);

        // Compact (reads ops log from disk, merges with data file)
        let t3 = Instant::now();
        let compacted = silo.compact().unwrap();
        let compact_time = t3.elapsed();
        println!("  Compact: {:.3}s ({} ops)", compact_time.as_secs_f64(), compacted);
        println!("  Total D: {:.3}s\n", (phase1_write + phase2_ops + compact_time).as_secs_f64());
    }

    println!("=== Done ===");
}
