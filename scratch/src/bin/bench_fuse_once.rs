//! Memory microbench for the per-snapshot fused-layer cache
//! (fix/fuse-once-per-snapshot, 2026-07-10 memory incident).
//!
//! Hypothesis: the pre-fix query path (per-query `fused_cow` over all bit
//! layers) allocates ~(dirty layers × base size) bytes on EVERY query, while
//! the fused-layer cache pays that once per SortField instance and serves
//! Arc bumps afterwards.
//!
//! Method: a counting global allocator measures GROSS allocated bytes (churn,
//! not peak) across K queries for:
//!   A) OLD path — per-query Vec<Cow> via layer_fused() (dirty layers clone)
//!   B) NEW path — fused_layers() cache (Arc bumps after first)
//!   C) end-to-end top_n ×K on the new code (sanity: absolute query cost)
//!
//! Scale: 8M slots / values in 0..65536 (16 populated layers, ~1MB each as
//! dense roaring) with a 50k-slot dirty diff touching all 16. Prod is 105M
//! slots (~13× layer size) — extrapolate linearly.
//!
//! Run: cargo run -p scratch --release --bin bench_fuse_once

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use roaring::RoaringBitmap;

struct CountingAlloc;
static ALLOCATED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATED.fetch_add(new_size as u64, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn allocated() -> u64 {
    ALLOCATED.load(Ordering::Relaxed)
}

fn gib(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0 * 1024.0)
}
fn mib(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

fn main() {
    use bitdex_v2::config::SortFieldConfig;
    use bitdex_v2::sort::SortField;

    const SLOTS: u32 = 8_000_000;
    const DIRTY: u32 = 50_000;
    const QUERIES: usize = 50;

    let config = SortFieldConfig {
        name: "reactionCount".to_string(),
        source_type: "uint32".to_string(),
        encoding: "linear".to_string(),
        bits: 32,
        eager_load: false,
        computed: None,
    };
    let mut sf = SortField::new(config);

    // Build 16 populated bit layers via or_layer (base path, stays clean).
    eprintln!("building {SLOTS} slots ...");
    let mut per_bit: Vec<RoaringBitmap> = (0..32).map(|_| RoaringBitmap::new()).collect();
    for slot in 0..SLOTS {
        let value = slot % 65_536;
        let mut v = value;
        while v != 0 {
            let bit = v.trailing_zeros() as usize;
            per_bit[bit].insert(slot);
            v &= v - 1;
        }
    }
    for (bit, bm) in per_bit.into_iter().enumerate() {
        if !bm.is_empty() {
            sf.or_layer(bit, &bm);
        }
    }
    // Dirty diff on all 16 low layers (metrics-poller-shaped churn).
    for i in 0..DIRTY {
        sf.insert(SLOTS + i, (SLOTS + i) % 65_536);
    }

    // 4M-slot candidate set (wide-window working set shape).
    let candidates: RoaringBitmap = (0..SLOTS).step_by(2).collect();
    eprintln!(
        "built: candidates={} dirty_diff={DIRTY} layers=32 (16 populated)",
        candidates.len()
    );

    // A) OLD path: per-query fuse via layer_fused (dirty layers materialize).
    let a0 = allocated();
    let t0 = Instant::now();
    for _ in 0..QUERIES {
        let layers: Vec<std::borrow::Cow<'_, RoaringBitmap>> =
            (0..32).filter_map(|b| sf.layer_fused(b)).collect();
        black_box(&layers);
    }
    let a_bytes = allocated() - a0;
    let a_time = t0.elapsed();

    // B) NEW path: fused-layer cache.
    let b0 = allocated();
    let t1 = Instant::now();
    for _ in 0..QUERIES {
        let fused = sf.fused_layers();
        black_box(&fused);
    }
    let b_bytes = allocated() - b0;
    let b_time = t1.elapsed();

    // C) end-to-end top_n on the new code (uses the cache internally).
    let c0 = allocated();
    let t2 = Instant::now();
    for _ in 0..QUERIES {
        let top = sf.top_n(&candidates, 200, true, None);
        black_box(&top);
    }
    let c_bytes = allocated() - c0;
    let c_time = t2.elapsed();

    println!("== bench_fuse_once (K={QUERIES} queries, {SLOTS} slots, 4M candidates) ==");
    println!(
        "A per-query fuse (old): {:>10.1} MiB gross ({:>8.2} MiB/query)  {:>8.1}ms",
        mib(a_bytes),
        mib(a_bytes) / QUERIES as f64,
        a_time.as_secs_f64() * 1000.0
    );
    println!(
        "B fused cache    (new): {:>10.1} MiB gross ({:>8.2} MiB/query)  {:>8.1}ms",
        mib(b_bytes),
        mib(b_bytes) / QUERIES as f64,
        b_time.as_secs_f64() * 1000.0
    );
    println!(
        "C top_n e2e      (new): {:>10.1} MiB gross ({:>8.2} MiB/query)  {:>8.1}ms",
        mib(c_bytes),
        mib(c_bytes) / QUERIES as f64,
        c_time.as_secs_f64() * 1000.0
    );
    println!(
        "fuse-churn reduction: {:.0}x  |  prod extrapolation (105M slots ≈ 13x layer size): \
         old ≈ {:.2} GiB per {QUERIES} queries vs new ≈ one-time {:.1} MiB per snapshot",
        a_bytes as f64 / b_bytes.max(1) as f64,
        gib(a_bytes) * 13.0,
        mib(b_bytes) * 13.0
    );
}
