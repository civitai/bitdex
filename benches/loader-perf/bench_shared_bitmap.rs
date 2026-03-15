//! Bench: shared DashMap<u32, Mutex<RoaringBitmap>> — zero merge.
//! Each thread inserts directly into shared bitmaps. 31K tags >> 8 threads = low contention.
//!
//! Run: cargo run -p scratch --release --bin bench_shared_bitmap

use std::hint::black_box;
use std::time::Instant;
use dashmap::DashMap;
use parking_lot::Mutex;
use roaring::RoaringBitmap;

const ROWS_PER_THREAD: usize = 2_000_000;
const NUM_TAGS: usize = 31_000;
const MAX_SLOT: u32 = 124_000_000;

fn main() {
    for num_threads in [1, 4, 8, 28] {
        let total_rows = ROWS_PER_THREAD * num_threads;
        let shared: DashMap<u32, Mutex<RoaringBitmap>> = DashMap::with_capacity(NUM_TAGS);

        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let map = &shared;
                s.spawn(move || {
                    let mut rng: u64 = 42 + t as u64 * 7919;
                    for _ in 0..ROWS_PER_THREAD {
                        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                        let tag = (rng >> 33) as u32 % NUM_TAGS as u32;
                        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                        let slot = (rng >> 32) as u32 % MAX_SLOT;

                        map.entry(tag)
                            .or_insert_with(|| Mutex::new(RoaringBitmap::new()))
                            .lock()
                            .insert(slot);
                    }
                });
            }
        });
        let elapsed = start.elapsed();
        let rate = total_rows as f64 / elapsed.as_secs_f64();
        let tags = shared.len();
        println!("{:2} threads: {:.1}M/s  ({:.2}s, {}M rows, {} tags) — extrapolated 4.48B: {:.0}s",
            num_threads, rate / 1e6, elapsed.as_secs_f64(),
            total_rows / 1_000_000, tags,
            4_480_000_000.0 / rate);
        black_box(&shared);
    }
}
