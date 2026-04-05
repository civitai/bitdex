//! Parse pipeline allocation strategy microbenchmarks.
//!
//! Measures per-row allocator overhead in the dump processor parse pipeline.
//! Each strategy is isolated to measure allocation cost only, not compute cost.
//!
//! Strategies benchmarked:
//!   S1: Baseline — Vec::new() per row (current behaviour for parse_delimited_line)
//!   S2: clear() reuse — one Vec allocated per thread, cleared each row (already done
//!       for indexed_fields_buf / enriched_buf, but NOT for parse_delimited_line)
//!   S3: ArrayVec<32> on the stack — zero heap allocation for <=32 columns
//!   S4: Fixed-size [Option<&str>; 64] array for indexed fields (replaces Vec<Option<&str>>)
//!   S5: Duplicate sort-val HashMap elimination — the row loop builds `row_sv` twice;
//!       measure cost of the second build vs reusing the first.
//!
//! Run with:
//!   cargo bench --bench parse_alloc_bench

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Simulated CSV data
// ---------------------------------------------------------------------------

/// Build a realistic mocked CSV line like the images dump.
/// ~20 tab-separated fields including a long GUID-style field.
fn make_csv_line() -> Vec<u8> {
    b"107834521\t42\t8675309\t2\t1711720800\t0\t1\t\
xG1nkqKTMzGDvpLrqFT7WA/a1b2c3d4-e5f6-7890-abcd-ef1234567890/width=400/image.jpeg\t\
32\t1\t0\t0\t1711720799\t1711634400\t0\t0\t1\t0\t3\t256"
        .to_vec()
}

/// Build a wider CSV line simulating the tags or resources dump (~8 fields).
fn make_narrow_csv_line() -> Vec<u8> {
    b"107834521\t42\t8675309\t2\t1711720800\t0\t1\t32".to_vec()
}

const NUM_ROWS: u64 = 100_000;

// ---------------------------------------------------------------------------
// S1: Baseline — Vec::new() every row (current parse_delimited_line behaviour)
// ---------------------------------------------------------------------------

#[inline(never)]
fn parse_delimited_alloc_every_row(line: &[u8], delimiter: u8) -> Vec<&[u8]> {
    let mut fields = Vec::new(); // heap allocation every call
    let mut start = 0;
    let mut in_quotes = false;
    let line = line.strip_suffix(&[b'\r']).unwrap_or(line);
    for i in 0..line.len() {
        match line[i] {
            b'"' => in_quotes = !in_quotes,
            d if d == delimiter && !in_quotes => {
                fields.push(&line[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    fields.push(&line[start..]);
    fields
}

// ---------------------------------------------------------------------------
// S2: Reuse buffer — clear() + refill, one Vec per thread, no heap alloc
// ---------------------------------------------------------------------------

#[inline(never)]
fn parse_delimited_reuse<'a>(line: &'a [u8], delimiter: u8, buf: &mut Vec<&'a [u8]>) {
    buf.clear(); // no deallocation — just sets len=0
    let mut start = 0;
    let mut in_quotes = false;
    let line = line.strip_suffix(&[b'\r']).unwrap_or(line);
    for i in 0..line.len() {
        match line[i] {
            b'"' => in_quotes = !in_quotes,
            d if d == delimiter && !in_quotes => {
                buf.push(&line[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    buf.push(&line[start..]);
}

// ---------------------------------------------------------------------------
// S3: Stack-allocated array — ArrayVec simulation via fixed [MaybeUninit; 32]
// We simulate arrayvec without the crate by using a fixed-size array + manual len.
// ---------------------------------------------------------------------------

struct StackFields<'a, const N: usize> {
    data: [*const u8; N], // raw pointer to slice start
    lens: [usize; N],     // slice lengths
    count: usize,
    // Phantom to tie lifetime to the input slice
    _marker: std::marker::PhantomData<&'a [u8]>,
}

impl<'a, const N: usize> StackFields<'a, N> {
    #[inline]
    fn new() -> Self {
        Self {
            data: [std::ptr::null(); N],
            lens: [0; N],
            count: 0,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    fn push(&mut self, s: &'a [u8]) -> bool {
        if self.count >= N {
            return false; // overflow
        }
        self.data[self.count] = s.as_ptr();
        self.lens[self.count] = s.len();
        self.count += 1;
        true
    }

    #[inline]
    fn len(&self) -> usize {
        self.count
    }

    #[inline]
    fn get(&self, i: usize) -> Option<&'a [u8]> {
        if i >= self.count {
            return None;
        }
        Some(unsafe { std::slice::from_raw_parts(self.data[i], self.lens[i]) })
    }
}

#[inline(never)]
fn parse_delimited_stack<'a>(line: &'a [u8], delimiter: u8, out: &mut StackFields<'a, 32>) {
    out.count = 0;
    let mut start = 0;
    let mut in_quotes = false;
    let line = line.strip_suffix(&[b'\r']).unwrap_or(line);
    for i in 0..line.len() {
        match line[i] {
            b'"' => in_quotes = !in_quotes,
            d if d == delimiter && !in_quotes => {
                out.push(&line[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&line[start..]);
}

// ---------------------------------------------------------------------------
// S4: Fixed-size [Option<&str>; 64] for indexed fields instead of Vec<Option<&str>>
// ---------------------------------------------------------------------------

#[inline(never)]
fn fill_indexed_fields_vec<'a>(raw_fields: &[&'a [u8]], buf: &mut Vec<Option<&'a str>>) {
    buf.clear();
    for bytes in raw_fields {
        buf.push(bytes_to_str(bytes));
    }
}

#[inline(never)]
fn fill_indexed_fields_array<'a>(
    raw_fields: &[&'a [u8]],
    buf: &mut [Option<&'a str>; 64],
    len: &mut usize,
) {
    *len = raw_fields.len().min(64);
    for (i, bytes) in raw_fields.iter().take(64).enumerate() {
        buf[i] = bytes_to_str(bytes);
    }
}

#[inline]
fn bytes_to_str<'a>(bytes: &'a [u8]) -> Option<&'a str> {
    if bytes.is_empty() {
        None
    } else {
        std::str::from_utf8(bytes).ok()
    }
}

// ---------------------------------------------------------------------------
// S5: Duplicate sort-val HashMap — measure cost of building it twice
// ---------------------------------------------------------------------------

/// Simulate the FIRST sort-val map build (lines ~1692-1731): used for deferred-alive + docstore.
/// Returns a Vec<(&str, i64)> of computed sort values.
#[inline(never)]
fn build_sort_vals_first<'a>(
    sort_field_names: &[&'a str],
    values: &[(&'a str, i64)],
) -> Vec<(&'a str, i64)> {
    let mut row_sv: HashMap<&str, u32> = HashMap::with_capacity(8);
    for &(name, val) in values {
        if sort_field_names.contains(&name) {
            row_sv.insert(name, val.max(0) as u32);
        }
    }
    // simulate the collect() at the end
    sort_field_names
        .iter()
        .map(|&sf| {
            let v = row_sv.get(sf).copied().unwrap_or(0);
            (sf, v as i64)
        })
        .collect()
}

/// Simulate the SECOND sort-val map build (lines ~1949-2006): for sort bitmap insertion.
/// Identical logic — duplicated in the actual code.
#[inline(never)]
fn build_sort_vals_second<'a>(
    sort_field_names: &[&'a str],
    values: &[(&'a str, i64)],
) -> HashMap<&'a str, u32> {
    let mut row_sort_vals: HashMap<&str, u32> = HashMap::with_capacity(8);
    for &(name, val) in values {
        if sort_field_names.contains(&name) {
            row_sort_vals.insert(name, val.max(0) as u32);
        }
    }
    row_sort_vals
}

/// Optimized: build once (first map), reuse for bitmap insertion.
#[inline(never)]
fn build_sort_vals_once<'a>(
    sort_field_names: &[&'a str],
    values: &[(&'a str, i64)],
) -> Vec<(&'a str, u32)> {
    let mut result: Vec<(&str, u32)> = Vec::with_capacity(sort_field_names.len());
    for &(name, val) in values {
        if sort_field_names.contains(&name) {
            result.push((name, val.max(0) as u32));
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Benchmark: S1 vs S2 — parse_delimited_line allocation
// ---------------------------------------------------------------------------

fn bench_parse_delimited(c: &mut Criterion) {
    let line = make_csv_line();
    let narrow = make_narrow_csv_line();

    let mut group = c.benchmark_group("parse_delimited_alloc");
    group.throughput(Throughput::Elements(NUM_ROWS));

    // S1: alloc every row (current code)
    group.bench_with_input(
        BenchmarkId::new("S1_alloc_every_row_wide", "20_fields"),
        &line,
        |b, line| {
            b.iter(|| {
                for _ in 0..NUM_ROWS {
                    let fields = parse_delimited_alloc_every_row(black_box(line), b'\t');
                    black_box(fields.len());
                }
            });
        },
    );

    // S2: reuse buffer (proposed fix)
    group.bench_with_input(
        BenchmarkId::new("S2_reuse_buf_wide", "20_fields"),
        &line,
        |b, line| {
            let mut buf: Vec<&[u8]> = Vec::with_capacity(32);
            b.iter(|| {
                for _ in 0..NUM_ROWS {
                    parse_delimited_reuse(black_box(line), b'\t', &mut buf);
                    black_box(buf.len());
                }
            });
        },
    );

    // S3: stack-allocated array (proposed, zero heap)
    group.bench_with_input(
        BenchmarkId::new("S3_stack_array_wide", "20_fields"),
        &line,
        |b, line| {
            let mut stack_buf = StackFields::<32>::new();
            b.iter(|| {
                for _ in 0..NUM_ROWS {
                    parse_delimited_stack(black_box(line), b'\t', &mut stack_buf);
                    black_box(stack_buf.len());
                }
            });
        },
    );

    // S1 narrow (8 fields)
    group.bench_with_input(
        BenchmarkId::new("S1_alloc_every_row_narrow", "8_fields"),
        &narrow,
        |b, line| {
            b.iter(|| {
                for _ in 0..NUM_ROWS {
                    let fields = parse_delimited_alloc_every_row(black_box(line), b'\t');
                    black_box(fields.len());
                }
            });
        },
    );

    // S2 narrow
    group.bench_with_input(
        BenchmarkId::new("S2_reuse_buf_narrow", "8_fields"),
        &narrow,
        |b, line| {
            let mut buf: Vec<&[u8]> = Vec::with_capacity(32);
            b.iter(|| {
                for _ in 0..NUM_ROWS {
                    parse_delimited_reuse(black_box(line), b'\t', &mut buf);
                    black_box(buf.len());
                }
            });
        },
    );

    // S3 narrow
    group.bench_with_input(
        BenchmarkId::new("S3_stack_array_narrow", "8_fields"),
        &narrow,
        |b, line| {
            let mut stack_buf = StackFields::<32>::new();
            b.iter(|| {
                for _ in 0..NUM_ROWS {
                    parse_delimited_stack(black_box(line), b'\t', &mut stack_buf);
                    black_box(stack_buf.len());
                }
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: S4 — Vec<Option<&str>> vs fixed [Option<&str>; 64] for indexed fields
// ---------------------------------------------------------------------------

fn bench_indexed_fields(c: &mut Criterion) {
    let line = make_csv_line();
    // Pre-parse the raw fields once (we're benchmarking the indexed-field fill, not the parse)
    let mut raw_buf: Vec<&[u8]> = Vec::with_capacity(32);
    parse_delimited_reuse(&line, b'\t', &mut raw_buf);
    let raw_fields: Vec<&[u8]> = raw_buf.clone();

    let mut group = c.benchmark_group("indexed_fields_alloc");
    group.throughput(Throughput::Elements(NUM_ROWS));

    // Vec<Option<&str>> with clear() reuse (current code uses fill_indexed_fields)
    group.bench_function("S4a_vec_reuse", |b| {
        let mut buf: Vec<Option<&str>> = Vec::with_capacity(32);
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                fill_indexed_fields_vec(black_box(&raw_fields), &mut buf);
                black_box(buf.len());
            }
        });
    });

    // Fixed [Option<&str>; 64] array — zero heap for the fill itself
    group.bench_function("S4b_array_64", |b| {
        let mut buf: [Option<&str>; 64] = [None; 64];
        let mut len: usize = 0;
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                fill_indexed_fields_array(black_box(&raw_fields), &mut buf, &mut len);
                black_box(len);
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: S5 — duplicate sort-val HashMap (built twice per row)
// ---------------------------------------------------------------------------

fn bench_sort_val_map(c: &mut Criterion) {
    // Simulate a row with 3 sort-relevant fields (existedAt, publishedAt, createdAt)
    let sort_fields = ["existedAt", "publishedAt", "createdAt", "sortAt"];
    let row_values: Vec<(&str, i64)> = vec![
        ("existedAt", 1711720799),
        ("publishedAt", 1711634400),
        ("createdAt", 1711634000),
        ("userId", 8675309),
        ("nsfwLevel", 2),
        ("postId", 42),
    ];

    let mut group = c.benchmark_group("sort_val_map");
    group.throughput(Throughput::Elements(NUM_ROWS));

    // Current code: build TWICE per row
    group.bench_function("S5a_build_twice", |b| {
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                let first = build_sort_vals_first(
                    black_box(&sort_fields),
                    black_box(&row_values),
                );
                let second = build_sort_vals_second(
                    black_box(&sort_fields),
                    black_box(&row_values),
                );
                black_box(first.len());
                black_box(second.len());
            }
        });
    });

    // Proposed: build ONCE, reuse for both docstore write and bitmap insertion
    group.bench_function("S5b_build_once", |b| {
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                let once = build_sort_vals_once(
                    black_box(&sort_fields),
                    black_box(&row_values),
                );
                black_box(once.len());
            }
        });
    });

    // Proposed: build once with HashMap (matches the map-lookup access pattern in bitmap insertion)
    group.bench_function("S5c_build_once_hashmap", |b| {
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                let once = build_sort_vals_second(
                    black_box(&sort_fields),
                    black_box(&row_values),
                );
                black_box(once.len());
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: combined per-row cost — full allocation chain for one row
// ---------------------------------------------------------------------------

fn bench_full_row_alloc_chain(c: &mut Criterion) {
    let line = make_csv_line();
    let sort_fields = ["existedAt", "publishedAt", "createdAt", "sortAt"];
    let row_values: Vec<(&str, i64)> = vec![
        ("existedAt", 1711720799),
        ("publishedAt", 1711634400),
        ("createdAt", 1711634000),
        ("userId", 8675309),
        ("nsfwLevel", 2),
    ];

    let mut group = c.benchmark_group("full_row_alloc_chain");
    group.throughput(Throughput::Elements(NUM_ROWS));

    // BASELINE: all allocations as they are in the current code
    // - Vec::new() for parse_delimited_line (alloc per row)
    // - Vec::new() for to_indexed_fields (alloc per row, but fill_indexed_fields exists)
    // - Vec::new() for config_computed_sort_vals (alloc per row)
    // - HashMap::with_capacity(8) built TWICE (alloc x2 per row when sort fields exist)
    group.bench_function("baseline_current_worst_case", |b| {
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                // Step 1: parse CSV line — Vec::new() alloc
                let fields = parse_delimited_alloc_every_row(black_box(&line), b'\t');

                // Step 2: indexed fields — Vec alloc (to_indexed_fields, not fill_indexed_fields)
                let indexed: Vec<Option<&str>> = fields
                    .iter()
                    .map(|b| bytes_to_str(b))
                    .collect();

                // Step 3: config_computed_sort_vals — Vec::new() alloc
                let sort_vals: Vec<(&str, i64)> = build_sort_vals_first(&sort_fields, &row_values);

                // Step 4: second sort val map for bitmap insertion — HashMap alloc
                let sort_map = build_sort_vals_second(&sort_fields, &row_values);

                black_box(fields.len());
                black_box(indexed.len());
                black_box(sort_vals.len());
                black_box(sort_map.len());
            }
        });
    });

    // OPTIMISED: reuse everything that can be reused
    // - Vec with clear() for parsed fields (S2)
    // - Vec with clear() for indexed fields (already done via fill_indexed_fields)
    // - Reuse Vec<(&str, i64)> for sort vals (no new allocation after warmup)
    // - Build sort val map only once, reuse for bitmap insertion
    group.bench_function("optimised_reuse_all", |b| {
        let mut fields_buf: Vec<&[u8]> = Vec::with_capacity(32);
        let mut indexed_buf: Vec<Option<&str>> = Vec::with_capacity(32);
        let mut sort_vals_buf: Vec<(&str, i64)> = Vec::with_capacity(8);

        b.iter(|| {
            for _ in 0..NUM_ROWS {
                // Step 1: parse CSV line — reuse buffer, no alloc after warmup
                parse_delimited_reuse(black_box(&line), b'\t', &mut fields_buf);

                // Step 2: indexed fields — reuse buffer, no alloc after warmup
                fill_indexed_fields_vec(black_box(fields_buf.as_slice()), &mut indexed_buf);

                // Step 3+4: build sort vals ONCE for both docstore write and bitmap insertion
                sort_vals_buf.clear();
                for &(name, val) in black_box(&row_values) {
                    if sort_fields.contains(&name) {
                        sort_vals_buf.push((name, val.max(0)));
                    }
                }

                black_box(fields_buf.len());
                black_box(indexed_buf.len());
                black_box(sort_vals_buf.len());
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: microcost of a single Vec::new() vs clear() at various capacities
// This isolates pure allocator overhead so we know what we're buying.
// ---------------------------------------------------------------------------

fn bench_alloc_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocator_baseline");
    group.throughput(Throughput::Elements(NUM_ROWS));

    // Vec::new() then push 20 elements (simulates parse_delimited_line for a 20-field row)
    group.bench_function("vec_new_push_20", |b| {
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                let mut v: Vec<u32> = Vec::new();
                for i in 0u32..20 {
                    v.push(black_box(i));
                }
                black_box(v.len());
            }
        });
    });

    // Vec with capacity then push 20 elements
    group.bench_function("vec_with_cap_push_20", |b| {
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                let mut v: Vec<u32> = Vec::with_capacity(20);
                for i in 0u32..20 {
                    v.push(black_box(i));
                }
                black_box(v.len());
            }
        });
    });

    // clear() + push 20 elements (no alloc after warmup)
    group.bench_function("vec_clear_push_20", |b| {
        let mut v: Vec<u32> = Vec::with_capacity(32);
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                v.clear();
                for i in 0u32..20 {
                    v.push(black_box(i));
                }
                black_box(v.len());
            }
        });
    });

    // Fixed array — no alloc at all
    group.bench_function("array_fill_20", |b| {
        let mut arr = [0u32; 32];
        let mut len = 0usize;
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                len = 0;
                for i in 0u32..20 {
                    arr[len] = black_box(i);
                    len += 1;
                }
                black_box(len);
            }
        });
    });

    // HashMap::with_capacity(8) (simulates row_sort_vals per row)
    group.bench_function("hashmap_with_cap_8", |b| {
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                let mut m: HashMap<&str, u32> = HashMap::with_capacity(8);
                m.insert(black_box("existedAt"), 1711720799u32);
                m.insert(black_box("publishedAt"), 1711634400u32);
                m.insert(black_box("createdAt"), 1711634000u32);
                black_box(m.len());
            }
        });
    });

    // Vec<(&str, u32)> with clear() — no HashMap alloc
    group.bench_function("vec_pairs_clear", |b| {
        let mut v: Vec<(&str, u32)> = Vec::with_capacity(8);
        b.iter(|| {
            for _ in 0..NUM_ROWS {
                v.clear();
                v.push((black_box("existedAt"), 1711720799u32));
                v.push((black_box("publishedAt"), 1711634400u32));
                v.push((black_box("createdAt"), 1711634000u32));
                black_box(v.len());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_delimited,
    bench_indexed_fields,
    bench_sort_val_map,
    bench_full_row_alloc_chain,
    bench_alloc_cost,
);
criterion_main!(benches);
