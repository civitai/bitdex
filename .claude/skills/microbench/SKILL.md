---
name: microbench
description: Guide for writing throwaway microbenchmarks to test performance hypotheses. Use when you need to measure bitmap ops, hash map latency, clone costs, or any other isolated performance question. Agents should use this instead of putting benchmarks in tests/.
disable-model-invocation: false
user-invocable: true
---

# Microbench — Throwaway Performance Experiments

When you need to test a performance hypothesis (bitmap op cost, hash map latency, clone overhead, etc.), write it as a microbench in the **scratch crate**, not in `tests/`.

Files in `tests/` get compiled and linked on every `cargo test` run, adding minutes to the test suite. The scratch crate only compiles when explicitly targeted.

## Where to write microbenchmarks

```
scratch/src/lib.rs          # Put #[test] benchmarks here
scratch/tests/*.rs          # Or as integration tests here
scratch/src/bin/*.rs        # Or as standalone binaries
```

## How to run them

```bash
# Run all scratch tests (release mode for accurate timings)
cargo test -p scratch --release -- --nocapture

# Run a specific benchmark by name
cargo test -p scratch --release -- --nocapture bench_name

# Run a scratch binary
cargo run -p scratch --release --bin my_bench
```

## Adding dependencies

The scratch crate already has `roaring`, `dashmap`, `parking_lot`, and `rand`. If you need more, add them to `scratch/Cargo.toml`. This crate is disposable — don't worry about keeping it clean.

## Writing a microbench

Use `#[test]` functions with `--nocapture` for output. Use `std::hint::black_box()` to prevent the optimizer from eliding work.

```rust
use std::time::Instant;

#[test]
fn bench_my_hypothesis() {
    let iterations = 10_000;

    // Setup
    let data = build_test_data();

    // Warmup
    for _ in 0..100 {
        std::hint::black_box(do_the_thing(&data));
    }

    // Measure
    let start = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(do_the_thing(&data));
    }
    let elapsed = start.elapsed();
    let ns_per_op = elapsed.as_nanos() / iterations as u128;

    println!("do_the_thing: {} ns/op ({:.2} ms total)",
        ns_per_op, elapsed.as_secs_f64() * 1000.0);
}
```

## Rules

1. **Never put microbenchmarks in `tests/`** — they compile on every `cargo test` and slow down the whole suite.
2. **Always use `--release`** — debug mode timings are meaningless for performance questions.
3. **Use `--nocapture`** — otherwise you won't see the println output.
4. **Warmup first** — cold cache and JIT effects skew early iterations.
5. **Use `black_box()`** — prevents LLVM from optimizing away the work you're measuring.
6. **Compare in the same session** — system load causes 2-3x variance between runs.

## When you're done

Delete your scratch files. The crate is disposable. Don't let experiments accumulate — that's exactly the problem this crate was created to solve.

## Referencing bitdex code

If your microbench needs to use bitdex types or functions, add a dependency in `scratch/Cargo.toml`:

```toml
[dependencies]
bitdex-v2 = { path = ".." }
```

But most microbenches are standalone — they test generic data structure or algorithm performance, not bitdex-specific logic.
