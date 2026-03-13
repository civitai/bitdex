# Bitdex V2 — justfile
# Run `just` or `just --list` to see all available recipes.

# Defaults (override on CLI: just server PORT=3002)
PORT           := "3001"
DATA_DIR       := "./data"
E2E_PORT       := "3100"
BENCH_STAGES   := "query"
NDJSON         := 'C:/Dev/Repos/open-source/bitdex/data/images-full-v2.ndjson'
LOADTEST_QPS   := "500"
LOADTEST_DUR   := "30"

# ─── Build ─────────────────────────────────────────────────────────

# Build core library (release)
build:
    cargo build --release

# Build HTTP server binary
build-server:
    cargo build --release --features server --bin bitdex-server

# Build loadtest binary
build-loadtest:
    cargo build --release --features loadtest --bin bitdex-loadtest

# Build pg-sync binary
build-pg-sync:
    cargo build --release --features pg-sync --bin bitdex-pg-sync

# Build with SIMD roaring (Linux only)
build-simd:
    cargo build --release --features simd,server --bin bitdex-server

# Build everything
build-all: build build-server build-loadtest

# ─── Code Quality ──────────────────────────────────────────────────

# Type-check without building (fast)
check:
    cargo check --all-features

# Run clippy lints
clippy:
    cargo clippy --all-features -- -D warnings

# Format code
fmt:
    cargo fmt

# Check formatting without modifying
fmt-check:
    cargo fmt -- --check

# ─── Tests ─────────────────────────────────────────────────────────

# Run Rust unit/integration tests
test:
    cargo test

# Run E2E tests (builds server, starts it, runs suites)
test-e2e:
    node tests/e2e/run-e2e.mjs --port {{E2E_PORT}}

# Run E2E tests (skip cargo build, server must already be built)
test-e2e-skip-build:
    node tests/e2e/run-e2e.mjs --port {{E2E_PORT}} --skip-build

# Run E2E tests with verbose server output
test-e2e-verbose:
    node tests/e2e/run-e2e.mjs --port {{E2E_PORT}} --verbose

# Run Rust tests + E2E tests
test-all: test test-e2e

# ─── Criterion Benchmarks ─────────────────────────────────────────

# Criterion: core engine operations
bench:
    cargo bench --bench engine_bench

# Criterion: radix sort index
bench-radix:
    cargo bench --bench radix_sort_bench

# Criterion: docstore merge operations
bench-docstore:
    cargo bench --bench docstore_merge

# Criterion: bitmap streaming
bench-bitmap-stream:
    cargo bench --bench bitmap_stream

# Criterion: slot loader
bench-slot-loader:
    cargo bench --bench slot_loader

# Criterion: columnar loader
bench-columnar-loader:
    cargo bench --bench columnar_loader

# Run ALL criterion benchmarks
bench-all:
    cargo bench

# ─── Benchmark Binary (large-scale) ───────────────────────────────

# Run benchmark binary (query-only by default)
benchmark:
    cargo run --release --bin bitdex-benchmark -- --stages {{BENCH_STAGES}}

# Run benchmark: insert stage only
benchmark-insert:
    cargo run --release --bin bitdex-benchmark -- --stages insert

# Run benchmark: query stage only
benchmark-query:
    cargo run --release --bin bitdex-benchmark -- --stages query

# Run benchmark: insert + query
benchmark-full:
    cargo run --release --bin bitdex-benchmark -- --stages insert,query

# Run benchmark: persist + restore cycle
benchmark-persist:
    cargo run --release --bin bitdex-benchmark -- --stages persist,restore

# ─── Server ────────────────────────────────────────────────────────

# Run server (release, port 3001)
server:
    cargo run --release --features server --bin bitdex-server -- --port {{PORT}} --data-dir {{DATA_DIR}}

# Run server (debug build, faster compile)
server-dev:
    cargo run --features server --bin bitdex-server -- --port {{PORT}} --data-dir {{DATA_DIR}}

# ─── Load Testing ──────────────────────────────────────────────────

# Loadtest against running server
loadtest:
    cargo run --release --features loadtest --bin bitdex-loadtest -- \
        --mode http --url http://localhost:{{PORT}} \
        --qps {{LOADTEST_QPS}} --duration {{LOADTEST_DUR}}

# Loadtest with embedded engine (no HTTP)
loadtest-direct:
    cargo run --release --features loadtest --bin bitdex-loadtest -- \
        --mode direct --duration {{LOADTEST_DUR}}

# ─── Docker ────────────────────────────────────────────────────────

# Build Docker image
docker:
    docker build -f docker/Dockerfile -t bitdex-v2 .

# Build Docker image with SIMD
docker-simd:
    docker build -f docker/Dockerfile.simd -t bitdex-v2-simd .

# ─── Cleanup ───────────────────────────────────────────────────────

# Remove build artifacts
clean:
    cargo clean

# Remove test data directories
clean-data:
    rm -rf .test-data
    rm -rf bench_data
