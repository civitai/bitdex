# Bitdex V2 — justfile
# Run `just` or `just --list` to see all available recipes.

set shell := ["powershell", "-NoProfile", "-Command"]

# Defaults (override on CLI: just dev PORT=3002)
PORT           := "3001"
DATA_DIR       := justfile_directory() / "data"
E2E_PORT       := "3100"
BENCH_STAGES   := "query"
NDJSON         := 'C:/Dev/Repos/open-source/bitdex/data/images-full-v2.ndjson'
LOADTEST_QPS   := "500"
LOADTEST_DUR   := "30"

# ─── Build (fast profile — use for daily work) ──────────────────────

# Build core library (fast profile: thin LTO, parallel codegen)
build:
    cargo build --profile fast

# Build HTTP server binary
build-server:
    cargo build --profile fast --features server --bin bitdex-server

# Build loadtest binary
build-loadtest:
    cargo build --profile fast --features loadtest --bin bitdex-loadtest

# Build pg-sync binary
build-pg-sync:
    cargo build --profile fast --features pg-sync --bin bitdex-pg-sync

# Build everything
build-all: build build-server build-loadtest

# ─── Build (full release — use for distribution only) ────────────────

# Build server with full optimization (fat LTO, single codegen unit)
dist:
    cargo build --release --features server --bin bitdex-server

# Build with SIMD roaring (Linux only, full release)
dist-simd:
    cargo build --release --features simd,server --bin bitdex-server

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
# Benchmarks always use full --release for accurate numbers.

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

# ─── Server (standalone, no daemon) ───────────────────────────────

# Run server without rebuilding (instant start, no daemon)
run:
    {{justfile_directory() / "target" / "fast" / "bitdex-server"}} --port {{PORT}} --data-dir {{DATA_DIR}} --log-level info

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

# ─── Release ──────────────────────────────────────────────────────

# Release: bump version, commit, tag, push (triggers CI image build)
# Usage: just release [patch|minor|major]
release BUMP="patch":
    bash tools/release.sh {{BUMP}}

# ─── Dev Server (managed instances via daemon) ──────────────────

_cli := ".claude/skills/dev-server/cli.mjs"

# Start server (or show status if already running)
dev *ARGS:
    node {{_cli}} start {{ARGS}}

# Start an additional server instance (for agents/parallel work)
dev-new *ARGS:
    node {{_cli}} new {{ARGS}}

# Open the dev-server TUI dashboard
dev-dash:
    node {{_cli}} dash

# Show status of all managed instances, datasets, and locks
dev-status:
    node {{_cli}} status

# Stop a server instance (defaults to first running; or specify ID)
dev-stop *ARGS:
    node {{_cli}} stop {{ARGS}}

# View server logs (defaults to first running; or specify ID)
dev-logs *ARGS:
    node {{_cli}} logs {{ARGS}}

# List known datasets and data directories
dev-datasets:
    node {{_cli}} datasets

# Coordinated build (acquires lock, builds, releases)
dev-build *ARGS:
    node {{_cli}} build {{ARGS}}

# Coordinated E2E test run (acquires lock, runs suite, releases)
dev-test-e2e:
    node {{_cli}} test-e2e

# Shut down all managed instances and the daemon
dev-shutdown:
    node {{_cli}} shutdown

# ─── Cleanup ───────────────────────────────────────────────────────

# Remove build artifacts
clean:
    cargo clean

# Remove test data directories
clean-data:
    rm -rf .test-data
    rm -rf bench_data
