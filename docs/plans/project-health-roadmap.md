# Project Health Roadmap

Tracked improvements from the structure audit (2026-03-16). External review from Gemini 3 Pro and GPT-5.1 Codex.

## Completed

- [x] Remove serde_yaml dependency and dead YAML/JSON config code
- [x] Slim tokio from `features=["full"]` to 9 specific features
- [x] Move deploy artifacts (docker/, grafana/, configs/) into deploy/
- [x] Update CI workflow and README for new deploy/ paths
- [x] Add target-test*/, debug.log, scheduled_tasks.lock to .gitignore

## Next Up

### Add sccache to daemon (Medium effort, high impact)
Shared compilation cache across all target dirs. When agent 2 compiles `roaring` in target-test2/, sccache serves the cached result from agent 1's build in target-test1/. Complementary to existing shadow copy and target isolation.
- Set `RUSTC_WRAPPER=sccache` in daemon's cargo spawn env
- Install sccache in Docker image for CI
- Coordinate with Ivy on daemon changes

### Add cargo-nextest to daemon (Medium effort, high impact)
Drop-in replacement for `cargo test`. Runs each test in its own process, better parallelism, faster output. The daemon's `test` command would swap `cargo test` for `cargo nextest run`.
- Install nextest: `cargo install cargo-nextest`
- Update daemon's `runTestInSlot` to use `cargo nextest run`
- Update SKILL.md docs

### Workspace split: core / server / pg-sync (Large effort, best long-term)
Split the monolith crate into a workspace with separate crates:
```
crates/
  bitdex/         # Core engine library (no async deps)
  bitdex-server/  # HTTP server (depends on core)
  bitdex-pg-sync/ # Postgres sync (depends on core)
```
Benefits:
- Faster incremental builds (change server → only recompile server)
- Better agent isolation (different crates = different compilation units)
- Cleaner deps (engine tests don't touch tokio)
- Feature flags become simpler (`cargo build -p bitdex-server`)

Risks:
- Big one-time migration touching every import path
- Cross-crate refactoring requires path dependencies
- Only worth it if subsystems are stable (they mostly are)

### Group src/ into submodules (Large effort, improves navigation)
30 flat files → 4-5 top-level modules:
```
src/
  engine/    # engine.rs, executor.rs, planner.rs, filter.rs, sort.rs
  storage/   # bitmap_fs.rs, docstore.rs, bound_store.rs
  bitmap/    # versioned_bitmap.rs, meta_index.rs
  cache/     # unified_cache.rs, radix_sort.rs, cache.rs, time_buckets.rs
  server.rs  # stays flat (only used with server feature)
```
Both reviewers recommended this at 30 files. GPT cautioned: don't over-nest.

### Convert tests/bench_*.rs to criterion benchmarks (Small effort)
`tests/bench_arena_memory.rs` and `tests/bench_memory.rs` are benchmarks using `#[test]` with manual timing. Converting to criterion and moving to `benches/` would give proper statistical analysis and correct project structure.

### Docs sweep (Ongoing)
Design docs in docs/design/ are intentionally a growing knowledge base. Periodic sweeps needed to:
- Flag superseded docs (e.g., 3 versions of query-metrics)
- Verify accuracy against current implementation
- Archive stale content in docs/learnings/ or docs/plans/
- Archer (ops agent) should do this routinely

### Disable incremental compilation in agent builds (Small, experimental)
GPT suggested `incremental = false` for short-lived isolated target dirs — may reduce cache bloat. Worth testing.

### Move rand behind feature flag (Small, optional)
rand is only used by benchmark and loadtest binaries. Could gate behind `bench` feature with `required-features`. Low priority since rand is small.
