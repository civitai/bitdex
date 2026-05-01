# Shard Rewrite Mystery — Audit 2026-05-01

## Symptom

BitDex v1.0.196-jemalloc in prod: PSI io.pressure full=0.39, 500+ MB/s disk writes, 17s freezes every ~70s.
`find -newermt "30 seconds ago"` showed 1727 bitmap shard files changing inode every ~5s.
PATCH /config `bitmap_compact_threshold=1_000_000_000` applied — ops=30 still triggered full rewrite.
`bitdex_compact_runs_total=0` ruled out the HTTP compact endpoint.

## Phase 1 — Audit Findings

### All paths that call `write_shard_file_atomic`

| Call site | File:line | When triggered |
|-----------|-----------|----------------|
| `compact_shard` | `shard_store.rs:874` | Merge thread auto-compaction |
| `write_snapshot` | `shard_store.rs:831` | Explicit snapshot save |
| Cold-path `append_op_opts` | `shard_store.rs:749` | First write to a shard (file absent/invalid) |
| Cold-path `append_ops_opts` | `shard_store.rs:790` | Same, multi-op variant |
| `write_filter_bucket_raw` | `shard_store_bitmap.rs:1182` | Dump processor / `write_snapshot_to_store` |
| `write_sort_layers` | `shard_store_bitmap.rs:1285` | Sort shard initial creation |

### The critical bug: `compaction_total` is never incremented

`bitdex_compact_runs_total=0` **only proves the HTTP `/compact` endpoint was not called**.

`MetricsBridge.compaction_total` is defined in `src/metrics.rs:99` and `concurrent_engine.rs` references it — but the merge thread compaction block (lines 2601–2688 of `concurrent_engine.rs`) **never calls `.inc()`**. The counter is dead code. Every compaction the merge thread runs is invisible to Prometheus.

Confirmed: `compact_runs_total.inc()` only fires in `server.rs:3990` (HTTP endpoint handler).

### Merge thread compaction path

```
bitdex-merge thread (merge_interval_ms=5000ms default)
  → wakes every 5s
  → if merge_dirty_flag.swap(false) is true:
      → list_current_shards()
      → for each shard: if fs_.needs_compaction(key) → compact_current(key)
          → compact_shard → write_shard_file_atomic (Compact source)
```

`needs_compaction` calls `should_compact(key, self.compact_threshold.load(Relaxed))` which reads the atomic. The atomic is the same `Arc<FilterBitmapStore>` shared with the engine — the PATCH does propagate.

### Why ops=30 triggered compaction despite threshold=1B

Three candidates (in order of likelihood):

1. **Timing race**: The PATCH arrived during or just after a merge cycle that read the old threshold (500 in pre-v196). The compaction was already in-flight. At 295MB per shard, compaction takes several seconds — the observation at T+15s captured a compaction decided at T-5s (before PATCH).

2. **`needs_compaction` reads header without the shard lock**: `ops_count()` at `shard_store.rs:671` reads the file header WITHOUT acquiring the shard mutex. If the flush thread was concurrently writing to the shard (updating ops_count in-place), the merge thread could read a stale or mid-write header value. This is a TOCTOU window.

3. **`merge_dirty_flag` reset behavior**: The flag is `swap(false)` — once cleared, new flush cycles must re-dirty it. If the merge thread runs, clears the flag, and then immediately reads `needs_compaction` on shards that just got new ops, it could compact immediately before the PATCH is effective.

### Why ~5s rewrite cadence

`default_merge_interval_ms()` in `src/config.rs` returns `5000`. The merge thread wakes every 5s and checks all shards. At 107M records, tagIds alone has 31K+ distinct values and hundreds of shard files. If the threshold is too low, every merge wake compacts hundreds of shards = 500MB/s disk writes = IO pressure.

### `save_snapshot` paths (ruled out for steady-state)

`save_snapshot` (which calls `write_filter_bucket` → `write_filter_bucket_raw` → `write_shard_file_atomic`) is only triggered by:
- Server shutdown (line 1686 of server.rs)
- HTTP `POST /snapshot` endpoint
- `PUT /cursors/{name}` handler (line 4735 of server.rs) — fires on every cursor set from metrics_poller, but metrics_poller runs on the sidecar process, not in steady-state hot path

None of these fire at 5s cadence.

## Phase 2 — Instrumentation Added

Added three global `AtomicU64` counters to `shard_store.rs` and wired them into Prometheus.

### New metric

```
bitdex_shard_rewrites_total{source="compact"}      — merge-thread compaction
bitdex_shard_rewrites_total{source="cold_create"}  — first write, shard file absent/invalid
bitdex_shard_rewrites_total{source="snapshot"}     — explicit snapshot write
```

### Implementation

**`src/shard_store.rs`** — globals + enum + counter increment in `write_shard_file_atomic`:
```rust
pub static SHARD_REWRITES_COMPACT: AtomicU64 = AtomicU64::new(0);
pub static SHARD_REWRITES_COLD:    AtomicU64 = AtomicU64::new(0);
pub static SHARD_REWRITES_SNAPSHOT: AtomicU64 = AtomicU64::new(0);

pub enum ShardRewriteSource { Compact, ColdCreate, Snapshot }

pub(crate) fn write_shard_file_atomic(path, header, snapshot_bytes, ops_bytes, source) {
    match source {
        Compact    => SHARD_REWRITES_COMPACT.fetch_add(1, Relaxed),
        ColdCreate => SHARD_REWRITES_COLD.fetch_add(1, Relaxed),
        Snapshot   => SHARD_REWRITES_SNAPSHOT.fetch_add(1, Relaxed),
    }
    // ... atomic write ...
}
```

**`src/metrics.rs`** — `IntGaugeVec` synced from atomics at scrape time in `gather()`:
```rust
self.shard_rewrites_total.with_label_values(&["compact"]).set(SHARD_REWRITES_COMPACT.load(Relaxed));
self.shard_rewrites_total.with_label_values(&["cold_create"]).set(SHARD_REWRITES_COLD.load(Relaxed));
self.shard_rewrites_total.with_label_values(&["snapshot"]).set(SHARD_REWRITES_SNAPSHOT.load(Relaxed));
```

**Call sites updated** (all 6 `write_shard_file_atomic` callers tagged):
- `compact_shard` → `Compact`
- `write_snapshot` → `Snapshot`
- `append_op_opts` cold path → `ColdCreate`
- `append_ops_opts` cold path → `ColdCreate`
- `write_filter_bucket_raw` → `Snapshot`
- `write_sort_layers` → `Snapshot`

`cargo check --features server` — clean (no new errors or warnings).

## What to Watch in Prod

After deploying, watch the rate of change (PromQL):

```promql
rate(bitdex_shard_rewrites_total{source="compact"}[1m])
rate(bitdex_shard_rewrites_total{source="snapshot"}[1m])
rate(bitdex_shard_rewrites_total{source="cold_create"}[1m])
```

Expected in steady-state with `compact_threshold=1B` and no load: all rates near zero.

If `compact` rate stays high despite threshold=1B: the TOCTOU race in `ops_count()` is likely — file header read without shard lock → stale value passes threshold check. Fix: add `compact_threshold` read inside the shard lock, or re-read ops_count after locking.

If `snapshot` rate is high: `save_snapshot` is being triggered more than expected — check cursor PATCH frequency from metrics_poller.

## Regression Risks

- `write_shard_file_atomic` signature change: all 6 call sites updated. Any future call site that omits `source` will fail to compile (compile-time safety).
- `gather()` atomics sync: Relaxed load is correct — these are independent monotonic counters, no ordering requirement.
- No behavior change: counters are read-only; they do not gate or alter the write path.
