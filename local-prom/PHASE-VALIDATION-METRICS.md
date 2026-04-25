# Phase Validation Metric Set

Every phase validation run captures the same metric set so we can diff
runs apples-to-apples. Run via `scripts/phase-validate.mjs`.

**Critical**: P99 spikes intermittently — cumulative quantiles smooth that out.
This harness captures **per-interval** quantiles (every 5 s default), spike
extremes, and linear trend slopes so we catch transient regressions the
end-of-run snapshot would miss.

## What every run captures

### 1. Per-interval Prometheus snapshots (default every 5 s)

For each histogram (`bitdex_query_duration_seconds`,
`bitdex_docstore_read_seconds`, `bitdex_wal_append_duration_seconds`),
the harness diffs cumulative buckets between consecutive snapshots → produces
**interval-only** P50/P95/P99/P99.9. This is the spike signal. The cumulative
view stays for long-window comparison but never gates alone.

For scalar metrics (counters + gauges), the harness records value at each
snapshot, then derives:
- **Start → end delta** for total change.
- **Per-interval rate of change** for counters → catches sudden bursts.
- **Linear regression slope** vs time → trend (rising / stable / falling).
- **Min / max across intervals** for gauges.

### 2. Trace ring-buffer snapshots (every 60 s)

`/api/indexes/<index>/traces?last=1000` — gates on count of `total_us > 1s`.
Catches what histogram bucket boundaries miss.

### 3. Engine stats (every 60 s)

`/api/indexes/<index>/stats` — preserved as ndjson for retrospective debugging.

### 4. Run metadata

`meta.json`: started_at, duration, interval, target, index, baseline label,
probe, gates, git rev, server `/api/config` snapshot.

## Captured metric inventory

### Histograms (per-interval quantiles + spike + trend)

| Metric | What it measures | Why it's load-bearing |
|---|---|---|
| `bitdex_query_duration_seconds` | End-to-end query latency | P99 < 1 s mission gate; spikes are the failure mode |
| `bitdex_docstore_read_seconds` | Single-doc read on `get_document` path | Justin: doc query times historically a regression source |
| `bitdex_wal_append_duration_seconds` | `/ops` WAL append + fsync (post task #19) | Confirms `block_in_place` keeps fsync off async runtime |

### Scalars (start, end, delta, per-interval rate where applicable)

**Query path**: `bitdex_query_total`, `bitdex_query_errors_total`

**Doc cache (the historical risk surface)**: `bitdex_doc_cache_hit_total`, `_miss_total`, `_entries`, `_bytes`, `_evictions_total`, `_generations`, `_backlog`

**Docstore**: `bitdex_docstore_concurrent_reads`, `_put_batch_fast_path_total`, `_put_batch_slow_path_total`

**Unified cache**: `bitdex_cache_hit_total`, `_miss_total`, `_inserts_total`, `_evictions_total`, `_bytes`, `_entries`, `bitdex_cache_tombstones_created`

**Flush / merge**: `bitdex_flush_cache_ns_total`, `bitdex_flush_total`, `bitdex_merge_total`

**WAL / ops**: `bitdex_wal_ops_processed_total`, `bitdex_wal_ops_failed_total`

**Process**: `process_resident_memory_bytes` (RSS — leak detector via slope), `process_virtual_memory_bytes`, `process_cpu_seconds_total`

**Bitmap memory** (when collector enabled): `bitdex_bitmap_memory_bytes_total`

**Filter indexed-lookup fallback** (post task #8): `bitdex_filter_indexed_lookup_fallback_total{reason}` — silent-fallback canary

**Relay** (when relay-mode active): `bitdex_relay_events_total`, `bitdex_relay_sse_subscribers`, `bitdex_relay_sse_lagged_events_total`

## Output structure

```
local-prom/runs/<label>/
├── meta.json                                  # config, git_rev, args
├── metrics.ndjson                             # raw prom snapshot per interval
├── traces.ndjson                              # trace ring buffer per minute
├── stats.ndjson                               # /stats per minute
├── summary.json                               # all derived: histograms, spikes, trends, rates
├── summary.md                                 # human-readable + gate verdict
├── diff-vs-<baseline>.md                      # cumulative + spike + trend diff vs baseline
├── timeseries-bitdex_query_duration_seconds.csv  # one row per interval, p50/p95/p99/p999
├── timeseries-bitdex_docstore_read_seconds.csv
├── timeseries-bitdex_wal_append_duration_seconds.csv  (when applicable)
├── timeseries-rss.csv                         # t_sec, rss_bytes per snapshot
├── timeseries-doc-cache-hit-ratio.csv         # interval hit ratio
└── timeseries-rate-<counter>.csv              # per-interval rate for tracked counters
```

CSVs are easy to import into Excel / Sheets / Grafana CSV plugin / quick `awk`
or Python plot. One row per interval (default 5 s).

## What gates check

### `always` (every run)

**Cumulative quantiles** (long-window):
- `bitdex_query_duration_seconds:p50_ms` ≤ 10
- `bitdex_query_duration_seconds:p95_ms` ≤ 100
- `bitdex_query_duration_seconds:p99_ms` ≤ 1000  *(mission gate)*
- `bitdex_query_duration_seconds:p999_ms` ≤ 1500

**Spike gates** (per-interval extremes — Justin's load-bearing concern):
- `bitdex_query_duration_seconds:p99_ms_max` ≤ 1000  *(no single interval ever exceeded)*
- `bitdex_query_duration_seconds:p999_ms_max` ≤ 2000
- `bitdex_query_duration_seconds:intervals_over_1000ms` = 0  *(zero intervals had P99 > 1s)*
- `bitdex_query_duration_seconds:longest_spike_over_1000ms` = 0  *(no contiguous spike)*

**Outliers**:
- `traces_outliers_over_1s` = 0  *(catches what histograms miss)*

**Doc cache**:
- `bitdex_doc_cache_hit_ratio` regress ≤ 2 %
- `bitdex_doc_cache_hit_ratio_min` ≥ 90 %  *(no interval where hit ratio collapsed)*

**RSS** (leak detector):
- `process_resident_memory_bytes:growth_per_min_pct` ≤ 1.5 %/min

### `doc_path` (whenever doc-fetch path is exercised)

- `bitdex_docstore_read_seconds:p95_ms` ≤ baseline + 10 %
- `bitdex_docstore_read_seconds:p99_ms` ≤ baseline + 10 %
- `bitdex_docstore_read_seconds:p99_ms_max` ≤ 100  *(spike cap on doc path)*
- `bitdex_docstore_read_seconds:intervals_over_1000ms` = 0
- `bitdex_doc_cache_bytes:growth_per_min_pct` ≤ 5 %/min

### `phase_1_async_cache` (PR #234 validation)

- `flush_tombstones_under_backpressure` ≥ 1  *(fallback path actually fires)*
- `bitdex_cache_miss_total:rate_per_min` regress ≤ 20 %  *(no thrash)*

### `phase_2_wal` (post task #18 + #19)

- `bitdex_wal_append_duration_seconds:p99_ms` ≤ 50
- `bitdex_wal_append_duration_seconds:p99_ms_max` ≤ 200  *(no single-interval spike)*
- `bitdex_wal_ops_failed_total:delta` = 0

### `phase_3_doc_batch` (post task #14)

- `bitdex_docstore_read_seconds:p95_ms` regress ≤ −10 %  *(MUST improve ≥10%)*
- `bitdex_docstore_read_seconds:p99_ms_max` ≤ 50  *(tighter spike cap post-batch)*

## Run lifecycle (every phase)

1. **Baseline run** before phase change lands. Tag e.g. `phase1-baseline-pre`.
2. **Phase run** after change with same metric set + `--baseline <prev label>`.
3. Script writes `summary.{json,md}` + `diff-vs-<baseline>.md` + per-histogram CSVs.
4. Exit code: non-zero if any gate fails. Wire into CI later.

## Reading a summary.md

The doc surfaces, in order:

1. **Cumulative quantiles** — coarse signal across the whole run.
2. **Spike summary** — per-interval extremes. Three columns matter most:
   - **Max P99** — worst single interval. If this is 5× the cumulative P99, your "good" run hides a periodic stall.
   - **Intervals >1000ms** — count of bad intervals. 0 = clean. >0 = investigate.
   - **Longest contig >1s** — duration of the worst spike (in interval count). 1 = transient blip; 5+ = sustained stall.
3. **Trend** — linear regression slope of P99 vs time. Slope > 1 ms/min = drifting up, possible saturation/leak.
4. **RSS time-series** — start, end, max, growth %/min, regression slope.
5. **Counter rate spikes** — max:mean ratio per counter. Ratio >> 1 = bursty.
6. **Gate results** — explicit PASS/FAIL/SKIP per metric.

## Reading a diff-vs-<baseline>.md

Three comparison tables, in order:

1. **Cumulative quantile diff** — long-window regression flag (>10% worse).
2. **Spike diff** — `current intervals >1s` vs `baseline intervals >1s`. If current
   has more spike intervals, **SPIKE-REGRESS** flag. This is the critical signal —
   the cumulative may not move but spikes can multiply.
3. **Trend slope diff** — `DRIFT-UP` flag if current trend is positive AND steeper
   than baseline. Catches "it starts the same but degrades over time".

## Example invocations

```bash
# Phase 0 baseline — PR #233 merged, no async, no probe, just observe
node scripts/phase-validate.mjs \
  --label phase0-baseline-sync-only \
  --duration-min 15 \
  --target http://localhost:3002 \
  --gates always,doc_path

# Phase 1a sync-mode steady-state baseline (cache.async_maintenance=false)
node scripts/phase-validate.mjs \
  --label phase1a-sync-baseline \
  --duration-min 15 \
  --baseline phase0-baseline-sync-only \
  --gates always,doc_path

# Phase 1b async-mode + write burst (forces backpressure path)
# Pre-step: PATCH /api/config { "async_maintenance": true }
node scripts/phase-validate.mjs \
  --label phase1b-async-on-burst \
  --duration-min 25 \
  --baseline phase1a-sync-baseline \
  --probe write-burst \
  --gates always,doc_path,phase_1_async_cache

# Phase 2 WAL + ops fsync fix (post task #18 + #19)
node scripts/phase-validate.mjs \
  --label phase2-wal-fix \
  --duration-min 25 \
  --baseline phase1b-async-on-burst \
  --probe write-burst \
  --gates always,doc_path,phase_2_wal

# Phase 3 doc batch (post task #12 + #14)
node scripts/phase-validate.mjs \
  --label phase3-doc-batch \
  --duration-min 25 \
  --baseline phase2-wal-fix \
  --probe postid-tail \
  --gates always,doc_path,phase_3_doc_batch
```

## Probes

- **`none`** — no synthetic traffic, observe steady state
- **`postid-tail`** — Donovan's `scripts/probe-postid-tail.mjs` (cold postId long-tail; 2000 random single-postId queries)
- **`write-burst`** — `scripts/ops-loadgen.mjs` at 500 ops/s for the run duration (forces async-cache backpressure)

External probes (run separately, results land in `bitdex_query_total` etc):
- `scripts/replay-prod-via-relay.mjs` — diverse-shape live traffic from prod relay (Linux box recommended; Windows kubectl PF unstable)
- `scripts/replay-captured.mjs` — corpus loop for QPS volume (unrealistic cache behavior — supplementary only)

## Why per-interval matters (the load-bearing point)

Cumulative `bitdex_query_duration_seconds_bucket` is monotonically increasing.
A 30-min run with one 5 s spike at P99 = 3 s averaged across 30 min of P99 = 50 ms
shows up as **cumulative P99 ≈ 60 ms — a passing gate**, while the spike itself
is invisible. The interval-diff approach diffs `bucket[t+1] - bucket[t]` and
computes the quantile **for that interval alone**, surfacing the 3 s spike as
a single bad interval. Gates that read `:p99_ms_max` and `:intervals_over_1000ms`
fail when this happens, even though the cumulative `:p99_ms` passes.

This is the same principle Prometheus's `histogram_quantile(0.99, rate(...[5m]))`
uses against `_bucket{le=...}` series in production. The harness embeds it
locally so we don't need a Prometheus server running.
