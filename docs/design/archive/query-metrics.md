# Query Metrics & EXPLAIN Design

**Status:** Proposal
**Date:** 2026-03-14
**Sources:** Gemini (bitmap internals), GPT (output format/structs), codebase analysis
**Related files:** `src/executor.rs`, `src/planner.rs`, `src/engine.rs`, `src/concurrent_engine.rs`

---

## Overview

A query metrics system that makes BitDex's bitmap execution model visible. Two modes:

- **EXPLAIN**: Planner estimates only (no execution)
- **EXPLAIN ANALYZE**: Full execution with per-clause timing, cardinality tracking, and sort metrics

Activated via `"explain": true` or `"explain": "analyze"` on query JSON. Zero overhead when not requested.

---

## 1. What Gets Measured

### Per-Clause Metrics

For each filter clause in execution order:

| Metric | Description | Source |
|--------|-------------|--------|
| `estimated_cardinality` | Planner's estimate over full corpus | `planner::estimate_cardinality()` |
| `actual_cardinality` | Real bitmap cardinality (full corpus) | `evaluate_clause().len()` |
| `accumulator_before` | Running intersection size before this clause | Tracked in `compute_filters` |
| `accumulator_after` | Running intersection size after AND | Tracked in `compute_filters` |
| `eval_time_us` | Time to build/retrieve the clause bitmap | `Instant::now()` delta |
| `and_time_us` | Time to AND with accumulator | `Instant::now()` delta |
| `fast_path` | Which fast-path was used, if any | `try_and_by_ref` return |
| `cache_outcome` | Hit/miss/n/a | Cache lookup result |

**Already partially implemented:** `compute_filters` in `executor.rs:312-346` already tracks `eval_elapsed`, `and_elapsed`, `bm_card`, and `result_card` via `tracing::debug!`. The metrics system formalizes this into a struct.

### Sort Metrics

| Metric | Description |
|--------|-------------|
| `input_candidates` | Cardinality entering sort phase |
| `output_rows` | Rows returned after limit |
| `bit_layers_visited` | How many of N bit layers were traversed |
| `total_bit_layers` | Total layers available (e.g., 32 for u32) |
| `early_stop` | Whether top-K terminated before visiting all layers |
| `sort_time_us` | Total sort + paginate time |

**From Gemini -- sort-specific metrics to consider later:**
- `branch_pruning_rate`: Percentage of bit-layer branches skipped
- `bitslice_cardinality_decay`: How fast candidates shrink per layer
- `sort_yield_ratio`: `requested_rows / bitmap_ops_performed`

### Estimator Quality

Q-error per clause: `max(estimated/actual, actual/estimated)`. Values near 1.0 are good.

- `mean_q_error` across all clauses
- `max_q_error` and which clause
- Per-clause q-error breakdown

### Cache Metrics

- Total lookups, hits, misses, hit ratio
- Per-clause cache key and outcome

---

## 2. Rust Struct Design

### Core Types

```rust
/// Top-level query metrics, only populated when explain is requested.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetrics {
    pub mode: ExplainMode,
    pub alive_count: u64,
    pub planning_time_us: u64,
    pub execution_time_us: u64,
    pub total_time_us: u64,
    pub clauses: Vec<ClauseMetrics>,
    pub sort: Option<SortMetrics>,
    pub cache: CacheSummary,
    pub estimator: EstimatorSummary,
    pub highlights: Vec<Highlight>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExplainMode { Explain, Analyze }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClauseMetrics {
    pub execution_order: usize,
    pub clause_repr: String,          // "nsfwLevel IN [1, 2]"
    pub clause_type: String,          // "Eq", "In", "Gte", etc.
    pub field: Option<String>,
    pub estimated_cardinality: u64,
    pub actual_cardinality: Option<u64>,       // None in EXPLAIN mode
    pub accumulator_before: Option<u64>,
    pub accumulator_after: Option<u64>,
    pub eval_time_us: Option<u64>,
    pub and_time_us: Option<u64>,
    pub fast_path: Option<String>,             // "by_ref_eq", "by_ref_in", etc.
    pub cache_outcome: Option<String>,         // "hit", "miss", "n/a"
    pub children: Vec<ClauseMetrics>,          // For nested And/Or/Not
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortMetrics {
    pub field: String,
    pub direction: String,
    pub strategy: String,                      // "bit_layer_traversal" or "simple_sort"
    pub input_candidates: u64,
    pub output_rows: u64,
    pub bit_layers_visited: Option<u32>,
    pub total_bit_layers: Option<u32>,
    pub early_stop: Option<bool>,
    pub time_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSummary {
    pub lookups: u64,
    pub hits: u64,
    pub misses: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EstimatorSummary {
    pub mean_q_error: f64,
    pub max_q_error: f64,
    pub worst_clause: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Highlight {
    pub kind: String,     // "most_selective", "most_expensive_eval", "dominant_operator"
    pub label: String,    // "tagIds = 12345"
    pub detail: String,   // "18M -> 116K" or "0.944ms" or "76.7%"
}
```

### Collector Pattern

```rust
/// Query-scoped collector. Created only when explain=true.
/// Passed through executor methods via Option<&mut QueryMetricsCollector>.
pub struct QueryMetricsCollector {
    metrics: QueryMetrics,
    clause_order: usize,
}

impl QueryMetricsCollector {
    pub fn new(mode: ExplainMode, alive_count: u64) -> Self;
    pub fn start_planning(&mut self);
    pub fn finish_planning(&mut self);
    pub fn record_clause(&mut self, m: ClauseMetrics);
    pub fn record_sort(&mut self, m: SortMetrics);
    pub fn record_cache_event(&mut self, hit: bool);
    pub fn finalize(self) -> QueryMetrics;
}
```

No `Arc<Mutex<>>` needed -- the collector lives on the query's stack and is passed as `&mut`. Single-threaded query execution means no contention.

---

## 3. Integration Points

### executor.rs: compute_filters

The existing tracing::debug calls in `compute_filters` (lines 312-346) already measure what we need. The change is minimal:

```rust
// Before (existing):
tracing::debug!("    clause[{}]: eval={:.1}ms ...", i, ...);

// After (additional, when collector present):
if let Some(ref mut collector) = metrics_collector {
    collector.record_clause(ClauseMetrics {
        execution_order: i,
        clause_repr: format!("{}", clause),
        eval_time_us: eval_elapsed.as_micros() as u64,
        and_time_us: and_elapsed.as_micros() as u64,
        accumulator_before: acc_before,
        accumulator_after: result_card,
        actual_cardinality: bm_card,
        // ... rest filled from context
    });
}
```

### planner.rs: plan_query_with_context

Record estimated cardinality per clause during planning. Already computed -- just capture it.

### executor.rs: sort_and_paginate / simple_sort_and_paginate

Wrap sort call with timing. Record bit_layers_visited from sort field internals (requires minor addition to `SortField::top_n` to return layer count).

### concurrent_engine.rs: execute_query

Thread the `Option<&mut QueryMetricsCollector>` through from `execute_query` to `executor.execute`.

---

## 4. EXPLAIN ANALYZE Output Format

### Text Output (for `/query?explain=analyze`)

```
BitDex EXPLAIN ANALYZE
----------------------------------------------------------------------
Query
  filters: nsfwLevel IN [1,2], tagIds = 12345, type = image
  sort:    reactionCount DESC
  limit:   100

Dataset: 105,000,000 records
Timing:  plan=0.18ms  exec=6.94ms  total=7.12ms
Cache:   3 hits / 1 miss (75%)

Clause Execution
 Ord  Clause                       EstCard     ActCard     Eval     AND      AccAfter     Mode          Cache
  1   type = image                 18,200,000  18,041,223  0.031ms  0.412ms  18,041,223   by-ref eq     hit
  2   tagIds = 12345                  420,000     398,211  0.022ms  0.147ms     116,430   by-ref eq     hit
  3   nsfwLevel IN [1,2]          62,000,000  61,712,808  0.018ms  0.006ms      79,104   by-ref in     hit
  4   sortAtUnix >= 1700000000    24,500,000  23,881,990  0.944ms  0.005ms      52,617   range scan    miss

Sort: reactionCount DESC
  candidates=52,617  returned=100  layers=19/32  early_stop=yes  time=5.32ms

Highlights
  most selective:      tagIds = 12345 (18M -> 116K, 99.4% reduction)
  most expensive eval: sortAtUnix >= 1700000000 (0.944ms)
  dominant phase:      sort (76.7% of execution)
  estimator quality:   mean q-error=1.07
```

### JSON Output (for `"explain": "analyze"` in query body)

Returns the `QueryMetrics` struct serialized as JSON alongside the normal query results:

```json
{
  "ids": [52341, 98712, ...],
  "cursor": "...",
  "total_matched": 52617,
  "explain": {
    "mode": "Analyze",
    "alive_count": 105000000,
    "planning_time_us": 182,
    "execution_time_us": 6941,
    "total_time_us": 7123,
    "clauses": [
      {
        "execution_order": 0,
        "clause_repr": "type = image",
        "clause_type": "Eq",
        "field": "type",
        "estimated_cardinality": 18200000,
        "actual_cardinality": 18041223,
        "accumulator_before": 105000000,
        "accumulator_after": 18041223,
        "eval_time_us": 31,
        "and_time_us": 412,
        "fast_path": "by_ref_eq",
        "cache_outcome": "hit",
        "children": []
      }
    ],
    "sort": {
      "field": "reactionCount",
      "direction": "Desc",
      "strategy": "bit_layer_traversal",
      "input_candidates": 52617,
      "output_rows": 100,
      "bit_layers_visited": 19,
      "total_bit_layers": 32,
      "early_stop": true,
      "time_us": 5321
    },
    "cache": { "lookups": 4, "hits": 3, "misses": 1 },
    "estimator": { "mean_q_error": 1.07, "max_q_error": 1.17, "worst_clause": "sortAtUnix >= 1700000000" },
    "highlights": [
      { "kind": "most_selective", "label": "tagIds = 12345", "detail": "18,041,223 -> 116,430" },
      { "kind": "dominant_phase", "label": "sort", "detail": "76.7%" }
    ]
  }
}
```

---

## 5. HTTP API

### Query with EXPLAIN

```json
POST /query
{
  "filters": [...],
  "sort": {"field": "reactionCount", "direction": "desc"},
  "limit": 100,
  "explain": "analyze"
}
```

`explain` field values:
- `true` or `"explain"`: Estimates only, no execution
- `"analyze"`: Full execution with timing

### Dedicated EXPLAIN endpoint

```
POST /explain
```

Same query body (without `explain` field), always returns EXPLAIN ANALYZE. Useful for debugging tools.

---

## 6. Future Enhancements (Not V1)

These came from Gemini's bitmap-internals analysis and are worth tracking but not building initially:

### Container-Level Metrics (from Gemini)

- **Container Interaction Matrix**: Count of array-array, array-bitset, bitset-bitset interactions per AND operation. Explains why two bitmaps with same cardinality have different AND cost.
- **High-key intersection count**: Number of overlapping 16-bit container keys between two bitmaps. Predicts AND cost better than raw cardinality.
- **Two-tiered density**: `active_containers_ratio` (global sparsity) + `avg_container_cardinality` with variance (local clustering).

### Memory Metrics (from Gemini)

- **CoW diff chain depth**: VersionedBitmap diff layers stacked on base. Long chains degrade read perf.
- **Array capacity slack**: `Vec::capacity() - len()` across Array containers for hidden bloat.

### Sort Branch Analysis (from Gemini)

- **Branch pruning rate**: % of bit-layer branches skipped during top-K
- **Bitslice cardinality decay curve**: How fast candidates shrink per layer
- **Useless intersections count**: `Alive AND Bit_N` ops that yielded cardinality 0

### TUI Dashboard (from GPT)

- Split-pane layout: plan tree left, node details right, runtime histogram bottom
- Color coding: green (selective), red (expensive), blue (cache hit), magenta (sort)
- Accumulator reduction bar chart showing per-clause shrinkage
- Could use ratatui for terminal rendering

---

## 7. Implementation Plan

### Phase 1: Core Structs + JSON (minimal)

1. Add `QueryMetrics` and related structs to new `src/query_metrics.rs`
2. Add `QueryMetricsCollector` with `record_clause`, `record_sort`, `finalize`
3. Thread `Option<&mut QueryMetricsCollector>` through `executor.execute` and `compute_filters`
4. Add `explain` field to `BitdexQuery`
5. Return `QueryMetrics` in JSON response when requested

**Touches:** `src/query_metrics.rs` (new), `src/executor.rs`, `src/query.rs`, `src/bin/server.rs`

### Phase 2: Sort Metrics

1. Add layer-visited tracking to `SortField::top_n`
2. Populate `SortMetrics` with real traversal data

**Touches:** `src/sort.rs`, `src/executor.rs`

### Phase 3: Text EXPLAIN Output

1. Implement `Display` for `QueryMetrics` producing the tabular format
2. Add `/explain` endpoint returning text/plain

**Touches:** `src/query_metrics.rs`, `src/bin/server.rs`

### Phase 4: Estimator Quality

1. Compute q-error per clause after execution
2. Populate `EstimatorSummary` and `Highlight` analysis

**Touches:** `src/query_metrics.rs`

---

## Design Decisions

1. **Stack-allocated collector, not Arc<Mutex<>>**: Query execution is single-threaded within a snapshot. No need for shared ownership or locking.

2. **Flat clause list, not full tree**: Top-level clauses are always implicitly ANDed. Nested And/Or/Not use `children` recursively. But the primary view is a flat table by execution order -- easier to scan than a deep tree.

3. **Microsecond integers, not Duration**: JSON-friendly, avoids float precision issues. Use `_us` suffix convention.

4. **Optional everything in ANALYZE mode**: `ClauseMetrics` fields like `actual_cardinality` are `Option` so EXPLAIN (no execution) can share the same struct with fields set to `None`.

5. **Bridge tracing, don't replace it**: Keep existing `tracing::debug!` calls. The metrics collector is an additional sink, not a replacement. Both write from the same measurement points.

---

## References

- [Gemini findings](query-metrics-gemini.md): Container internals, density metrics, cost prediction, sort traversal analysis
- [GPT findings](query-metrics-gpt.md): EXPLAIN format, Rust structs, TUI design, tracing integration
- BitDex executor: `src/executor.rs:305-350` (existing per-clause tracing)
- BitDex planner: `src/planner.rs:155-206` (cardinality estimation)
