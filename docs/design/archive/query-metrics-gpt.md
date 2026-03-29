# Query Metrics Design: GPT Consultation

**Source:** OpenAI GPT-5.4 Codex (via OpenRouter)
**Date:** 2026-03-14
**Topic:** EXPLAIN output format, TUI visualization, structured metrics, and tracing integration

---

## 1. EXPLAIN Output Format

### Two Modes

- **EXPLAIN**: Planner-focused, estimates only
- **EXPLAIN ANALYZE**: Planner + runtime + actual cardinalities

### Layout Structure

```text
BitDex EXPLAIN ANALYZE
----------------------------------------------------------------------
Query
  filters: nsfwLevel IN [1,2], tagIds = 12345, type = image, sortAtUnix >= 1700000000
  sort:    reactionCount DESC
  limit:   100

Dataset
  records:              105,000,000
  planning time:        0.182 ms
  execution time:       6.941 ms
  total time:           7.123 ms

Plan Summary
  reorder strategy:     ascending estimated cardinality
  fast-path by-ref:     enabled
  sort strategy:        bit-layer traversal (MSB -> LSB), DESC
  cache:                3 hits / 1 miss
```

### Detailed Clause Table

Key columns: Ord, Clause, EstCard, ActCard, Sel%, Eval, AND, AccBefore, AccAfter, Delta, Mode, Cache

```text
| Ord | Clause                      | EstCard    | ActCard    | Eval    | AND     | AccBefore  | AccAfter   | Delta       | Mode          | Cache |
|-----|-----------------------------|------------|------------|---------|---------|------------|------------|-------------|---------------|-------|
| 1   | type = image                | 18,200,000 | 18,041,223 | 0.031ms | 0.412ms | 105,000,000| 18,041,223 | -86,958,777 | by-ref eq     | hit   |
| 2   | tagIds = 12345              | 420,000    | 398,211    | 0.022ms | 0.147ms | 18,041,223 | 116,430    | -17,924,793 | by-ref eq     | hit   |
| 3   | nsfwLevel IN [1,2]          | 62,000,000 | 61,712,808 | 0.018ms | 0.006ms | 116,430    | 79,104     | -37,326     | by-ref in     | hit   |
| 4   | sortAtUnix >= 1700000000    | 24,500,000 | 23,881,990 | 0.944ms | 0.005ms | 79,104     | 52,617     | -26,487     | range bitmap  | miss  |
```

Mode values: `by-ref eq`, `by-ref in`, `by-ref noteq`, `by-ref notin`, `materialized or`, `materialized not`, `range bitmap`, `bucket bitmap`

### Estimator Quality Section

```text
Estimator Quality
  mean q-error:         1.07
  max q-error:          1.17
  worst offender:       tagIds = 12345 (est=420K, act=398K, qerr=1.05)
```

Q-error = max(estimated/actual, actual/estimated). Helps detect bad planning choices.

### Sort Metrics Section

```text
Sort
  field:                reactionCount
  order:                DESC
  input candidates:     52,617
  output rows:          100
  early stop:           yes
  bit layers visited:   19 / 32
  frontier expansions:  1,482
  candidate checks:     3,904
  tie-break scans:      17
  time:                 5.321 ms
```

### Highlights / Bottleneck Callout

```text
Highlights
  most selective:       tagIds = 12345           (18M -> 116K)
  most expensive eval:  sortAtUnix >= 1700000000 (0.944 ms)
  dominant operator:    sort                      (76.7% of execution time)
```

---

## 2. QueryMetrics Rust Struct Design

### Core struct hierarchy

```rust
pub struct QueryMetrics {
    pub query_id: String,
    pub explain_mode: ExplainMode,
    pub dataset: DatasetMetrics,
    pub planning: PlanningMetrics,
    pub execution: ExecutionMetrics,
    pub root: PlanNodeMetrics,           // Tree root
    pub estimator: Option<EstimatorMetrics>,
    pub cache: Option<CacheMetrics>,
    pub sort: Option<SortMetrics>,
    pub highlights: Vec<HighlightMetric>,
}
```

### PlanNodeMetrics (recursive tree node)

```rust
pub struct PlanNodeMetrics {
    pub node_id: u32,
    pub kind: PlanNodeKind,              // Limit, SortTopK, BitmapAnd, BitmapOr, BitmapNot, Clause
    pub label: String,
    pub clause: Option<ClauseMetrics>,   // Present for leaf clauses
    pub bitmap_op: Option<BitmapOpMetrics>,
    pub sort_op: Option<SortMetrics>,
    pub children: Vec<PlanNodeMetrics>,  // Recursive for nested And/Or/Not
}
```

### ClauseMetrics

```rust
pub struct ClauseMetrics {
    pub clause_type: ClauseType,
    pub field: Option<String>,
    pub expr_repr: String,
    pub execution_order: Option<usize>,
    pub estimated_cardinality: Option<u64>,
    pub actual_cardinality: Option<u64>,
    pub accumulator_before: Option<u64>,
    pub accumulator_after: Option<u64>,
    pub eval_time_us: Option<u64>,
    pub and_time_us: Option<u64>,
    pub fast_path: Option<FastPathKind>,  // ByRefEq, ByRefIn, ByRefNotEq, ByRefNotIn
    pub cache: Option<CacheEventMetrics>,
}
```

### SortMetrics

```rust
pub struct SortMetrics {
    pub field: String,
    pub order: SortOrder,
    pub strategy: String,                // "bit_layer_msb_to_lsb"
    pub input_candidates: Option<u64>,
    pub output_rows: Option<u64>,
    pub early_stop: Option<bool>,
    pub bit_layers_visited: Option<u32>,
    pub total_bit_layers: Option<u32>,
    pub frontier_expansions: Option<u64>,
    pub candidate_checks: Option<u64>,
    pub time_us: Option<u64>,
}
```

### EstimatorMetrics

```rust
pub struct EstimatorMetrics {
    pub mean_q_error: Option<f64>,
    pub max_q_error: Option<f64>,
    pub per_clause: Vec<EstimateErrorMetrics>,
}
```

All structs derive `Serialize, Deserialize` for JSON output.

---

## 3. TUI Tree Rendering

### Layout: Split-pane design

```text
+-- BitDex Explain Analyze --------- Query q_01HRZK7M4K2Q --+
| filters: nsfwLevel IN [1,2], tagIds = 12345, ...           |
| sort: reactionCount DESC  limit: 100  total: 7.123 ms      |
+------------- Plan Tree ---------------+-- Node Details -----+
| Limit(100)                             | type = image        |
|  +- SortTopK reactionCount DESC        | est: 18.2M          |
|     +- BitmapAnd reordered             | act: 18.0M          |
|        +- [1] Eq(type, image)    17.2% | eval: 0.031 ms     |
|        +- [2] Eq(tagIds, 12345)  0.4%  | and:  0.412 ms     |
|        +- [3] In(nsfwLevel)      58.8% | acc: 105M -> 18M   |
|        +- [4] Gte(sortAtUnix)    22.7% | cache: hit          |
+------------- Runtime Histogram --------+--------------------+
| Eval  [type]| [tag]| [nsfw]| [sortAt]======                 |
| AND   [type]=== [tag]= [nsfw]| [sortAt]|                    |
| Sort  [reactionCount]=========================               |
+--------------------------------------------------------------+
```

### Color Coding Rules

| Condition | Color |
|---|---|
| Selectivity < 1% | Bright green |
| Selectivity 1-10% | Green |
| Selectivity 10-30% | Yellow |
| Selectivity > 30% | Red/yellow |
| Cache hit | Blue |
| Cache miss | Red |
| Q-error near 1.0 | Green |
| Q-error > 2.0 | Red |
| Dominant time contributor | Bold magenta |

### Histogram Bars

Three types:
1. **Selectivity bar**: Width = actual_cardinality / total_records
2. **Runtime bar**: Scaled by maximum operator time
3. **Accumulator reduction bar**: Shows how much each clause shrank the running set

### Nested Boolean Clauses

```text
BitmapAnd                                   act=52,617  time=1.61ms
+- Eq(type, image)                          act=18,041,223 eval=0.03ms and=0.41ms
+- Or                                       act=612,882    eval=0.28ms
|  +- Eq(tagIds, 12345)                     act=398,211    eval=0.02ms
|  +- Eq(tagIds, 67890)                     act=230,441    eval=0.02ms
+- Gte(sortAtUnix, 1700000000)              act=23,881,990 eval=0.94ms and=0.01ms
```

---

## 4. JSON Output Format

Full JSON mirrors the Rust struct hierarchy. Key design decisions:

- Tree structure with `root.children[].children[]` for nested boolean ops
- All times in microseconds (`_us` suffix) for integer precision
- Optional fields use `skip_serializing_if = "Option::is_none"`
- Stable field names for programmatic consumption
- `highlights` array for quick bottleneck identification

See the full JSON example in the GPT raw output for a complete 960-line response with every field populated.

---

## 5. Bridging Tracing with Structured Metrics

### Recommended Architecture

**Dual-write approach**: Directly update a `QueryMetricsCollector` in code AND emit tracing events alongside.

```rust
pub struct QueryMetricsCollector {
    inner: Arc<Mutex<QueryMetrics>>,
}

impl QueryMetricsCollector {
    pub fn record_clause_estimate(&self, node_id: u32, est: u64);
    pub fn record_clause_actual(&self, node_id: u32, actual: u64);
    pub fn record_clause_eval_time(&self, node_id: u32, us: u64);
    pub fn record_clause_and_time(&self, node_id: u32, us: u64);
    pub fn record_accumulator(&self, node_id: u32, before: u64, after: u64);
    pub fn record_cache_event(&self, node_id: u32, event: CacheEventMetrics);
    pub fn record_sort_metrics(&self, sort: SortMetrics);
}
```

### Tracing Event Taxonomy

- `bitdex.query.start` / `bitdex.query.finish`
- `bitdex.query.plan`
- `bitdex.clause.estimate`
- `bitdex.clause.eval.start` / `bitdex.clause.eval.finish`
- `bitdex.bitmap.and.finish`
- `bitdex.sort.start` / `bitdex.sort.finish`
- `bitdex.cache.lookup`

Each event includes `query_id` and clause events include `node_id`.

### Key Principle

`QueryMetrics` is the source of truth. Tracing is a mirror for observability. The collector is only created when `explain=true` on the query, so zero overhead on the hot path.

---

## Key Takeaways

1. **Two distinct modes** (EXPLAIN vs EXPLAIN ANALYZE) serve different audiences: planners vs debuggers
2. **Clause table is the centerpiece** -- estimated vs actual cardinality, accumulator shrinkage, and mode (fast-path vs materialized)
3. **Q-error metric** (max(est/act, act/est)) cleanly surfaces bad estimates
4. **Fast-path visibility** is critical -- surface `by-ref eq/in/noteq/notin` prominently
5. **Sort metrics** should show bit-layer traversal depth, early stop, and frontier expansions
6. **Dual-write collector + tracing** avoids hot-path overhead while keeping observability
7. **JSON output** mirrors the Rust struct tree exactly for programmatic consumption
