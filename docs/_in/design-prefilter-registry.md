# Prefilter Registry — Design Doc

**Author:** Ivy
**Date:** 2026-04-14
**Status:** Proposed
**Estimated effort:** 2-3 days

## Problem

At 107M records, every Civitai feed query starts with a ~7-clause safety prefix that returns ~98M slots:

```
NOT(availability = Private)
AND blockedFor NOT IN [tos, moderated, CSAM, AiNotVerified]
AND postId IS NOT NULL
AND NOT(poi = true)
AND NOT(minor = true)
AND NOT((nsfwLevel IN [4,8,16,32] AND baseModel IN [SD 3, SD 3.5, ...]))
AND isPublished = true
```

Every query re-evaluates this prefix against a 100M-bit accumulator. Trace analysis (v1.0.217, 2026-04-14):

| Clause | Cost (1 query) |
|--------|----------------|
| 7 simple ANDs on ~72M acc | ~330ms |
| Compound NOT (nsfwLevel × baseModel) | ~588ms |
| **Total safety prefix** | **~900ms** |

The gap between `filter_us` total and sum of clause costs is <1ms — it's real bitmap work, not lock contention.

## Prior measurement (validates theory)

Tested via prod API with `skip_cache=true`:
- Safety prefix alone (6 clauses, no nsfwLevel): 100M records, compute 140-300ms
- Compound NOT alone: 110M records, compute 90-300ms
- Combined ~98M records, ~500ms cold compute

Expected per-feed-query savings with precomputed combined bitmap: **300-800ms** (1 clone + 1 AND replaces 7-8 clause evaluations).

## Goals

1. **Live-registerable** — orchestrator (or ops) POSTs a clause-set + name, server precomputes, caches, and auto-substitutes on matching queries. No code deploys.
2. **Stale-while-revalidate** — periodic refresh without blocking queries. Mild drift is acceptable for the theory test.
3. **Zero risk to existing queries** — queries that don't match a registered prefilter run exactly as before.
4. **Observable** — metrics for substitution rate, cardinality, refresh time.

## Non-goals (for this phase)

- Auto-detection of common shapes (save for Phase 2 — requires fingerprint telemetry)
- Hot sync with ops pipeline (accept interval-based refresh + drift)
- Multi-level tree (save for Phase 2 — one flat registry per index is enough)
- Substitution when clauses are *subset* of registered prefilter (exact clause match only for Phase 1)

## Architecture

### Data structures

```rust
// In src/prefilter.rs (new module)
pub struct PrefilterRegistry {
    entries: DashMap<String, Arc<PrefilterEntry>>,
}

pub struct PrefilterEntry {
    pub name: String,
    pub clauses: Vec<FilterClause>,          // Canonical form (sorted, deduped)
    pub bitmap: ArcSwap<RoaringBitmap>,      // Atomic swap on refresh
    pub refresh_interval_secs: u64,
    pub last_refreshed: AtomicI64,           // Unix seconds
    pub compute_duration_nanos: AtomicU64,   // Last refresh duration
    pub cardinality: AtomicU64,              // Cache len() for fast access
}
```

The registry lives on `InnerEngine` (not `ConcurrentEngine`) so it participates in the ArcSwap snapshot model. Queries access via `snap.prefilters.lookup_matching(...)`.

### HTTP API

```
POST /api/indexes/{index}/prefilters
Body: { "name": "civitai_safe", "clauses": [...FilterClause...], "refresh_interval_secs": 300 }
Response: { "cardinality": 98274619, "compute_time_ms": 487 }

GET /api/indexes/{index}/prefilters
Response: { "prefilters": [
  { "name": "civitai_safe", "cardinality": 98274619, "last_refreshed": "2026-04-14T03:15:22Z",
    "refresh_interval_secs": 300, "last_compute_ms": 487 }
]}

DELETE /api/indexes/{index}/prefilters/{name}
Response: 204 No Content
```

### Background refresh thread (stale-while-revalidate)

Single thread per index, spawned at boot. Loop:

```
every tick (e.g., 10s):
  for entry in registry:
    if now - entry.last_refreshed >= entry.refresh_interval_secs:
      let new_bitmap = compute(entry.clauses)   // ~300-500ms, off the query path
      entry.bitmap.store(Arc::new(new_bitmap))  // Atomic swap
      entry.last_refreshed.store(now)
      entry.cardinality.store(new_bitmap.len())
```

Queries in flight during the swap keep using the old bitmap (ArcSwap semantics). Next query gets the new one.

**Compute path:** reuse `executor.compute_filters(&entry.clauses)` — same code the query planner uses.

### Query planner substitution

In `resolve_filters_traced` (src/concurrent_engine.rs:5581), before `plan_query_with_context`:

```rust
let (effective_filters, substituted_prefilter) = if let Some(registry) = self.prefilters() {
    registry.substitute(filters)  // Returns (new_clauses, Some(prefilter_name)) or (filters, None)
} else {
    (filters, None)
};
```

**Substitution algorithm:**

1. Canonicalize the query's clause set (sorted, deduped).
2. For each registered prefilter, check if its clause set is a subset of the query's clause set (exact clause matching — `FilterClause: PartialEq` already works).
3. Prefer the prefilter with the **largest** matched clause set (most work saved).
4. If match: remove the matched clauses from the query's list, prepend a `FilterClause::BucketBitmap { field: "__prefilter", bucket_name: prefilter.name, bitmap: prefilter.bitmap.load_full() }`.
5. Log + metric: `prefilter_substitutions_total{name}`.

`BucketBitmap` evaluation is already just `acc &= bitmap` (executor.rs:466) — zero new code on the hot path.

### Canonical clause form (matching key)

For exact-match detection, both registered and query clauses must be normalized:

- `In`/`NotIn` value lists sorted
- Top-level `And` flattened into the clause list
- Preserve `Not(Not(X))` = X collapsing
- Hashable canonical form (for future fingerprinting)

Implementation: new `canonicalize_clause(&FilterClause) -> FilterClause` in src/prefilter.rs. Uses `FilterClause::PartialEq` after canonicalization.

### Race considerations

**Register while query in flight:** New prefilter doesn't affect in-flight queries — they've already picked their snapshot. First query after publish sees the new registry.

**Refresh during substitution:** Substitution calls `bitmap.load_full()` once, holds Arc until query completes. Refresh thread may swap in parallel — new snapshot won't affect the Arc already held.

**Invalidation on schema change (index config PATCH):** If a filter field is removed or changed, registered prefilters with that field become invalid. For Phase 1, leave them stale; operator responsibility to re-register. Phase 2 can auto-invalidate.

## Correctness

**Exact clause match** means result equivalence is trivially guaranteed — we're substituting:
```
acc = compute(A ∧ B ∧ C ∧ rest_of_query)
```
with:
```
acc = prefilter_bitmap(A ∧ B ∧ C) ∧ rest_of_query
```

Both evaluate to the same set, assuming the prefilter bitmap is an accurate materialization of A ∧ B ∧ C at refresh time.

**Drift:** Between refreshes, data changes mean the prefilter bitmap diverges from a fresh evaluation. At 107M records with 5min refresh:
- ~100 ops/sec → ~30,000 ops per refresh interval
- Most ops don't touch the prefilter's fields; only those that do cause drift
- Worst case: a newly-published image shows up as "not in prefilter" until next refresh = user sees result ~5min late
- Not a correctness bug; same semantics as any eventually-consistent cache

**Drift-free fallback (optional):** If drift is unacceptable, we can track ops that touch prefilter fields and apply them incrementally (same pattern as alive_bitmap). Phase 2 work.

## Testing plan

### Unit tests (src/prefilter.rs)
- Register + lookup round trip
- Substitute on matching query
- No substitute on non-matching query
- Prefer largest match when multiple prefilters apply
- Clause canonicalization (sorted In values, flattened And, Not(Not(X)) = X)

### Integration test (crates/bitdex/tests/)
- Full server boot, register prefilter, run matching + non-matching queries, verify result equivalence
- Run 100 queries mid-refresh, verify correctness throughout swap

### Local 109M test
- Register civitai_safe via POST
- Run feed queries with and without matching prefix
- Verify: result counts identical, filter_us drops 300-800ms
- Monitor RSS, refresh latency

## Rollout plan

1. **Phase 1a: Register + compute + GET/DELETE (no substitution)** — Deploy, orchestrator registers prefilters, no query-path change. Validates compute feasibility at prod scale without risk.
2. **Phase 1b: Enable substitution behind query flag** — New `apply_prefilter: Option<String>` on `BitdexQuery`. Orchestrator opts in per-query. Zero impact on non-opted-in queries.
3. **Phase 1c: Auto-substitute by clause match** — Remove the flag requirement; substitute automatically when a registered prefilter's clauses are a subset of the query's. Defaults to on, has config kill switch.

Each phase bakes for ~1 hour in prod before advancing.

## Metrics

| Name | Type | Purpose |
|------|------|---------|
| `bitdex_prefilter_registered_total{index}` | gauge | Number of registered prefilters |
| `bitdex_prefilter_cardinality{index, name}` | gauge | Current bitmap cardinality |
| `bitdex_prefilter_substitutions_total{index, name}` | counter | Queries that matched this prefilter |
| `bitdex_prefilter_refresh_seconds{index, name}` | histogram | Time to recompute bitmap |
| `bitdex_prefilter_refresh_errors_total{index, name}` | counter | Refresh failures (compute error, etc) |

Dashboard: substitution rate by prefilter, last-refresh age per prefilter, filter_seconds histogram grouped by `substituted` label.

## Open questions

1. **Clause ordering for canonicalization** — should `In([1,2,3])` and `Or(Eq(1), Eq(2), Eq(3))` canonicalize to the same form? Proposed: no, keep them distinct. Orchestrator can register both if they want coverage.
2. **Max registered prefilters** — bound the registry size to prevent memory explosion. Proposed: 32 per index, configurable.
3. **Persistence** — should the registry survive restart? Proposed: no for Phase 1; orchestrator re-registers on startup (health check triggers push). Phase 2 can add disk persistence.
4. **Compute during eager_load blocking boot** — don't compute prefilters until after the server is serving queries. First refresh happens asynchronously after boot.

## Files changed (estimate)

- `src/prefilter.rs` — new module, ~300 lines
- `src/concurrent_engine.rs` — hook into resolve_filters_traced, ~40 lines
- `src/server.rs` — 3 endpoints (POST/GET/DELETE), ~150 lines
- `src/query.rs` — canonicalize_clause helper, ~50 lines
- `src/metrics.rs` — 5 new metrics, ~30 lines
- `tests/prefilter_integration.rs` — new test, ~200 lines

Total: ~770 lines of new/modified code.
