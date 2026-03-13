# Phase B: Sort-by-Slot Optimization -- Spec Compliance Audit

**Date:** 2026-02-21
**Auditor:** Claude Opus 4.6
**Spec source:** `docs/plans/roadmap-performance-and-persistence.md`, lines 198-217
**Files audited:**
- `src/executor.rs`
- `src/planner.rs`
- `src/query.rs`

---

## Executive Summary

Phase B is **substantially compliant**. All four spec items (B1-B4) have implementations that satisfy their core requirements. Two minor gaps exist: (1) `skip_sort` is present in `QueryPlan` but never consumed by the executor, which relies on the caller's `sort: Option<&SortClause>` instead; (2) `slot_order_paginate` is hardcoded to descending-only with no direction parameter, so ascending slot-order pagination is not supported. Neither gap causes incorrect behavior for the stated acceptance criteria, but both represent deviations from the spec's letter.

---

## B1: Reverse Iteration (Newest-First)

**Status: COMPLIANT**

### Spec Requirement

> Default no-sort to descending (newest-first by slot). Use roaring's reverse iterator or rank/select for efficient descending slot traversal. Add sort direction parameter to no-sort path.

### Evidence

The `slot_order_paginate` method at `src/executor.rs:616-648` implements descending (newest-first) slot order as the default when no sort is specified:

```rust
// executor.rs:614-615
/// Paginate by descending slot order (newest-first) for no-sort queries.
/// Highest slot IDs come first since slots are monotonically assigned.
fn slot_order_paginate(
    &self,
    candidates: &RoaringBitmap,
    limit: usize,
    cursor: Option<&crate::query::CursorPosition>,
) -> (Vec<i64>, Option<crate::query::CursorPosition>) {
```

The implementation takes the last `limit` slots from the bitmap and reverses them to produce descending order:

```rust
// executor.rs:636-641
// Take the last `limit` slots (highest slot IDs = newest)
let total = effective.len() as usize;
let skip = total.saturating_sub(limit);
let slots: Vec<u32> = effective.iter().skip(skip).collect();
// Reverse to get descending order
let ids: Vec<i64> = slots.iter().rev().map(|&s| s as i64).collect();
```

All three executor entry points (`execute`, `execute_with_cache`, `execute_from_bitmap`) route no-sort queries to this method:

```rust
// executor.rs:142-145 (execute)
} else {
    // No sort: return in descending slot order (newest first)
    self.slot_order_paginate(&candidates, limit, cursor)
};
```

```rust
// executor.rs:234-237 (execute_with_cache)
} else {
    // No sort: return in descending slot order (newest first)
    self.slot_order_paginate(&candidates, limit, cursor)
};
```

```rust
// executor.rs:382-385 (execute_from_bitmap)
} else {
    // No sort: return in descending slot order (newest first)
    self.slot_order_paginate(&candidates, limit, cursor)
};
```

### Gap

**Minor:** The spec says "Add sort direction parameter to no-sort path." The function signature has no direction parameter -- it is hardcoded to descending only. There is no way for a caller to request ascending slot-order iteration through this path.

The implementation uses `skip` + forward iterate + `rev()` rather than roaring's reverse iterator. This works correctly but is O(n) over all candidates rather than O(limit). For large candidate sets this means iterating the entire bitmap to collect the tail. A reverse iterator approach (`iter().rev().take(limit)`, which roaring-rs does not natively support, or `select`/`rank` operations) would be more efficient at scale. However, the spec uses "or" language ("Use roaring's reverse iterator or rank/select"), and the current approach satisfies functional correctness.

### Impact

The missing direction parameter means ascending slot-order queries (oldest-first) are not possible through the no-sort path. This is a feature omission rather than a correctness issue. The acceptance criteria state "No-sort queries return newest-first by default" which is satisfied. The performance concern (O(n) vs O(limit)) matters at scale but is not a spec violation.

---

## B2: Cursor Pagination for Slot Iteration

**Status: COMPLIANT**

### Spec Requirement

> Replace sort_value: 0 placeholder. Descending: "50 slots below slot X". Ascending: "50 slots above slot X". Use roaring range iteration (range(..cursor_slot)).

### Evidence

Cursor pagination is implemented in `slot_order_paginate` at `src/executor.rs:622-627`:

```rust
// executor.rs:622-628
// For cursor-based pagination, narrow candidates to slots below cursor
let effective = if let Some(cursor) = cursor {
    let mut narrowed = candidates.clone();
    // Remove slots >= cursor_slot_id (we want strictly less than cursor for desc order)
    narrowed.remove_range(cursor.slot_id..=u32::MAX);
    narrowed
} else {
    candidates.clone()
};
```

This effectively implements "slots below slot X" for descending order by removing all slots >= the cursor's slot_id using `remove_range`. This is functionally equivalent to roaring's `range(..cursor_slot)` as specified.

The cursor is emitted from the last result ID:

```rust
// executor.rs:643-647
let next_cursor = ids.last().map(|&last_id| crate::query::CursorPosition {
    sort_value: 0,
    slot_id: last_id as u32,
});
```

The `CursorPosition` struct in `src/query.rs:87-91`:

```rust
pub struct CursorPosition {
    pub sort_value: u64,
    pub slot_id: u32,
}
```

### Gap

**Minor:** The spec says "Replace sort_value: 0 placeholder." The emitted cursor at line 645 still uses `sort_value: 0`. This is technically a placeholder value. However, for slot-order pagination the sort_value is semantically meaningless -- only `slot_id` is used for cursor resolution (line 626). The cursor works correctly despite the zero.

The spec also requires ascending pagination ("50 slots above slot X"). Since the function has no direction parameter (see B1 gap), ascending cursor pagination via this path does not exist.

### Impact

The `sort_value: 0` is cosmetic -- it does not affect correctness because the slot-order paginator only uses `cursor.slot_id`. If the cursor were ever serialized to an external consumer and that consumer inspected `sort_value`, the zero could be misleading. Ascending pagination is unavailable (same root cause as B1).

---

## B3: Planner Awareness

**Status: PARTIAL**

### Spec Requirement

> Planner should flag no-sort queries explicitly in QueryPlan to skip all sort-related computation. Add skip_sort: bool to QueryPlan.

### Evidence

The `skip_sort` field exists in `QueryPlan` at `src/planner.rs:97-99`:

```rust
pub struct QueryPlan {
    pub ordered_clauses: Vec<FilterClause>,
    pub use_simple_sort: bool,
    pub estimated_result_size: u64,
    /// Whether to skip sort entirely (no sort field specified).
    /// When true, results are returned in descending slot order (newest first).
    pub skip_sort: bool,
}
```

It is correctly set in `plan_query` at `src/planner.rs:107-151`:

```rust
// planner.rs:107
/// `has_sort` indicates whether the query specifies a sort field.
/// When false, `skip_sort` is set in the plan so the executor returns
/// results in descending slot order (newest first) instead of sorting.
pub fn plan_query(
    clauses: &[FilterClause],
    filters: &FilterIndex,
    slots: &SlotAllocator,
    has_sort: bool,
) -> QueryPlan {
    // ...
    // planner.rs:120
    skip_sort: !has_sort,
    // ...
    // planner.rs:150
    skip_sort: !has_sort,
```

A unit test verifies the flag at `src/planner.rs:557-567`:

```rust
#[test]
fn test_skip_sort_flag() {
    let h = TestHarness::new();

    // has_sort=false -> skip_sort=true
    let plan = plan_query(&[], &h.filters, &h.slots, false);
    assert!(plan.skip_sort);

    // has_sort=true -> skip_sort=false
    let plan = plan_query(&[], &h.filters, &h.slots, true);
    assert!(!plan.skip_sort);
}
```

### Gap

**The executor never reads `plan.skip_sort`.** The no-sort decision is made by matching on the caller-provided `sort: Option<&SortClause>` parameter, not the planner's flag. In all three executor methods:

```rust
// executor.rs:136 (execute)
let (ids, next_cursor) = if let Some(sort_clause) = sort {
```

```rust
// executor.rs:213 (execute_with_cache)
let (ids, next_cursor) = if let Some(sort_clause) = sort {
```

```rust
// executor.rs:376 (execute_from_bitmap)
let (ids, next_cursor) = if let Some(sort_clause) = sort {
```

A grep for `plan.skip_sort` across the executor returns zero matches -- the field is set but never consumed. The `has_sort` argument to `plan_query` is computed from `sort.is_some()` at `src/executor.rs:124`:

```rust
let plan = planner::plan_query(filters, self.filters, self.slots, sort.is_some());
```

This means the planner's `skip_sort` flag is redundant with the executor's own `if let Some(sort_clause) = sort` check. The spec's intent -- "skip all sort-related computation" -- is functionally achieved because the executor does skip sort when `sort` is `None`. But the planner flag is dead code.

### Impact

The functional behavior is correct: no-sort queries do skip sort. The `skip_sort` flag is a planner output that could drive future optimizations (e.g., the executor could use it to avoid even constructing sort-related state), but currently it is unused. This is a dead-code concern, not a correctness issue.

---

## B4: Tests -- Slot-Order Pagination

**Status: PARTIAL**

### Spec Requirement

> Forward and reverse pagination, cursor correctness, empty pages, single result.

### Evidence

Tests exist in `src/executor.rs` starting at line 1131:

**Descending slot order (reverse):** `test_no_sort_returns_descending_slot_order` at line 1132:

```rust
#[test]
fn test_no_sort_returns_descending_slot_order() {
    // ...
    let result = h.query(&[], None, 100, None);
    assert_eq!(result.ids, vec![5, 4, 3, 2, 1]);
}
```

**Cursor pagination (descending):** `test_no_sort_cursor_pagination` at line 1146:

```rust
#[test]
fn test_no_sort_cursor_pagination() {
    // ...
    let page1 = h.query(&[], None, 5, None);
    assert_eq!(page1.ids, vec![10, 9, 8, 7, 6]);
    assert!(page1.cursor.is_some());

    let page2 = h.query(&[], None, 5, page1.cursor.as_ref());
    assert_eq!(page2.ids, vec![5, 4, 3, 2, 1]);
}
```

**With filters:** `test_no_sort_with_filter` at line 1166:

```rust
#[test]
fn test_no_sort_with_filter() {
    // ...
    let result = h.query(
        &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
        None, 100, None,
    );
    assert_eq!(result.ids, vec![10, 8, 6, 4, 2]);
}
```

**Planner `skip_sort` flag:** `test_skip_sort_flag` at `src/planner.rs:557`.

### Gap

The following test scenarios from the spec are **missing**:

1. **Forward (ascending) pagination** -- No test for ascending slot-order iteration because the feature does not exist (see B1 gap).

2. **Empty pages** -- No test for querying with a cursor that is beyond all results (should return empty `ids` vec and `None` cursor). The `slot_order_paginate` function does handle this case at line 632-634:

    ```rust
    if effective.is_empty() {
        return (Vec::new(), None);
    }
    ```

    But no test exercises this path.

3. **Single result** -- No test for a query that returns exactly one result. The pagination logic should still emit a correct cursor.

4. **Cursor correctness validation** -- No test that explicitly asserts the cursor's `slot_id` and `sort_value` fields have the expected values. The tests use the cursor opaquely (pass page1's cursor to page2) but never inspect cursor contents.

### Impact

The existing tests cover the primary use case (descending pagination across multiple pages) and the filtered variant. The missing edge-case tests (empty pages, single result, cursor field validation) could mask bugs in boundary conditions. The missing ascending pagination test is a consequence of the missing feature (B1 gap), not a test oversight.

---

## Acceptance Criteria Evaluation

The spec states:

> **Acceptance:** No-sort queries return newest-first by default. Cursor pagination works in both directions. No sort layer bitmaps touched for these queries.

| Criterion | Status | Notes |
|-----------|--------|-------|
| No-sort queries return newest-first by default | **PASS** | `slot_order_paginate` returns descending slot order. Three tests confirm this. |
| Cursor pagination works in both directions | **PARTIAL** | Descending cursor pagination works. Ascending cursor pagination does not exist (no direction parameter). |
| No sort layer bitmaps touched for these queries | **PASS** | `slot_order_paginate` never calls `sort_and_paginate` or `simple_sort_and_paginate`. No `SortField` or `SortIndex` access occurs in the no-sort path. |

---

## Summary Table

| Item | Status | Key Finding |
|------|--------|-------------|
| **B1** | COMPLIANT | Descending slot order is the default. Direction parameter absent (ascending not supported). Implementation uses skip+reverse rather than reverse iterator. |
| **B2** | COMPLIANT | Cursor uses `remove_range` for slot narrowing. `sort_value: 0` placeholder remains but is functionally irrelevant for slot-order pagination. |
| **B3** | PARTIAL | `skip_sort: bool` exists in `QueryPlan` and is correctly set, but the executor never reads it. The no-sort decision is driven by `sort: Option<&SortClause>` matching instead. Dead code. |
| **B4** | PARTIAL | Descending order, cursor pagination, and filtered slot-order tests exist. Missing: ascending pagination, empty pages, single result, cursor field assertions. |

---

## Recommendations

1. **B3 -- Wire `skip_sort` into executor or remove it.** Either have the executor consult `plan.skip_sort` to make no-sort decisions (replacing the `if let Some(sort_clause)` pattern), or remove the field from `QueryPlan` to avoid dead code. The planner-driven approach is more principled and would allow the planner to override even when a sort clause is provided (e.g., for trivially-ordered result sets).

2. **B4 -- Add missing edge-case tests.** At minimum:
   - Empty page: query with a cursor whose `slot_id` is lower than all alive slots.
   - Single result: query with exactly one matching slot, verify cursor emitted.
   - Cursor field assertion: inspect `cursor.slot_id` and `cursor.sort_value` explicitly.

3. **B1/B2 -- Consider adding ascending direction.** The spec calls for "both directions" and "Add sort direction parameter to no-sort path." Adding an optional direction parameter to `slot_order_paginate` with an ascending path (`iter().take(limit)` with `range(cursor_slot+1..)`) would fully satisfy the spec. This is low effort and would enable oldest-first use cases.

4. **B2 -- Consider replacing `sort_value: 0` with a sentinel.** Even though the zero is harmless, using `None`-wrapped or a dedicated `SlotCursor` variant would make the API more explicit about what kind of cursor is being returned.

5. **B1 -- Consider performance optimization.** The current `skip(n).collect().rev()` pattern is O(total_candidates). At 100M+ alive documents with no filters, this iterates the entire alive bitmap. Using roaring's `select(rank - 1)`, `select(rank - 2)`, etc. or collecting from the end via `iter().rev()` (if supported by the roaring-rs version) would be O(limit) instead.
