# Query Metrics Design: Gemini Consultation

**Source:** Google Gemini 3 Pro (via OpenRouter)
**Date:** 2026-03-14
**Topic:** Bitmap-specific metrics for a roaring bitmap query engine at 105M scale

---

## 1. Container-Level Stats and Operation Costs

At 105M records, a fully populated universe spans ~1,602 containers (105M / 65,536).

### Useful Container Metrics

- **`container_mix`**: Ratios of Array, Bitset, Run containers per bitmap
- **`container_keys_count`**: Number of allocated 16-bit high keys (max 1,602)
- **`promotion_count` / `demotion_count`**: How often operations cause Array-to-Bitset promotions (cardinality > 4096) or vice versa

### Container Interaction Cost Model

| Interaction | Cost | Notes |
|---|---|---|
| Bitset vs Bitset | O(1) per container | Pure SIMD bitwise, 8KB memory |
| Array vs Array | O(len(A) + len(B)) | SIMD intersection or galloping search |
| Array vs Bitset | O(len(Array)) | Iterate array, probe bitset bits |
| Run vs Anything | Variable | AND with Run is fast (binary search on intervals), OR can be expensive if it fragments runs |

### Container Interaction Matrix

During an AND operation, emit a metric counting each interaction type:
```
op_matrix_and: {bitset_bitset: 450, array_bitset: 120, array_array: 12}
```

This reveals the "physical execution profile" of each bitmap operation.

---

## 2. Density/Sparsity Metrics

Global density (cardinality / universe) is insufficient. A bitmap with 100K records could be uniformly spread (1,602 Array containers of length 62) or perfectly clustered (2 full Bitsets and 1,600 empty containers).

### Two-Tiered Approach

1. **Global Sparsity (`active_containers_ratio`)**:
   - Formula: `container_keys_count / max_possible_containers (1602)`
   - If bitmap A has 10 active containers and bitmap B has 1000, the planner knows the maximum number of container-level comparisons is 10

2. **Local Density (`avg_container_cardinality` and `container_variance`)**:
   - Variance reveals clustering. High variance = mix of dense Bitsets and sparse Arrays
   - Rule of thumb: If local density > 4096 consistently, the bitmap is Bitset-heavy and SIMD operations dominate

---

## 3. Operation Cost Prediction

A naive planner uses `Cost(A AND B) = Card(A) * Card(B)`. A roaring-aware planner uses **Key Overlap**.

### Predictive Metrics

- **`high_key_intersection_count`**: Before the actual bitwise AND, intersect the 16-bit high keys (container IDs). If bitmaps A and B share only 5 container keys, cost is limited to those 5 containers regardless of total cardinality

- **`predicted_op_cost`**:
  ```
  Cost = SUM over k in (Keys_A INTERSECT Keys_B) of CostWeight(Type(A_k), Type(B_k))
  ```
  Assign weights: Bitset-Bitset = 1, Array-Bitset = 2, Array-Array = 3

- If `high_key_intersection_count` is 0, the AND is O(1) (empty result). The planner should track this for aggressive reordering to trigger early empty-set returns.

---

## 4. Memory Profiling Metrics

Given the 913MB budget and VersionedBitmap (MVCC) architecture:

- **`cow_diff_chain_depth`**: How many diff bitmaps stacked on the base? Long chains degrade reads and balloon memory
- **`cow_wasted_bytes`**: Memory consumed by ANDNOT tracking in the diff layer for deleted records. Triggers compaction
- **`run_inefficiency_bytes`**: Run containers take 2x16-bit ints per run. If a container has alternating 1s and 0s, Run storage is worse than Array/Bitset. Track how many Run containers are mathematically larger than their optimal representation
- **`array_capacity_slack`**: Rust Vec over-allocates. Track `capacity() - len()` across all Array containers for hidden memory bloat

---

## 5. Visualization: Bitmap Flow Diagram

### Sankey/Heatmap Approach

- **Nodes (Operations)**: Represent AND, OR, NOT
  - Heatmap color: Based on Container Interaction Matrix (blue for fast Bitset-Bitset, red for expensive fallbacks)

- **Edges (Bitmaps flowing through)**:
  - Thickness: Represents cardinality (shrinks through AND nodes)
  - Texture/sparkline: Stacked bar of container_mix (Array vs Bitset vs Run)
  - Drop-off rate: `1 - (output_cardinality / input_cardinality)`. An AND node with 99% drop-off should have been executed earlier

---

## 6. Sort Traversal Metrics (MSB-to-LSB)

Sorting via bit-sliced bitmaps is essentially traversing a binary trie.

- **`sort_branch_pruning_rate`**: When finding Top-N, if the '1' bit-slice branch has enough cardinality to satisfy N, the '0' branch is entirely pruned. Track percentage of bit-slices skipped

- **`useless_intersections_count`**: How many `Alive AND Bit_N` operations resulted in cardinality 0? High numbers indicate sparse bit-slices doing work for no records

- **`bitslice_cardinality_decay`**: How rapidly the candidate set shrinks at each bit layer
  - Example: `[MSB: 10M -> MSB-1: 5M -> MSB-2: 4.9M -> ...]`
  - If decay stalls, the engine is grinding through low-entropy bits that don't differentiate data

- **`sort_yield_ratio`**: `Records_Requested / Bitwise_Ops_Performed`. A perfect sort reads exactly the bits needed. Low ratio = many heavy intersections for few rows

---

## Key Takeaways for BitDex

1. **Container Interaction Matrix** is the single most informative new metric -- it explains why two bitmaps with the same cardinality can have wildly different AND performance
2. **High-key intersection count** is cheap to compute and predicts operation cost better than raw cardinality
3. **Two-tiered density** (global sparsity + local variance) captures clustering effects that global density misses
4. **Sort branch pruning rate** directly measures sort traversal efficiency
5. **CoW diff chain depth** is specific to VersionedBitmap and critical for detecting read performance degradation
