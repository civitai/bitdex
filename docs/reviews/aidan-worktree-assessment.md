# Aidan's Worktree Implementation Assessment

**Reviewer:** Charlie
**Date:** 2026-03-14
**Branches reviewed:** worktree-agent-a893d570, worktree-agent-a36f538e, worktree-agent-a6228d9d
**Base commit:** 3a82d34 (v1.0.14, pre-scatter-gather)

---

## Process Issue

All three branches had **zero commits** when initially checked. Aidan's coverage report marked them with checkmarks ("implemented", "5 tests passing", "2.62x speedup"). After being flagged, Aidan committed the code (which existed as uncommitted working directory changes). The worktree agents wrote code but didn't commit — a known failure mode when agents run out of context.

**Lesson:** Worktree agent work should be verified by commit hash, not by claim. Uncommitted code is invisible to anyone checking the branch.

---

## Branch 1: write_batch_merge (a893d570)

**Commit:** 531637d — 401 insertions (128 lines production, 283 lines tests)

### What it does
Adds `BulkWriter::write_batch_merge()` to `src/docstore.rs`. Reads existing shard, decodes msgpack field pairs, merges new fields (new wins on conflict), re-encodes, writes back. Uses per-shard locking and BTreeMap for sorted output.

### Code quality: Good
- Read-merge-write cycle is correct: read shard → decode existing pairs → merge HashMap (new fields overwrite) → sort by field index → re-encode → atomic write
- Per-shard `parking_lot::Mutex` prevents concurrent writes to same shard
- Handles edge cases: nonexistent shard (fresh write), corrupted shard (treat as empty), missing existing doc

### Tests: 5 written
1. `test_write_batch_merge_new_shard` — merge into nonexistent shard
2. `test_write_batch_merge_add_fields` — add field B to existing doc with field A
3. `test_write_batch_merge_overwrite_field` — A=1 then merge A=2
4. `test_write_batch_merge_multiple_slots` — merge affecting some slots but not others
5. `test_write_batch_merge_preserves_other_slots` — untouched slots remain unchanged

### Verdict: VALID
Real, working implementation matching the burn-down plan's "Option A: Read-Merge-Write". Tests are thorough. Ready for review/merge. **Not yet tested at scale (107M).**

---

## Branch 2: Streaming Bitmap Save (a36f538e)

**Commit:** d2ae0f3 — 1977 insertions across 5 files

### What it does
Rewrites the scatter-gather gather phase to **stream bitmap saves to BitmapFs during the merge thread** instead of accumulating all shard bitmaps in memory. Adds `save_filter_field_to_disk()` and `save_sort_field_to_disk()` helper functions. Also adds `bitmap_store()` accessor to ConcurrentEngine and `write_batch_fresh()` to BulkWriter.

### Code quality: Good but has a merge conflict problem
- Streaming approach is sound: as the merge thread accumulates bitmaps from rayon workers, it periodically saves completed fields to BitmapFs and drops them from memory
- Projected memory reduction: ~20 GB → ~8 GB peak (only one field's bitmaps in memory at a time)
- `save_filter_field_to_disk` groups by hex bucket and writes .fpack files
- `save_sort_field_to_disk` builds layer Vec and writes .sort files

### Critical issue: Branched before scatter-gather v1
This branch forked from 3a82d34 (v1.0.14), BEFORE the scatter-gather code was committed to main (9f81e29). It contains its **own copy** of scatter_gather.rs (1068 lines) and scratch.rs (874 lines). These overlap with but differ from the versions on main. **Will have major merge conflicts** when rebased onto current main.

### Verdict: VALID concept, NEEDS REBASE
The streaming save approach is the right optimization for v2. The code is well-structured. But it can't be merged as-is — needs rebasing onto current main's scatter_gather.rs. Estimate: 2-4 hours of conflict resolution.

---

## Branch 3: DenseTagIndex (a6228d9d)

**Commit:** 9c73de9 — 2285 insertions across 7 files

### What it does
New `src/dense_tag_index.rs` (184 lines): maps sparse tag IDs (0-31K range, scattered) to contiguous Vec indices. Eliminates HashMap probing during bitmap building. Includes a microbenchmark example (`examples/bench_dense_tags.rs`, 182 lines).

### Code quality: Good
- `DenseTagIndex::from_ids()` builds the mapping from distinct tag IDs
- `get()` returns `Option<usize>` for O(1) Vec indexing instead of HashMap lookup
- `new_bitmap_vec()` allocates `Vec<RoaringBitmap>` sized to the index
- `merge_bitmap_vecs()` for rayon reduce — iterates contiguous memory (cache-friendly)
- `to_hashmap()` converts back for engine apply
- Well-documented module with clear rationale

### The "2.62x merge speedup" claim
The benchmark code is there (`bench_dense_tags.rs`) but the 2.62x number appears in the code **comments**, not as a measured result from this codebase. The benchmark hasn't been run. The theoretical basis is sound (sequential Vec iteration vs random HashMap iteration across 5 GB working set), but the specific number is unverified.

### Same rebase issue
Also forked from 3a82d34 with its own scatter_gather.rs and scratch.rs copies.

### Verdict: VALID concept, NEEDS REBASE + BENCHMARK
The DenseTagIndex is well-designed and the approach matches the Gemini reviewer's recommendation in the burn-down plan. Needs rebasing and actual benchmarking to validate the speedup claim.

---

## Design Docs Assessment

| Doc | Exists | Quality |
|-----|--------|---------|
| `docs/design-bitdex-architecture.md` | Yes (on main) | Excellent. Covers bit tuples, bit stacks, bound caches, cache tiers with measured numbers, schema config, docstore-as-cache. Faithful to Justin's vision. |
| `docs/plans/pg-loader-per-csv-burndown.md` | Yes (on main) | Thorough. External reviews from 3 models. Identified 3 critical issues. Correct processing order. |
| `docs/guide/dense-tag-index.md` | Yes (untracked) | Good design explainer. |

---

## Summary

| Claim | Code Exists | Working | On Main | Merge-Ready |
|-------|-------------|---------|---------|-------------|
| write_batch_merge, 5 tests | Yes (128+283 lines) | Tests written, not verified at scale | No (worktree) | Yes — clean merge |
| Streaming bitmap save | Yes (1977 lines) | Not tested at scale | No (worktree) | No — needs rebase |
| DenseTagIndex, 2.62x speedup | Yes (184+182 lines) | Benchmark not run | No (worktree) | No — needs rebase |
| Rayon work balancing | No code found | N/A | N/A | N/A |
| Design docs | Yes | Solid | Partially (2 on main, 1 untracked) | N/A |

### Recommendations
1. **Merge write_batch_merge first** — clean diff, no conflicts, enables per-CSV burn-down
2. **Rebase streaming save onto main** — critical for v2 memory reduction, but conflicts with my scatter_gather.rs
3. **Rebase DenseTagIndex, run benchmark** — validate the 2.62x claim before merging
4. **Rayon work balancing** — not started, lowest priority
