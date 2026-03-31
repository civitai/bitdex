# PR #112 Test Verification — sortAt Docstore Overwrite Fix

**Date:** 2026-03-31
**PRs:** #111 (re-apply lost clamps), #112 (computed sort values win over raw writes)
**Status:** ✅ FULL 6/6 PHASE VERIFICATION PASSED (2026-03-31, updated with zero-skip fix)

---

## Root Cause

The `data_schema` maps `sortAtUnix` -> `sortAt` with `ms_to_seconds: true`. This is a V1 NDJSON-era mapping. In V2 CSV dumps, `sortAtUnix` doesn't exist as a row field. The data_schema write produces 0 for sortAt, which overwrites the correct `GREATEST(existedAt, publishedAt)` value written by `extra_i64_fields`.

**Fix (PR #112):** Two-part fix in `dump_processor.rs`:
1. **Skip-set:** Build `extra_skip` HashSet from `extra_i64_fields` targets — prevents data_schema/computed writes for those fields
2. **Zero-skip guard:** In the `extra_i64_fields` write loop, skip writing when `value == 0` — prevents non-images phases (tags, resources, tools, techniques, metrics) from overwriting the correct sortAt with 0 (since they have no existedAt/publishedAt, computing GREATEST(0,0)=0)

**Fix (PR #111):** Re-apply 12 lost `.max(0)` clamps for negative sort values (lost during rebase of PR #100).

---

## Full 6-Phase Local Verification (2026-03-31)

### Test Setup
- Fresh server (port 3006), empty data directory
- PR #112 + zero-skip guard applied
- Test CSVs: 3 images (slots 1001, 1002, 1003) with posts enrichment
- All 6 phases run in production order

### Phase Results

| Phase | Rows | Key Result |
|-------|------|-----------|
| images | 3 | sortAt=1710000000 (GREATEST(scannedAt, publishedAt)) written correctly |
| tags | 3 | tagIds populated: 1001→[9999,7337], 1002→[7337] |
| resources | 2 | modelVersionIds populated: 1001→200, 1002→201; sortAt NOT overwritten (zero-skip) |
| tools | 2 | toolIds populated |
| techniques | 3 | techniqueIds populated |
| metrics | 3 | reactionCount/commentCount/collectedCount written; sortAt NOT overwritten (zero-skip) |

### Document Verification (after all 6 phases)

| Slot | sortAt | reactionCount | commentCount | collectedCount | tagIds | modelVersionIds |
|------|--------|--------------|-------------|---------------|--------|----------------|
| 1001 | **1710000000** ✅ | **5** ✅ | 2 ✅ | 3 ✅ | [9999,7337] ✅ | 200 ✅ |
| 1002 | **1710000000** ✅ | **10** ✅ | 0 ✅ | 1 ✅ | [7337] ✅ | 201 ✅ |
| 1003 | **1710000000** ✅ | 0 ✅ | 0 ✅ | 0 ✅ | [] | [] (no resources) |

### Query Verification

| Query | Expected | Result |
|-------|----------|--------|
| Sort sortAt DESC | All 3 docs, non-zero sortAt | ✅ ids=[1003,1002,1001], sortAt=1710000000 all |
| Filter tagIds=[7337] | Slots 1001, 1002 | ✅ ids=[1002,1001] |
| Filter modelVersionIds=[200] | Slot 1001 only | ✅ ids=[1001] |
| Sort reactionCount DESC | 1002 (10), 1001 (5), 1003 (0) | ✅ ids=[1002,1001,1003] |

### Diagnostic Log Evidence

Resources phase (critical — was the bug):
```
[diag] slot=1001 config_computed_sort_vals=[("sortAt", 0)] enriched_fields=[("baseModel", "SD 1.5")]
```
The zero-skip guard (`if value == 0 { continue; }`) prevents this 0 from overwriting sortAt=1710000000.

Metrics phase:
```
[diag] slot=1001 config_computed_sort_vals=[("sortAt", 0)] enriched_fields=[]
```
Same protection — sortAt=0 skipped, original value preserved.

---

## What Still Needs Testing (Production)

| # | Test | Status |
|---|------|--------|
| 1 | Full 109M production dump completes without error | NOT TESTED (needs deploy) |
| 2 | Docstore size ~33GB (not 144GB from bloat) | NOT TESTED |
| 3 | poi / isPublished boolean correct | NOT TESTED (regression) |
| 4 | Shadow mode comparison vs Meilisearch | NOT TESTED |
| 5 | Negative sort values clamped (PR #111) with actual negative data | NOT TESTED (need CH data with -1 reactionCount) |

---

## Production Field Verification Matrix

After deploy, check 5-10 random documents:

| Field | Expected | Check |
|-------|----------|-------|
| sortAt | > 0 (epoch seconds ~1.7B) | Non-zero, matches GREATEST(existedAt, publishedAt) |
| existedAt | > 0 (epoch seconds) | Non-zero |
| publishedAt | > 0 or absent | Non-zero for published images |
| reactionCount | >= 0 | Non-negative (PR #111 clamp) |
| commentCount | >= 0 | Non-negative |
| collectedCount | >= 0 | Non-negative |
| tagIds | array of ints | Non-empty for tagged images |
| modelVersionIds | array of ints | Non-empty for images with resources |
| url | string | Present (doc_only field) |
| hash | string | Present (doc_only field) |

---

## Recommended Deploy Plan

1. **Merge PR #112** (PR #111 already merged)
2. **Cut new release** (includes #111 clamps + #112 computed sort fix + zero-skip guard)
3. **Full PVC nuke** (Aidan — everything under /data except load_stage/)
4. **Deploy** — server starts fresh
5. **Wait for dump completion** (~15-20 min for 109M records, 6 phases)
6. **Run field verification** — check 5-10 random documents for all fields above
7. **Run sort/filter queries** to confirm sortAt ordering correct
8. **Check docstore size** — should be ~33GB, not 144GB
9. **Enable shadow mode** — compare against Meilisearch
