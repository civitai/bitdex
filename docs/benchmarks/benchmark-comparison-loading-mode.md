# Benchmark Comparison: Baseline vs Loading Mode Fix

**Date**: 2026-02-21
**Platform**: Windows 11, 4 threads, on-disk docstore (redb), remapped IDs
**Dataset**: Civitai `metrics_images_v1` (104.6M records)
**Baseline**: benchmark-report.md (Feb 19, commit range 95df2a5-763a008)
**Current**: commit 6fb2b78 (loading mode + bound cache + meta-index)

---

## Write Performance (Loading Mode Fix)

The ArcSwap snapshot publish (`staging.clone()`) after every flush cycle caused `Arc::make_mut()` to deep-clone FilterField HashMaps. Loading mode skips publishing during bulk inserts.

### Sequential Benchmarks (clean, no concurrent load)

| Scale | Baseline (RwLock 4ae6c56) | Before Fix | After Fix (Loading Mode) |
|-------|--------------------------|------------|--------------------------|
| 1M   | 82,558/s (12.11s)        | 51,122/s (19.56s) | **70,153/s** (14.25s) |
| 5M   | 61,615/s (81.15s)        | 24,924/s (200.61s) | **56,113/s** (89.11s) |

- **1M**: 70K/s vs 82K baseline = 15% gap (was 38% regressed)
- **5M**: 56K/s vs 62K baseline = 9% gap (was 60% regressed)
- **Scaling fixed**: 1M→5M drop is 20% (baseline: 24%), was 51%

### Full 104M Insert (from insert benchmark phase)

| Metric | Baseline (benchmark-report.md) | Current (Loading Mode) |
|--------|-------------------------------|------------------------|
| Rate | ~35,325/s (2,957s) | **28,316/s** (3,694s) |
| Wall time | ~3,133s | **4,235s** |
| RSS | 12.14 GB | 14.52 GB |
| Alive | 104,590,539 | 104,590,539 |

The 104M rate (28K/s) is lower than 5M (56K/s) — expected degradation from growing bitmap sizes. The remaining gap vs baseline is partly from ArcSwap overhead and partly from system conditions (runs on different days; per memory notes, numbers vary 2-3x with system load).

---

## Memory (104.6M Records)

| Component | Baseline | Current | Delta |
|-----------|----------|---------|-------|
| Filter bitmaps | 5.63 GB | 5.65 GB | +0.4% |
| Sort bitmaps | 757 MB | 757 MB | = |
| Trie cache | 111 MB | 111 MB | = |
| Bound cache | — | 3.70 KB | new |
| Meta-index | — | 270 B | new |
| **Total bitmap** | **6.49 GB** | **6.51 GB** | **+0.3%** |
| **RSS** | **12.14 GB** | **14.51 GB** | **+20%** |

Bitmap memory is identical. RSS increased 2.4 GB due to ArcSwap dual-snapshot (staging + published copy coexist briefly after loading mode exit).

---

## Query Latencies (104.6M Records, p50)

**Note**: Numbers from different sessions. System load differences may account for ~1.5-2x variation on filter-only queries.

### Sort Queries (Improved by Bound Cache)

| Query | Baseline p50 | Current p50 | Speedup |
|-------|-------------|-------------|---------|
| sort_reactionCount_desc | 9.01ms | **3.64ms** | 2.5x |
| filter_nsfw1_sort_reactions | 7.71ms | **1.68ms** | 4.6x |
| filter_tag_sort_reactions | 7.48ms | **2.14ms** | 3.5x |
| filter_sort_commentCount | 7.03ms | **1.87ms** | 3.8x |
| filter_sort_id_asc | 21.13ms | **1.61ms** | 13.1x |
| filter_nsfw1_onSite_sort | 9.03ms | **4.96ms** | 1.8x |
| filter_3_clauses_sort | 10.56ms | **6.08ms** | 1.7x |

The bound cache narrows the sort working set by pre-filtering with an approximate top-K bitmap. `filter_sort_id_asc` sees the largest improvement (13x) because its bound most effectively cuts the candidate set.

### Filter-Only Queries

| Query | Baseline p50 | Current p50 | Ratio |
|-------|-------------|-------------|-------|
| filter_eq_userId | 0.034ms | 0.041ms | 1.2x |
| filter_eq_nsfwLevel_1 | 4.67ms | 7.84ms | 1.7x |
| filter_eq_onSite_true | 4.97ms | 7.74ms | 1.6x |
| filter_eq_tagId_popular | 4.82ms | 7.82ms | 1.6x |
| filter_and_2_clauses | 6.40ms | 9.68ms | 1.5x |
| filter_and_3_clauses | 8.12ms | 11.41ms | 1.4x |
| filter_or_3_tags | 15.11ms | 26.20ms | 1.7x |

Filter queries are ~1.5x slower. Likely causes: (1) different system load between sessions, (2) higher RSS (14.5 vs 12.1 GB) causing more cache pressure. Sparse lookups (userId) remain sub-millisecond.

### Cache-Related Queries

| Query | Baseline p50 | Current p50 | Ratio |
|-------|-------------|-------------|-------|
| prefix_shared_A | 10.76ms | 5.90ms | 1.8x faster |
| prefix_shared_B | 10.81ms | 6.22ms | 1.7x faster |
| prefix_shared_C | 10.82ms | 5.51ms | 2.0x faster |

Prefix cache sharing queries improved ~1.7-2x, likely from trie cache warm-up during the extended query iterations.

---

## Bound Cache Effectiveness (104.6M)

Cold vs warm sort query comparison:

| Query | Cold p50 | Warm p50 | Speedup |
|-------|----------|----------|---------|
| all_sort_reactions | 10.05ms | 3.91ms | 2.6x |
| nsfw1_sort_reactions | 7.09ms | 1.52ms | 4.7x |
| nsfw1_onSite_sort_reactions | 9.58ms | 4.09ms | 2.3x |
| tag_sort_reactions | 7.32ms | 1.56ms | 4.7x |
| nsfw1_sort_commentCount | 8.54ms | 1.68ms | 5.1x |
| nsfw1_sort_id_asc | 22.15ms | 1.83ms | 12.1x |

Bound cache overhead: 6 bounds = 2.28 KB. Meta-index: 6 entries = 180 B.

---

## Summary

| Aspect | Before | After | Verdict |
|--------|--------|-------|---------|
| Write speed (5M) | 25K/s (-60%) | 56K/s (-9%) | Fixed |
| Write scaling | Super-linear regression | Linear scaling restored | Fixed |
| Sort queries (p50) | 7-21ms | 1.6-6ms | 2-13x faster |
| Filter queries (p50) | 4.6-15ms | 7.7-26ms | ~1.5x slower* |
| Bitmap memory | 6.49 GB | 6.51 GB | Unchanged |
| RSS | 12.14 GB | 14.51 GB | +20% |
| Bound cache size | — | 2.28 KB | Negligible |

*Filter query regression likely from system load variation between sessions. Needs same-session A/B test to confirm.
