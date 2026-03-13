# Bitdex V2 — Benchmark Report

**Date**: 2026-02-19
**Platform**: Windows 11, 4 threads, on-disk docstore (redb), remapped IDs
**Dataset**: Civitai `metrics_images_v1` (~104.6M records, 59 GB NDJSON)

---

## Executive Summary

Bitdex V2 scales predictably from 5M to 104.6M records. Bitmap memory grows roughly linearly with document count — dominated by `tagIds` which accounts for 70–80% of all filter memory. At full production scale (104.6M records), the engine uses 6.49 GB of bitmap memory and 12.14 GB RSS. All queries complete under 22ms at p50, with sparse-key lookups (e.g. `userId`) at 34 microseconds.

| Scale | Bitmap Memory | RSS | Load Time | Worst Query p50 | Best Query p50 |
|------:|-------------:|----:|----------:|----------------:|---------------:|
| 5M | 328 MB | 1.20 GB | 126s | 0.828ms | 0.009ms |
| 50M | 2.95 GB | 6.09 GB | 1,402s | 13.47ms | 0.040ms |
| 100M | 6.19 GB | 11.66 GB | 2,689s | 18.67ms | 0.052ms |
| ~104.6M | 6.49 GB | 12.14 GB | 2,957s | 21.13ms | 0.034ms |

---

## 5 Million Records

### Memory Breakdown

| Component | Size | Detail |
|-----------|-----:|--------|
| **Slots** | 617 KB | alive + clean |
| **Filter bitmaps** | 290.19 MB | |
| — tagIds | 230.84 MB | 10,542 bitmaps (79.6%) |
| — modelVersionIds | 37.15 MB | 173,625 bitmaps |
| — userId | 13.10 MB | 64,748 bitmaps |
| — nsfwLevel | 3.03 MB | 7 bitmaps |
| — onSite | 1.20 MB | 2 bitmaps |
| — hasMeta | 1.20 MB | 2 bitmaps |
| — techniqueIds | 887 KB | 8 bitmaps |
| — type | 884 KB | 2 bitmaps |
| — toolIds | 724 KB | 135 bitmaps |
| — poi | 618 KB | 2 bitmaps |
| — minor | 648 KB | 2 bitmaps |
| **Sort bitmaps** | 37.22 MB | |
| — sortAt | 18.67 MB | |
| — id | 14.05 MB | |
| — reactionCount | 3.00 MB | |
| — collectedCount | 1.45 MB | |
| — commentCount | 52 KB | |
| **Trie cache** | 5.32 MB | 10 entries (after queries) |
| **Total** | **333 MB** | |

### Query Latencies

| Query | p50 | p95 | p99 | mean |
|-------|----:|----:|----:|-----:|
| filter_eq_userId | 0.009ms | 0.010ms | 0.014ms | 0.009ms |
| filter_eq_nsfwLevel_1 | 0.092ms | 0.151ms | 0.269ms | 0.100ms |
| filter_eq_onSite_true | 0.094ms | 0.133ms | 0.253ms | 0.101ms |
| filter_not_eq_nsfwLevel | 0.097ms | 0.129ms | 0.296ms | 0.104ms |
| filter_eq_tagId_popular | 0.106ms | 0.140ms | 0.177ms | 0.112ms |
| filter_in_nsfwLevel | 0.116ms | 0.223ms | 0.306ms | 0.129ms |
| filter_and_2_clauses | 0.124ms | 0.165ms | 0.246ms | 0.131ms |
| filter_and_3_with_userId | 0.139ms | 0.263ms | 0.446ms | 0.161ms |
| filter_tag_sort_reactions | 0.146ms | 0.241ms | 0.326ms | 0.161ms |
| filter_nsfw1_sort_reactions | 0.152ms | 0.295ms | 0.497ms | 0.183ms |
| sort_reactionCount_desc | 0.172ms | 0.361ms | 0.479ms | 0.198ms |
| filter_sort_commentCount | 0.173ms | 0.300ms | 0.404ms | 0.189ms |
| filter_nsfw1_onSite_sort | 0.179ms | 0.282ms | 0.364ms | 0.195ms |
| filter_and_3_clauses | 0.223ms | 0.351ms | 0.476ms | 0.244ms |
| prefix_shared_B | 0.203ms | 0.378ms | 0.431ms | 0.230ms |
| prefix_shared_C | 0.202ms | 0.381ms | 0.507ms | 0.234ms |
| filter_or_3_tags | 0.405ms | 0.481ms | 0.580ms | 0.417ms |
| filter_sort_id_asc | 0.596ms | 0.833ms | 0.967ms | 0.620ms |
| filter_3_clauses_sort | 0.815ms | 1.083ms | 1.238ms | 0.837ms |
| prefix_shared_A | 0.828ms | 1.430ms | 1.722ms | 0.914ms |

---

## 50 Million Records

### Memory Breakdown

| Component | Size | Detail |
|-----------|-----:|--------|
| **Slots** | 5.97 MB | alive + clean |
| **Filter bitmaps** | 2.55 GB | |
| — tagIds | 2.02 GB | 16,675 bitmaps (79.2%) |
| — modelVersionIds | 336.93 MB | 390,991 bitmaps |
| — userId | 124.26 MB | 323,502 bitmaps |
| — nsfwLevel | 29.73 MB | 7 bitmaps |
| — onSite | 11.70 MB | 2 bitmaps |
| — hasMeta | 11.93 MB | 2 bitmaps |
| — poi | 7.32 MB | 2 bitmaps |
| — type | 6.43 MB | 2 bitmaps |
| — minor | 6.50 MB | 2 bitmaps |
| — toolIds | 1.85 MB | 212 bitmaps |
| — techniqueIds | 1.82 MB | 8 bitmaps |
| **Sort bitmaps** | 357.44 MB | |
| — sortAt | 181.15 MB | |
| — id | 130.47 MB | |
| — reactionCount | 29.64 MB | |
| — collectedCount | 15.52 MB | |
| — commentCount | 667 KB | |
| **Trie cache** | 52.27 MB | 10 entries (after queries) |
| **Total** | **2.95 GB** | |

### Query Latencies

| Query | p50 | p95 | p99 | mean |
|-------|----:|----:|----:|-----:|
| filter_eq_userId | 0.040ms | 0.046ms | 0.073ms | 0.041ms |
| filter_eq_nsfwLevel_1 | 2.505ms | 3.339ms | 3.647ms | 2.527ms |
| filter_eq_onSite_true | 2.595ms | 3.550ms | 3.942ms | 2.597ms |
| filter_not_eq_nsfwLevel | 2.653ms | 3.735ms | 4.963ms | 2.704ms |
| filter_in_nsfwLevel | 2.810ms | 3.579ms | 4.031ms | 2.834ms |
| filter_eq_tagId_popular | 2.832ms | 4.650ms | 5.388ms | 3.037ms |
| filter_and_2_clauses | 3.886ms | 6.204ms | 6.795ms | 4.178ms |
| filter_sort_commentCount | 3.996ms | 5.720ms | 6.684ms | 4.219ms |
| filter_nsfw1_sort_reactions | 4.140ms | 5.285ms | 5.642ms | 4.099ms |
| filter_and_3_with_userId | 4.715ms | 6.830ms | 7.544ms | 4.929ms |
| filter_tag_sort_reactions | 4.735ms | 7.091ms | 8.184ms | 5.024ms |
| sort_reactionCount_desc | 5.205ms | 8.310ms | 10.295ms | 5.622ms |
| filter_and_3_clauses | 5.231ms | 6.460ms | 6.866ms | 5.280ms |
| filter_nsfw1_onSite_sort | 5.550ms | 6.857ms | 7.435ms | 5.632ms |
| prefix_shared_B | 6.489ms | 8.588ms | 9.865ms | 6.598ms |
| prefix_shared_C | 7.143ms | 10.012ms | 11.392ms | 7.298ms |
| prefix_shared_A | 7.891ms | 11.205ms | 14.232ms | 8.241ms |
| filter_3_clauses_sort | 8.457ms | 10.881ms | 12.076ms | 8.507ms |
| filter_sort_id_asc | 11.251ms | 17.063ms | 20.657ms | 11.680ms |
| filter_or_3_tags | 13.470ms | 16.489ms | 18.399ms | 13.647ms |

---

## 100 Million Records

### Memory Breakdown

| Component | Size | Detail |
|-----------|-----:|--------|
| **Slots** | 11.93 MB | alive + clean |
| **Filter bitmaps** | 5.37 GB | |
| — tagIds | 4.28 GB | 30,511 bitmaps (79.7%) |
| — modelVersionIds | 707.21 MB | 657,788 bitmaps |
| — userId | 251.91 MB | 531,784 bitmaps |
| — nsfwLevel | 58.79 MB | 7 bitmaps |
| — onSite | 23.64 MB | 2 bitmaps |
| — hasMeta | 23.86 MB | 2 bitmaps |
| — type | 14.21 MB | 2 bitmaps |
| — poi | 13.43 MB | 2 bitmaps |
| — minor | 12.76 MB | 2 bitmaps |
| — techniqueIds | 7.70 MB | 8 bitmaps |
| — toolIds | 7.14 MB | 219 bitmaps |
| **Sort bitmaps** | 723.28 MB | |
| — sortAt | 364.37 MB | |
| — id | 266.49 MB | |
| — reactionCount | 60.54 MB | |
| — collectedCount | 30.69 MB | |
| — commentCount | 1.18 MB | |
| **Trie cache** | 106.14 MB | 10 entries (after queries) |
| **Total** | **6.19 GB** | |

### Query Latencies

| Query | p50 | p95 | p99 | mean |
|-------|----:|----:|----:|-----:|
| filter_eq_userId | 0.052ms | 0.067ms | 0.094ms | 0.051ms |
| filter_eq_nsfwLevel_1 | 4.158ms | 5.129ms | 5.565ms | 4.188ms |
| filter_eq_tagId_popular | 4.166ms | 5.401ms | 5.812ms | 4.252ms |
| filter_eq_onSite_true | 4.271ms | 6.938ms | 8.423ms | 4.628ms |
| filter_not_eq_nsfwLevel | 4.376ms | 7.695ms | 8.398ms | 4.857ms |
| filter_in_nsfwLevel | 4.481ms | 5.740ms | 6.819ms | 4.568ms |
| filter_and_2_clauses | 5.954ms | 8.345ms | 10.828ms | 6.158ms |
| filter_sort_commentCount | 6.206ms | 9.700ms | 12.438ms | 6.616ms |
| filter_nsfw1_sort_reactions | 6.432ms | 10.581ms | 12.838ms | 6.888ms |
| filter_tag_sort_reactions | 6.532ms | 8.215ms | 9.101ms | 6.605ms |
| filter_and_3_clauses | 7.175ms | 11.019ms | 12.437ms | 7.694ms |
| filter_and_3_with_userId | 7.304ms | 12.304ms | 14.834ms | 7.967ms |
| sort_reactionCount_desc | 7.478ms | 9.130ms | 10.769ms | 7.662ms |
| filter_nsfw1_onSite_sort | 8.718ms | 11.898ms | 15.773ms | 9.043ms |
| prefix_shared_C | 9.425ms | 14.923ms | 16.632ms | 10.007ms |
| prefix_shared_A | 9.553ms | 15.078ms | 18.382ms | 10.135ms |
| prefix_shared_B | 9.643ms | 15.490ms | 18.424ms | 10.396ms |
| filter_3_clauses_sort | 10.387ms | 15.982ms | 20.830ms | 10.860ms |
| filter_or_3_tags | 13.812ms | 21.923ms | 24.810ms | 14.565ms |
| filter_sort_id_asc | 18.669ms | 25.449ms | 30.109ms | 19.574ms |

---

## ~104.6 Million Records (Full Dataset)

### Memory Breakdown

| Component | Size | Detail |
|-----------|-----:|--------|
| **Slots** | 12.48 MB | alive + clean |
| **Filter bitmaps** | 5.63 GB | |
| — tagIds | 4.48 GB | 31,284 bitmaps (79.6%) |
| — modelVersionIds | 738.38 MB | 686,991 bitmaps |
| — userId | 263.07 MB | 550,152 bitmaps |
| — nsfwLevel | 61.56 MB | 7 bitmaps |
| — hasMeta | 24.95 MB | 2 bitmaps |
| — onSite | 24.73 MB | 2 bitmaps |
| — type | 15.10 MB | 2 bitmaps |
| — poi | 13.98 MB | 2 bitmaps |
| — minor | 13.35 MB | 2 bitmaps |
| — techniqueIds | 8.79 MB | 8 bitmaps |
| — toolIds | 7.98 MB | 219 bitmaps |
| **Sort bitmaps** | 756.78 MB | |
| — sortAt | 381.25 MB | |
| — id | 279.28 MB | |
| — reactionCount | 63.09 MB | |
| — collectedCount | 31.92 MB | |
| — commentCount | 1.23 MB | |
| **Trie cache** | 111.07 MB | 10 entries (after queries) |
| **Total** | **6.49 GB** | |

### Query Latencies

| Query | p50 | p95 | p99 | mean |
|-------|----:|----:|----:|-----:|
| filter_eq_userId | 0.034ms | 0.060ms | 0.069ms | 0.042ms |
| filter_eq_nsfwLevel_1 | 4.665ms | 5.934ms | 6.761ms | 4.774ms |
| filter_in_nsfwLevel | 4.749ms | 5.905ms | 6.616ms | 4.747ms |
| filter_eq_tagId_popular | 4.820ms | 6.684ms | 7.240ms | 4.965ms |
| filter_eq_onSite_true | 4.968ms | 7.324ms | 9.211ms | 5.244ms |
| filter_not_eq_nsfwLevel | 4.976ms | 10.161ms | 15.171ms | 5.597ms |
| filter_and_2_clauses | 6.404ms | 9.319ms | 10.899ms | 6.716ms |
| filter_sort_commentCount | 7.034ms | 8.583ms | 10.319ms | 7.113ms |
| filter_tag_sort_reactions | 7.477ms | 10.987ms | 13.167ms | 7.858ms |
| filter_nsfw1_sort_reactions | 7.706ms | 10.542ms | 11.985ms | 7.816ms |
| filter_and_3_with_userId | 7.817ms | 11.647ms | 13.625ms | 8.175ms |
| filter_and_3_clauses | 8.123ms | 12.589ms | 16.015ms | 8.642ms |
| filter_nsfw1_onSite_sort | 9.030ms | 13.231ms | 15.373ms | 9.341ms |
| sort_reactionCount_desc | 9.010ms | 12.545ms | 16.034ms | 9.327ms |
| filter_3_clauses_sort | 10.555ms | 15.814ms | 18.533ms | 10.986ms |
| prefix_shared_A | 10.763ms | 14.406ms | 18.447ms | 11.183ms |
| prefix_shared_B | 10.809ms | 15.430ms | 17.189ms | 11.241ms |
| prefix_shared_C | 10.819ms | 15.707ms | 17.673ms | 11.328ms |
| filter_or_3_tags | 15.112ms | 21.640ms | 25.794ms | 15.676ms |
| filter_sort_id_asc | 21.126ms | 27.918ms | 31.802ms | 21.793ms |

### Notable: Load Characteristics

- **Loaded**: 104,613,193 records from NDJSON
- **Alive after dedup**: 104,590,539 (22,654 duplicates removed via ID remapping)
- **Load time**: 2,957s (~49 min) — 28.3 μs/record
- **RSS**: 12.14 GB

---

## Scaling Analysis

### Memory Scaling (5M → 50M → 100M → 104.6M)

| Component | 5M | 50M | 100M | 104.6M | 5→100M | 100→104.6M |
|-----------|---:|----:|-----:|-------:|-------:|-----------:|
| Filter bitmaps | 290 MB | 2,550 MB | 5,499 MB | 5,765 MB | 19.0x | 1.05x |
| — tagIds | 231 MB | 2,020 MB | 4,383 MB | 4,587 MB | 19.0x | 1.05x |
| — modelVersionIds | 37 MB | 337 MB | 707 MB | 738 MB | 19.1x | 1.04x |
| — userId | 13 MB | 124 MB | 252 MB | 263 MB | 19.4x | 1.04x |
| Sort bitmaps | 37 MB | 357 MB | 723 MB | 757 MB | 19.5x | 1.05x |
| Total bitmaps | 328 MB | 2,950 MB | 6,190 MB | 6,490 MB | 18.9x | 1.05x |
| RSS | 1,200 MB | 6,090 MB | 11,940 MB | 12,431 MB | 10.0x | 1.04x |

Memory scaling is nearly linear — 2x data yields ~2x memory. At full scale (104.6M), bitmap memory (6.49 GB) accounts for 52% of RSS (12.14 GB), with redb + allocator responsible for the rest.

### Latency Scaling (p50)

| Query Type | 5M | 50M | 100M | 104.6M | 5→104.6M |
|------------|---:|----:|-----:|-------:|---------:|
| filter_eq_userId (sparse) | 0.009ms | 0.040ms | 0.052ms | 0.034ms | 3.8x |
| filter_eq_nsfwLevel (dense) | 0.092ms | 2.505ms | 4.158ms | 4.665ms | 50.7x |
| filter_and_3_clauses | 0.223ms | 5.231ms | 7.175ms | 8.123ms | 36.4x |
| sort_reactionCount_desc | 0.172ms | 5.205ms | 7.478ms | 9.010ms | 52.4x |
| filter_or_3_tags | 0.405ms | 13.470ms | 13.812ms | 15.112ms | 37.3x |
| filter_sort_id_asc | 0.596ms | 11.251ms | 18.669ms | 21.126ms | 35.4x |

Dense bitmap scans scale super-linearly (35-52x for 21x data) due to larger working sets exceeding L3 cache. However, the 100M→104.6M step (1.05x data) shows only 1.0–1.2x latency increase — growth is perfectly proportional at scale.

Sparse lookups (userId) remain nearly free at any scale — 34 microseconds at 104.6M.

### Extrapolation to 150M

Using the observed linear memory scaling:

| Scale | Est. Bitmap Memory | Est. RSS | Est. Worst p50 |
|------:|-------------------:|---------:|---------------:|
| 150M | ~9.3 GB | ~17.4 GB | ~30ms |

The original 7–11 GB design target for bitmap memory at 150M is on track. Latencies should remain under 35ms for all queries.

### Key Observations

1. **tagIds dominates everything**: Consistently 79–80% of filter memory across all scales (231 MB → 2.02 GB → 4.28 GB → 4.48 GB). Any memory optimization effort should focus here first.
2. **Sparse filters are nearly free**: userId lookups take 9–52 microseconds regardless of scale — the bitmap for a single user is tiny.
3. **Latency growth flattens past 50M**: The big jump is 5M→50M (cache miss cliff). From 50M→104.6M, most queries only grow 1.0–1.7x for 2x data, suggesting good cache behavior once working sets stabilize.
4. **Sort layers scale linearly**: 37 MB → 357 MB → 723 MB → 757 MB tracks document count perfectly.
5. **Trie cache scales with data**: 5 MB → 52 MB → 106 MB → 111 MB for 10 cached entries. Cost is proportional to bitmap density.
6. **RSS overhead ratio stabilizes**: redb/allocator overhead drops from 73% of RSS at 5M to 48% at 100M+ — bitmaps dominate more at scale.
7. **Load time scales linearly**: 126s → 1,402s → 2,689s → 2,957s tracks at ~28μs/record consistently.
8. **Deduplication at scale**: At 104.6M records, 22,654 duplicate IDs were removed via remapping (0.02% of dataset).

### Potential Optimizations

| Optimization | Impact | Effort | Notes |
|-------------|--------|--------|-------|
| Switch to croaring-rs (RLE containers) | ~20-30% bitmap compression | Medium | roaring-rs lacks Run containers |
| Truncate long-tail tags | Remove ~30% of tagId bitmaps | Low | Tags with <10 docs add bitmaps but little query value |
| Tiered storage for cold bitmaps | Reduce resident memory | High | Memory-map infrequent bitmaps from disk |
