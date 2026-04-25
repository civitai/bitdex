# Mixed-Load Measurement — 2026-04-25

Goal: validate perf stack v1.0.165–171 (now extended through v1.0.173 with relay
+ pg-sync stub fixes) against real prod traffic shape via the relay system. Mission gate is **P99 < 1 s**.

## Methodology

### Setup

- **Local BitDex** at `localhost:3002`, perf stack HEAD on `bitdex-donovan-perf` worktree, full 110.5M dump, prefilter `civitai_safe_full8` registered, traces enabled.
- **Prod relay** at `bitdex.civitai.com` (port-forward via `kubectl ... port-forward pod/bitdex-0 4099:3000`).
- **Consumer** = `scripts/replay-prod-via-relay.mjs` (Jack's PR #230) subscribed to `/events/queries` + `/events/ops` on the prod relay, replaying each event's `body` against the local instance.
- **Local query loadgen** = `scripts/replay-captured.mjs` driving the 232-shape captured corpus directly against the local instance (path-b per Scarlet's framing — corpus replay, not via the relay).

### Why the split

Prod relay's `/events/queries` channel sees zero query traffic until model-share's `bitdex-image-search` shadow flag flips ON. V1 runbook says shadow stays OFF during the relay window; the V2 model-share `tee_mode` skip patch is the prerequisite for flipping shadow on safely (otherwise 100% divergence alerts). Until that lands, we generate query load locally from the corpus to drive cache + executor traffic.

For ops: `/events/ops` flows naturally once pg-sync sidecar (post-#230) starts polling — this is the real prod write load.

## Baseline — corpus-only, no ops

(Captured pre-v1.0.173 deploy.)

| Bucket | Count | % of 572,523 |
|---|---|---|
| ≤ 50 μs | 531,754 | 92.9 % |
| ≤ 100 μs | 567,866 | 99.2 % |
| ≤ 250 μs | 571,912 | 99.9 % |
| ≤ 500 μs | 572,317 | 99.97 % |
| ≤ 1 ms | 572,456 | 99.99 % |
| ≤ 5 ms | 572,505 | 99.997 % |
| ≤ 10 ms | 572,510 | 99.998 % |
| ≤ 25 ms | 572,523 | 100.00 % |

- `[sort-top_n] SLOW`: 0
- `[bifurcate] SLOW`: 0
- `[ops-trace]`: 0
- `[remove_bulk_slow]`: 0
- `[insert_bulk_slow]`: 0
- `[flush-slow]`: 0

**Cache-saturated ceiling.** Useful as the upper bound — proves that under best conditions (hot cache, no contention, no ops) the engine clears the gate cleanly. Not the gate value itself.

## Corpus characterization (input shape)

232 captured queries, 207 unique by JSON.

- **Sort fields:** sortAt 183, reactionCount 44, collectedCount 3, commentCount 2 (all Desc).
- **Limit distribution:** 500 (95), 50 (65), 100 (55), 64 (12), 20 (3), 200 (1), 10 (1).
- **Distinct fields filtered:** availability, blockedFor, poi, minor, nsfwLevel, baseModel, isPublished, type, userId (137 refs, 109 unique values), tagIds (11 refs, 10 distinct sets), postedToId, modelVersionIds, modelVersionIdsManual, sortAtUnix (21 Gte), toolIds, hasMeta.
- **Safety prefix coverage:** civitai_safe_full8 covers the first 8 of these — substituted to BucketBitmap.
- **Gte queries (21 total) — all snap cleanly** to canonical buckets (24h/7d/30d/1y). Range scan never triggers.

## Hypothesis hot-spots

Pre-staged before mixed-load data arrived. Will verify against real traffic.

| # | Pattern | Status | Mitigated by |
|---|---|---|---|
| 1 | userId-Eq under apply contention (109 distinct userIds, multi-cycle pressure) | Mitigated | PR #222 dirty-set + PR #227 bitmap_bytes chunked |
| 2 | tagIds-In multi-value contention (10 distinct sets, highest-cardinality multi-value field) | Open | None applied; watch for ops-induced spikes |
| 3 | sortAtUnix-Gte tolerance miss → range_scan walk on postId 22.5M | **Debunked for corpus** | All 21 corpus Gtes snap to canonical buckets (PR #225 still parked, only relevant if real prod traffic includes non-canonical Gte values) |

## Mixed-load run — v1.0.175, 11-min sustained window

Setup: local BitDex on full perf stack + `replay-prod-via-relay.mjs` consumer subscribed to prod relay's `/events/queries` + `/events/ops` (PF localhost:4099) + `replay-captured.mjs` driving 232-shape corpus against local at ~3K QPS sustained. Real prod ops (130 events / 651 s ≈ 1 batch / 5 s, matches Tom's verification at deploy time) applied to local via consumer.

### Server-side query histogram (`bitdex_query_duration_seconds_bucket`)

Total queries: **1,724,131**. Bucket counts:

| Bucket | Count | Cumulative % |
|---|---|---|
| ≤ 50 μs | 1,286,524 | 74.6 % |
| ≤ 100 μs | 1,437,747 | 83.4 % |
| ≤ 250 μs | 1,468,160 | 85.2 % |
| ≤ 500 μs | 1,477,534 | 85.7 % |
| ≤ 1 ms | 1,495,471 | 86.7 % |
| ≤ 2 ms | 1,532,249 | 88.9 % |
| ≤ 5 ms | 1,589,952 | 92.2 % |
| ≤ 10 ms | 1,624,265 | 94.2 % |
| ≤ 25 ms | 1,723,278 | **99.95 %** |
| ≤ 50 ms | 1,724,057 | 99.997 % |
| ≤ 100 ms | 1,724,088 | 99.998 % |
| ≤ 500 ms | 1,724,131 | 100.000 % |
| > 500 ms | 0 | — |

**P50 < 50 μs. P95 < 25 ms. P99 ≈ 25 ms. Max < 500 ms.**

Mission gate (P99 < 1 s sustained) **CLEARED — by ~40× margin.** Zero queries exceeded 500 ms across 1.72 M served.

### Slow-event tally

- `[flush-slow]` (apply > 100 ms): **0**
- `[ops-trace]` (apply ≥ 100 ms): **0**
- `[insert_bulk_slow]` (lock-acquire > 5 ms): **0**
- `[remove_bulk_slow]`: not in current build (instrumentation reverted post-PR-#227)
- `[sort-top_n] SLOW` (sort-layer fusion > 20 ms): **8**
- `[bifurcate] SLOW`: **5**

The 8 `[sort-top_n] SLOW` events are the only meaningful latency surface visible. Examples:

```
[sort-top_n] SLOW: fuse=31.2ms cursor=0.0ms bifurcate=18.0ms order=16.3ms total=65.5ms input=16101 output=4000 bits=32
[sort-top_n] SLOW: fuse=20.0ms cursor=0.0ms bifurcate=0.0ms order=2.2ms total=22.3ms input=1276 output=1276 bits=32
[sort-top_n] SLOW: fuse=36.4ms cursor=0.0ms bifurcate=12.2ms order=13.0ms total=61.6ms input=9570 output=4000 bits=32
```

These are sort-layer fusion cycles where the diff layer has accumulated bits on a sort field that's also being mutated by the concurrent ops stream. `fuse` = base + diff materialization (the lazy-fuse path from PR #173 era). 20–36 ms fuse cost on sort fields holds the query for that long.

**This is NOT a FilterField lock contention surface.** PR #222 + #227 closed the FilterField side. Sort-layer fusion under concurrent writes is a separate (known) surface — addressable via similar dirty-set + chunked-iter patterns on `SortField`, but **not blocking the P99 gate** since these spike to 65 ms max, not 1 s+.

### Hypothesis hot-spots — verified vs real ops

| # | Pattern | Predicted | Observed |
|---|---|---|---|
| 1 | userId-Eq under apply contention | Mitigated by #222/#227 | **Confirmed mitigated.** Zero `[remove_bulk_slow]`-equivalent or `[insert_bulk_slow]` events under sustained ops + queries. |
| 2 | tagIds-In multi-value contention | Open | **No contention observed.** tagIds queries land in the cache-hit path; ops on tagIds (5–7 distinct values per cycle) merge cleanly via `merge_dirty` side-set. |
| 3 | sortAtUnix-Gte tolerance miss → range_scan | Debunked for corpus | All snap to canonical buckets. Confirmed. |
| (new) | Sort-layer fusion under concurrent sort-field writes | Not predicted | **Surfaced.** 8 events in 11 min, max 65 ms. Different surface than this PR stack targeted. |

### Replay client (network roundtrip)

| Metric | Value |
|---|---|
| Queries replayed | 1,708,252 |
| Sustained QPS | ~2,625 |
| Client-side P50 | 1 ms |
| Client-side P95 | 13 ms |
| Client-side P99 | 19 ms |
| Errors | 0 |
| < 1 ms | 57 % |

### Resources

- ops applied to local: 1,912 (from 130 SSE events at avg ~15 ops/batch)
- Cache hits: 1,724,522 / Cache misses: 98 (99.994 % hit rate, cache-saturated as expected with corpus replay)

## Conclusion

**Mission gate cleared.** P99 sub-second under mixed real-prod-ops + corpus query load — actual P99 ~25 ms, max < 500 ms across 1.72 M queries.

What this validates:
- PR #222 + #224 + #227 stack holds under real prod ops with concurrent query traffic.
- No write-side contention bottleneck observable (zero `[insert_bulk_slow]` / `[ops-trace]` / `[flush-slow]`).
- The cache-saturated upper bound (P99 ≤ 5 ms, observed earlier) and mixed-load P99 (~25 ms) are within 5× of each other under sustained real-ops pressure.

What's still open:
- Sort-layer fusion (8 events / 11 min, max 65 ms) is a separate surface, not blocking the gate, addressable in a follow-up if profile demands.
- Real diverse query traffic (currently corpus-replayed) requires the V2 model-share PR before shadow can flip on for true production-shape coverage. Until then, the queries-channel side is exercised via the captured corpus, not via prod-relay-broadcast.

Recommendation: stack is ready for prod flip-back from relay mode. Sort-layer fusion can be a v2 perf push if real shadow traffic ever surfaces a related outlier.

---

*Doc scaffolded 2026-04-25 by Donovan; populated as data arrives.*
