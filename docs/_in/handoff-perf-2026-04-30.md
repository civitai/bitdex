# BitDex Perf Handoff — 2026-04-30

**Branch:** `perf/p99-v2` (off `origin/main`)
**Tags shipped this session:** `v1.0.179-jemalloc`, `v1.0.180-jemalloc`, `v1.0.181-jemalloc`
**Live image:** `ghcr.io/civitai/bitdex:1.0.181-jemalloc`
**Open PR:** [#250](https://github.com/civitai/bitdex/pull/250) — needs merge to main

## TL;DR for the next agent

**Pod CPU is 14-19c sustained on shadow-ON load, ~70 QPS. That's bonkers.** Justin's words. Mission targets P50/P95/P99 are partially met (P50 ~3ms ✅, P95/P99 cold-tail ❌, 0 shed ✅, but ~14% queries > 1s sustained warm). Real bottleneck is NOT rayon as initially diagnosed — that was disproven mid-session. Suspected tokio blocking pool from docstore `spawn_blocking`. Needs profiling to confirm before designing a fix.

## What's deployed on prod (talos-infra)

| Knob | Value | Notes |
|---|---|---|
| Image | `1.0.181-jemalloc` | Hot-reloadable par_iter threshold |
| `BITDEX_MAX_QUERY_CONCURRENCY` | 256 | Up from 32 — needed for shadow-ON load |
| `_RJEM_MALLOC_CONF` | `dirty_decay_ms:5000,muzzy_decay_ms:10000` | RSS bounded |
| `RAYON_NUM_THREADS` | (default num_cpus = 24) | Was experimental; reverted after v1.0.180 |
| `doc_cache.max_bytes` (in ConfigMap) | `10737418240` (10 GB) | Was 1 GB default, hitting cap with 13M evictions |
| Flipt `bitdex-image-search` | **ON** | Set during validation; left on for further data |

## What's NOT deployed (followups)

- Mission task #75 — `BITDEX_DOC_CACHE_BYTES` env var support (currently has to be set in ConfigMap `config:` block; env override would be cleaner)
- Mission task #74 — persist runtime PATCH config across restarts (PATCH knobs lost on pod restart)
- Mission task #79 — incremental `bitmap_bytes()` (disproven not the floor; mem-scanner avg 4.6μs/tick)

## Hot-reloadable knobs (PATCH /api/indexes/civitai/config)

```bash
# All knobs return current config; PATCH only the field you want to change.

# Disable par_iter on hot path entirely (perf experiment)
curl -s -X PATCH -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"par_iter_min_threshold":10000000}' http://localhost:3000/api/indexes/civitai/config

# Re-enable
curl -s -X PATCH -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"par_iter_min_threshold":8}' http://localhost:3000/api/indexes/civitai/config

# Capture only slow traces (>1s)
curl -s -X PATCH -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"trace_min_us":1000000}' http://localhost:3000/api/indexes/civitai/config

# Pull recent slow traces
curl -s -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" \
  http://localhost:3000/api/indexes/civitai/traces?last=200
```

## What was tried (and what we learned)

### Failed attempts at the CPU floor

| Experiment | Result | Conclusion |
|---|---|---|
| `RAYON_NUM_THREADS=4` | CPU 14c → 5c at low traffic, but WAL fell 5k ops behind | Lower pool starves write path |
| `RAYON_NUM_THREADS=1` | CPU → 1c BUT WAL apply went 10ms → 2270ms per batch | Sequential par_iter unviable at write rate |
| `par_iter_min_threshold=10000000` (disable par_iter via hot reload) | **CPU stayed at 14-19c** | **Rayon is NOT the floor source.** This was the smoking gun. |
| `RAYON_NUM_THREADS=16` (compromise) | CPU 14c, WAL keeping up | Same as default — confirms threads aren't rayon spin |

### What DID help (smaller wins)

- **doc_cache 1 GB → 10 GB**: was hitting cap with 13M evictions and 38% hit rate. After bump: 0 evictions, 41% hit climbing toward target. Slight tail improvement (~3% more queries < 1s).
- **`BITDEX_MAX_QUERY_CONCURRENCY` 32 → 256**: eliminated 77% shed under shadow ON.
- **`trace_min_us=1000000`**: makes the trace ring buffer keep ONLY slow queries — invaluable for tail diagnosis.

## Real top suspects (untested)

1. **Tokio spawn_blocking pool**: PR #237 (in v1.0.176) introduced `spawn_blocking` for docstore reads. Default tokio blocking pool max = 512 threads, no name override → threads inherit binary name `bitdex-server`, identical to rayon workers in `ps`. With cap=256 concurrent queries × ~150 doc fetches/query, blocking pool stays warm constantly. **`bitdex_docstore_concurrent_reads = 0` at scrape only because it ramps up/down quickly, but per-thread sample catches them mid-syscall.**

2. **Cold per_value_lazy disk reads**: trace analysis showed `lazy_load_us = 2.5s` on `clauses=['postId', '__prefilter']`. Each unique postId value triggers a disk read. Histogram (per-field): postId 12k loads avg 8.3ms, postedToId 6k avg 7ms, modelVersionIds 7.8k avg 3ms. Long tail per field can be seconds. PR #233 (positioned-read) helped but didn't fully fix.

3. **flush_sort_promote**: `sort_promote_nanos` cumulative was 8.3s across 565 cycles = 14.7ms avg. Per-cycle gauge spiked to 436ms in one observation. sortAt has 32 bit-layers — when dirty, merge_dirty clones+modifies each. Not the steady floor but contributes to spikes.

## What to do next (prioritized)

### Priority 1 — Confirm what those 24 threads ARE

The whole CPU mystery hinges on this. Options:
- **`perf record -F 99 -g -p <PID> -- sleep 30`** on a stuck pod, then `perf report`. Top symbols will tell you immediately:
  - Tokio blocking pool: `tokio::runtime::blocking::worker::*`
  - Rayon: `rayon_core::registry::WorkerThread::wait_until_cold`
  - Real I/O: `pwrite64`, `pread64`, `__libc_open`
  - Memory: `je_arena_ralloc`, `je_*_huge`
- Or simpler: name the tokio runtime workers explicitly via `Builder::thread_name("bitdex-tokio")` so per-thread sample disambiguates them from rayon `bitdex-server` workers.
- Or: wire a `bitdex_spawn_blocking_total` counter at every `spawn_blocking` callsite. Rate × duration = blocking pool CPU.

### Priority 2 — If tokio blocking pool confirmed

Real fixes ranked by effort:
1. **Drive doc_cache to 90%+ hit rate** (less per-query disk fan-out) — already in motion as cache fills toward 10 GB
2. **Bound tokio blocking pool size** via `Builder::max_blocking_threads(N)` — limits max but won't reduce per-thread CPU
3. **Async docstore reads** (replace `spawn_blocking` with `tokio::fs`) — biggest fix, biggest change

### Priority 3 — Cold per_value_lazy tail

- Eager-load postId/postedToId/modelVersionIds at startup (mark `eager_load: true` in config) — trade boot time for tail elimination
- Or: keep lazy but make load async (concurrent ensure_fields_loaded) so multiple cold queries share one disk read

## Files changed this session

| File | What |
|---|---|
| `src/concurrent_engine.rs` | Added `par_iter_min_threshold: Arc<AtomicUsize>` field + getters/setters. Threshold check at flush filter+sort sites (1893, 1926). Fanout metrics from cherry-picked PR #247. Mem-scanner duration metric. |
| `src/shard_store_doc.rs` | Same threshold check at 3 doc writer flush callsites (1216, 1258, 1309). `set_par_iter_min_threshold_handle` for engine wiring. |
| `src/server.rs` | `MetricsBridge` extended with new metric handles. `ConfigPatch.par_iter_min_threshold` + handler. |
| `src/metrics.rs` | New histograms: `bitdex_wal_apply_batch_seconds`, `bitdex_bitmap_mem_scan_tick_seconds`. Plus PR #247: `bitdex_query_op_set_*`. Plus `query_total` bumped on QueryOpSet path. |
| `src/ops_processor.rs` | Wraps `apply_ops_batch` body in HistogramTimer. `query_total` bump on `apply_query_op_set`. |
| `Cargo.toml` | Version bumped 1.0.177 → 1.0.181-jemalloc |

## Talos-infra commits

```
e484277c — bump 1.0.179 → 1.0.180-jemalloc, drop RAYON_NUM_THREADS
c9bb0df4 — bump 1.0.180 → 1.0.181-jemalloc — hot-reloadable par_iter threshold
2c80d022 — doc_cache.max_bytes 1GB → 10GB
8292e7d1 — bump concurrency cap 128 → 256
f0717e9d — bump 1.0.179 (initial)
22d35031 / ca073eb0 / ebaee39c — RAYON_NUM_THREADS experiments (reverted in v1.0.180)
6f38e958 / 7a2f2be2 — relay flip on/off
```

## Sanity checks before you change anything

1. **Pod state:** `kubectl --context civit-datapacket -n bitdex get pod bitdex-0` — should be Running 2/2, low restarts
2. **Image:** `kubectl --context civit-datapacket -n bitdex get pod bitdex-0 -o jsonpath='{.spec.containers[0].image}'` should be `1.0.181-jemalloc`
3. **Hot reload alive:** PATCH `par_iter_min_threshold` to 5 then back to 8, GET /config to confirm
4. **Per-thread CPU sample (top suspects):**

```bash
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- sh -c '
collect() {
  for t in /proc/1/task/*; do
    tid=$(basename "$t")
    comm=$(cat "$t/comm" 2>/dev/null)
    st=$(awk "{print \$14+\$15}" "$t/stat" 2>/dev/null)
    echo "$tid $st $comm"
  done | sort -k1
}
collect > /tmp/t0; sleep 30; collect > /tmp/t1
join -1 1 -2 1 -o 1.1,2.2,1.2,1.3 /tmp/t0 /tmp/t1 | \
  awk "{delta=\$2-\$3; if (delta > 30) print delta, \$1, \$4}" | sort -rn | head -30
'
```

You'll see ~24 unnamed `bitdex-server` threads each at ~50-80% CPU. Those are the mystery threads.

## How to flip prod modes

- **Shadow flag**: `node .claude/skills/flipt/flipt.mjs shadow on|off`
- **Relay mode** (for local-iter): edit `clusters/production/apps/bitdex/deployment.yaml`, add `BITDEX_MODE=relay` + `BITDEX_RELAY_CONFIG=/etc/bitdex/relay-config.yaml`, push, flux reconcile. **Pre-flip:** safety-hold cursor, see `docs/_in/relay-cursor-snapshot-2026-04-29.md`. **Post-flip-back:** reset `pg-sync-bitdex-0` cursor in PG to pre-flip value so relay-window ops re-deliver.

## Cursor / flux operations cheatsheet

```bash
# Flux reconcile
kubectl --context civit-datapacket -n flux-system annotate kustomization bitdex \
  reconcile.fluxcd.io/requestedAt=$(date +%s) --overwrite
kubectl --context civit-datapacket -n flux-system annotate gitrepository flux-system \
  reconcile.fluxcd.io/requestedAt=$(date +%s) --overwrite

# PG primary
kubectl --context civit-datapacket -n cnpg-database get cluster cnpg-cluster-nvme0 \
  -o jsonpath='{.status.currentPrimary}'

# PG cursor read
kubectl --context civit-datapacket -n cnpg-database exec cnpg-cluster-nvme0-3 -- \
  psql -U postgres -d civitai -t -c \
  "SELECT replica_id, last_outbox_id FROM bitdex_cursors;"

# Force pod restart (e.g. to pick up ConfigMap change)
kubectl --context civit-datapacket -n bitdex delete pod bitdex-0
```

## Don't

- Don't trust the prior agent's "rayon spin" theory. Disabling par_iter did not move the needle.
- Don't bump RAYON_NUM_THREADS — already tested 1, 4, 16, default. Real fix is elsewhere.
- Don't restart the pod casually — cache warm-up costs ~30 min to reach 50% hit rate again. Use the hot-reload PATCH for experiments.
- Don't merge PR #250 without `/ultrareview`. CI rot blocks normal review; admin-bypass authorized but get AI review first.

## Open mysteries to crack next

1. **What are the 24+ unnamed threads?** Profile to find out. (Priority 1)
2. **Why was bumping doc_cache to 10 GB only marginal?** Avg docstore read 137ms is suspicious — disk should be much faster. Could be lock contention on shard files, or page-cache eviction.
3. **Cold sort_us 317ms on input=1501** — sort field disk reads despite eager_load=true. Suggests background eager-load not completing before queries hit, OR positioned-read fast path has cold-cache cost per query.

## Final speak

Mission target was "beat v1.0.157 baseline." We didn't. We found the original mission attribution doc was wrong, fixed concurrency cap and doc cache, shipped attribution metrics + hot-reload, but the actual CPU mystery and tail latency remain. Ball is in the next agent's court.
