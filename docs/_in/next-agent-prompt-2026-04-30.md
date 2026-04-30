# Next-Agent Prompt — BitDex Perf Mission Continuation

Copy everything below into the new agent session.

---

You are continuing the BitDex P99 perf mission. Prior agent shipped v1.0.179 → v1.0.181 with attribution metrics + a hot-reload knob. **Mission targets NOT met.** Pod is at `1.0.181-jemalloc` on prod, shadow ON, sustaining ~17 cores of CPU at ~70 QPS — Justin called this "bonkers." Your job: find what's actually burning CPU and fix it.

## Read these first (in order)

1. `docs/_in/handoff-perf-2026-04-30.md` — full state of what shipped, what failed, what's open. **Critical.**
2. `docs/_in/mission-status-2026-04-29.md` — original mission. Note: its "8.4c queryOpSet fan-out" attribution is **wrong** per measured data; ignore that part, keep the targets.
3. `docs/guide/prod-ops.md` — talos-infra deploy procedure, kubectl/flux/flipt cheatsheet.

## Don't repeat these wasted experiments

| Tried | Result | What this rules out |
|---|---|---|
| `RAYON_NUM_THREADS=1, 4, 16, 24` | Linear with WAL apply latency tradeoff. Floor stays 14-19c at default. | Rayon pool size isn't the lever. |
| `par_iter_min_threshold=10000000` (disable par_iter via hot reload) | **CPU stayed at 14-19c** | **Rayon par_iter is NOT the floor source.** Don't keep digging here. |
| Mission's planned PR #76 (paginate `apply_query_op_set` fan-out) | Disproven by metric — actual fan-out avg 1.4 slots/call, 0.93 calls/sec, <0.1c CPU | Fan-out is not the problem. |
| Mission's planned #79 incremental `bitmap_bytes()` | Disproven — mem-scanner avg tick 4.6μs, peak 500ms once per ~500 ticks | Mem scanner ~0.03c, not the floor. |

## Top suspect (untested) — verify FIRST

**Tokio blocking pool from `spawn_blocking` on docstore reads (PR #237 in v1.0.176).** Tokio default blocking pool max = 512 threads, no name override → threads inherit binary name `bitdex-server`, look identical to rayon workers in `ps`. With cap=256 concurrent queries × ~150 doc fetches/query, blocking pool stays warm constantly.

`bitdex_docstore_concurrent_reads = 0` at /metrics scrape only because spawn_blocking ramps up/down quickly between scrape moments. Per-thread `/proc/<tid>/stat` sample catches the workers mid-syscall.

**Step 1: prove or disprove this.** Methods, ranked:

1. **`perf record -F 99 -g -p <PID> -- sleep 30`** on the pod, then `perf report`. Top symbols give you the answer in 30 seconds:
   - Tokio blocking: `tokio::runtime::blocking::pool::*`
   - Rayon: `rayon_core::registry::WorkerThread::wait_until_cold`
   - Real I/O: `pwrite64`, `pread64`, `__libc_open`, `je_arena_*`
2. Cheaper: name tokio runtime workers explicitly (`Builder::thread_name("bitdex-tokio")`) so per-thread sample disambiguates from rayon `bitdex-server` workers.
3. Cheaper still: add a `bitdex_spawn_blocking_dispatches_total{site=...}` counter at every `spawn_blocking` callsite. Rate × duration = blocking pool CPU.

## If tokio blocking pool confirmed, real fixes (ranked)

1. **Drive doc_cache hit rate up.** Currently 41% on 10 GB cap, 27% full. Expected to climb as cache fills. Watch `bitdex_doc_cache_hit_total / (hit + miss)`. If plateaus < 80%, look at why — high diversity of unique slot reads, generation rotation evicting hot entries, etc.
2. **Async docstore reads.** Replace `spawn_blocking(|| docstore.read(slot))` with `tokio::fs` async I/O. Removes pool entirely.
3. **Cap blocking pool size** via `tokio::runtime::Builder::max_blocking_threads(N)`. Doesn't fix per-thread CPU but caps damage.

## Live hot-reload knobs (NO restart needed)

```bash
# Get all current config
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  sh -c 'curl -s -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" http://localhost:3000/api/indexes/civitai/config'

# PATCH any of: par_iter_min_threshold, max_query_concurrency, query_tee_mode,
#               enable_traces, trace_min_us, trace_buffer_size, cache.*
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  sh -c 'curl -s -X PATCH -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d "{\"trace_min_us\":1000000}" http://localhost:3000/api/indexes/civitai/config'
```

**Use hot reload for experiments. Restart loses 30+ min of cache warm-up.**

## Mission targets (still open)

| Metric | Target | Current (warm, shadow ON, 70 QPS) |
|---|---|---|
| P50 | < 2 ms | ~3 ms ⚠️ close but barely over |
| P95 | < 350 ms | ~3 s ❌ |
| P99 | < 1 s | ~7 s ❌ |
| 0 shed | yes | ✅ sustained 0 |
| Few queries > 1s | yes | ❌ ~14% sustained |
| CPU (no explicit target but reasonable) | "not 17 cores" | 17c — Justin says bonkers |

## Branch / PR / image state

- **Branch:** `perf/p99-v2` off `origin/main`
- **Open PR:** #250 (this branch → main). Needs `/ultrareview` + merge. CI rot blocks normal CI; admin-bypass authorized.
- **Latest tag:** `v1.0.181-jemalloc` (deployed)
- **talos-infra:** `clusters/production/apps/bitdex/deployment.yaml` has the env config. Pull latest before editing.

## Constraints

- **Don't restart the pod casually.** Cache warm-up takes ~30 min to reach 50% hit. Use hot-reload PATCH for experiments.
- **Don't merge PRs without `/ultrareview`.** Justin's standing rule. He doesn't review himself.
- **Justin is away.** Operate autonomously. Send him a summary via send_mail at any meaningful milestone (root cause confirmed, ship landed, mission complete).
- **Talos-infra config push needs `git pull --rebase`** before push (renovate bots create churn).

## How to validate progress without losing cache

1. Set `trace_min_us=1000000` via PATCH. Trace ring keeps only slow queries.
2. Pull traces: `GET /api/indexes/civitai/traces?last=200` → JSON. Look at `total_us`, `filter_us`, `sort_us`, `docs_us`, `lazy_load_us`. The slow ones tell you which phase is the tail.
3. Per-thread CPU sample (script in handoff doc).
4. `kubectl top pod bitdex-0 --containers` for total CPU.

## Final note

The prior agent's investigation labeled "Theory A confirmed" was wrong. **Multiple signals pointed at rayon spin, but the definitive PATCH-disable test rejected it.** Be skeptical of inferred theories without controlled experiments. The hot-reload pattern shipped this session is precisely so you can run those experiments cheaply.

Good luck.
