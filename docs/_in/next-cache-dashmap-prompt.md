# Prompt for the next cache-perf agent (DashMap refactor)

Paste this verbatim as the opening prompt for the new agent.

---

Pick up the BitDex P95/P99 cache architecture refactor. Prior agent shipped v1.0.190 (form_and_store split) and v1.0.191 (Mutex→RwLock + atomics + lookup_for_read) to prod. v191 got prod queries > 1s from 2.0% → 1.6% — marginal. Mission targets (P50<2ms, P95<350ms, P99<1s, 0% > 1s sustained) still NOT met. The architectural fix is yours.

## Read first (in order)

1. `docs/_in/handoff-cache-dashmap-2026-04-30.md` — full state of v1.0.191, why it didn't fully land, the v1.0.192 implementation plan with field-by-field DashMap conversion, method API rework with closure-passing, ~50 callsite plan, local rig setup, prod rig, traps, operating constraints, and what tonight's two failed attempts learned. **Critical — has everything you need.**

2. `docs/_in/handoff-perf-cache-mutex-2026-04-30.md` — prior handoff covering v1.0.182–v1.0.190 (rayon spin fix, lazy_load 5s→100ms, async_maintenance, trace instrumentation, Arc::make_mut hoist, sampled-LRU, form_and_store split). Background context.

3. `docs/_in/local-iter-howto.md` — local iteration loop (build, replay, prom, trace pulls).

4. `docs/guide/prod-ops.md` — talos-infra deploy procedure.

5. `MEMORY.md` at `C:\Users\Zipp4\.claude\projects\C--Dev-Repos-open-source-bitdex-v2\memory\MEMORY.md`. Recent entries `project_v191_rwlock_outcome_2026_04_30.md`, `project_cache_perf_v187_189_2026_04_30.md`, `project_cache_mutex_contention_2026_04_30.md`, `project_lazy_load_5s_wait_2026_04_30.md`, `project_rayon_spin_floor_2026_04_30.md` cover today's findings.

## What you're shipping (v1.0.192-jemalloc)

`Arc<RwLock<UnifiedCache>>` → `Arc<UnifiedCache>` — interior mutable, DashMap-backed entries. Justin's framing: he authorized DashMap (option 2 of three I proposed: shard / DashMap / roll back) with "Ok. Let's start on those changes."

The rationale: parking_lot RwLock is task-fair. Readers queue behind ANY pending writer. Prod traces show hit-path `cache_lock_us` median = 605 ms even though those reads use `RwLock::read()` — they're queued behind cache_worker (Phase A/C every cycle) and form_and_store (per cache miss, ~67% during warmup). Lock-type swap alone doesn't fix the shared queue. Interior mutability eliminates it: DashMap shards internally (16-way default), so reads + writes on different keys never block each other. Writes on the same key still serialize (per-shard parking_lot lock) but for << ms.

Plan in §4 of `handoff-cache-dashmap-2026-04-30.md`. Summary:

| Field | Current | New |
|---|---|---|
| `entries` | `HashMap` | `DashMap<UnifiedKey, UnifiedEntry, ARandomState>` |
| `meta_id_to_key` | `HashMap` | `DashMap` |
| `shard_to_keys` | `HashMap<ShardKey, HashSet>` | `DashMap` |
| `meta` | `MetaIndex` | `RwLock<MetaIndex>` |
| `config` | `UnifiedCacheConfig` | `ArcSwap<UnifiedCacheConfig>` |
| `total_bytes` | `usize` | `AtomicUsize` |
| `meta_dirty` / `persistence_enabled` / `restoring` | `bool` | `AtomicBool` |
| `pending_shards` / `loading_shards` / `shard_dirty` | `HashSet` | `Mutex<HashSet>` |
| `meta_has_more` / `meta_total_matched` | `HashMap` | `Mutex<HashMap>` |
| stats counters | `AtomicU64` (already done in v191) | unchanged |

`UnifiedCache::lookup` and `lookup_for_read` cannot return entry refs from DashMap — `Ref<'_, K, V>` holds a shard lock and can't escape the method. Convert to closure-passing: `with_lookup<R>(&self, key, f) -> Option<R>`, `with_lookup_mut<R>(&self, key, f) -> Option<R>`, `with_get<R>(&self, key, f) -> Option<R>`. Snippets in §4.2 of the handoff.

All `UnifiedCache::&mut self` methods → `&self`. They'll mutate via the now-interior-mutable fields. List in §4.4 of the handoff.

Outer wrapper change: `concurrent_engine.rs:253`, `:712` (constructor), `:4476` (load_shard_background sig), `cache_worker.rs:240,252` — `Arc<RwLock<UnifiedCache>>` → `Arc<UnifiedCache>`. ~50 callsite changes in concurrent_engine.rs (mass-replace `unified_cache.write().X` and `unified_cache.read().X` with `unified_cache.X`). cache_worker.rs body: `let mut uc = self.cache.write();` → `let uc = &self.cache;`. `timed_cache_read` / `timed_cache_write` helpers in `query_metrics.rs` become no-ops or remove.

## DashMap API differences to mind (gotchas from tonight's failed attempt)

- `entries.values()` doesn't exist → use `entries.iter().map(|r| r.value())`
- `entries.remove(k)` returns `Option<(K, V)>` not `Option<V>` → destructure as `if let Some((_, v)) = ...`
- `entries.iter()` yields `RefMulti<'_, K, V>` not `(&K, &V)` → use `r.key()` / `r.value()`
- `entries.iter_mut()` yields `RefMutMulti<'_, K, V>` not `(&K, &mut V)` → use `let mut r = ...; r.value_mut()`
- DashMap construction: `DashMap::with_hasher(ARandomState::new())`
- `Ref` lifetime can NOT escape — closure-passing is the only pattern that works for `&`-returning methods

## Don't repeat tonight's mistakes

- **Don't try incremental "just entries field as DashMap".** It cascades into 50+ method changes. Tonight I started incremental, hit 50 compile errors mid-conversion, had to revert. Do FULL conversion in one push: struct + all methods + outer wrapper + callsites.
- **Don't ship a marginal v192.** Tonight I built and tested a cache_worker write-lock split — local 3-min loadgen showed P95 89ms vs v191's 42ms (within variance, no clear win). Reverted. If your refactor doesn't drop hit-path cache_lock_us p99 to < 10ms locally, it's not done.
- **Don't switch back to `Mutex<UnifiedCache>` if it gets messy.** The point is concurrent reads/writes across keys.
- **Don't try to install perf or run kubectl debug on prod** — PodSecurity blocks both. Use `/proc/1/task/*/stat` per-thread sample if you need CPU attribution.
- **Don't trust trace `total_us` as full HTTP latency** — it's engine-only. Use Prom histogram for full roundtrip.
- **Don't restart pod casually** — cache warm-up is 25-45 min. Use hot-reload PATCH for experiments.
- **Don't bump RAYON_NUM_THREADS** — it's pinned at 4 for a reason. CPU floor is solved.
- **Don't merge PR #250 without `/ultrareview`.** Justin's standing rule.

## Acceptance

Local: `node scripts/loadgen-cache-contention.mjs 180 70 64` then pull traces → hit-path `cache_lock_us` p99 < 10 ms (was 605 ms median in prod on v191, 647 ms locally). Cumulative P95 should be < 50 ms; steady-state windows past 100s should be P50/P95/P99 all 1-2 ms.

Prod: 30+ min uptime with cache warmed past 70% hit rate, `bitdex_query_duration_seconds` Prom histogram ≤1s bucket holds ≥99% sustained, ideally 100% (mission target = 0% > 1s).

If local result hits but prod doesn't, look for OS-level blockers (jemalloc, page cache, lazy_load on cold per_value_lazy fields) — not the cache mutex. Cache mutex is gone after this ships.

## State of the world

- **Branch:** `perf/p99-v2` (off main). PR #250 open against main, description updated to summarize v179 → v191.
- **Latest tag:** v1.0.191-jemalloc, deployed.
- **Pod:** bitdex-0 on talos-wjh-tgy, stable, server mode, shadow ON via Flipt.
- **Prod headline numbers (~25 min uptime on v191, cache 33% warmed):** ≤1s bucket = 98.4% (was 98.0% on v190, marginal). Hit-path cache_lock_us median = 605 ms (the problem this refactor fixes).
- **Memory:** 5+ docs at `C:\Users\Zipp4\.claude\projects\C--Dev-Repos-open-source-bitdex-v2\memory\` cover the v181 → v191 march and tonight's findings.
- **Local rig:** pre-staged at `data/full-dump` (110 M alive, ~94K cache entries from disk).
- **Synthetic loadgen:** `scripts/loadgen-cache-contention.mjs` (created in prior session). Reproduces the cyclical contention pattern locally.
- **Branch state:** clean, no uncommitted work.
- **Open hub TaskList items:** #10–#13 cover the DashMap refactor stages. Local TaskCreate not durable across sessions.

## Communication norms (do not skip)

- **Speak via `mcp__hub-channel__speak` on every response.** First action. Justin hears you via TTS.
- **Update status via `mcp__hub-channel__update_status`** on the same response. Goal once, task often.
- **Mail per milestone.** Don't go silent during a refactor — Justin async, but mail him at root cause confirmed, ship landed, mission complete.
- The `caveman` mode ("CAVEMAN MODE ACTIVE") is on by default — drop articles, filler, pleasantries, hedging in user-facing text. Code, commits, PRs, security warnings stay normal prose.

## On day one

1. `mcp__hub-channel__speak({ text: "...", emoji: "..." })` to announce yourself.
2. `mcp__hub-channel__update_status({ goal: "Ship v1.0.192 DashMap refactor", task: "Reading handoff", state: "working" })` — set goal once.
3. Read `docs/_in/handoff-cache-dashmap-2026-04-30.md` end-to-end.
4. `mcp__hub-channel__check_inbox` — read latest from Justin (and anyone else).
5. Verify pod still on v1.0.191: `kubectl --context civit-datapacket -n bitdex get pod bitdex-0 -o jsonpath='{.spec.containers[0].image}'`
6. Start the refactor. Do it in one push, not incremental.

Justin is async. Operate autonomously. Mail him at meaningful milestones (refactor builds, local validation hits acceptance, ship landed, mission complete) via `send_mail`.

Good luck. The diagnostic chain is in place — every major lock acquisition is timed in trace fields. Local rig + synthetic load gives < 2 min iteration cycles. The architectural fix is well-scoped; the work is mechanical now that the path is clear.
