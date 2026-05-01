# BitDex Perf Handoff — Cache DashMap Refactor

**Date written:** 2026-04-30 night by 005dd81a-b69c-4fcb-8e95-fb702f30d6ad (ava)
**Branch:** `perf/p99-v2`
**Latest tag:** `v1.0.191-jemalloc` (deployed)
**Open PR:** [#250](https://github.com/civitai/bitdex/pull/250) — needs merge
**Pod:** `bitdex-0` on `talos-wjh-tgy`, server mode, shadow ON via Flipt
**Justin authorization:** "Ok. Let's start on those changes" (referring to: shard / DashMap / roll back)

---

## 1. Mission summary

Justin's call: **P95/P99 are horrific.** Targets:

| Metric | Target |
|---|---|
| P50 | < 2 ms |
| P95 | < 350 ms |
| P99 | < 1 s |
| Shed | 0 events sustained |
| Tail | Few queries > 1s, each with a clear root cause |

Status after v1.0.191 deploy (~25 min uptime, cache 33% warmed):

| Metric | v190 baseline | v191 prod | target |
|---|---|---|---|
| ≤500 ms | 94.6% | 95.4% | — |
| ≤1 s | 98.0% | 98.4% | 100% |
| > 1 s | 2.0% | **1.6%** | 0% |
| > 5 s | 0.005% | 0.002% | 0% |

Marginal improvement, not enough. Mission target NOT met.

---

## 2. What v1.0.191 shipped

`Mutex<UnifiedCache>` → `RwLock<UnifiedCache>` plus atomic stats / last_used / needs_rebuild plus `lookup_for_read(&self)` plus pre-check pending_bucket_diffs to choose read vs write lock for fast path.

### Files touched

| File | What changed |
|---|---|
| `src/unified_cache.rs` | hits/misses/inserts/updates/evictions/invalidations/extensions/wall_hits/prefetches → AtomicU64. UnifiedEntry::last_used: Instant → AtomicU64 ms-since-epoch. UnifiedEntry::needs_rebuild: bool → AtomicBool. touch/mark_for_rebuild/record_* take &self. Added `lookup_for_read(&self) -> Option<&UnifiedEntry>` |
| `src/concurrent_engine.rs` | `Arc<Mutex<UnifiedCache>>` → `Arc<RwLock<UnifiedCache>>`. `.lock()` → `.write()` mass-replaced. Hot fast-path lookup at line 4949 pre-checks `pending_bucket_diffs.current_cutoff()` — if 0 (common case) takes read lock + `lookup_for_read`, else write lock + mutating `lookup`. Slow-path lookup at 5169 uses read lock + `lookup_for_read`. Config reads, `stats()`, `entry_details()`, `record_wall_hit/record_prefetch` also moved to `.read()`. Prefetch worker reads moved to `.read()` |
| `src/cache_worker.rs` | `Arc<Mutex<UnifiedCache>>` → `Arc<RwLock<UnifiedCache>>`. All `.lock()` → `.write()` (cache_worker remains write-locked) |
| `src/query_metrics.rs` | `timed_cache_lock` removed. Added `timed_cache_write` (RwLockWriteGuard) + `timed_cache_read` (RwLockReadGuard) variants |
| `Cargo.toml` | 1.0.190 → 1.0.191 |

---

## 3. The remaining problem — fair RwLock readers queue behind writers

Local 3-min loadgen (cold-cache start, 70 QPS, 64 concurrent):

| Metric | v190 | v191 |
|---|---|---|
| P50 | 1 ms | 1 ms |
| P95 | 90 ms | **42 ms** (-53%) |
| P99 cum | 657 ms | 656 ms (~same) |
| Steady P99 | 2 ms | 2 ms |

Local trace inspection on cumulative slow window: `cache_lock_us` p99 = 0, max = 0. Looks great locally.

**Prod is different.** With shadow comparator ON, real query mix produces sustained write pressure (cache_worker every cycle + form_and_store on every miss; miss rate is ~67% during cold warmup). Prod traces (slow window, >500ms):

| | hits (53) | misses (447) |
|---|---|---|
| cache_lock_us median | **605 ms** | 486 ms |
| cache_lock_us p99 | 1117 ms | 1204 ms |

**Hit-path read lock waits 605 ms median.** Even though `lookup_for_read` runs from `RwLock::read()`, parking_lot's RwLock is task-fair: readers queue behind any pending writer. cache_worker (Phase A/C) and form_and_store (per miss) hold write locks long enough that readers serialize behind them.

The architecture switch was correct in theory. Lock-type swap alone is not enough. Need **interior mutability** so concurrent reads + writes across keys don't share a global queue.

---

## 4. Proposed implementation — DashMap interior-mutable UnifiedCache (v1.0.192)

### 4.1 Field-by-field plan

| Field | Current | New |
|---|---|---|
| `entries` | `HashMap<UnifiedKey, UnifiedEntry>` | `DashMap<UnifiedKey, UnifiedEntry, ARandomState>` |
| `meta_id_to_key` | `HashMap<CacheEntryId, UnifiedKey>` | `DashMap<CacheEntryId, UnifiedKey, ARandomState>` |
| `shard_to_keys` | `HashMap<ShardKey, HashSet<UnifiedKey>>` | `DashMap<ShardKey, HashSet<UnifiedKey>, ARandomState>` (or `Mutex<HashSet>` per value) |
| `meta` | `MetaIndex` | `RwLock<MetaIndex>` |
| `config` | `UnifiedCacheConfig` | `ArcSwap<UnifiedCacheConfig>` (hot reads, PATCH endpoint swaps) |
| `total_bytes` | `usize` | `AtomicUsize` |
| `meta_dirty` | `bool` | `AtomicBool` |
| `persistence_enabled` | `bool` | `AtomicBool` |
| `restoring` | `bool` | `AtomicBool` |
| `pending_shards` | `HashSet<ShardKey>` | `Mutex<HashSet<ShardKey>>` |
| `loading_shards` | `HashSet<ShardKey>` | `Mutex<HashSet<ShardKey>>` |
| `shard_dirty` | `HashSet<ShardKey>` | `Mutex<HashSet<ShardKey>>` |
| `meta_has_more` | `HashMap<CacheEntryId, bool>` | `Mutex<HashMap<CacheEntryId, bool>>` (loaded once at startup) |
| `meta_total_matched` | `HashMap<CacheEntryId, u64>` | `Mutex<HashMap<CacheEntryId, u64>>` (same) |
| stats counters | `AtomicU64` | unchanged |

### 4.2 Method API rework

`UnifiedCache::lookup` (`&mut self -> Option<&mut UnifiedEntry>`) and `lookup_for_read` (`&self -> Option<&UnifiedEntry>`) cannot return entry refs from DashMap — `Ref<'_, K, V>` and `RefMut<'_, K, V>` hold a shard lock and can't escape the method scope.

Convert to closure-passing:

```rust
pub fn with_lookup_mut<R>(&self, key: &UnifiedKey, f: impl FnOnce(&mut UnifiedEntry) -> R) -> Option<R> {
    let mut entry = self.entries.get_mut(key)?;
    if entry.needs_rebuild() {
        self.misses.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    self.hits.fetch_add(1, Ordering::Relaxed);
    entry.touch();
    Some(f(entry.value_mut()))
}

pub fn with_lookup<R>(&self, key: &UnifiedKey, f: impl FnOnce(&UnifiedEntry) -> R) -> Option<R> {
    let entry = self.entries.get(key)?;
    if entry.needs_rebuild() {
        self.misses.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    self.hits.fetch_add(1, Ordering::Relaxed);
    entry.touch();
    Some(f(entry.value()))
}

pub fn with_get<R>(&self, key: &UnifiedKey, f: impl FnOnce(&UnifiedEntry) -> R) -> Option<R> {
    self.entries.get(key).map(|r| f(r.value()))
}
```

Also need: `iter_mut` (used internally only) → convert internal callsites to `for r in self.entries.iter_mut() { let v = r.value_mut(); ... }`. Public `iter_mut` method may be removed entirely or replaced with a `for_each_mut(closure)` form.

### 4.3 Other DashMap API differences (caught these in mid-attempt)

- `entries.values()` doesn't exist → `entries.iter().map(|r| r.value())`
- `entries.remove(k)` returns `Option<(K, V)>` not `Option<V>` → destructure as `if let Some((_, v)) = ...`
- `entries.iter()` yields `RefMulti<'_, K, V>` not `(&K, &V)` → use `r.key()` / `r.value()`
- `entries.iter_mut()` yields `RefMutMulti<'_, K, V>` not `(&K, &mut V)` → use `let mut r = ...; r.key(); r.value_mut()`
- HashMap construction: `HashMap::new()` → `DashMap::with_hasher(ARandomState::new())`

### 4.4 All UnifiedCache methods → `&self`

Most methods that took `&mut self` only mutated state that's now interior-mutable. They can become `&self` with no behavior change.

Methods that need conversion (audit each — most are mechanical):
- `lookup` → renamed `with_lookup_mut(&self, key, f)` (closure-passing)
- `store` → `&self` (mutates entries DashMap + meta_id_to_key + shard_to_keys + total_bytes Atomic + meta_dirty Atomic + shard_dirty Mutex + meta RwLock)
- `form_and_store` → `&self`
- `allocate_meta_id` → `&self`
- `evict_lru` / `evict_batch` → `&self`
- `clear` / `reconcile_bytes` → `&self`
- `enable_persistence` → `&self`
- `mark_shard_loading/loaded` / `add_pending_shards` → `&self`
- `insert_restored_entry` / `begin_restore` / `finish_restore` → `&self`
- `tombstone_entry` / `finalize_shard_write` / `tombstone_unloaded_for_filter/sort/all_unloaded` → `&self`
- `maintain_filter_changes` / `maintain_sort_changes` / `maintain_alive_changes` / `maintain_bucket_changes` → `&self`
- `remove_slot_from_all` / `remove_slots_from_all_batch` → `&self`
- `apply_maintenance_results` → `&self`
- `mark_for_rebuild_batch` → `&self`
- `invalidate_filter_field` → `&self`
- `clear_shard_entry_dirty` / `mark_shard_dirty` / `clear_shard_dirty` / `clear_meta_dirty` / `set_meta_dirty` → `&self`

Methods that stay `&self` and need internal lock acquisition:
- `meta()` returns `&MetaIndex` → can't return ref through RwLock guard. Either remove (callers use `with_meta(closure)` pattern) or change return type to Guard.
- `meta_mut()` same issue
- `pending_shards()` returns `&HashSet` → same; replace with `with_pending_shards(closure)` or `pending_shards_clone()` returning `HashSet`

### 4.5 Outer wrapper change

`concurrent_engine.rs:253`:
```rust
unified_cache: Arc<parking_lot::RwLock<UnifiedCache>>,
```
becomes:
```rust
unified_cache: Arc<UnifiedCache>,
```

`concurrent_engine.rs:712` constructor:
```rust
let unified_cache = Arc::new(parking_lot::RwLock::new(uc));
```
becomes:
```rust
let unified_cache = Arc::new(uc);
```

`cache_worker.rs:240,252`: same change — `Arc<RwLock<UnifiedCache>>` → `Arc<UnifiedCache>`.

`cache_worker.rs` body: `let mut uc = self.cache.write();` → `let uc = &self.cache;` (just a reference).

### 4.6 ~50 callsite changes in concurrent_engine.rs

Pattern audit (run after struct change):
- `self.unified_cache.write().X` → `self.unified_cache.X`
- `self.unified_cache.read().X` → `self.unified_cache.X`
- `pf_cache.write().X` / `pf_cache.read().X` → `pf_cache.X`
- `flush_unified_cache.write().X` → `flush_unified_cache.X`
- `merge_unified_cache.write().X` → `merge_unified_cache.X`
- `uc_arc.write().X` → `uc_arc.X`
- The `lookup` callsites at concurrent_engine.rs:4949 and 5169 (already in a closure pattern from v191) use `with_lookup` / `with_lookup_mut`
- Callsites that did `let mut uc = ... lock(); uc.X(...); uc.Y(...);` (multiple ops) collapse into individual method calls on `&Arc<UnifiedCache>`
- `timed_cache_read` / `timed_cache_write` helpers in `query_metrics.rs` become no-ops or removed entirely (cache_lock_us trace field stays but always reports 0)

### 4.7 cache_worker rewrite

Today (v191):
```rust
let mut uc = self.cache.write();  // long write lock
if !uc.is_empty() && !merged.alive_removes.is_empty() { uc.remove_slots_from_all_batch(...); }
if uc.persistence_enabled() { tombstones; }
let (fw, fob) = uc.collect_filter_work(...);
let (sw, sob) = uc.collect_sort_work(...);
// ... (whole block under write lock)
```

After v192:
```rust
// All &self calls — no lock acquisition for the cache itself, just internal
// per-field locks where needed.
if !self.cache.is_empty() && !merged.alive_removes.is_empty() {
    self.cache.remove_slots_from_all_batch(&merged.alive_removes);
}
if self.cache.persistence_enabled() { /* tombstones */ }
let (fw, fob) = self.cache.collect_filter_work(...);
let (sw, sob) = self.cache.collect_sort_work(...);
// ... etc
```

remove_slots_from_all_batch internally uses `self.entries.iter_mut()` (DashMap) — each shard's lock is taken briefly per shard, queries reading other shards never block.

### 4.8 Acceptance

Local rig (see §6):
```bash
node scripts/loadgen-cache-contention.mjs 180 70 64
```

Pull traces, check `cache_lock_us` for hits — should be < 10ms p99 (was 605ms median in prod on v191, 647ms locally).

Prod: 30+ min uptime, 70%+ cache hit rate, Prom histogram `bitdex_query_duration_seconds` ≤1s bucket should hold ≥99% sustained, ideally 100%.

---

## 5. Mission progression scoreboard

| Version | CPU | P50 cum | P95 cum | P99 cum | > 1s | > 5s |
|---|---|---|---|---|---|---|
| v1.0.181 baseline | 17 c | ~26 ms | ~3 s | 5-10 s | ~14% | several |
| v1.0.183 (lazy fix) | 17 c | ~7 ms | ~5 s | ~5-10 s | ~12% | 0 |
| v1.0.186 | 5 c | ~10 ms | ~500 ms | 1-5 s | ~3% | 0.002% |
| v1.0.188 | 6 c | ~10 ms | ~500 ms | 1-5 s | ~2% | 0 |
| v1.0.190 | 6 c | ~10 ms | ~500 ms | 1-5 s | 1.9% | 0 |
| v1.0.191 (current prod) | 6-9 c | ~10 ms | ~500 ms | 1-5 s | **1.6%** | 0.002% |
| **target** | — | **<2 ms** | **<350 ms** | **<1 s** | **0%** | **0** |

---

## 6. Local rig — quick iteration loop

`docs/_in/local-iter-howto.md` is the canonical doc.

### 6.1 Build & boot

```bash
cargo build --profile fast --features "server,pg-sync" --bin bitdex-server

cmd.exe /c "taskkill /F /IM bitdex-server.exe"

BITDEX_ADMIN_TOKEN=test123 \
BITDEX_QUERY_STREAM=1 \
BITDEX_MAX_QUERY_CONCURRENCY=256 \
RAYON_NUM_THREADS=4 \
nohup ./target/fast/bitdex-server.exe \
  --port 3002 \
  --data-dir "C:/Dev/Repos/open-source/bitdex-v2/data/full-dump" \
  > /tmp/bitdex-server.log 2>&1 &

until curl -s http://localhost:3002/api/indexes 2>/dev/null | grep -q "civitai"; do sleep 5; done
```

`data/full-dump` is pre-staged with 110 M alive records + persistent cache shards. Boot loads ~94K cache entries from disk (warm-ish start).

### 6.2 Synthetic load gen

`scripts/loadgen-cache-contention.mjs`:
```bash
node scripts/loadgen-cache-contention.mjs 180 70 64
# duration_sec target_qps max_concurrency
```

Outputs P50/P95/P99/max latency every 5 s + final cumulative.

### 6.3 Trace pulls

```bash
curl -sS -X PATCH -H "Authorization: Bearer test123" -H "Content-Type: application/json" \
  -d '{"enable_traces":true,"trace_min_us":500000,"trace_buffer_size":500}' \
  http://localhost:3002/api/indexes/civitai/config

curl -sS -H "Authorization: Bearer test123" \
  "http://localhost:3002/api/indexes/civitai/traces?last=500" > traces.json
```

Trace fields per query: `total_us`, `setup_us`, `cache_lock_us`, `shard_load_us`, `lazy_load_us`, `filter_us`, `sort_us`, `docs_us`, `cache_hit`, `clauses[]`, `sort{...}`.

After v192 lands: `cache_lock_us` should be ~0 across the board. If anything else dominates (filter_us, sort_us, lazy_load_us), look there next.

### 6.4 Hot-reload knobs (no restart)

```bash
curl -sS -X PATCH -H "Authorization: Bearer test123" -H "Content-Type: application/json" \
  -d '{"cache":{"max_maintenance_ms":50,"max_maintenance_work":1000}}' \
  http://localhost:3002/api/indexes/civitai/config
```

Other hot-reloadable: `enable_traces`, `trace_min_us`, `trace_buffer_size`, `par_iter_min_threshold`, `query_tee_mode`, `max_query_concurrency`. **`async_maintenance` is NOT hot-reloadable** — needs ConfigMap edit + pod restart.

---

## 7. Prod rig

### 7.1 Pod state

```bash
kubectl --context civit-datapacket -n bitdex get pod bitdex-0 -o jsonpath='{.spec.containers[0].image}{"\n"}'
# expect: ghcr.io/civitai/bitdex:1.0.191-jemalloc

kubectl --context civit-datapacket -n bitdex top pod bitdex-0 --containers

kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- bash -c \
  'curl -s http://localhost:3000/metrics | grep "^bitdex_query_duration_seconds_bucket" | tail -10'
```

Admin token: `uct7hZhiWtjynKkmQ5wYB4hZ-C0HI7OCAG8eMxQ37P0` (also in `$BITDEX_ADMIN_TOKEN` inside pod).

### 7.2 Prod env

```
RAYON_NUM_THREADS=4
BITDEX_MAX_QUERY_CONCURRENCY=256
_RJEM_MALLOC_CONF=dirty_decay_ms:5000,muzzy_decay_ms:10000
```

ConfigMap (`bitdex-index-config` in `talos-infra/clusters/production/apps/bitdex/deployment.yaml`):
```yaml
config:
  cache:
    async_maintenance: true
  doc_cache:
    max_bytes: 10737418240        # 10 GB
```

### 7.3 Deploy procedure

`docs/guide/prod-ops.md` is the canonical reference. Summary:
1. `cargo update -p bitdex-v2` after Cargo.toml version bump
2. Commit + tag + push tag separately (`git push origin v1.0.NNN-jemalloc`)
3. Watch build: `node .claude/skills/deploy/cli.mjs watch-build` (~9 min)
4. Edit `clusters/production/apps/bitdex/deployment.yaml` image: line in talos-infra (BOTH `bitdex` and `pg-sync` containers — same image)
5. `git pull --rebase`, commit, push trunk
6. Reconcile flux:
   ```bash
   kubectl --context civit-datapacket -n flux-system annotate kustomization bitdex \
     reconcile.fluxcd.io/requestedAt=$(date +%s) --overwrite
   kubectl --context civit-datapacket -n flux-system annotate gitrepository flux-system \
     reconcile.fluxcd.io/requestedAt=$(date +%s) --overwrite
   ```
7. Pod restart ~3 min for cold start with cache load; then ~25-45 min to cache warmup steady state

### 7.4 Re-enable runtime knobs after every restart

```bash
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- bash -c '
curl -sS -X PATCH -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d "{\"enable_traces\":true,\"trace_min_us\":500000,\"trace_buffer_size\":500,\"cache\":{\"max_maintenance_ms\":50,\"max_maintenance_work\":1000}}" \
  http://localhost:3000/api/indexes/civitai/config'
```

---

## 8. Tonight's failed attempts (don't repeat)

### 8.1 Cache_worker write-lock split (v192-attempt-1)

Tried splitting cache_worker.rs Phase A's combined write lock into write phase (alive_removes + tombstones) → release → read phase (collect_filter_work + collect_sort_work, both `&self`) → release.

Built clean. Local 3-min loadgen P95 89 ms vs v191's 42 ms — within variance, no clear improvement. Trace inspection showed cache_lock_us still dominated hit-path locally too (647 ms median). Concluded: cache_worker is not the only writer; form_and_store from misses dominates write contention. Reverted.

**Lesson:** splitting a single lock acquisition into multiple acquisitions doesn't help when the OTHER writers (queries doing form_and_store on miss) also hold the write lock. The shared queue is the problem, not the per-acquisition hold time.

### 8.2 Partial DashMap migration (v192-attempt-2)

Started with just `entries: HashMap` → `entries: Arc<DashMap>`. Hit ~50 compile errors across UnifiedCache methods due to DashMap API differences (see §4.3). Mid-conversion realized incremental partial-DashMap is harder than full conversion — every internal method touching entries needs API rewrite, plus the public methods returning `&UnifiedEntry` / `&mut UnifiedEntry` can't return DashMap Refs whose lifetime is the shard lock. Reverted to clean state.

**Lesson:** do the DashMap migration as ONE PUSH, not incremental. The struct change cascades into every method, every callsite. Trying to do "just entries" leaves the file half-converted.

### 8.3 What I had working before reverting

- `EntriesMap = Arc<DashMap<UnifiedKey, UnifiedEntry, ARandomState>>` type alias
- Added `pub fn entries_handle(&self) -> EntriesMap` — clones the Arc
- Closure-passing API for `with_lookup_mut` / `with_lookup` / `with_get` (snippets in §4.2)
- Fixed `evict_lru` / `evict_batch` / `reconcile_bytes` / `stats` / `entry_details` for DashMap iter API
- `store` updated for `entries.remove(k)` returning `Option<(K, V)>`

But still had ~30 errors elsewhere. The full conversion across all methods is the right move.

---

## 9. Things that didn't work / common traps (carry forward from prior handoff)

### 9.1 "Rayon par_iter is the floor" — wrong on v181

Prior agent disabled par_iter via `par_iter_min_threshold=10000000` and observed CPU stayed at 14-19 c. Concluded "rayon isn't the floor". Reality: rayon-core's `wait_until_cold` keeps workers spinning regardless of dispatch. Cap-the-pool (`RAYON_NUM_THREADS=4`) was the actual fix.

See `memory/project_rayon_spin_floor_2026_04_30.md`.

### 9.2 `kubectl debug` for perf profiling — blocked

Prod cluster's PodSecurity policy blocks SYS_PTRACE and privileged containers. Tried `kubectl debug bitdex-0 --image=alpine --target=bitdex --profile=sysadmin/general` — both rejected. Pod runs as nonroot user 65532, can't `apt-get install perf` either.

Workaround: per-thread CPU sample via `/proc/1/task/*/stat` (utime+stime). See `handoff-perf-cache-mutex-2026-04-30.md` §9.

### 9.3 Trace `total_us` is engine-only, not full HTTP roundtrip

`QueryTraceCollector::start` is created inside `execute_query_traced` (`concurrent_engine.rs:4830`). Does NOT include HTTP body parse, prefilter substitution, `block_in_place` migration, spawn_blocking dispatch for doc fetch, response serialization.

For full-roundtrip latency use `bitdex_query_duration_seconds` Prom histogram (recorded at `server.rs:2843-2861`). For sub-phase timing inside the engine, use trace fields.

### 9.4 Local loadgen cumulative is dominated by cold-fill

Local rig boots with ~94K cache entries from disk shards. First ~100s of loadgen has high miss rate as queries hit unmapped (key, sort, direction) combos and form_and_store fires. Cumulative P99 over the full 3-min run includes this cold fill. **Watch the per-5s windows past the 100s mark for steady-state behavior.** v190/v191/v192-attempt-1 all show P99 ~2 ms steady-state once warmed.

### 9.5 Hit-path read-lock waits don't show locally as much as in prod

Local loadgen has fewer concurrent writers (no shadow comparator, no real op stream). Cache_worker is busier in prod. The 605 ms median hit-path cache_lock_us seen in prod won't reproduce locally — but the architectural fix (DashMap) eliminates the shared lock queue regardless, so local rig is still useful for correctness validation.

---

## 10. Memory docs (knowledge files in user's memory system)

Located at `C:\Users\Zipp4\.claude\projects\C--Dev-Repos-open-source-bitdex-v2\memory\`:

- `project_rayon_spin_floor_2026_04_30.md` — rayon worker spin behavior, `RAYON_NUM_THREADS=4` mitigation
- `project_lazy_load_5s_wait_2026_04_30.md` — v183 fix for the recv_timeout(5s) watchdog
- `project_cache_mutex_contention_2026_04_30.md` — original mutex contention finding
- `project_cache_perf_v187_189_2026_04_30.md` — bulk slot ops, sampled-LRU, throttle reconcile
- `project_v191_rwlock_outcome_2026_04_30.md` — **THIS SESSION** — v191 outcome + v192 plan + tonight's failed attempts. Read this before §4 of this doc — it has the same plan in a different format.

---

## 11. Reference: code touched in v1.0.191 (prior commit)

| File | What changed |
|---|---|
| `Cargo.toml` | 1.0.190 → 1.0.191-jemalloc |
| `src/unified_cache.rs` | atomic stats + atomic last_used + atomic needs_rebuild + lookup_for_read |
| `src/concurrent_engine.rs` | Mutex → RwLock type. `.lock()` mass-replaced with `.write()`. Hot fast-path uses read lock + lookup_for_read when no bucket diff to apply. Slow-path lookup uses read lock |
| `src/cache_worker.rs` | Mutex → RwLock type. `.lock()` → `.write()` (cache_worker still write-only) |
| `src/query_metrics.rs` | timed_cache_lock removed; added timed_cache_write + timed_cache_read |

Commit: `8a79e13 release: v1.0.191-jemalloc — Mutex→RwLock for UnifiedCache, concurrent reads`

---

## 12. Open work / next session checklist

1. **v1.0.192 — DashMap interior-mutable refactor** (this is the handoff target)
   - Step 1: struct field changes per §4.1
   - Step 2: closure-passing lookup APIs per §4.2
   - Step 3: convert all `&mut self` methods to `&self` per §4.4
   - Step 4: fix DashMap API mismatches per §4.3
   - Step 5: outer wrapper change per §4.5
   - Step 6: ~50 callsite changes in concurrent_engine.rs per §4.6
   - Step 7: cache_worker rewrite per §4.7
   - Step 8: build, local test (acceptance §4.8), ship to prod
   - Estimate: 4-8 hours focused work

2. **PR #250 merge** — perf/p99-v2 branch into main. Needs `/ultrareview` (Justin's standing rule). Description was updated tonight to summarize v179 → v191; add v192 release notes after refactor lands.

3. **If DashMap doesn't get all the way to mission target**, follow-up moves (per §4 of `handoff-perf-cache-mutex-2026-04-30.md`):
   - Singleflight for form_and_store (dedupe concurrent misses on same key)
   - Reduce write frequency in cache_worker (already throttled reconcile to 1/30 cycles in v189; could go further)

---

## 13. Justin's exchange tonight (paraphrased)

- "@ava any updates?" → I sent local-rig validation summary mid-refactor.
- 👍 reaction → ack.
- "How are things looking now?" → I sent prod-rig finding (605ms median hit-path cache_lock_us behind writers); proposed three options (shard / DashMap / roll back).
- "Ok. Let's start on those changes" → green-light to begin DashMap. I attempted, hit the mid-refactor wall described in §8.2, reverted clean rather than ship broken.
- "Prepare a handoff doc for them and a prompt for the next agent like was given to you when you started" → this document + `next-cache-dashmap-prompt.md`.

---

## 14. Operating constraints

- **Don't merge PRs without `/ultrareview`.** Justin's standing rule.
- **Don't restart the pod casually.** Cache warm-up costs ~30 min. Use hot-reload PATCH for experiments.
- **Talos-infra config push needs `git pull --rebase`** before push (renovate bots churn the branch).
- **Cargo.toml version bump must be followed by `cargo update -p bitdex-v2`** to update Cargo.lock — release.mjs is broken on -jemalloc tags.
- **`git push origin v1.0.NNN-jemalloc`** as a separate command from branch push to avoid stale-collision.
- **ConfigMap changes need `kubectl delete pod bitdex-0`** after flux reconcile.
- **Don't ship a marginal v192.** If local validation shows hit-path cache_lock_us still > 50 ms, the refactor isn't done — keep iterating, don't push.

---

Good luck. The diagnostic chain is in place — every major lock acquisition is timed in trace fields. Local rig + synthetic load gives <2 min iteration cycles. The architectural fix is well-scoped; the work is mechanical now that the path is clear.
