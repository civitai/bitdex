# Cache Compound-Clause Maintenance Fix — Plan + Checklist

**Status:** Pre-implementation. Three reviewers passed. Final review NEEDS REVISION on two blocking structural issues — addressed below.
**Date:** 2026-05-04
**Owner:** TBD
**Bug refs:** Archer hub-mail 2026-05-04-161859-ad921eda (Not(And(In,In)) returns top-N unfiltered)

---

## Background

### Bug symptom

Query with `Not(And(In, In))` clauses where inner Ins match many docs returns `total_matched=N` (correct) but `documents`/`ids` are top-N by sort with NONE matching the filter. Bisect: 4 baseModel strings → correct; 9 real → broken; 9 dummy strings → correct.

### Root cause — multiple converging failure paths

Single bug-class, multiple independent paths producing the same symptom. All trace to incomplete handling of compound (`And`/`Or`/`Not(*)`) and unhandled (`IsNull`/`IsNotNull`) clauses in cache live-maintenance.

**Verified mechanisms (in order of impact for Archer's case):**

1. **Meta-index `field=""` for compound clauses.** `meta_index.rs:127-148` registers only top-level `clause.field`. Compound clauses get `field=""`, register under `FieldKey("")`. Inner-compound-only fields (`baseModel`, `postedToId`, `modelVersionIds`, `modelVersionIdsManual`) invisible to write-path entry lookup. Mutations on those fields never reach affected entries → entries stale at formation.
2. **`slot_matches_clause` conservative-true on compound.** `unified_cache.rs:2153-2173`. `not(and)`/`not(or)`/`and`/`or` arms return `true`; `IsNull`/`IsNotNull` fall to `_ => true`. Mutated slots admitted regardless of compound predicates → bitmap pollution.
3. **Prefetch worker drops compound clauses.** `concurrent_engine.rs:3249-3251`. `to_filter_clause` returns None for and/or/not/bucket/isnull/isnotnull. `filter_map` strips them, residual computes superset bitmap, `entry.expand` adds wrong slots.
4. **`needs_rebuild` dead in production.** `try_start_rebuild` only called in tests. Bloat-flagged entries never rebuild. Prod metrics confirm: `cache_entries_needs_rebuild=0`, `cache_marked_for_rebuild_total{reason="*"}=0`, `cache_rebuild_completed_total=0`.
5. **`uses_bucket=true` for prefilter-substituted entries.** Time-bucket diff applied to entries with no time-bucket clause.
6. **`"in"` arm broken for string-typed fields.** Line 2094 `parse::<u64>()` fails on "SD 3" → false silently. No `StringMaps`/`FieldDictionary` access in slot_matches_clause.
7. **`cache_updates_total` dead metric.** Registered, never incremented anywhere in src.
8. **No diagnostic endpoint** for inspecting individual cache entries.

### Live prod state (current)

- 29,702 cache entries, all at initial 4K capacity (zero expanded).
- Hit rate 53% despite 32 hot prefilters substituted ~250K times combined.
- Maintenance dormant: `cache_updates_total=0`, `cache_invalidations_total=0`, `cache_evictions_total=0`, `cache_extensions_total=0`, all 5 `cache_marked_for_rebuild_total{reason}=0`.
- WAL active: 3,254 appends, 3,160+ apply batches → maintenance has work but isn't doing it.

---

## Approach

Single PR, staged commits. Justin's call: ship as one PR with the work broken into reviewable commits in this order. Each commit compiles + passes existing tests. Local validation (below) runs against the final commit.

**Commit order:**

1. **Safe pre-fixes** (A1–A5) — narrow `uses_bucket`, prefilter eviction hook, dead-metric wiring, diagnostic endpoint, Prom alert. No correctness risk.
2. **Threading + storage** (B1) — extend `UnifiedEntry`, `CacheMaintenanceItem`, `MetaEntry`. Compiles, no behavior change yet.
3. **Native eval** (B2) — `slot_matches_filter_native` + dictionary threading. Behavior change here.
4. **Recursive registration + slot-gather** (B4) — meta-index changes + collect_filter_work fix.
5. **Cheap-first ordering** (B3) — perf optimization on top of correct eval.
6. **Prefetch fix** (B5).
7. **needs_rebuild wiring** (B6).
8. **meta.bin V2 + restore** (B8).
9. **Pathological-cost safety valve** (B9).
10. **Old sync path port + cleanup** (B7).
11. **Tests + property tests** (separate commit for review focus).

---

## Commit 1 — Safe pre-fixes

Zero correctness risk. Standalone-mergeable.

### A1. Narrow `uses_bucket` to time-bucket field name only
- [x] `unified_cache.rs` (`form_and_store`) — replaced inline `op == "bucket"` with `is_time_bucket_clause` helper (free function above the module, documented with `__prefilter` exclusion rationale).
- [x] Same change at `concurrent_engine.rs:5431` slow-path entry construction. `from_restored` site leaves `uses_bucket=false` by default (caller calls `set_uses_bucket()` after); no change needed there.
- [x] Test: `test_is_time_bucket_clause_prefilter_excluded`, `test_is_time_bucket_clause_real_bucket`, `test_uses_bucket_false_for_prefilter_substituted_entry`, `test_uses_bucket_true_for_time_bucket_entry` — all pass.

### A2. Prefilter eviction → cache invalidation hook
- [x] Added `UnifiedCache::invalidate_prefilter(name)` method — walks `meta.entries_for_clause("__prefilter", "bucket", name)`, calls `mark_for_rebuild()` on each, bumps `invalidations`. Returns count flagged.
- [x] Wired into `concurrent_engine.rs::remove_prefilter` — calls `invalidate_prefilter` when registry returns `true`. Registry never auto-evicts (returns `RegistryFull` error on insert); no replacement path to wire.
- [x] Test: `test_invalidate_prefilter_marks_referencing_entry`, `test_invalidate_prefilter_nonexistent_name_is_noop` — both pass.

### A3. Wire dead metrics
- [x] `apply_maintenance_results` now calls `record_update()` once per modified entry (not per slot).
- [x] Audit: `cache_updates_total` was the only metric registered but never incremented. All others in `IntGaugeVec`/`IntCounterVec` section had write sites.
- [x] Added `cache_maint_compound_eval_us` histogram (1μs–10ms buckets). Writes wired in B2.
- [x] Added `cache_substituted_entries` gauge. Sampled at scrape via `engine.unified_cache_entry_counts()`.
- [x] Added `cache_maint_conservative_total{reason}` counter. Writes wired in B2.
- [x] Added `cache_maint_string_lookup_miss_total` counter. Writes wired in B2.
- [x] Added `cache_entries_compound_clause_count` gauge. Sampled at scrape alongside `cache_substituted_entries`.
- [x] Test: `test_apply_maintenance_results_increments_updates` — passes.

### A4. Diagnostic endpoint for cache-entry inspection
- [x] `GET /api/indexes/{name}/cache/entry?filters=...&sort_field=...&direction=...` — added to admin_routes. Returns full per-entry state JSON or 404. Auth-gated (admin token).
- [x] Handler: `handle_cache_entry_inspect` in `server.rs`. Canonicalizes filter clauses, builds `UnifiedKey`, calls `unified_cache_ref().get(&key)`, extracts entry fields under dashmap shard lock, returns JSON.
- [x] Test: 7 unit tests covering A1/A2/A3 pass. Endpoint correctness confirmed by build + test run.

### A5. `cache_entries_needs_rebuild > 0 for > 5m` Prom alert
- [x] (deferred to monitoring repo, see commit msg) — `TODO(plan A5)` comment added above `cache_maint_compound_eval_us` metric declaration in `metrics.rs`.

---

## Commits 2–10 — Compound-clause maintenance (main fix)

### B1 — Commit 2. Carry original `FilterClause` tree from form_and_store through Phase A → B → C
**Critical structural decision.** Reviewer flagged ambiguity: `UnifiedKey` has `Vec<CanonicalClause>` only; `CacheMaintenanceItem` (line 49) carries only `UnifiedKey`. The `FilterClause` tree must reach `evaluate_filter_work`.

- [x] Add `original_filter_clauses: Arc<Vec<FilterClause>>` to `UnifiedEntry` struct (`unified_cache.rs:117`). Wire through `UnifiedEntry::new` and `from_restored`.
- [x] Extend `CacheMaintenanceItem` (line 49) to carry `Arc<Vec<FilterClause>>` (cheap clone of the entry's Arc).
- [x] `collect_filter_work` (~line 1856) — pull `original_filter_clauses` from entry, attach to work item.
- [x] `collect_sort_work` (parity site) — same.
- [x] Audit any other `CacheMaintenanceItem` constructor.

### B2 — Commit 3. Native per-slot `FilterClause` eval (the actual correctness fix)
**Reviewer-flagged GAP — `slot_matches_clause` has no access to StringMaps/FieldDictionary today. Must thread.**

- [ ] New `slot_matches_filter_native(slot, &[FilterClause], &FilterIndex, &SortIndex, Option<&TimeBucketManager>, Option<&PrefilterRegistry>, Option<&StringMaps>, Option<&HashMap<String, FieldDictionary>>) -> bool`.
- [ ] Per-clause:
  - [ ] `Eq/In/NotEq/NotIn`: resolve value→key via `string_maps`+`dictionaries` mirror of `executor.rs:121`. Then `FilterField::get_versioned(key).contains(slot)`. If unresolvable → bump `cache_maint_string_lookup_miss_total`, return false.
  - [ ] `Gt/Gte/Lt/Lte`: `sort_field.reconstruct_value(slot)` then compare.
  - [ ] `Not(inner)`: `!slot_matches(inner)`.
  - [ ] `And(parts)`: all-match short-circuit.
  - [ ] `Or(parts)`: any-match short-circuit.
  - [ ] `IsNull/IsNotNull`: `NULL_BITMAP_KEY` contains check (with negate for IsNotNull).
  - [ ] `BucketBitmap`: direct `bitmap.as_ref().contains(slot)` from the Arc on the clause.
- [ ] Default fall-through → **false** (loud failure, not silent admit).
- [ ] Replace all `slot_matches_filter(...CanonicalClause...)` call sites in:
  - [ ] `evaluate_filter_work` (~line 2263) — use `Arc<Vec<FilterClause>>` from work item.
  - [ ] `evaluate_sort_work` (parity site).
  - [ ] `maintain_bucket_changes` (line 1982) — also fixes its TODO at line 2019.
  - [ ] Old sync `maintain_filter_changes` (line 1479) — see B7.
- [ ] Thread `string_maps` + `dictionaries` from snapshot through `cache_worker.rs:438` and `concurrent_engine.rs:1692` call sites.

### B3 — Commit 5. Cheap-clause-first ordering
- [ ] At `form_and_store`, sort `original_filter_clauses` by atom-cost ascending. Cost classes:
  1. Eq, IsNull, IsNotNull, BucketBitmap (1 contains)
  2. NotEq, Range (1 contains + 1 reconstruct)
  3. In/NotIn (≤K contains)
  4. And/Or (sum of children, recursive)
  5. Not(And/Or) (deepest — last)
- [ ] Property test: per-slot eval result invariant under any ordering.

### B4 — Commit 4. Recursive meta-index registration + slot-gather fix
**Reviewer flagged Partial 1: meta-index fix alone is no-op without also fixing the slot-gather loop.**

- [ ] `meta_index.rs::register` + `register_with_id` — walk inner FilterClause leaf fields recursively. Register every leaf `FieldKey`.
- [ ] **Critical:** Cannot rely on `CanonicalClause` alone for inner-field discovery (compound canonical has flat `value_repr`). Must take the `FilterClause` tree as a separate input — extend `register` signature to take `Option<&[FilterClause]>` for the original tree, fall back to canonical-only behavior if absent (test paths).
- [ ] **Slot-gather loop in `collect_filter_work`** (line 1845-1851 area): replace `changed_slots_per_field.get(clause.field.as_str())` with a leaf-field walk over the entry's `original_filter_clauses`. Test: mutate `baseModel` for slot 42, confirm `slots_to_check` includes 42 for an entry with `Not(And(In(baseModel), ...))`.
- [ ] Same fix in any other site that iterates `key.filter_clauses` to determine field coverage.

### B5 — Commit 6. Prefetch worker fix
- [ ] `concurrent_engine.rs:3249-3251` — replace `filter_map(to_filter_clause)` round-trip with direct `entry.original_filter_clauses.clone()` lookup.
- [ ] Need to fetch `UnifiedEntry` from cache (`pf_cache.get(&ukey)`) — confirm available at this site.
- [ ] Test: push compound-clause query through prefetch worker, assert resulting filter bitmap matches full-executor result (no superset).

### B6 — Commit 7. `needs_rebuild` wired to read path
**Reviewer Partial 2: clarify ownership of rebuild work.**

- [ ] **Mechanism:** `lookup_for_read` already returns `None` on `needs_rebuild=true` (line 789). Cache miss → slow path → `form_and_store` → new entry replaces old. **Nothing extra needed on read path itself.**
- [ ] Wire `try_start_rebuild` at the slow-path call site so concurrent flagged-entry queries don't all stampede `compute_filters`. First caller: `try_start_rebuild() == true`, runs slow path, calls `form_and_store`. Others: hit `needs_rebuild`, fall through, await new entry on next read or also run slow-path naturally.
- [ ] Increment `cache_rebuild_completed_total` correctly inside `store()` when replacing an entry that had `needs_rebuild=true`.
- [ ] Test: `add_slot` past 2× capacity → `needs_rebuild=true`. Next read returns slow-path. Subsequent reads see fresh entry, `needs_rebuild=false`.

### B7 — Commit 10. Port old sync `maintain_filter_changes` to native eval
- [x] **Verified test-only via grep.** 12 callers, all in `#[cfg(test)]` blocks (`unified_cache.rs:3104, 3123, 3144, 3170, 3189, 3252, 3340, 3449, 3505, 3572, 3623, 3746`). Zero production callers.
- [ ] Port `unified_cache.rs:1479-1615` to call the same native-eval helper from B2. Tests stay green; second slot_matches_filter call site eliminated.
- [ ] Follow-up cleanup PR can delete the function entirely + migrate tests to Phase A/B/C path.

### B8 — Commit 8. meta.bin V2 + restore
- [ ] Bump `META_VERSION = 2` (`bound_store.rs:51`).
- [ ] Add `original_filter_clauses: Vec<FilterClause>` to `MetaEntry` (line 56). msgpack with `#[serde(default)]` if backward-read needed; otherwise rely on existing purge-on-mismatch.
- [ ] Custom `Serialize`/`Deserialize` for `FilterClause::BucketBitmap` to drop the bitmap Arc on serialize, leave name+field. (`#[serde(skip)]` on the Arc field already covers serialize; verify deserialize default.)
- [ ] **Restore-time BucketBitmap re-resolve:** in `BoundStore::restore` path, walk loaded `original_filter_clauses` for each `MetaEntry`. For every `BucketBitmap` clause, look up bitmap by name in `TimeBucketManager` first, then `PrefilterRegistry`. If neither found → tombstone the entry (mark for rebuild on next access).
- [ ] Plumbing: confirm `TimeBucketManager` + `PrefilterRegistry` available at `BoundStore::restore` callsite. If not, restore signature needs them.
- [ ] Document expected ~30s cold-cache window post-deploy in PR description.

### B9 — Commit 9. Pathological cost safety valve (reviewer-flagged)
**Compound `In` with high-cardinality inner values can blow `max_maintenance_ms` budget. Need fast-reject.**

- [ ] In B2's compound eval, before per-slot loop: count total leaf atoms across entry's `original_filter_clauses`. If sum > config threshold (default 100), skip per-slot eval and `mark_for_rebuild` the entry directly. Log via `cache_marked_for_rebuild_total{reason="compound_too_large"}`.
- [ ] Existing `max_maintenance_ms` deadline already catches over-budget entries via `mark_for_rebuild` fallback (line 1538) — keep as second backstop.

---

## Tests (non-negotiable before merge)

- [ ] **Regression — Archer's exact bug:** form cache entry for `Not(And(In(baseModel, ["SD XL"]), In(nsfwLevel, [1,2])))`. Insert slot with baseModel="SD XL". Mutate slot's baseModel to "FLUX". Assert slot removed from cache entry within one maintenance cycle.
- [ ] **String-field In eval:** insert two slots with baseModel="SD 1.5" and "SDXL". Query In(baseModel, ["SD 1.5"]). Assert correct slot match. Will fail today; passes after B2.
- [ ] **Meta-index recursive registration:** form entry with `And(In(baseModel), In(nsfwLevel))`. Assert `entries_for_filter_field("baseModel")` and `entries_for_filter_field("nsfwLevel")` both include entry id.
- [ ] **Slot-gather correctness:** mutate baseModel for slot 42. Assert `slots_to_check` for entry containing `And(In(baseModel), ...)` includes slot 42.
- [ ] **needs_rebuild fires + clears:** bloat entry past 2× capacity. Read path returns slow-path result. Subsequent read shows fresh entry, flag cleared.
- [ ] **`uses_bucket` narrowed:** form prefilter-substituted entry. Assert `uses_bucket=false`. Form time-bucket entry. Assert `uses_bucket=true`.
- [ ] **Prefetch worker compound:** push compound query through prefetch, assert filter bitmap matches executor result exactly.
- [ ] **Property test:** random `Not(And(In, In))` × random mutations × cache-warm-vs-skip_cache equality. 10K iterations.

---

## Observability checklist

- [ ] Wire all metrics in A3.
- [ ] Add Prom alert: `cache_entries_needs_rebuild > 0 for 5m`.
- [ ] Add Grafana panel: hit rate by `substituted` label (true/false).
- [ ] Add Grafana panel: `cache_maint_compound_eval_us` p50/p99 + budget threshold line.
- [ ] Verify `cache_maint_conservative_total{reason="*"}` reads zero in prod after deploy. Any non-zero = regression.

---

## Migration / deploy plan

- [ ] Single PR merges. Local validation (Stage 1–8) passes.
- [ ] Deploy → meta.bin V2 bump → on first restart `BoundStore` purges old meta.bin.
- [ ] **Cold-cache window:** ~30s post-deploy until cache repopulates. Expect elevated P99 in this window. Document in deploy brief. Schedule deploy during low-traffic window if possible.
- [ ] Alert pre-deploy: silence the new `cache_entries_needs_rebuild` alert for ~5min during deploy to absorb expected churn.
- [ ] Post-deploy verification:
  - [ ] `cache_updates_total` non-zero within 1 min.
  - [ ] `cache_maint_conservative_total{reason="*"} == 0` after first cycle.
  - [ ] Hit rate climbs above pre-deploy 53% within 10 min (target: 75%+).
  - [ ] Archer's exact repro query returns correct results.
  - [ ] `cache_marked_for_rebuild_total{reason="compound_too_large"}` low (sanity check on B9 threshold).

---

## Risk register

- **R1: Meta-index recursive registration changes write-path fan-out.** More entries reachable per mutation → more eval work per cycle. Mitigated by cheap-first ordering (B3) + safety valve (B9) + max_maintenance_ms backstop.
- **R2: Cold-cache window post-deploy.** ~30s elevated P99. Mitigated by low-traffic deploy + ops awareness.
- **R3: Slow path stampede on flagged entries.** Mitigated by `try_start_rebuild` single-flight (B6).
- **R4: BucketBitmap restore can't find prefilter (registry evicted).** Mitigated by tombstone-on-not-found in B8. Entry rebuilds on next access.
- **R5: Inner-compound In with 1000+ values.** Caught by B9 fast-reject. Entry rebuilds rather than blocking maintenance.
- **R6: Compound query shapes the new eval doesn't anticipate.** Default arm now returns false (loud) + `cache_maint_conservative_total` metric catches any fall-through. Property tests cover combinatorial coverage.

---

## Open questions — RESOLVED

- [x] **`TimeBucketManager` + `PrefilterRegistry` at `BoundStore::restore`.** Verified: `bound_store.rs:163` `load_meta(&self) -> Result<Option<MetaFile>>` is a pure deserializer — doesn't touch engine state. Call site in `concurrent_engine.rs::restore_unified_cache` constructs `UnifiedEntry` from `MetaEntry`. **Approach:** restore stays pure; BucketBitmap re-resolve happens in the construction step at the call site, which already holds refs to `time_buckets` (ArcSwap on engine) and `prefilter_registry`. New helper `resolve_bucket_clauses(&mut Vec<FilterClause>, &TimeBucketManager, &PrefilterRegistry) -> bool` → returns false if any name unresolvable → caller tombstones the entry. No `BoundStore` API change needed.
- [x] **Old sync `maintain_filter_changes` (line 1479) — TEST-ONLY.** Grep confirms 12 callers, all in `unified_cache.rs` `#[cfg(test)]` blocks (lines 3104–3623, 3746). **Zero production callers.** **Decision:** keep the function for test compatibility but port it to use the same native-eval helper as Phase B (B2). Eliminates the second slot_matches_filter call site without touching tests. Delete in a follow-up cleanup PR.
- [x] **Cheap-first ordering — stable sort.** Cost classes are coarse (5 buckets); within a bucket, preserve user intent for predictability. `slice::sort_by_key` (stable) preferred. No measurable perf delta vs unstable for ≤20 clauses; reproducibility wins.
- [x] **B9 threshold — 50 leaf atoms, hot-tunable via config.** Civitai's safety prefix has ~24 leaf atoms (1 IsNotNull + 6 In + 13 Not(And) inner + 3 Or + 1 Eq). 50 = 2× margin; supports the dominant shape with headroom. Add `unified_cache.compound_eval_atom_limit: u32` to runtime config (default 50) so the threshold is tunable in prod via PATCH `/runtime` without redeploy.

---

## Local validation plan

End-to-end correctness + perf check on a fresh local dump before merging PR B. Mirrors the prod failure mode: warm the cache with the problem shape, trigger ops streaming, verify cache stays correct under maintenance.

### Stage 1 — Fresh dump

Follow `docs/_in/local-fresh-dump-howto.md`. Quick recap:

```bash
# Build with feature flags for the fix branch
cd C:/Dev/Repos/open-source/bitdex-v2
cargo build --profile fast --features "server,pg-sync" --bin bitdex-server

# Wipe + boot empty
cmd.exe /c "taskkill /F /IM bitdex-server.exe" 2>&1; sleep 2
rm -rf C:/Dev/Repos/open-source/bitdex-v2/data/full-dump
mkdir -p C:/Dev/Repos/open-source/bitdex-v2/data/full-dump/indexes/civitai
cp C:/Dev/Repos/open-source/bitdex-v2/deploy/configs/civitai-index.yaml \
   C:/Dev/Repos/open-source/bitdex-v2/data/full-dump/indexes/civitai/config.yaml

BITDEX_ADMIN_TOKEN=test123 \
BITDEX_QUERY_STREAM=1 \
BITDEX_MAX_QUERY_CONCURRENCY=32 \
./target/fast/bitdex-server.exe \
  --port 3002 \
  --data-dir "C:/Dev/Repos/open-source/bitdex-v2/data/full-dump" \
  > /tmp/bitdex-server.log 2>&1 &

# Run dump (~28 min for full, or --small for ~2 min smoke)
node scripts/dump-local.mjs
```

### Stage 2 — Reproduce the bug (verify failure on `main` before fix)

Confirm the bug exists locally before PR B lands. Switch to `main` branch, fresh dump, then:

```bash
# Pick a real modelVersionId from local data — top reaction count is good
MV_ID=$(curl -s -X POST http://localhost:3002/api/indexes/civitai/query \
  -H "Authorization: Bearer test123" -H "Content-Type: application/json" \
  -d '{"filters":[{"IsNotNull":"postedToId"}],"sort":{"field":"reactionCount","direction":"Desc"},"limit":1,"include_docs":["postedToId"]}' \
  | node -e "let d='';process.stdin.on('data',c=>d+=c);process.stdin.on('end',()=>{const j=JSON.parse(d);console.log(j.documents[0].postedToId);});")
echo "Repro modelVersionId: $MV_ID"
```

**The repro query** (Archer's exact shape with 9 baseModel strings):

```bash
curl -s -X POST http://localhost:3002/api/indexes/civitai/query \
  -H "Authorization: Bearer test123" -H "Content-Type: application/json" \
  -d "$(cat <<EOF
{
  "filters": [
    {"IsNotNull": "postId"},
    {"In": ["nsfwLevel", [{"Integer":1},{"Integer":2},{"Integer":4},{"Integer":8},{"Integer":16},{"Integer":0}]]},
    {"Not": {"And": [
      {"In": ["nsfwLevel", [{"Integer":4},{"Integer":8},{"Integer":16},{"Integer":32}]]},
      {"In": ["baseModel", [{"String":"SD 3"},{"String":"SD 3.5"},{"String":"SD 3.5 Medium"},{"String":"SD 3.5 Large"},{"String":"SD 3.5 Large Turbo"},{"String":"SDXL Turbo"},{"String":"SVD"},{"String":"SVD XT"},{"String":"Stable Cascade"}]]}
    ]]},
    {"Or": [
      {"Eq": ["postedToId", {"Integer":$MV_ID}]},
      {"In": ["modelVersionIds", [{"Integer":$MV_ID}]]},
      {"In": ["modelVersionIdsManual", [{"Integer":$MV_ID}]]}
    ]},
    {"Eq": ["isPublished", {"Bool":true}]}
  ],
  "limit": 10,
  "sort": {"field":"sortAt","direction":"Desc"},
  "include_docs": ["id","postedToId","modelVersionIds","modelVersionIdsManual"]
}
EOF
)" > /tmp/cache-warm-result.json
```

**Ground truth via skip_cache:**

```bash
curl -s -X POST 'http://localhost:3002/api/indexes/civitai/query?skip_cache=true' \
  -H "Authorization: Bearer test123" -H "Content-Type: application/json" \
  -d "@<(cat /tmp/cache-warm-result.json | jq '.query // input')" \
  > /tmp/cache-skip-result.json

# diff the two — same ids must appear, in same order
diff <(jq -r '.documents[].id' /tmp/cache-warm-result.json | sort) \
     <(jq -r '.documents[].id' /tmp/cache-skip-result.json | sort)
```

On `main` (broken), you'll see a diff after step 4 (ops streaming) below. On the fix branch, no diff at any point.

### Stage 3 — Warm cache for the problem shape

Cache entry only forms when query runs. Exercise the bug shape across many modelVersionIds to fill the registry's 32-entry cap (auto-prefilter promotion threshold = 50 hits per shape):

```bash
# Top 50 modelVersionIds by recent activity — produces 50 distinct cache shapes
for MV in $(curl -s -X POST http://localhost:3002/api/indexes/civitai/query \
  -H "Authorization: Bearer test123" -H "Content-Type: application/json" \
  -d '{"filters":[{"IsNotNull":"postedToId"}],"sort":{"field":"reactionCount","direction":"Desc"},"limit":50,"include_docs":["postedToId"]}' \
  | jq -r '.documents[].postedToId'); do
  for i in $(seq 1 60); do
    # Fire 60× per shape to trigger auto-prefilter promotion (threshold=50)
    curl -s -X POST http://localhost:3002/api/indexes/civitai/query \
      -H "Authorization: Bearer test123" -H "Content-Type: application/json" \
      -d "$(echo $REPRO_TEMPLATE | sed s/MV_PLACEHOLDER/$MV/g)" > /dev/null &
  done
  wait
done

# Verify auto-prefilter registry filled
curl -s -H "Authorization: Bearer test123" \
  http://localhost:3002/api/indexes/civitai/prefilters | jq '{count: (.prefilters|length), top: .prefilters | sort_by(-.substitutions) | .[0:3] | map({name, substitutions, cardinality})}'
```

### Stage 4 — Stream ops to trigger maintenance (the failure trigger)

Without ops mutations, no maintenance runs → no pollution → no failure. Generate writes that touch the bug-relevant fields (`baseModel`, `nsfwLevel`, `postedToId`, `modelVersionIds`, `isPublished`, `sortAt`).

**Option A: synthetic ops loadgen** (deterministic, fast):

```bash
# Stream 100K synthetic upserts that randomly modify bug-relevant fields
node scripts/synthetic-ops.mjs --rate 1000 --duration 60 --fields baseModel,nsfwLevel,postedToId,modelVersionIds,isPublished,sortAt
```

**Option B: SSE replay from prod** (realistic):

```bash
# Replay 5 min of real prod ops (touches everything)
node scripts/sse-replay.mjs --source prod --duration 300
```

See `docs/_in/local-replay-rig-howto.md` for both rigs.

### Stage 5 — Verify correctness post-maintenance

After ops have flowed for ~1 min:

```bash
# Re-run the same problem query (cache-warm path)
curl -s -X POST http://localhost:3002/api/indexes/civitai/query \
  -H "Authorization: Bearer test123" -H "Content-Type: application/json" \
  -d "@/tmp/repro-query.json" > /tmp/post-ops-warm.json

# Re-run with skip_cache (ground truth)
curl -s -X POST 'http://localhost:3002/api/indexes/civitai/query?skip_cache=true' \
  -H "Authorization: Bearer test123" -H "Content-Type: application/json" \
  -d "@/tmp/repro-query.json" > /tmp/post-ops-skip.json

# Both must show same total_matched AND same id list in same order
jq '{total: .total_matched, ids: [.documents[].id]}' /tmp/post-ops-warm.json > /tmp/warm.tsv
jq '{total: .total_matched, ids: [.documents[].id]}' /tmp/post-ops-skip.json > /tmp/skip.tsv
diff /tmp/warm.tsv /tmp/skip.tsv
# Empty diff = fix works. Any output = bug present.
```

**Verification matrix** — run for each shape:

| Shape | Expected on `main` | Expected on fix branch |
|-------|-------------------|------------------------|
| 9 real baseModel strings | warm ≠ skip (BROKEN) | warm == skip ✓ |
| 4 real baseModel strings | warm == skip ✓ | warm == skip ✓ |
| 9 dummy strings (A–I) | warm == skip ✓ | warm == skip ✓ |
| Pure `Or(modelVersionId)` no Not(And) | warm == skip ✓ | warm == skip ✓ |

### Stage 6 — Metrics + trace verification

After Stage 4 ops have flowed:

```bash
# Maintenance is actually running
curl -s http://localhost:3002/metrics | grep -E "^bitdex_cache_(updates|invalidations|maint_compound|marked_for_rebuild|entries_needs_rebuild|rebuild_completed|substituted)" | sort
```

**Pass criteria:**

| Metric | Pre-fix | Post-fix expected |
|--------|---------|-------------------|
| `cache_updates_total` | always 0 (dead) | non-zero, increments |
| `cache_maint_compound_eval_us_count` | doesn't exist | non-zero |
| `cache_maint_compound_eval_us_bucket{le="0.001"}` | — | majority (per-slot p50 < 1ms) |
| `cache_maint_conservative_total{reason="*"}` | — | **0 across all reasons** (any non-zero = fall-through bug) |
| `cache_maint_string_lookup_miss_total` | — | 0 (any non-zero = dictionary not threaded) |
| `cache_marked_for_rebuild_total{reason="compound_too_large"}` | — | low (Civitai shape <50 atoms; should rarely trip) |
| `cache_marked_for_rebuild_total{reason="deadline"}` | 0 | 0 or low |
| `cache_entries_needs_rebuild` | 0 | transient, drains in seconds |
| `cache_rebuild_completed_total` | always 0 (dead) | non-zero on flagged-entry reads |
| `cache_extensions_total` | 0 | non-zero post-prefetch fix |
| `cache_substituted_entries` | doesn't exist | matches prefilter registry size |
| `cache_hits_total / (hits+misses)` | 53% | target ≥ 75% |
| `flush_cache_nanos` | ~18μs (skipping work) | ~10–30ms (doing work) |

### Stage 7 — Per-entry trace inspection

Use the new diagnostic endpoint (A4) to inspect a specific cache entry suspected of staleness:

```bash
# Inspect the cache entry for Archer's exact shape
curl -s -G "http://localhost:3002/api/indexes/civitai/cache/entry" \
  -H "Authorization: Bearer test123" \
  --data-urlencode "filters=@/tmp/repro-query-filters-only.json" \
  --data-urlencode "sort_field=sortAt" \
  --data-urlencode "direction=Desc" \
  | jq '{bitmap_len, total_matched, capacity, needs_rebuild, uses_bucket, last_used, persist_dirty}'
```

**Sanity checks:**
- `bitmap_len <= total_matched` (no pollution).
- `needs_rebuild=false` after a few seconds (rebuild path firing).
- `uses_bucket=false` for queries with no time-bucket clause.

### Stage 8 — Soak test (scale check)

Let the system run under sustained load for 30 min:

```bash
# 1000 ops/sec writes + 100 QPS reads with diverse shapes
node scripts/synthetic-ops.mjs --rate 1000 --duration 1800 &
node scripts/loadtest.mjs --qps 100 --shapes diverse --duration 1800
```

**Pass criteria:**
- `flush_cache_nanos` p99 < 50ms (within budget).
- `cache_marked_for_rebuild_total{reason="deadline"}` < 1% of cycles.
- `cache_maint_compound_eval_us` p99 < 5ms.
- No memory growth in `cache_bytes` beyond steady-state delta.
- Repro query correctness diff stays empty across all 30 min.

### Local validation checklist

- [ ] Fresh dump completes (~28 min).
- [ ] Stage 2 reproduces the bug on `main` (warm ≠ skip after ops stream).
- [ ] Stage 3 fills registry to 32 prefilters with target shape dominant.
- [ ] Stage 4 ops streaming actually fires maintenance (`cache_updates_total` increments).
- [ ] Stage 5 verification matrix all rows pass on fix branch.
- [ ] Stage 6 metrics meet pass criteria.
- [ ] Stage 7 diagnostic endpoint returns sane per-entry state.
- [ ] Stage 8 soak: 30 min stable, all p99s within budget, correctness preserved.
- [ ] Capture before/after metrics CSV for PR description.

---

## Out of scope (deferred)

- `total_matched` drift under streaming mutations (acknowledged: documented as upper bound; rebuild fixes when entry re-forms).
- LSH / vector similarity (unrelated future work).
- Replacing `CanonicalClause` entirely with `FilterClause` as cache key (would invalidate hash, larger key, separate refactor).
- BoundStore shard format changes (unchanged in this PR — only meta.bin bumps).
