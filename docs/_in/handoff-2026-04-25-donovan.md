# Handoff — 2026-04-25 (Donovan → next agent)

P99 sub-second perf push. Stack v1.0.165–175 in main + relay V1 in prod.
Mission gate cleared at strict P99; long tail localized to `load_field_values`
full-bucket-read on per_value_lazy fields.

---

## Read this first

1. `docs/_in/regression-resolution-2026-04-25.md` — full session history through PR #228 (warm persist). Context for the apply-side fixes.
2. `docs/_in/mixed-load-measurement-2026-04-25.md` — first mission-gate measurement (corpus only, no shadow). 11-min window cleared P99 by 40×.
3. `docs/_in/lazy-load-localization-2026-04-25.md` — root-cause analysis of the long-tail outliers under shadow-on traffic. **Read carefully — this is where the next session likely starts.**
4. `docs/_in/per-value-lazy-indexed-lookup-design.md` — V2 fix spec (BIDX index format, 3-PR ship sequence, backwards-compat fallback). Awaiting Justin's A1/A2/A3 call before engineering time goes in.
5. `docs/_in/relay-system-design.md` — relay V1 architecture (Jack's). Read if you'll touch relay code.
6. `docs/_in/relay-consumer-howto.md` + `scripts/replay-prod-via-relay.mjs` — how to consume prod relay's SSE channels from a local rig.

---

## What shipped this session (newest first)

| Tag | PR | Net behavior |
|---|---|---|
| v1.0.175-jemalloc | #232 | Full pg-sync method matrix coverage in relay smoke harness. Closes the v1.0.172/.173 stub-discover loop. |
| v1.0.174-jemalloc | #231 | Axum router merge fix — `any()` fallback at duplicate path was causing startup panic. |
| v1.0.173-jemalloc | #230 | Pg-sync stub routes for `/dumps`, `/stats`, `/cursors` so the sidecar boots. |
| v1.0.172-jemalloc | #229 (Jack) | Relay V1 env-var dispatch — `BITDEX_MODE=relay` skips engine bootstrap, runs as pure HTTP→SSE proxy. |
| v1.0.171-jemalloc | #228 | Warm registry persist fix — `original_filters_for_warm` captured before prefilter substitution so `BucketBitmap` doesn't bork serde. |
| v1.0.170-jemalloc | #224 | Merge-thread brace fix — stray `}` had been silently disabling prefilter refresh, auto-prefilter promotion, warm persist, RSS eviction, idle cache eviction since whoever-introduced-it. |
| v1.0.169-jemalloc | #227 | `bitmap_bytes()` chunked iteration — bounded read-lock-hold (was the actual write-lock-blocker post-#224 unlock). |
| v1.0.168-jemalloc | #226 | Skip 0-cardinality auto-prefilter promotion. |
| v1.0.167-jemalloc | #223 | Snapshot save bucket writes parallelized via rayon. |
| v1.0.166-jemalloc | #222 | `merge_dirty` uses side dirty-set, walks O(dirty) not O(N). 27× apply-mean improvement. |
| v1.0.165-jemalloc | #221 | WAL reader skips unreadable tail on closed gen — fixes silent stall. |

Everything tagged + on main.

## Doc commits on main this session

- `0ed4232 docs(perf): regression resolution audit 2026-04-25 — final`
- `2e77663 docs(perf): mixed-load measurement audit 2026-04-25 — mission gate cleared`
- `7f9cf02 docs(perf): lazy-load localization 2026-04-25 — P99 outlier root cause`
- `bb18906 docs(perf): per-value-lazy indexed lookup design — V2 P99 outlier fix spec`

---

## Mission gate status

| Reading | Number | Status |
|---|---|---|
| Strict P99 (99th pct) under shadow + ops | ~150 ms (climbed from ~25 ms at 11-min mark to ~150 ms at 25-min as diversity expanded) | **Cleared** with 7× margin |
| P99.6 | ~1 s | Boundary |
| P99.99 | ~5 s | Violation |
| 0.014 % above 1 s | ~600 of 4.27 M queries during the 25-min sample | Real but rare |

Long-window run (45 min config) was still in flight at handoff time. State: see `tail /tmp/relay-consumer-long.log` and `curl http://localhost:3002/metrics | grep query_duration_seconds_bucket`.

**Justin's path call awaiting (per Scarlet's mail):**
- **A1** — ship perf stack as-is. Strict P99 cleared. Tail addressed in V2.
- **A2** — build indexed-value-lookup fix (PR ladder PR-A → PR-B → PR-C in design doc) **before** flip-back from relay mode.
- **A3** — hybrid: ship as-is + queue indexed-lookup as immediate next priority for V2.

Scarlet leaning A3 per the framing. Don't start engineering until Justin picks.

---

## Localized root cause of the long tail

`FilterBitmapStore::load_field_values` at `src/shard_store_bitmap.rs:891` reads the **entire bucket snapshot** (deserializes 30–80 MB / ~89K entries) to extract a single requested value. Cold path → 350–900 ms typical, observed up to 3.16 s.

Affected fields (per 25-min shadow-on observation, 27 K lazy-load events):
- postId 36 % (single_value, `per_value_lazy: true`)
- modelVersionIds 18 % (multi_value, lazy by default)
- postedToId 18 % (single_value, `per_value_lazy: true`)
- modelVersionIdsManual 3 %
- tagIds 1 %
- rest marginal

Single fix at `load_field_values` benefits all of them (shared code path).

Trace data sample saved at `local-prom/runs/shadow-traces-snapshot.json` (1000 traces, top-N slow visible via the script in the localization doc).

---

## Local rig state at handoff

Worktree: `C:/Dev/Repos/open-source/bitdex-donovan-perf` (off `main`, fast-forwarded). Use this for further iteration; don't touch the main `bitdex-v2` checkout (Jack works there).

**Server:** `target/fast/bitdex-server.exe`, port 3002, data dir `C:/Dev/Repos/open-source/bitdex-v2/data/full-dump`. Boot ~150 s (110.5 M dump restore). Boot env vars: `BITDEX_ADMIN_TOKEN=test123`, `BITDEX_QUERY_STREAM=1`, `BITDEX_MAX_QUERY_CONCURRENCY=32`. Re-register prefilter post-boot (it doesn't survive restart).

**Consumer:** `scripts/replay-prod-via-relay.mjs` subscribed to prod relay via `kubectl port-forward pod/bitdex-0 4099:3000`. Both `/events/queries` + `/events/ops` channels accepting 200 + text/event-stream. Auth: `BITDEX_ADMIN_TOKEN=uct7hZhiWtjynKkmQ5wYB4hZ-C0HI7OCAG8eMxQ37P0`.

**Query loadgen:** `scripts/replay-captured.mjs` driving local at ~3 K QPS from 232-shape captured corpus. Used as cache-warmer + relative-comparison baseline. Real diverse traffic comes via the consumer once shadow flag is on.

**Shadow flag:** `bitdex-image-search` flipt flag enabled=true, variant=shadow. Live traffic flowing through prod relay → `/events/queries` SSE → local BitDex via consumer.

**Prod relay state:** v1.0.175-jemalloc, both containers ready, restarts=0, pg-sync polling steady. Periodic ops events at ~5 / 30 s on `/events/ops`.

---

## Open follow-ups

| ID | Topic | Why pending |
|---|---|---|
| #6 | Capture prod CPU flame graph | Real BitDex pod still in relay mode; reschedule when flip-back |
| #13 | Cache `bitmap_bytes` with TTL or update incrementally on mutation | PR #227 chunked the iteration; this is the durable fix that eliminates the periodic re-scan entirely |
| #18 | Indexed value lookup for per_value_lazy fields (postId) | The V2 P99 outlier fix surface. Awaits Justin's path call. Spec at `docs/_in/per-value-lazy-indexed-lookup-design.md`. |
| #19 | LRU bucket-snapshot cache for per_value_lazy reads | Defense-in-depth complement to #18 |

(Task #17 sort-layer-fusion was deleted — misdiagnosis. Real cause is #18.)

---

## Common pitfalls + gotchas (learned this session)

1. **PG port-forward dies on Windows kubectl idle.** Symptom: relay metrics + SSE stop responding mid-run. Fix: `taskkill //IM kubectl.exe //F` + restart `kubectl --context civit-datapacket -n bitdex port-forward pod/bitdex-0 4099:3000`. Keep retry handy.

2. **Don't use `pkill -f node`.** Hook blocks it (would kill VS Code). Use `kill <pid>` or `taskkill //IM bitdex-server.exe //F`. Track PIDs by `$!` or `tasklist` filter.

3. **Worktree discipline matters.** Multiple agents share the bitdex-v2 checkout. Use `git worktree add <path> <branch>` for isolation. I clobbered Jack's Dockerfile once; cost 10 min of coordination. Pattern: `git worktree add ../bitdex-<name>-<topic> <branch>`.

4. **`cargo test --lib` on this repo has pre-existing rot.** Tests in `src/filter.rs`, `src/ops_processor.rs`, `src/ingester.rs`, `src/unified_cache.rs` reference removed methods (`get_field_mut`, `fields_mut`). Compile fails with 11–18 errors regardless of branch. Don't try to add lib-tests; they won't run. Use runtime evidence instead. Filed as latent task; not session-blocking.

5. **`cargo test --bin` for the actual server binary works fine.** That's what CI uses.

6. **Re-register prefilter after every server restart.** `civitai_safe_full8` doesn't survive restart. Without it, the safety prefix gets evaluated as raw clauses every query → cardinality ~102 M dominates the executor. Symptom: P99 climbs into seconds.

7. **`enable_traces: true`** must be PATCH'd in after boot — also doesn't survive restart. Required for `/api/indexes/{name}/traces` ring buffer + `[ops-trace]` log lines.

8. **Auto-prefilter promotion fills the registry to cap=32 fast** under diverse traffic. PR #226 skips 0-cardinality entries; without it, registry pollution at ~150 entries / minute. Don't disable that PR.

9. **`docs/_in/justin-mission-2026-04-25.md`** has Scarlet's mission framing. Read it for context on "why we're doing this" and the team-coordination rules (`team-lead` skill).

10. **Justin's permissions broadcast (in flight this session):** sub-agent spawning OK, OpenRouter spend on GPT/Gemini reviews unrestricted, shadow + tee toggles unrestricted, infra changes OK if no impact to other workloads. Don't round-trip through Scarlet for those.

11. **Hub-channel agents online during this session:** Scarlet (team lead, merge authority), Jack (relay implementation), Aidan (deploy + flipt), Tom (Talos infra). Justin offline most of session per design.

---

## What the next agent likely picks up

If Justin lands on **A1** (ship as-is): write a release note, no engineering. Mission complete. Tag a release that flip-backs from relay mode and wait for prod confidence.

If **A2** (fix-first): start PR-A from `docs/_in/per-value-lazy-indexed-lookup-design.md`. The doc has the full spec. Branch off main as `perf/indexed-value-lookup-read-side`. Implement read path first (backwards compat — no shard format change yet, just the BIDX side-car if it exists). 250-400 LOC.

If **A3** (hybrid): same as A2 but no rush; ships in V2 batch.

For all three: keep the local rig hot. The replay-prod-via-relay consumer + corpus replay against local is the validation harness for the next fix. Same pattern that proved out PR #222 + #227.

---

## Session metadata

- Duration: ~25 hours real-time across multiple compaction-survived sessions
- PRs shipped: 8 (#221, #222, #223, #224, #226, #227, #228) + tag #229 (Jack) + #230, #231, #232 (Jack)
- Tags pushed: v1.0.165 through v1.0.175 jemalloc
- Docs committed: 4 (regression-resolution, mixed-load, lazy-load-localization, indexed-lookup-design)
- Correctness fixes: 4 (WAL rotation, merge-thread brace, warm-persist serde, axum router merge)
- Perf fixes: 4 (merge_dirty side-set, snapshot parallel, bitmap_bytes chunked, 0-card prefilter skip)
- Big findings: 1 mission-gate root cause (load_field_values full-bucket-read)
- Misdiagnoses caught + corrected: 1 (sort-layer fusion was wrong direction; lazy-load is the real surface)

---

*Filed 2026-04-25 by Donovan. Standing by for Justin's A1/A2/A3 call. Worktree clean for resumption.*
