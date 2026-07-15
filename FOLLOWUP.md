# FOLLOWUP — non-urgent issues & idle-time work

## CI never compiles `tests/` — 178 integration tests are invisible, and one is already broken (2026-07-15)

`.github/workflows/test.yml` runs ONLY:
```
cargo test --lib
cargo test --features pg-sync --lib
cargo check --features server                       # not --tests
cargo check --features server,pg-sync,heap-prof --bin bitdex-server
```
Nothing compiles the `tests/` directory. **29 files / 178 `#[test]` fns never build or run in CI.**

**Already-rotted proof:** `tests/bulk_load_fixture_test.rs:24` does `use bitdex_v2::pg_sync::single_pass;` — `pg_sync` exports no `single_pass`. The file cannot compile. CI is green anyway, and has been. Found incidentally while reviewing an unrelated change; nobody would otherwise notice.

**Why this matters more than one dead file:** the tests exist, look maintained, and read as coverage. Anyone adding an integration test to `tests/` reasonably assumes CI runs it. It doesn't. That's coverage theatre — worse than no tests, because it's trusted.

**Related, same root:** the `server`-feature lib tests also never run (no `--features server --lib` line), so ~60 `src/metrics.rs` tests are equally invisible.

**Fix:** add `cargo test --features server,pg-sync` (no `--lib`, so integration tests build+run) to the gate. Expect it RED initially — at minimum `bulk_load_fixture_test.rs` won't compile, `test_min_tracked_value_after_expansion` DEADLOCKS (must be `#[ignore]`d), and `query_stream_full_channel_drops_oldest` is a known Windows-timing flake. Triage each: fix, `#[ignore]`, or delete. Deleting a rotted test is honest; leaving it to look like coverage is not.

## /debug/memory mixes STABLE and NOISY fields with no distinction — it manufactures trends (2026-07-15)

Two engineers independently drew false conclusions from this endpoint within ten minutes of each
other, in opposite directions, by comparing single samples of fields that happen to be noisy. The
endpoint presents stable counters and volatile derived values side by side with nothing marking
which is which. **Measured spreads (8-10 samples, ~4s apart, same pod, steady state):**

| field | spread | safe to compare across time? |
|---|---|---|
| `untracked_live_gap` (= allocated − tracked_total) | **0.35 GB** | YES — stablest; both terms stable |
| `tracked_total` | 0.70 GB | YES |
| `allocated` | 0.74-0.94 GB | YES-ish — **median-of-N**, it spikes (observed 21.5 median vs 23.5 single outlier) |
| `rss_bytes` | 1.70 GB | NO without median-of-N |
| `untracked` (= rss − tracked_total) | **1.95 GB** | **NO** — noisiest; inherits rss's swing |
| `fragmentation` (= resident − active) | **2.5 GB** (2.3→4.8 in 30s) | **NO** — see below |

Root cause: jemalloc's background decay purges dirty pages, so `resident` oscillates hard; every
field DERIVED from resident (`rss_bytes`, `untracked`, `fragmentation`) inherits and amplifies that
swing. Fields derived from `allocated` (`untracked_live_gap`) are stable because both terms are.

**Guidance:** to measure the tracker's blind spot use **`untracked_live_gap`** (live heap the
serialized-size tracker misses) — NOT `untracked`, which is rss-minus-accounting and sweeps in
fragmentation + metadata + mapped overhead (none of it live heap, all of it noisy). For any
comparison across time, median-of-N. Never threshold a derived difference without first sampling its
variance.

**Suggested fix (#317 follow-up):** annotate the endpoint itself — mark each field stable vs
volatile (or emit `_median`/`_p50` variants), so the next reader can't make this mistake by reading
it the obvious way. The endpoint currently invites the error.

## Verifier orphan detection is under-sensitive — needs a later pass, not a longer barrier (2026-07-15)

CORRECTED 2026-07-15 (was titled "only ~50% sensitive"). The original quoted a barrier-timeout rate
of "~50% / ~1-in-2". **That number was n=2 and is RETRACTED** — as n grew it fell (1/2 → 2/5 → 2/6 →
2/7) toward roughly 25-30%, and no n worth quoting exists yet. The rate is deliberately absent below:
the argument never needed it, and quoting one is how it spread. A barrier that misses even 1-in-4
publishes is a defect; the exact rate changes nothing about the fix.

`Inconclusive` (v1.1.48) makes `bitdex_activation_verify_redriven_total` SOUND — a slot is counted
only when the publish barrier COMPLETED and the slot was still absent, so no phantoms. It is not
fully SENSITIVE: the verifier's barrier (`publish_barrier_ms`) times out at a material rate, and a
REAL drop during a slow promote would land in `activation_verify_inconclusive_total`, NOT
`redriven_total`. **An alarm on `redriven_total` alone can stay silent on a genuine drop.** No data
is at risk — `Inconclusive` still re-drives, so repair happens either way. What is missing is
DETECTION, not repair.

WHY NOT JUST RAISE `publish_barrier_ms` — the economics, not the size, are why it fails: **every
extra ms is paid as WAL-reader stall**, so you can never buy enough. The promote is an unbounded CoW
clone-cascade with no fixed ceiling, so no cap drives `barrier_ok == false` to zero; each increment
buys a diminishing slice of the tail for real stall. **A deferred re-check inverts this: waiting
costs NOTHING** — the slot sits in a ring while the WAL reader keeps working. Same "wait longer"
lever, opposite economics. That, not the wait itself, is what escapes the asymptote.

⇒ STRUCTURAL FIX (v1.1.49): re-check on a LATER PASS instead of waiting longer — off the WAL reader.
**This is a SWAP, not a deletion, and the scheduler IS the work:** the ring (`activation_verify`) is
a bare `VecDeque<u32>` with **no time dimension**, drained every WAL batch, so a naive "just requeue"
yields a delay of ~0 — a hot re-check loop that reads pre-publish state exactly like the barrier
does, and re-drives false drops. It needs `ready_at_ms` + `passes` per entry and a `drain_ready`.
Design: N=10s re-check delay, k=2 passes. **N is grounded in the worst lag ever OBSERVED (897ms,
583ms) ⇒ ~11×; NOT in the `[flush-slow] promote=` field, which is a broken instrument (it doesn't
sum — `total=142ms` with `promote=184.9ms`) — see the entry below.** The decider is the asymmetry:
N too short = a FALSE REAL-DROP that corrupts `redriven_total`, the very metric this protects; N too
long = slow repair, harmless behind the v1.1.43 backstop. Err high. k=2 is SAFETY margin (a lag
between N and 2N gets a second chance rather than becoming a false drop), NOT measurement.

**Do NOT size this work from `inconclusive_total`'s rate** (the original entry said to — it can't):
it fires only on `barrier_ok=false AND reread_present=false`, and `reread_present=false` has occurred
ZERO times, with zero confirmed drops ever. `inconclusive_total = 0` cannot distinguish "the barrier
hid nothing" from "there was nothing to hide" — an ambiguous quiet counter, unresolvable by waiting,
because the resolving event has never occurred. It is also strictly dominated: a T+10s re-check sees
everything the 500ms barrier could, with ~20× the margin and no stall. **v1.1.49 DELETES
`Inconclusive` and `inconclusive_total`** — the barrier's removal deletes `barrier_ok`, and with it
the ambiguity `Inconclusive` existed to hold. Its protection carries forward as k=2; its uncertainty
("was N long enough?") moves from a per-slot verdict to a `verify_passes_to_present` histogram, which
is where a property of the TUNING belongs. Nothing it did is lost.

## Query freshness guarantee is illusory — the 100ms ForcePublish barrier times out 93-98% (2026-07-15)

MEASURED in prod (v1.1.46, both pods): `ensure_fields_loaded`'s ForcePublish barrier is capped at
**100ms** and times out on **93-98% of promotes** — **13/14 and 112/114 timeouts COUNTED DIRECTLY**,
which is what this finding rests on. A query that triggers a per-value lazy-load during a promote
therefore reads PRE-publish state. The barrier exists to make a query see prior writes; it
essentially never succeeds, so that guarantee is illusory today.

⚠️ The promote figures below (**158ms / 208ms**) come from `[flush-slow] promote=`, a **BROKEN
instrument — its fields do not sum** (`total=142ms` with `promote=184.9ms`); see the measurement
caveat in the PERF entry below. They are quoted here only as a plausible ORDER-OF-MAGNITUDE reason
the 100ms cap is exceeded. **Do not build a decision on them** — the 93-98% rate above is counted
directly and needs no such support. (This caveat is repeated here rather than left to the entry
below because a number and its caveat must travel together; the split is exactly how the figure got
laundered into an N=10s justification it could not support.)

REAL-WORLD COST IS SMALL — deliberately NOT fixed: the effect is sub-second staleness on a
just-published post for a query that happens to lazy-load mid-promote; it self-resolves on the next
read. It is a freshness lag, NOT data loss. This is what made the post-activation verifier raise
FALSE orphans (fixed separately, verifier-side — see below).

WHY NOT JUST RAISE THE CAP: the barrier sits on the USER query path, so raising it trades user query
latency for freshness — a bad deal for a sub-second lag. And the root cause isn't a tuning knob:
the 158-208ms promote is ~30 dirty layers × ~12-14ms **clone** each (e.g. L13 base=59.4M
clone=14121μs) = the Arc CoW clone-cascade at 59M-element sort layers. Making 100ms sufficient means
attacking that clone cost, i.e. `docs/design/write-pipeline-overhaul.md` territory.

⇒ Logged as a KNOWN LIMITATION, deferred to the write-pipeline overhaul. Revisit if the freshness
gap ever produces a user-visible complaint, or fold it into the overhaul's success criteria.

## PERF: sort promote dominates the flush cycle (clone-dominated) (2026-07-15)

`sort-promote` (merge_dirty on staging.sorts) dominates every other flush phase by ~20-40×:
observed `[flush-slow]` breakdowns show apply/cache/compact/tb/publish all ~0-10ms while promote
is 100-185ms, across ~30 dirty layers, driven by per-layer CoW clones of 59M-element bases
(~12-14ms each, e.g. L13 base=59.4M clone=14121μs). Same rate both pods (11.4-11.7 promotes/min,
dirty_layers median 30). It's the underlying cost several other things (the query barrier above,
the verifier's false orphans) are downstream of. Not urgent: no correctness impact, flush keeps up
(lag=0). The write-pipeline overhaul's snapshot-only design is the structural answer; a cheaper
interim would be reducing dirty-layer count or avoiding full-base clones per merge.

⚠️ MEASUREMENT CAVEAT — do NOT quote promote timings as precise per-cycle costs. `[flush-slow]`'s
fields DO NOT SUM: an observed line reads `total=142ms ... promote=184.9ms` — a component exceeding
its own total. So `promote` is measured over some overlapping/async/cumulative span, not a clean
per-cycle attribution. The DIRECTION (promote dominates; clones are the cost) is well supported;
the exact "158-208ms per cycle" figure is not. Note that the barrier finding above does NOT rest on
this field — it's observed directly by the v1.1.46 diagnostic (a 500ms barrier succeeds while the
verifier's 100ms one fails on the same slot in the same window). Fix the accounting if you ever need
promote numbers to be trustworthy.

## OBSERVABILITY GAP: the v1.1.44 persist-ring cost is unmeasurable (2026-07-15)

The v1.1.44 activation-verify ring persists to `meta/activation_verify.bin` on the flush thread each
activation cycle. Review flagged a cost caveat (full-ring snapshot+serialize under lock; bounded by
ring_cap 262144, normally tiny since it drains every WAL batch — only a starved WAL reader makes it
large). That caveat CANNOT currently be watched: there is no log line (grep for persist/ring/
serialize/activation_verify.bin = 0 hits), `[flush-slow]`'s breakdown has no persist field (its
vocabulary is total/apply/promote/cache/compact/tb/publish/post_apply_total), and no metric exists
(only activation_verify_checked_total / redriven_total / publish_lag_total). So it's a declared BLIND
SPOT, not a monitored risk — if the cost ever mattered it would surface only as unattributed
inflation in `[flush-slow] total`. Interim proxy: alarm on `[flush-slow] total` >400ms (observed band
is p50 118ms / p90 142ms / max 151ms). Fix = add a persist-duration field to the flush-slow breakdown
or a dedicated metric.

## Residual activation-orphan — whole-batch op loss (root cause NARROWED, fix pending) — 2026-07-15

~0.3% of scheduled-post activations (both pods) land as orphans: activate_due sets the
ALIVE bit, but ~150ms later the whole batch's filter+sort ops (postId, isPublished,
publishedAt) are absent from the published snapshot. **The v1.1.43 verify-at-activation
backstop catches + re-drives every one — ZERO data loss.** Non-urgent.

Root cause converged (full detail: memory `project_activation_orphan_residual_2026_07_15.md`;
branch `hunt/whole-batch-op-loss` off v1.1.45): NOT the main-loop flush (that publishes
alive+ops together, verified) and NOT save-and-unload (prod logs: is_loading=false, no
unload lines). It's a NON-main-loop publish — a ForcePublish command-handler
(`concurrent_engine.rs` 3032/3050/3239/3272/3387/3457/3466) or the force_publish_blocking
5s-timeout aftermath — storing a `staging.clone()` inconsistent with the just-applied
batch (missing filter+sort diffs, keeping alive). PRIME suspect: the verifier's own
`ensure_fields_loaded` (postId eq P) triggers a ForcePublish that republishes a staging
that lost the batch. Conclusive specimen: bitdex-1 04:01:00, slots 136717883+136717882
(postId 29781934), has_membership=true@activation but all fields absent@verify, shadow
diff_sets=0/clears=3 base 109.8M loaded.

POD ASYMMETRY (decisive, sky-verified): redriven bitdex-1=4 / bitdex-0=0 (comparable
exposure). ALL orphans on the STANDBY; ZERO on the active pod. HAProxy active-passive →
bitdex-0 serves all users → **ZERO prod-user impact**. Correlates the clobber with a
COLD→FRESH VB disk load (only on the traffic-starved standby); the warm active pod keeps
VBs resident so activation sets land hot and survive. Next trace must exercise the
cold-VB + concurrent-activation + query-triggered-load + ForcePublish combination
(cross-thread), NOT a sequential same-engine load (which hunter's repro did — it passed).

NEXT UNIT: trace the ForcePublish + force_publish-timeout path (what happens to in-flight
coalescer ops + staging on timeout; can ForcePublish publish a batch-losing clone) →
deterministic repro (activation batch concurrent with ForcePublish → assert ops survive)
→ fix (serialize ForcePublish vs flush apply, or drain pending before clone) → Opus
review → v1.1.46 (image-only). Also watch: `deferred-reach` (#313) scanning 90,648
deferred slots (>10k warn) on bitdex-1 — backlog growing, investigate if it keeps climbing.

## Desc tie-band pagination — FIXED in PR #304 (2026-07-09 night)

Core sort bug, present since the bifurcation engine: descending keyset pagination dropped
the remainder of every tied-value band straddling a page boundary (bifurcate tail-take
ascending vs (value desc, slot desc) contract used by order_results + cursor resume).
Masqueraded for months as: top-200 feed misses, "time-bucket membership churn", and — per
review — a slice of historical shadow-comparator divergence and dup/gap reports, because
the CACHE-HIT paths (sorted-keys/simple-sort) already used the correct contract, so
cache-hit and cache-miss pages returned DIFFERENT slots for the same query+cursor on tied
bands. Deploy note: shadow-vs-Meili divergence will SHIFT on the release (tie order now
consistent, likely closer to Meili recency tiebreak) — expected, not a regression.
Follow-up (non-blocking): engine-level E2E sweep alternating cache-hit/cache-miss pages to
pin cross-path tie consistency.

## Ops-poller cursor skip on out-of-order commits — FIX IN PR (this branch, 2026-07-09)

**Root cause of the residual ~1% publish-weighted loss** (specimen: post 29674681's publish
fan-out; also explains bitdex-0/bitdex-1 divergence — per-pod cursors skip independently).
BIGSERIAL ids are allocated at INSERT but visible at COMMIT; the poller read
`WHERE id > cursor` and advanced the cursor to the max VISIBLE id, silently passing rows
from still-open transactions (publishing = the longest transactions in the product). The
bitdex_cursors cleanup trigger then deleted the skipped row — evidence destroyed.
Deterministic repro: `tests/integration/skip-race-repro.sh` (fails on old semantics) /
`skip-race-fixed.sh` (passes with the fix).

**Fix** (`fix/ops-poller-skip-race`, reworked after adversarial review REJECT — 3 blockers
in the first cut, all closed): gap-aware frontier walk — the durable cursor never passes an
allocated-but-invisible id. Rows beyond a gap still POST immediately, tracked as a per-id
`posted_ids` SET (a high-water mark silently drops the gap row when it fills by commit —
review finding 1). An id leaves the missing set ONLY by turning visible (commit → POSTs) or
by the ATOMIC finished-and-still-invisible check (`gap_status`, one statement = one
snapshot; separate checks reintroduce the race — finding 2). There is NO time-based skip: a
held gap alerts every 60s but never skips (a slower-than-timeout txn is exactly the row
being protected — finding 3). Multi-id holes resolve as one set under one snapshot
(finding 5); cursor==0 boots seed to MIN(id)-1 against trimmed tables; >100k holes flag
oversized and hold.

**Merge gate:** `tests/integration/run-skip-race-rust.sh` — REAL `poll_and_process`
against live PG 16 + mock engine: commit-fill delivers the gap row, rollback proven dead
without POSTing, trimmed-table boot seed. The SQL scripts are semantics demos only.

**Residual risk accepted:** during a gap hold, rows beyond the gap apply to the engine
before the gap row (absolute ops converge; the reorder window is unbounded if the writer
txn is stuck, not ≤60s). A genuinely stuck writer txn holds the cursor indefinitely —
delivery of already-visible rows continues ONLY within the first `batch_limit` (5000) ids
above the held cursor; once the backlog exceeds that, NEW ops stop flowing until the gap
resolves (minutes into a long hold at prod volume). **Runbook:** remedy for a wedged
writer is `pg_terminate_backend(<pid>)` on the PG side — NEVER a manual cursor bump,
which deliberately reproduces the original data loss. For an oversized hole (>100k ids,
e.g. a rolled-back bulk txn): verify it is rollback/trim, then manually advance the
cursor past it. Alert-only observability via "ALERT — cursor held" log lines (sidecar has
no metrics endpoint). Boot at cursor==0 seeds to MIN(id)-1; if txns are in flight at seed
time a boot guard pins the durable cursor at 0 until they finish, then sweeps late
commits below the seed (re-review N1). If a replica dies PERMANENTLY while pinned, its
0-value bitdex_cursors row freezes cleanup table-wide — unexplained BitdexOps growth +
a 0-valued cursor row from a dead replica means: delete that stale row (pre-existing
stale-cursor-row class; live-replica guard windows are bounded by the longest boot-time
txn). Requires PG >= 13 (`pg_current_snapshot`); prod
CNPG is 16. Merge-gate script is NOT in CI (needs docker) — run it manually before
touching ops_poller.rs; listed in the release playbook.
Detection tooling: lossless trap trigger `bitdex_trap_lossless` on BitdexOps (prod,
2026-07-09) captures every queryOpSet emission inside the inserting txn — skip rate is now
measurable (captured fan-outs >5min old vs engine applied-state).

## LCS dictionary durability hole — FIX IN PR (this branch, 2026-07-08)

**Status: interim fix implemented** (`fix/dict-durability`): atomic tmp+fsync+rename in
`save_dictionary`; dirty dictionaries persisted on the WAL-reader/ops path after every
applied batch; AND the persist-path bug fixed — `persist_dirty_dictionaries`/`save_snapshot`
wrote to `<bitmap_path>/shardstore/dictionaries`, a directory boot's `load_dictionaries`
NEVER reads (it reads `<bitmap_path>/dictionaries`, which only the dump path wrote). Every
steady-state dictionary persist ever made was dead I/O. Loader now also probes the legacy
stray dir as a fallback (higher-max-key wins) so pre-fix strays aren't abandoned.

Original finding (simplify-review G1, 2026-07-08): ops path mints keys (`get_or_insert`,
ops_processor.rs) but only HTTP-upsert/dump/save paths persisted; bare `fs::write`; crash →
boot reloads stale dict → `next_key = max+1` re-issues an on-disk-referenced key to a
different string → silent permanent value aliasing in filter bitmaps and served docs.
**Structural fix** remains the overhaul's domain #9 (DictAssign log records + checkpoint
dict snapshots — write-pipeline-overhaul.md REV 4).

## P1 — WAL retention gated on the wrong cursor — FIXED in #293 (2026-07-08)

`ops_wal.rs delete_consumed()` deleted a WAL gen on the READER's in-memory gen hop while the
DURABLE cursor (merge-thread persisted, up to a cycle later + #291 gate holds) could still
point into the deleted gen; boot then silently skipped to the next gen at offset 0 —
unreplayable gap. Fixed: reading never deletes; the WAL-reader loop deletes gens strictly
below the durably-persisted cursor's generation. Regression test simulates the crash window.

## Fan-out silent no-op — MECHANISM PINNED (stakeout 2026-07-08): DocCache evicts wrong slots

**Verdict (catch-in-the-act stakeout, specimen post 29660803):** NOT trigger-no-emit, NOT
zero-match, NOT lazy-shadow (refuted by PR #295's test suite). The Post trigger emits, the
fan-out applies to bitmaps correctly and writes the docstore on disk — but the WAL-reader
loop evicts the DocCache by `entry.entity_id` (the Post), never the MATCHED image slots, so
`GET /documents/{image}` serves the stale pre-fan-out doc (publishedAt=0/isPublished=false)
for ~20+ min until LRU eviction. Search/bitmaps unaffected. Stakeout sizing (n=80 recently
published posts): 25/80 doc-cache staleness (served-doc only), 2/80 TRUE bitmap miss
(separate residual, below). **FIX: this branch** — `apply_query_op_set` flushes its doc
writes and evicts the matched slot set itself (regression test proves it bites).

**Zero-match counter (#292) = red herring for this class:** ~46% of postId fan-outs
legitimately match 0 (draft-post INSERT fan-out firing before any image exists). Publish
fan-outs don't zero-match. Keep the counter, read it with that baseline.

**Bitmap-miss residual — ROOT-CAUSED + FIXED (race-hunt, 2026-07-08, fix branch
fix/fanout-deferred-activation):** a publish fan-out whose publishedAt is even 1s in the
future of the pod clock at apply time (PG now() vs pipeline latency vs pod clock) takes the
DEFERRED branch (doc writes only, slots into the deferred map). activate_due then replays
diff_document(None, doc) — which had NO exists_boolean derivation, and the deferred branch
deliberately keeps isPublished out of the doc (plus the insert-time doc carries a stale
explicit isPublished=false) — so activation restored publishedAt sort layers but NEVER
flipped the shadow: the surviving core of audit Mode A, post-#291. Deterministic in-process
repro (tests/fanout_deferred_activation.rs) fails pre-fix, passes post-fix. FIX: shadow
targets are derive-only in diff_document (authoritative derivation from sibling-field
presence, full-overwrite pair); activation also writes derived shadows into the doc +
evicts DocCache (flush-then-evict, #296 contract).

**SECOND bug found same hunt: durable-watermark under-count wedged WAL-cursor persistence.**
Deferred-map mutations drained by the fan-out barrier's ForcePublish handler
(concurrent_engine coalescer.flush path) bypassed deferred_applied_total — prod showed
seq 1951 vs durable 1863, gap growing 14h, merge thread holding cursor persist the whole
time (crash in that state = large WAL replay, not loss). Fixed: the handler bumps the
watermark from the drained batch's deferred count. (Barrier-drained ops still skip the
OPSLOG — pre-existing crash-durability gap, overhaul territory, documented there.)

## Sync-config canonical source (from 2026-07-08 nuke op)

Prod triggers regenerate from the talos ConfigMap (v2-sync-config.yaml), which drifted from
`deploy/configs/sync-config-civitai.yaml` (metrics phase: repo=placeholder, prod=real
entityMetricDailyAgg_v2 query). Pick ONE canonical source; make the other generated or
CI-validated via a `trigger_gen::trigger_name` hash-equality gate against the live ConfigMap.

## Observability gaps (carried from #289/#291)

- CORRECTED 2026-07-08 (Sky): `enabled_metrics`/`disabled_metrics` gate only the 3 expensive
  scrape-time groups (bitmap_memory, eviction_stats, boundstore_disk); all counters emit
  unconditionally, and the PodMonitor now scrapes /metrics — so `zero_match_total` (#292) and
  `time_bucket_*` are live as of the v1.1.29 roll with NO manifest change.
- `bitdex_fanout_barrier_skips_total` NEVER EXISTED — aspirational comment only
  (ops_processor.rs:872). Barrier-timeout skips today surface as errors, not a counter;
  decide post-overhaul whether to add it (fan-out intents make skips retryable anyway).
- Post-overhaul drift metric (doc-sampling sweep hit counter) MUST be allowlisted from day
  one (review finding F6c).

## backfill.rs legacy BitmapFs writes (review W1)

`src/pg_sync/backfill.rs` still writes collectionIds bitmaps via legacy BitmapFs while live —
outside ShardStore AND outside the proposed apply-log (durability domain #8). Fold into the
overhaul's log, kill the path, or explicitly exclude with rationale.

## Mem-limit 40Gi → 48Gi (load-peak OOM elimination)

Every fresh dump+load takes exactly one self-healing OOMKill (exit 137) at load peak under
the 40Gi limit (both pods reproduced during the 2026-07-08 v1.1.28 full nuke, ~15-30s
recovery via skip-dump restart). Bump the bitdex container limit to 48Gi in talos-infra to
make fresh loads restart-free. Owner: ava/infra; non-urgent.

## Flux ImagePolicy footgun (from 2026-07-08 incident, ava)

The bitdex ImagePolicy range is a wide `>=1.0.0`. Currently INERT — bitdex's deployment.yaml
has no `$imagepolicy` setter markers (manual-bump only), so the ImageUpdateAutomation cannot
rewrite the image lines. But if anyone ever adds a setter marker, the wide range would
happily auto-pin known-bad tags (e.g. the dead 1.1.31/1.1.32 boot-hang builds). Before any
marker is added: floor/pin the policy range or add known-bad exclusions. Also from the same
incident, the durable emergency sequence: SUSPEND the Flux Kustomization FIRST, then
kubectl set image, then bounce pods, then git revert PR, resume Flux only when cleared —
set-image-then-bounce without the suspend recreates deleted pods on Flux's (bad) spec.

## Boot restore gated on persisted alive bitmap — deferred-only edge (hang-hunt, 2026-07-08)

Boot's slot-state restore only runs when a persisted alive bitmap exists; an engine whose
only slots are deferred (alive bitmap absent/empty on disk) never loads the deferred map
into staging on reopen — activation silently never runs for those entries. Edge case (real
deployments always have alive slots) but a latent trap for tests and tiny indexes; found
while building the boot-hang repro (PR #300 works around it). Fix candidate: load the
deferred map unconditionally, not inside the alive-bitmap branch (concurrent_engine boot
restore path).

## Fan-out total no-op — ROOT-CAUSED + FIXED (2026-07-09, fix/fanout-dedup-collapse)

The last surviving fan-out loss class (~3% of publishes; ALL scheduled posts — minute-
boundary publishedAt): `op_dedup.rs` deduped queryOpSets by (entity, query string) with
wholesale LAST-WINS. Every Post fan-out shares the identical query ("postId eq X"), so when
one user action produced two Post updates inside a single poller/WAL batch (schedule sets
publishedAt=Tf; a second update carries only availability — unchanged publishedAt emits
nothing under IS DISTINCT FROM), the LATER fan-out silently discarded the publish Set.
Nothing deferred → and PG emits NOTHING at go-live for scheduled posts (confirmed live:
post 29666669's Tf passed with zero BitdexOps rows) → permanently invisible.

Evidence chain: pre-go-live predictor (pending scheduled posts whose image docs lack
doc.publishedAt=Tf) found 29666669 40 minutes before its Tf and correctly prophesied the
loss; unit + E2E repros fail on the old dedup and pass with the fix (nested ops now MERGE
per query string and dedup per-FIELD, preserving last-wins per field). Both dedup layers
(ops_poller pre-send + WAL reader pre-apply) share this helper — one fix covers both.

Backfill after deploy: stuck scheduled posts enumerable via bitmap-vs-PG (isPublished
false + PG published, publishedAt % 60 == 0 dominant); pre-go-live pending posts self-heal
if rescheduled, else need the same re-emit.

## Dictionary save tmp-file race (found by #308 round-2 external review, 2026-07-13)

`save_dictionary` (src/dictionary.rs, shipped in #294) writes to a fixed
`<name>.dict.tmp` before atomic rename. Two concurrent savers — WAL reader's
`persist_dirty_dictionaries()` per key-minting batch vs an HTTP
`/api/indexes/:name/snapshot` calling `save_snapshot()` — can truncate/interleave the
same tmp file and rename corrupted JSON over the live dict → next boot fails to parse.
Low likelihood (needs same-instant saves of the same dict) but boot-fatal. Fix: unique
tmp name per invocation (NamedTempFile) or a per-dict save mutex. Out of #308 scope.

## Pre-fix legacy sort-op tails remain shadowed on upgrade (#308 round-2 review finding)

#308 stops NEW dead writes but does not migrate pre-fix legacy per-layer ops tails that
were appended while a valid packed shard existed — those stay invisible to boot forever.
DELIBERATE: handled operationally for the one affected deployment (pre-roll POST
/snapshot persists correct in-memory state; wave-2 heal covers the ~29k historical
victims). Code-level boot reconciliation was rejected as riskier — legacy tail ops'
ordering relative to the packed snapshot lineage is unknowable, and replaying stale
legacy ops over a newer packed snapshot could corrupt. If another deployment ever
upgrades across this boundary, it must do the same snapshot-before-roll step.

## 12h post-nuke sweep: 45 stuck publishes healed; sweep page-cap confirmed biting (2026-07-14)

B1-style probe over ALL 12.5k publish-capture posts since the nuke found 45 deficient on
bitdex-0 (0.39%): dominant class = reschedule/publish ops dropped in the OLD binary's
boot-replay window (pre-#310; ages clustered 14-17h; specimen 29762566 = originally
scheduled +24h, user moved it earlier, reschedule op dropped → stuck deferred with stale
date; bitdex-1 correct). Healed via wave-2 nudge (−1/true two-batch, 254+251 ops), both
pods verified 0/44 after. NOTE: the overdue-deferred sweep did NOT reach these in 15h —
consistent with the known sweep page-cap issue (same 200-candidate head each cycle);
bump its priority. Cosmetic: nudged docs can retain publishedAt−1 while bitmaps/sortAt
are exact (doc-cache race on batch-2 write); self-corrects on next organic op.

## query_stream_full_channel_drops_oldest fails deterministically on Windows (2026-07-14)

`server::tests::query_stream_full_channel_drops_oldest` panics at "first event: Lagged(1)"
(server.rs:7665) on clean origin/main (87247d2), verified in a fresh worktree — pre-existing,
not from the sweep page-cap branch. Repeated locally, deterministic on this Windows box.
Likely broadcast-channel timing (drop-oldest semantics racing the subscriber). If Linux CI
is green, it's a Windows-only timing hole in the test; fix the test, not rerun-once it.

UPDATE 2026-07-14: sweep page-cap FIXED — the sweep now pages through the candidate
space via keyset cursor (max_page_size per query, up to sweep_limit checked per cycle)
and carries a rotation cursor across cycles; full coverage bounded at
ceil(population / sweep_limit) cycles, "full candidate pass complete" log marks wraps.
Regression tests: test_overdue_deferred_sweep_paginates_past_page_cap,
test_overdue_deferred_sweep_cursor_rotates_across_cycles.
