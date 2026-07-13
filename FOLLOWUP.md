# FOLLOWUP — non-urgent issues & idle-time work

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

## Flipt skill API path broken — server moved to v2 environments (2026-07-13)

flipt.civitai.com now requires environment scoping: `/api/v1/*` returns
`failed to get environment "" from context` unless the request carries
`X-Flipt-Environment: civitai-app` (or uses `/api/v2/environments/civitai-app/...`).
The skill's `get`/`list`/API-verify in `shadow on|off` all 500. GitOps path (flipt-state
push) still works; `shadow off` exits 6 after pushing successfully. Fix: add the header to
apiRequest() in flipt.mjs. Also: local flipt-state clone was 74 commits stale with an
uncommitted duplicate of a remote change — sync + verify direction before patching.
