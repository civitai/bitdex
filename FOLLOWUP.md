# FOLLOWUP — non-urgent issues & idle-time work

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

## Fan-out silent no-op on fresh pods — LAZY-SHADOW HYPOTHESIS (P1-adjacent, 2026-07-08)

Live specimen 136063341: image inserted on draft post 02:42:41, post 29651893 published
02:42:51, Post→Image fan-out never applied (pub=0/isPub=false) on BOTH pods. Ruled OUT by
logs: barrier-timeout skip, fan-out cap reject. Residual: trigger-no-emit vs silent 0-match;
BitdexOps cleanup ate the row, so the split needs a catch-in-the-act repro.

**Prime suspect: lazy-shadow.** The post was created AFTER both dumps, so its postId value
bitmap existed only as an in-memory sync diff; if the fan-out's queryOpSet lazy-loads the
NONEXISTENT disk shard and the empty result shadows the diff, the query deterministically
matches 0 on every fresh pod — same family as the known collectionIds shadowing gotcha.
Identical behavior at 17min vs 87min post-dump fits deterministic-shadow, not a timing race.

**Repro / split:** tail BitdexOps during a draft→publish, capture the queryOpSet row before
cleanup, inspect engine-side match count: no row = trigger-no-emit; row + 0 match on a value
PG says has rows = shadow (check whether the value's disk shard exists).

**Interim mitigations:** (a) fix per-value lazy load to MERGE, not shadow, in-memory diffs
when the disk shard is missing (collectionIds fix pattern); (b) zero-match fan-out counter —
SHIPPED (#292), needs prod enabled_metrics allowlisting at next deploy; (c) optionally warm
postId value shards at dump completion before `.ready`.

**Structural fix:** overhaul's durable fan-out intents + forced value-shard load
(write-pipeline-overhaul.md §2 fan-out axis).

## Sync-config canonical source (from 2026-07-08 nuke op)

Prod triggers regenerate from the talos ConfigMap (v2-sync-config.yaml), which drifted from
`deploy/configs/sync-config-civitai.yaml` (metrics phase: repo=placeholder, prod=real
entityMetricDailyAgg_v2 query). Pick ONE canonical source; make the other generated or
CI-validated via a `trigger_gen::trigger_name` hash-equality gate against the live ConfigMap.

## Observability gaps (carried from #289/#291)

- `bitdex_fanout_barrier_skips_total`, `bitdex_query_op_set_zero_match_total` (#292), and
  `time_bucket_*` not in prod `enabled_metrics` allowlist.
- Prometheus does not scrape bitdex directly (audit-proxy measurements only).
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
