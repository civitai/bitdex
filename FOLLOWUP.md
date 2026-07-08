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

**REMAINING OPEN — true bitmap-miss residual (~2.5% of publishes in the sample):** posts
29660027 (16 imgs) + 29660973 (1 img) had isPublished bitmap=0 with a captured fan-out row —
a real engine-side apply miss, mechanism unknown. Re-arm the stakeout trap (tables retained:
bitdex_fanout_capture / bitdex_post_publish_capture / bitdex_zm_log; script /tmp/trap.sql in
pod bitdex-psql) and hunt with pod logs around the captured rows' created_at. The overhaul's
durable fan-out intents cover this class structurally.

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
