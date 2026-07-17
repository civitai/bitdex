# W3 — Local full-scale proof of the scheduled-publish / ingested-sortAt change (#325+#326)

Owner: alexandra (W3 gate). Verifies `docs/design/scheduled-publish-execution-plan.md` §W3 and
`docs/design/scheduled-publish-design.md` §2.D against `origin/main` (#325 fan_out_per_row +
`sortAtUnix` shared ops, #326 ingested `sortAt` + `model3dId` + per-image Post fan-out).

The **CI-friendly regression pin** is `tests/e2e/e2e-scheduled-publish-sortat.mjs` (self-contained,
no PG — creates its own index and drives `/ops`). The scripts here are the **full-scale reproducible
harness** used to produce the gate evidence below.

## Results (all gates PASS)

### Gate 1 — full-scale dump (114.8M images, new #326 config)
- 6-phase dump: **23m13s** (images 6m8s/114,806,025 · tags 13m43s/4,595,522,986 · resources 24s ·
  tools 3s · techniques 3s · metrics 2m52s). No regression vs the ~29-33min baseline (faster,
  despite a larger dataset).
- `sortAt` invariant `sortAt == max(publishedAt, existedAt)`: **600/600 sampled slots, 0 bad**
  (structural: the deduped file has unique ids, and `sortAt` column = `GREATEST(publishedAt,
  scannedAt, createdAt)` = `max(publishedAt, existedAt)` by construction). Ingested seconds land in
  the sort layers with no unit corruption (`fallback: sortAt`, not ms-converted).
- `model3dId` filter: `model3dId Eq 42` = 108,567 matches (~0.1%); forward + reverse membership
  0 bad. New posts→image enrichment field populates the single_value filter.
- No deferred-branch regression: isPublished true 102,631,599 / false 6,558,121; availability=Public
  109,183,181.

### Gate 2 — steady-state scheduled-post lifecycle (PG → triggers → poller → BitDex)
All PASS: published (sortAt==publishedAt, visible); scheduled (deferred, not in isPublished feed);
**CRITICAL no-op: crossing Tf with no further op — the overdue sweep alone flips isPublished AND
preserves the ingested future sortAt (sortAt == publishedAt == Tf exactly)**; unpublish→invisible;
republish→visible; [PR-m1] postId change (sortAt recomputes from new post, postId bit moves).

### Gate 3 [PR-m4] — re-emitter heal-path
Disabled the bitdex Image trigger → inserted a published post+image → 0 BitdexOps rows (genuine
missed op) → "not found" in BitDex. The re-emitter (`w3-reemitter.sql`) healed it to isPublished=true,
sortAt==publishedAt, availability/postedToId correct. Note: the re-emitter heals *publish-state*
fields, not structural fields (postId, nsfwLevel) — correct for its designed class (a missed publish
op on an already-indexed image, where structural fields are already set).

### Gate 4 — latency (measured in-PG; conservative, under dump-flush load)
| case | P50 | P95 | P99 |
|---|---|---|---|
| publish_20img (P99 images/post) | 1.70ms | 2.46ms | 3.79ms |
| publish_200img | 15.2ms | 17.9ms | 23.7ms |
| mv_publish 50posts×20img (1000-img cascade) | 83.6ms | 111ms | 122ms |
| per-image update WITH triggers [PR-M2] | 94.5µs | 141µs | 260µs |
| per-image update WITHOUT triggers | 9.0µs | 22µs | 60µs |

Publish scales ~120µs/image; a very-popular model-version publish (10k+ images in one txn) would be
~1s+ — flagged for the statement-level-trigger mitigation if a MV can touch that many posts. Per-image
update overhead is +85µs P50 (sub-ms), negligible at prod write rates.

### [PR-B3] dump == trigger `sortAt`
On the same rows, the dump-formula `sortAt`, the model-share trigger-written `Image.sortAt` column,
and the BitDex-stored value are identical.

## Reproduce

Prereqs: server+sync built `--features server,pg-sync`; staged CSVs in `data/load_stage/`; Docker.

**Data prep** (staged CSVs predate #326 — add `sortAt` to images, `model3dId` to posts; dedup to
unique ids as prod has). `posts.w3.csv` = staged posts + a recomputable `model3dId` (42 where
`postId % 1000 == 0`); `sortAt` = `GREATEST(post.publishedAt, scannedAt, createdAt)` (== the #326
copy_query COALESCE-fallback, since prod `Image.sortAt` is unpopulated):
```
node tests/w3/w3-prep2.mjs full     # -> images.w3d.csv (quote-aware splice + dedup)
```

**Gate 1 dump + assert:**
```
node tests/w3/dump-local-w3.mjs full            # PUT /dumps, #326 shape
BITDEX_URL=http://localhost:3007 node tests/w3/w3-assert.mjs
```

**Gates 2-4 steady state** (local Docker PG + generated triggers + poller):
```
docker run -d --name w3-pg -e POSTGRES_USER=bitdex -e POSTGRES_PASSWORD=bitdex -e POSTGRES_DB=civitai -p 5433:5432 postgres:16
docker exec -i w3-pg psql -U bitdex -d civitai < tests/w3/w3-schema.sql      # schema + model-share sortAt triggers
bitdex-sync --config tests/w3/w3-sync.toml setup                            # generate + apply #326 triggers
# steady BitDex server with sweep enabled (sweep_interval_secs in deferred_alive), then:
START_CURSOR=0 BITDEX_URL=http://localhost:3010 node tests/w3/w3-poller.mjs &   # minimal BitdexOps->/ops poller
node tests/w3/w3-lifecycle.mjs                                              # gate 2 lifecycle
docker exec -i w3-pg psql -U bitdex -d civitai < tests/w3/w3-latency.sql    # gate 4 latency
```

## Amendments (post-#328, 2026-07-16)
- **#328 (63d9d16): dump `sortAt` is pure `GREATEST(post.publishedAt, scannedAt, createdAt)`, never
  reads `Image.sortAt`** (no-backfill decision; prod column is 88.1% stale `now()`-at-migration
  garbage). **Gate 1 remains valid without a re-dump:** `w3-prep2.mjs` computes exactly this pure
  GREATEST from the staged images↔posts join and never reads a `sortAt` column (staged CSVs have
  none), so the dumped values are byte-identical to #328's output.
- **Model-share BEFORE trigger widened to ALL Image updates** (`w3-schema.sql` `ms_image_sortat_trg`
  is already `BEFORE INSERT OR UPDATE`). The steady-state emission stays COALESCE-shaped
  (`COALESCE(NEW."sortAt", GREATEST(...))`) — safe ONLY because this widened trigger refreshes
  `NEW.sortAt` on every write. **Stale-column test (`w3-stale-column.sql`) PASSES:** injected a wrong
  past `sortAt` (2020-01-01) with the BEFORE trigger disabled → BitDex showed the stale value; then an
  **nsfwLevel-only** UPDATE through the widened trigger emitted `set sortAtUnix = 1784253406000` (the
  CORRECT recomputed GREATEST, not the stale column) → BitDex healed to the correct value.
- **[PR-M2] widened-trigger per-update overhead** (nsfwLevel-only UPDATE now runs the Post subselect):
  P50 56µs / P99 486µs WITH triggers vs 9µs / 95µs without = **+47µs P50 / +390µs P99**. Sub-ms;
  negligible at prod write rates.
- Perf note: bitmap memory at 114.8M reports 0.57GB EAGER-resident (filter 0.12 / sort 0.43 / slot
  0.02); NOT comparable to the 6.51GB full-RSS 104.6M baseline — `tagIds` is lazy/on-disk here and RSS
  instrumentation isn't wired in the `fast` build. A true baseline needs a release build + warmed tagIds.

## Test-data notes (not BitDex defects)
- Staged `images-small.csv` duplicates every id (50%); `images.csv` (full) has unique ids. Prod image
  ids are unique — dedup to match. Bulk-dump of conflicting duplicate slot ids yields per-field
  "frankenstein" docs (latent bulk-load characteristic; irrelevant to prod).
- The `hash` blurhash column's base83 alphabet includes `,`, so PG quotes it — `w3-prep2.mjs` splices
  `sortAt` quote-aware to preserve bytes.
- **`bitdex-sync pg`'s boot-dump writes COPY output into `stage_dir` — keep it ISOLATED from a live
  dump's `data/load_stage`.** During W3 this emptied the ORIGINAL `data/load_stage/images.csv` and
  `posts.csv` (both superseded by the derived `images.w3d.csv` / `posts.w3.csv` — no loss to this
  work, but a fresh agent must re-pull them from prod or use the derived files). The minimal
  `w3-poller.mjs` avoids the boot-dump entirely.
