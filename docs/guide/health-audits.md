# BitDex Health Audits — standing verification checklist

Born from the 2026-07-07→10 campaign, where every "all clear" was overturned by the next
deeper probe. These are the checks that caught real bugs, in the form that caught them.
Run them on the stated cadence; every release additionally runs the POST-RELEASE set.
**Rule zero: every reading comes from counters/probes, never from pod-log greps on the
serving pod** (its kubectl log retention is ~78 seconds under query load — log-based
liveness checks manufactured a phantom scheduler stall on 2026-07-10).

Conventions: `PF0` = `kubectl -n bitdex port-forward pod/bitdex-0 3098:3000` (3096 for
bitdex-1); queries are typed-JSON `POST /api/indexes/civitai/query` with `skip_cache:true`;
psql = `kubectl -n bitdex exec bitdex-psql -- psql "$DATABASE_URL"`.

## A. Feed integrity (caught: tie-band drop #304, ms-units #305, split-brain layers)

### A1. Site-alignment compare — 4 periods, classified
- **How:** `%TEMP%/bitdex-cmp/site_feed.mjs <Period> "Most Reactions" 1 200` per period
  (Day/Week/Month/Year), then per site row a scoped probe
  `postId Eq + nsfwLevel Eq 1 + sortAtUnix Gte (now−span)·1000`; classify misses via batch
  `POST /documents` into REAL (published+nsfw1+doc-sortAt-in-window) vs boundary vs other.
- **Healthy:** 0 real misses per period. Boundary singles (items that slid past the exact
  rolling cutoff between fetch and probe) are expected noise.
- **Traps:** wait ≥10 min after a pod boot (bucket convergence) or probe the standby; the
  site's period anchor ≠ rolling now−span, so classify before alarming.
- **Cadence:** post-release + daily.

### A2. Enumeration completeness (tie bands)
- **How:** paginate the Day sweep (nsfw1 + 24h, reactionCount Desc, limit 200) to floor;
  record page-end `cursor.sort_value` sequence and total collected.
- **Healthy:** repeated sort_values across consecutive page-ends where bands are wider
  than a page (the 2026-07-09 bug's signature was a strictly-decreasing sequence over
  thousand-deep bands); collected count consistent with `total_matched`.
- **Cadence:** post-release (any change touching sort/executor/cache).

### A3. Layer-vs-doc split (THE discriminator for the split-brain class)
- **How:** for a sample of recent scheduled-post activations (and any A1 real miss):
  layer value via sorted-cursor walk (`postId Eq`, sort sortAt Asc, step cursor until the
  slot; `cursor.sort_value` = its layer value) vs `POST /document` doc field.
- **Healthy:** layer == doc for every sample. Layer==0 with a correct doc = the split-brain
  family — escalate immediately, and check whether a restart occurred since last merge.
- **Cadence:** post-release + after ANY pod restart + weekly sweep sized ≥50.

## B. Write-path integrity (caught: skip-race #303, dedup collapse #302, fan-outs #296/#297)

### B1. Publish completeness (the 0-of-1,142 method)
- **How:** psql `SELECT post_id FROM bitdex_post_publish_capture WHERE published_at
  BETWEEN <window>` (lossless trap keeps this fed); probe every post:
  `postId Eq + isPublished Eq true` → total_matched.
- **Healthy:** 0 stuck after excluding zero-image posts (LEFT JOIN Image → img_total=0
  is legitimate absence). Pre-#303 baseline was 2.5%.
- **Cadence:** post-release (2h window) + weekly (24h window).

### B2. Poller skip-rate + gap health
- **How:** counters/log-lines on the SIDECAR (low-volume, greps OK there): zero
  unresolved "ALERT — cursor held" beyond transient; lossless-trap join: captured
  publish fan-outs >5 min old vs engine applied-state → 0 unapplied.
- **Healthy:** gap holds resolve in seconds; skip-rate 0.
- **Cadence:** daily.

### B3. Go-live crossing
- **How:** pick 3-5 posts with Tf in the next ~30 min (deferred map / PG publishedAt
  future); verify images enter `isPublished Eq true` within 2 min of Tf.
- **Healthy:** 100% cross on time. Also verify sortAt layers non-zero right after
  activation AND after the next pod restart (split-brain regression gate).
- **Cadence:** post-release.

## C. Time buckets (caught: interval PATCH no-op, benign-edge misreads)

### C1. Bucket membership persistence
- **How:** `/time-buckets/audit?sample=1000&order=lowest_id` twice, 200s apart; diff the
  missing sets.
- **Healthy:** ZERO ids persist across both samples (missing = rolling insert front that
  clears ≤200s; stale = expiry tail). Persistence = real add-path/layer bug.
- **Cadence:** daily.

### C2. Rebuild scheduler liveness — COUNTERS ONLY
- **How:** `bitdex_time_bucket_full_rebuild_total` delta over ≥2 intervals, BOTH pods.
- **Healthy:** delta ≈ elapsed/interval. NEVER grep serving-pod logs for this.
- **Cadence:** post-release + daily.

### C3. Config knob truth
- **How:** hot-reload keys are FLAT (e.g. `time_bucket_full_rebuild_interval_secs`) — a
  nested body 200s and echoes config as a SILENT NO-OP. After any PATCH, grep the pod log
  for the explicit "Config patch: … set to …" line (handler logs unconditionally when the
  field parses). After any restart, re-PATCH (boot-seed from mounted yaml unverified).
- **Cadence:** every restart/roll.

## D. Replication & serving

### D1. Pod parity
- **How:** 12+ fresh publishes probed identically on both pods.
- **Healthy:** 100% agree. Divergence = per-pod pipeline loss (the #303 signature).
- **Cadence:** post-release + weekly.

### D2. Latency
- **How:** wide-window warm probe (30 combos: {7d,30d,1y} × nsfw levels × 2 sorts).
- **Healthy:** worst ≤1s warm (baseline 0.3-0.5s). P50/P95/P99 from Prometheus over a
  window, never a single gauge read.
- **Cadence:** post-release.

## E. Memory (three corrections in one day taught these rules)

- **Signal:** `/debug/memory` `rss_bytes`, WARM-STATE ONLY (≥90 min under traffic; cold
  boot reads ~12Gi vs warm ~26Gi and comparisons across states are meaningless).
- **Healthy (v1.1.38 era):** active pod RSS band 23-29Gi; alarm = sustained trend toward
  40Gi. cgroup memory will PIN NEAR THE LIMIT by design (page cache using budget) — it is
  informational only, never an alarm by itself.
- **Cadence:** continuous (ava's watch) + post-release warm reading.

## F. Post-release checklist (run in order)
1. Staged roll gates (bitdex-1/Zen3 boot first; halt on hang/exit-132).
2. C3 config re-PATCH + log-line verify, both pods.
3. Wide-bucket warm (D2), both pods.
4. Wait 10 min (bucket convergence) → A1 classified compare.
5. B1 publish completeness over the roll window; B3 go-live crossing.
6. C2 counter cadence, both pods; D1 parity.
7. E warm RSS reading at +90 min.
8. A3 layer-vs-doc split on post-roll activations — **plus after the NEXT restart**, the
   restart-survival check: the same cohort's layers must still be non-zero.

Version this doc with the checks: when an audit's "healthy" definition changes, say why
here, dated. History: created 2026-07-10 after the write-path/read-path campaign
(v1.1.35–38); the split-brain sort-shard bug survived four "all clears" because no audit
compared layers to docs — A3 exists so that never happens again.
