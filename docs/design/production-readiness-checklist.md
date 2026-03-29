---
status: ACTIVE
updated: 2026-03-28
---

# Sync V2 Production Readiness Checklist

> Prepared by Tom (CTO oversight), 2026-03-28. Based on team briefings from
> Scarlet (team lead), Adam (architect), Aidan (infra), and codebase analysis.
> This is the single document that gates production deployment.

---

## Gate 1: Dump Pipeline Validation (Phase 1)

**Owner:** Josh | **Status:** CLEAR (2026-03-28) — V1.1-V1.6, V1.8-V1.9 pass. V1.7 skipped (non-blocking)

- [ ] **V1.1** 107M CSV load completes under 15 GB RSS, under 10 min
- [ ] **V1.2** Bitmap spot checks pass:
  - `nsfwLevel eq 1` count matches images.csv
  - `tagIds eq {known_tag}` count matches tags.csv (minus disabled)
  - `type eq "image"` uses LCS dictionary correctly
  - `baseModel eq "SDXL"` only from Checkpoint model types
- [ ] **V1.3** Sort correctness: `sort=reactionCount desc limit 10` matches metrics TSV
- [ ] **V1.4** Deferred alive: future publishedAt images not in query results
- [ ] **V1.5** Docstore: `GET /documents/{slot}` returns all fields (url, hash, width, height)
- [ ] **V1.6** Dictionary persistence: restart server, LCS queries still work
- [ ] **V1.7** Crash recovery: kill during resources phase, restart resumes correctly
- [ ] **V1.8** Per-phase memory: RSS drops after each phase's save+drop
- [ ] **V1.9** Config-driven: add field to dump request, dump picks it up without code changes

**Resilience test (from Adam's review):**
- [ ] **V1.7b** Crash recovery: kill mid-dump (e.g., during resources phase), restart — completed phases (tags, images) are preserved, resources phase resumes

**Risk level:** MEDIUM — Adam rates most items HIGH confidence, but V1.7 (crash recovery) and V1.9 (config-driven at 107M) could need iteration.

---

## Gate 2: Steady-State Pipeline Validation (Phase 2)

**Owner:** Lucy | **Status:** CLEAR (2026-03-28) — 17 passed, 0 failed, 2 skipped (unit-tested only)

- [ ] **V2.1** Single op roundtrip: POST /ops set → query shows change → docstore updated
- [ ] **V2.2** Multi-value add/remove: tagIds add/remove reflected in queries
- [ ] **V2.3** Delete: clean delete clears all filter+sort bits, reads stored doc
- [ ] **V2.4** queryOpSet: fan-out to 1000+ slots, bitmap bulk update + batched docstore
- [ ] **V2.5** Deferred alive via ops: future publishedAt creates_slot not queryable until time
- [ ] **V2.6** WAL cursor restart: kill server, restart, no duplicate processing
- [ ] **V2.7** PUT/PATCH → WAL: endpoints generate ops in WAL (not direct staging)
- [ ] **V2.8** Op dedup: duplicate ops in same batch deduped (LIFO, last wins)
- [ ] **V2.9** Non-alive slot ops: set/add ops on non-alive slots silently dropped
- [ ] **V2.10** Delete docstore read: delete reads stored doc, clears correct bitmaps
- [ ] **V2.11** Prometheus metrics: /api/internal/sync-lag returns cursor/lag data

**Bug found & fixed during validation:** queryOpSet entity_id was filtered by non-alive check when it shouldn't be. E2E regression script committed at `tools/e2e-phase2-validation.mjs`.

---

## Gate 3: Trigger Validation (Phase 2.5)

**Owner:** Lucy | **Status:** PARTIAL — crafted tests pass 18/18, but real PG triggers NOT deployed or tested. BitdexOps table does not exist yet.

- [ ] **V2.5.1** PG tunnel access established (Aidan building self-service command)
- [ ] **V2.5.2** BitdexOps table created on PG replica
- [ ] **V2.5.3** Trigger SQL generated from sync config
- [ ] **V2.5.4** Triggers deployed to PG replica
- [ ] **V2.5.5** Wait for organic traffic / make test changes
- [ ] **V2.5.6** Read BitdexOps rows — verify ops structure for all entity types:
  - Image UPDATE → remove + set ops
  - Tag INSERT → add op (disabled tags filtered)
  - Post UPDATE → queryOpSet with publishedAt null handling
  - ModelVersion UPDATE → queryOpSet with Checkpoint filter
  - Model UPDATE → MV id resolution + queryOpSet
- [ ] **V2.5.7** POST ops to local BitDex, verify bitmap changes
- [ ] **V2.5.8** Null handling: publishedAt null↔value transitions produce correct ops
- [ ] At least 100 ops from each trigger type verified
- [ ] Fan-out ops resolve correct slot counts
- [ ] Null transitions produce remove ops (not set with null)

**Blocker:** PG tunnel access. Aidan building self-service tunnel command NOW.

---

## Gate 4: Activation Infrastructure (Phase 3)

**Owner:** Nate (confirmed by Scarlet 2026-03-28, Scarlet briefing him) | **Status:** 60% built, 3.4/3.5/3.6 NOT YET IMPLEMENTED

- [ ] **3.4** Trigger reconciliation: read config, generate SQL, CREATE OR REPLACE, DROP stale
- [ ] **3.5** Boot sequence orchestration:
  - [ ] Wait for BitDex health check
  - [ ] Check dump history (GET /dumps)
  - [ ] For each undumped source: COPY → CSV, PUT /dumps
  - [ ] Poll task status until complete
  - [ ] K8s readiness probe → 200
- [ ] **3.6** Config hash detection: dump names include `{table}-{hash8}`, mismatch triggers re-dump
- [ ] Deploy config committed: existedAt sort field + computed sortAt GREATEST in civitai-index.json (Scarlet has local change, needs commit to feat/sync-v2)

**Dependency:** PG tunnel access (for testing triggers)
**Clarification needed:** Does shadow mode need to be OFF before V2 sidecar deploy, or does V2 sidecar replace the comparison path? (Ask Donovan)

---

## Gate 5: Local Integration Testing

**Owner:** Lucy + Aidan | **Status:** PARTIAL — crafted data tests pass 18/18, but real PG integration NOT done

**What passed:** Local pipeline exercise with crafted test data (dump → ops → restart → persistence).

**What has NOT been done (required for real CLEAR):**
- [ ] Run `bitdex-sync` against real Postgres via PG tunnel
- [ ] Download real CSVs from PG via COPY scripts
- [ ] Load real CSVs through the V2 dump processor
- [ ] Verify queries return correct results with production-scale data
- [ ] Debug config/enrichment issues that surface with real data

- [ ] Fresh CSVs generated from PG via COPY scripts (Aidan)
- [ ] CSV download server running (Aidan)
- [ ] `bitdex-sync all` runs locally: loads all CSVs, ops polling starts
- [ ] Queries return correct results after local load
- [ ] Debug logging validates enrichment chains
- [ ] Verify bitdex-sync binary builds clean with all subcommands
- [ ] civitai-index.json updated with existedAt sort field + computed sortAt

---

## Gate 6: Production Deployment

**Owner:** Aidan (deploy) + Tom (coordination) | **Status:** BLOCKED on Gates 1-5

### Pre-Deploy
- [ ] All validation gates (1-5) passed
- [ ] PR merged to main (Justin approval required for all sync-v2 merges)
- [ ] Docker image built and pushed to GHCR
- [ ] Shadow mode OFF (coordinate with Donovan)
- [ ] Production memory stable (currently 28.4/32 GB, monitor trend)

### Deploy Sequence
- [ ] K8s staging deploy via Aidan
- [ ] Staging smoke test: load data, run queries, verify correctness
- [ ] Production deploy: rolling restart via Flux
- [ ] Verify V2 sidecar starts, boots, and transitions to ops polling
- [ ] Monitor Prometheus metrics for 30 min:
  - `bitdex_rss_bytes` < 28 GB
  - `bitdex_query_duration_seconds` p99 < 100ms
  - `bitdex_pgsync_cursor_position` advancing
  - Zero `bitdex_sync_errors`
- [ ] Re-enable shadow mode (Donovan)
- [ ] Monitor comparison metrics for divergence

### Rollback Plan
- [ ] If V2 sidecar fails: revert to V1 outbox poller (keep running until V2 proven)
- [ ] If queries degrade: disable shadow mode immediately
- [ ] If memory spikes: check eviction metrics, reduce cache max_bytes via PATCH /config

---

## Gate 7: Production Stability Verification

**Owner:** Tom + Aidan | **Status:** NOT STARTED

- [ ] Shadow mode running for 2+ hours with comparison metrics clean
- [ ] Query latency comparable to V1 baseline
- [ ] Memory trend stable (not climbing)
- [ ] Cache hit rate comparable (44% was V1 shadow mode baseline)
- [ ] Sync lag < 30s (ops polling catching changes in near-real-time)
- [ ] No 6-second stalls (known V1 issue from shadow mode)
- [ ] Notify Justin via mailbox: "sync-v2 stable in production"

---

## Current Blockers (Priority Order)

| # | Blocker | Owner | Status | ETA |
|---|---------|-------|--------|-----|
| 1 | Josh 107M validation | Josh | Running NOW | ~15 min |
| 2 | Lucy PATCH semantics bug | Lucy | Fix approach sent | Hours |
| 3 | PG tunnel access | Aidan | Building self-service command | Hours |
| 4 | Phase 3 agent assignment | Scarlet | Deciding (likely Nate) | Today |
| 5 | Fresh CSV generation | Aidan | Queued, not started | After tunnel |
| 6 | Donovan shadow mode coordination | Donovan | Offline, message queued | Unknown |

---

## Team Assignments

| Person | Current Focus | Next Up |
|--------|--------------|---------|
| **Josh** | 107M validation run | Report results → merge |
| **Lucy** | PATCH bug fix + V2 validation | Phase 2.5 trigger validation |
| **Nate** | Released (available) | Phase 3 remaining (3.4/3.5/3.6) |
| **Adam** | Design review | Review Phase 3 implementation |
| **Aidan** | PG tunnel + CSV prep | Staging deploy |
| **Scarlet** | Team coordination | Merge management |
| **Donovan** | Offline | Shadow mode on/off coordination |
| **Tom** | CTO oversight | Monitor, unblock, report to Justin |
