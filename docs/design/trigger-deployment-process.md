---
status: ACTIVE
updated: 2026-03-28
---

# Trigger Deployment Process

> How PG triggers get generated, reviewed, deployed, tested, and cleaned up.
> This is a safety-critical process — triggers run inside Postgres and affect production data.

---

## Overview

BitDex V2 uses PG triggers to detect changes in source tables and write ops to the `BitdexOps` table. The sync sidecar (`bitdex-sync`) polls `BitdexOps` and POSTs ops to the BitDex server.

Triggers are generated from the sync config YAML by `src/pg_sync/trigger_gen.rs`. They are NOT hand-written SQL.

---

## Step 1: Generate Trigger SQL

The `trigger_gen.rs` module reads the sync config and generates PL/pgSQL trigger functions for each source table.

**Two table types:**
- **Direct tables** (slot = PG column value): Image, Tag, Tool, Technique, Metric
  - `track_fields`: scalar fields emit remove/set pairs via `IS DISTINCT FROM`
  - Multi-value join tables: emit add/remove ops
  - `on_delete: delete_slot`: emit delete op
  - `sets_alive: true`: only the Image table creates alive slots
- **Fan-out tables** (slots resolved by BitDex query): Post, ModelVersion, Model
  - `query`: BitDex query template with `{column}` placeholders
  - Resolve affected slots via bitmap query, then emit ops per slot

**Named triggers:** Trigger names include a configurable prefix/suffix so that dev and prod triggers can coexist on the same PG database without conflicts. Example: `bitdex_dev_image_update_trigger` vs `bitdex_prod_image_update_trigger`.

**Config hash:** Each trigger name includes a hash of its config block (`{table}-{hash8}`). When the config changes, the hash changes, and stale triggers are detected for cleanup.

---

## Step 2: Safety Review

Before deploying any trigger to PG, the generated SQL must be reviewed.

**Process:**
1. System generates the full SQL for each trigger function
2. Sub-agents review each trigger individually, checking:
   - Column references match the actual PG schema
   - Expression templates resolve correctly for OLD and NEW
   - Null handling produces correct remove/set ops
   - Fan-out queries are correctly formed
   - No performance risks (missing indexes, expensive joins)
3. Sub-agents report findings to the team lead or Justin
4. Justin approves deployment

**No trigger is deployed without explicit approval.**

---

## Step 3: Deploy Triggers

**Prerequisites:**
- PG tunnel access established (Aidan provides self-service tunnel command)
- `BitdexOps` table created on the target database
- Safety review complete and approved

**Deployment:**
- Task 3.4 in the implementation plan: `CREATE OR REPLACE` for each trigger
- Triggers are idempotent — re-running deployment is safe
- Stale triggers (hash mismatch) are `DROP`ped automatically

**Target database:** PG replica `cnpg-cluster-nvme0-1` in `cnpg-database` namespace (via port-forward tunnel from Aidan)

---

## Step 4: Test with Real Traffic

**Process:**
1. Deploy triggers to PG replica
2. Wait for organic traffic (or make targeted test changes)
3. Read `BitdexOps` rows — verify ops structure for all entity types:
   - Image UPDATE: remove + set ops with old/new values
   - Tag INSERT: add op with tagId (disabled tags filtered)
   - Post UPDATE: queryOpSet with `"postId eq {id}"`, publishedAt null handling
   - ModelVersion UPDATE: queryOpSet with Checkpoint filter
   - Model UPDATE: MV id resolution + queryOpSet
4. POST sampled ops to local BitDex, verify bitmap changes
5. Verify null transitions: `publishedAt null→value` and `value→null` produce correct ops

**Gate 3 requirement:** At least 100 ops from each trigger type verified.

---

## Step 5: Cleanup

After testing (or when triggers need to be removed):

**Aidan's cleanup script:**
1. DROP all BitDex triggers (identified by naming prefix)
2. DROP the `BitdexOps` table
3. Verify no orphaned triggers remain

This cleanup is essential after testing on shared databases to avoid:
- Performance overhead from unused triggers
- Stale ops accumulating in BitdexOps
- Confusion about which triggers are active

---

## Key Safety Rules

1. **Never deploy triggers without safety review** — generated SQL must be reviewed by sub-agents
2. **Named triggers are mandatory** — dev/prod must be distinguishable on shared databases
3. **Always clean up after testing** — triggers on a shared PG are a shared resource
4. **Verify with real data** — crafted test data passing is necessary but NOT sufficient (Gates 3+5 lesson)
5. **Justin approves all sync-v2 trigger deployments** — no exceptions

---

## Related Files

| File | Purpose |
|------|---------|
| `src/pg_sync/trigger_gen.rs` | Generates PL/pgSQL trigger SQL from sync config |
| `src/pg_sync/sync_config.rs` | Parses YAML sync config |
| `docs/design/sync-v2-final-implementation-plan.md` | Phase 2.5 task list (trigger validation) |
| `docs/design/production-readiness-checklist.md` | Gate 3 (trigger validation) status |
