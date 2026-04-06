# FOLLOWUP

Non-urgent issues, latent risks, and improvement opportunities. Agents with idle time should pick items from here. Check items off when done (replace `[ ]` with `[x]` and add PR/commit ref).

---

## Dump Pipeline (PR #129)

- [x] **u8 field index cap** — fixed in 6819886, now u16. (MEDIUM)
- [ ] **EnrichmentTable::get() panics on Mmap** — public method panics if called on Mmap-backed table. Should return `Option`/`Result` or be removed from public API. (MEDIUM)
- [ ] **DocFieldSource::Computed drops string results** — `execute_doc_plan` silently skips `NateExprValue::Str` from computed fields. Add warning or handle the case. (MEDIUM)
- [x] **serialize_frozen_into().ok() discards errors** — fixed in 6819886, now logs errors. (MEDIUM)
- [ ] **No-silo mode silently drops bitmaps** — when `engine.bitmap_silo` is `None`, `write_dump_maps` discards all filter/sort bitmaps with no warning. Add assertion or eprintln. (MEDIUM)
- [ ] **Dead EnrichedComputed variant** — `DocFieldSource::EnrichedComputed` defined but never inserted into FieldPlan by `build_doc_field_plan`. Remove dead code. (LOW)
- [ ] **200M enrichment key cap** — dense Vec in `load_fast` warns but doesn't error for keys above 200M. Could silently drop data. (LOW)
- [ ] **Mmap offset panic on corrupt CSV** — `mmap_line_at` panics on out-of-bounds offset from corrupt CSV key. Add bounds check with graceful return. (LOW)
- [ ] **Deferred-alive path divergence** — `collect_doc_op` (legacy path for deferred rows) can produce different docstore output than `execute_doc_plan` for the same row. Unify. (LOW)
- [ ] **Unnecessary merged_deferred.clone()** — in `process_dump_phase`, `merged_deferred` is cloned under slot write lock then also moved into PhaseResult. Clone is wasteful. (LOW)
- [ ] **reload_after_dumps unused parameter** — `_had_alive_phase: bool` is unused. Remove or document as deprecated. (NIT)
- [ ] **streaming_merge flag untested** — no end-to-end test verifying `streaming_merge: true` produces same results as `false`. (LOW)
- [ ] **Enrichment nested lookup allocation** — `enrich_key_into` recursive calls allocate fresh Vecs, defeating `lookup_buf` reuse for multi-level chains. (NIT)
- [ ] **Enrichment map built before deferred check** — `enriched_map` allocated every row even for filtered-out deferred rows. Swap order to defer map-building. (NIT)

## Dump Pipeline — Test Coverage

- [ ] **Mmap enrichment path untested** — no test exercises `MmapIndex` lookup, column parsing, nested child enrichment, or 200M key cap. All tests use small tables (HashMap path). (MEDIUM)
- [ ] **write_dump_maps untested** — new direct-write BitmapSilo path has no round-trip test. (MEDIUM)

## Cache (PR #130)

- [ ] **Live maintenance not restored** — epoch-based staleness is a correctness fix but not performance-equivalent to the old UnifiedCache live maintenance (maintain_slot_insert/remove). Justin's preference is to restore live maintenance eventually. See `docs/design/v3-architecture-cleanup.md` for design. (MEDIUM)
- [ ] **Pre-existing test failure** — `test_save_snapshot_after_deletes` fails on main (unrelated to cache epoch changes). Delete-persistence bug. (MEDIUM)

## V3 Architecture Cleanup

See `docs/design/v3-architecture-cleanup.md` for the full plan. Task list tracked in Claude Code tasks #7-#20.

- [ ] **BitmapSilo string manifest overhead** — heap HashMap + format!() + RwLock per query. Replace with deterministic u64 key encoding (tasks #7-#10). (HIGH)
- [ ] **Sort traversal requires in-memory SortIndex** — needs frozen-only path (task #11). (HIGH)
- [ ] **Alive bitmap eagerly loaded to heap** — should use ops-on-read (task #12). (HIGH)
- [ ] **Range scans depend on in-memory FilterField** — need silo-based key enumeration (task #13). (HIGH)
- [ ] **Time buckets depend on in-memory SortIndex** — migrate to BitmapSilo (task #14). (HIGH)
- [ ] **Cache live maintenance dead** — stale_fields collected and discarded in flush.rs:159 (partially fixed by PR #130 epoch approach). (HIGH)
- [ ] **PendingBucketDiffs computed but never consumed** — lazy diff application TODO from commit 0ec1610 was never wired up. Entire mechanism is dead weight. (MEDIUM)
- [ ] **Planner cardinality uses in-memory base_len()** — needs silo-based alternative (task #19). (LOW)

## V3 Cleanup — External Review Findings (GPT-5.4, 2026-04-05)

- [ ] **HIGH: ConcurrentEngine public APIs read stale in-memory state** — `alive_count()`, `slot_counter()`, `reconstruct_sort_value()` still read from in-memory slots/sorts, which are no longer updated when silo is present. Must read from silo or be removed from production use. (HIGH)
- [ ] **HIGH: merge_bitmap_maps() bypasses silo** — bulk-loaded data merged only into in-memory state, invisible to silo-based reads. Must also write to silo when present. (HIGH)
- [ ] **HIGH: Cache staleness ignores AliveInsert/AliveRemove** — `bump_field_epochs()` only tracks filter/sort ops, not alive changes. Deleted docs can be served from cache. Must bump epoch on alive changes too. (HIGH)
- [ ] **HIGH: Time bucket maintenance reads stale in-memory sort state** — flush thread skips sort apply when has_silo, but bucket insert still calls `sort_field.reconstruct_value(slot)` from in-memory. Must use silo-based reconstruction. (HIGH)
- [ ] **MEDIUM: alive_count() not consistent with alive_bitmap() OnceCell** — planner uses alive_count() (recomputed) while filter uses alive_bitmap() (cached). Should derive count from cached bitmap for per-query consistency. (MEDIUM)
- [ ] **MEDIUM: Epoch bump before silo append — failed writes poison cache** — bump_field_epochs called before silo write. Partial failures leave advanced epoch with missing data. Move bump after successful append or document as conservative. (MEDIUM)
- [ ] **MEDIUM: save_and_unload() can drop pending mutations in no-silo path** — no flush synchronization before snapshot. Pending coalescer mutations may be lost. (MEDIUM)

## General

- [ ] **FIX 5: Cache hit bitmap clone** — `Arc::new(entry.bitmap.clone())` at query.rs:136 clones full bitmap on every cache hit. Should Arc-wrap bitmaps inside cache entries. (MEDIUM)
- [ ] **FIX 9: UnifiedKey/UnifiedCache naming** — naming remnants from the old unified cache don't match current CacheSilo architecture. (NIT)
