# Unified Cache Migration — Code Review & Owner Feedback

**Date:** 2026-03-12
**Scope:** ~3100-line diff across 19 files (net -2100 lines). Consolidates trie cache + bound cache into unified cache.
**Reviewed by:** 4-agent team (architecture, code quality, performance, API surface)
**Owner feedback:** Justin (inline below)

---

## Justin's Feedback

> - Production traffic will **always** include a sort. We intentionally gutted filter-only caching in favor of the unified cache — it covers most queries, and cold misses just warm the cache for the next request. Acceptable tradeoff.
> - `hasBlockedFor` → `blockedFor` rename was intentional. Model-share requires `blockedFor`. If something downstream still uses `hasBlockedFor`, that's their problem — our consumer is updated.
> - Stats/metrics shape change was intentional.
> - The time bucket empty-bitmap behavior is **news to me** — needs investigation.
> - Dead code cleanup makes sense — just verify it's actually dead before removing.
> - Housekeeping (.gitignore, scratch files) — do it.
> - Alive insert invalidation gap — "All caches are live now, so I don't think that's an issue." (See note below — reviewer still flags this for NotEq/Not edge case, but low priority given workload.)

---

## Actionable Items

### Must Fix

| # | Issue | Details | Effort |
|---|-------|---------|--------|
| 1 | **Time bucket empty-bitmap edge case** | `query.rs` `snap_clause()` returns empty bitmap when a time range doesn't match any configured bucket. Previously fell through to standard evaluation. Edge case: `Gt("sortAtUnix", 0)` returns zero results instead of all documents. Justin was unaware of this change — needs investigation to determine if it's a bug or needs a guard for extreme ranges. | Small |
| 2 | **Dead `CacheConfig` fields** | 5 unused fields remain: `bound_target_size`, `bound_max_size`, `bound_max_count`, `decay_rate`, `max_entries`. Stale comment says "Trie cache configuration." Also `maintain_alive_changes()` only called from a test. Verify dead, then remove. | Small |
| 3 | **Hardcoded `SHARD_SHIFT = 9`** | `rebuild_fields_from_docstore` uses magic number `>> 9` instead of `DocStore::SHARD_SHIFT`. Will silently break if constant changes. | Trivial |
| 4 | **Silent shard errors in rebuild** | `get_shard()` errors in the rebuild rayon fold are silently `continue`d. Should at minimum `eprintln!` a warning and track error count. | Small |
| 5 | **Missing unified cache Prometheus metrics** | `/metrics` endpoint dropped 4 bound_cache gauge families but added nothing for unified cache. Stats endpoint has the data — just needs gauge registration. | Small |
| 6 | **`.gitignore` + scratch files** | Add `*.log`, `cache_test.js`, `timing_test.js`, `pagination_bench.js`, `RESUME.md` to `.gitignore`. Commit `tools/workload.json` and `tools/mixed-workload.mjs`. | Trivial |

### Low Priority / Monitor

| # | Issue | Details |
|---|-------|---------|
| 7 | **Alive insert + NotEq/Not cache staleness** | When a new doc is inserted matching a cached entry that uses `NotEq`/`Not` (which implicitly depend on the alive bitmap), the cached result may be stale until `maintain_filter_changes` touches it. Justin says all caches are live-maintained, and the workload rarely uses `NotEq`/`Not` in hot paths. **Monitor but don't block on this.** |
| 8 | **`entry_by_meta_id()` is O(n) linear scan** | Could be O(1) with a reverse map. Only runs on flush thread with n <= 5000. Not urgent. |
| 9 | **Rebuild race window** | Two rebuild requests in the same tick could both pass the status check before either sets `Loading`. Extremely narrow window, returns 409 on retry. Not worth fixing now. |
| 10 | **`_elapsed` unused variable** | `concurrent_engine.rs:502` — computation with no consumer. Trivial cleanup. |

---

## Not Doing (Per Owner Feedback)

| # | Issue | Why crossed out |
|---|-------|-----------------|
| ~~11~~ | ~~Filter-only cache regression~~ | Intentional. Production always includes sort. Unified cache warms on first miss. |
| ~~12~~ | ~~`hasBlockedFor` → `blockedFor` consumer coordination~~ | Intentional rename. Model-share already uses `blockedFor`. |
| ~~13~~ | ~~Stats/metrics response shape breaking change~~ | Intentional. Old fields were for removed systems. |
| ~~14~~ | ~~Prefix matching loss in unified cache~~ | Accepted tradeoff. Unified cache exact-match is sufficient for the workload. |
| ~~15~~ | ~~Deep pagination tiered bounds loss~~ | Dynamic expansion (1K→16K) replaces tiered bounds. Acceptable. |
| ~~16~~ | ~~Benchmark validation of sort performance~~ | Bound cache replacement is functionally equivalent. Owner confident in unified cache. |

---

## Architecture Summary (For Context)

**What changed:** Two caches (trie cache for filter results + bound cache for sort working-set reduction) consolidated into one unified cache keyed by `(canonical_filters, sort_field, direction)`.

**What was deleted:**
- `src/bound_cache.rs` — entire module (838 lines)
- `src/cache.rs` — trie cache internals (~500 lines removed, canonicalization preserved)
- `src/executor.rs` — cache/bound integration (~400 lines removed, core sort traversal intact)
- `src/concurrent_engine.rs` — dual-cache maintenance replaced by unified cache maintenance

**What was added:**
- `src/unified_cache.rs` — minor additions (stats fields, `needs_rebuild` lookup guard, `remove_slot_from_all`)
- Case-insensitive MappedString matching (cross-cutting: config, loader, executor, docstore, server)
- `POST /rebuild` endpoint for bitmap reconstruction from docstore
- Merge thread skip-clean-fields optimization
- Lazy-load-aware snapshot persistence

**Design principle compliance:** All 9 inviolable principles pass. Bitmaps all the way down. No sorted data structures. Sort-layer bit traversal untouched. Docstore unchanged.

**Test status:** 385 tests, clean compilation, zero warnings.

---

## Full Agent Reports

Individual review reports available at:
- `/tmp/review-architecture.md` — Design principles compliance
- `/tmp/review-quality.md` — Code quality and correctness
- `/tmp/review-performance.md` — Performance and caching impact
- `/tmp/review-api.md` — API surface and server changes
