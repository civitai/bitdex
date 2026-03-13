# Bitdex V2 Documentation

## Folder Structure

### `design/` — Durable Design Documents
Architecture and design decisions for each major subsystem. **Agents MUST read the relevant design doc before proposing changes to that subsystem.**

Each design doc has front matter with a status field:
- **IMPLEMENTED** — Built and in production code
- **APPROVED** — Reviewed and approved, not yet built
- **PROPOSED** — Needs validation (benchmarks, review) before implementation

| File | Subsystem | Status |
|---|---|---|
| `design-concurrency.md` | ArcSwap snapshots, CoW, flush/merge threads, loading mode | IMPLEMENTED |
| `design-storage.md` | BitmapFs, DocStore, persistence lifecycle | IMPLEMENTED |
| `design-idle-eviction.md` | Per-value eviction for multi_value fields | IMPLEMENTED |
| `design-unified-cache-final.md` | Unified cache (trie + bound + time buckets merged) | IMPLEMENTED |
| `design-unified-cache-persistence.md` | Cache warm-start from disk | APPROVED |
| `design-rolling-restart-cursors.md` | Named cursors for zero-downtime restarts | IMPLEMENTED (Ph 1-3) |
| `design-radix-sort-trie.md` | 8-bit radix bucketing for large cache entries | IMPLEMENTED (Ph 1) |
| `meilisearch-syntax-reference.md` | Meilisearch filter/sort syntax mapping to BitDex | IMPLEMENTED |
| `query-language-proposals.md` | Query language design proposals and evaluation | IMPLEMENTED (compact) |

### `guide/` — How to Use Bitdex
Reference documentation for operators and integrators.

- `api.md` — HTTP API reference (all endpoints, request/response examples)
- `query-formats.md` — Query format guide (bitdex, compact, meilisearch — syntax, examples, when to use)
- `config-schema.md` — Configuration schema reference
- `bitdex-civitai-schema.md` — Civitai dataset field mapping
- `testing.md` — Test suite guide (Rust tests, E2E tests, benchmarks)

### `_in/` — Input Documents
Documents dropped in by Justin for agents to process. Design conversations, specs, schema examples, and requirements.

- `architecture-conversations.md` — Merged design conversations with navigable summary
- `prepared-prompt.md` — Canonical project specification
- `config-schema-v2.md` — JSON config schema reference
- `storage-overhaul.md` — Requirements for the redb-to-filesystem pivot
- `preferred-schema-format.yml` — YAML schema format example

### `benchmarks/` — Performance Data
Benchmark results, baselines, and regression thresholds. `performance-baseline.md` is the authoritative consolidated reference.

### `learnings/` — What We Tried That Didn't Work
Approaches we evaluated and rejected, organized by topic. **Read before proposing designs in these areas** to avoid re-trying things that have already been ruled out.

### `plans/` — Implementation Plans
Active implementation plans and roadmaps. Plans are ephemeral — used during implementation, then archived. The durable record is in `design/`.

### `reviews/` — Architecture Reviews and Audits
Code reviews, architecture audits, and Q&A documents. Reviews with open action items are kept active; completed reviews are archived.

### `test-results/` — E2E Test Output
JSON results from the automated E2E test runner. Gitignored except for `.gitkeep`.
