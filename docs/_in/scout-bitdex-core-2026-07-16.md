# Scout report: bitdex-core (B3) — status & design for rebuild-vs-patch

Scouted 2026-07-16. Repo: `C:\Dev\Repos\open-source\bitdex-core`, C#/.NET 10, branch `docstore/slot-entry`, HEAD `856fffa`. This report focuses on what is NOT already in `roadmap.md` / `spec/11-sync.md`.

## Size / effort basis
- **30.3K LOC hand-written** (excl generated `obj/`): Core 11.2K, Server 1.3K, Tests 7.8K, Bench 9.9K.
- **256 xUnit test methods, 47 files.** Correctness kept strictly out of benches. 212-242 green depending on milestone.
- Bench+test = 17.7K of 30K LOC — reflects the design→prove→ADR discipline; much of it is throwaway harness, not shippable surface.
- 31 ADRs, 15 spec files, a roadmap, an independent audit. Docs are unusually rigorous.

## Milestone status (one-liner each)
- M0 Foundation ✅ / M1 Document Store ✅ / M2 Indexes + cross-silo arch ✅ / M3 Mutations ✅ / M4.1+4.2 Config lang + load pipeline ✅ / M6 Transport ✅.
- **M4.3 time buckets ⬜ NOT STARTED** — `MutationBatch.BucketMove` is a literal no-op (`roadmap.md:130`).
- **M5 Sync 🟡 CORE ONLY** — loader convergence + 114.8M validation + schema-freeze guard done; **sync surface + doc-level /ops NOT started**.
- **M7 Observability/deploy ⬜** / **M8 Hardening ⬜** — not started.

## Production-readiness gaps BEYOND the audit P0-P3 list
The audit covers exception isolation, /ddl-index-not-maintained, fold burn, index validation, auth, pagination, cache descope. Additional gaps not framed as audit items:

1. **No sync/ingestion engine at all.** `CursorStore` does not exist (`roadmap.md:147`). The `.bdx` `source … poll/fetch` blocks parse into a `SourcePull` AST but **nothing executes it**. This is the single biggest gap — the layer where v2's current pain lives is 100% absent in B3.
2. **No query-format parsers.** v2 serves `bitdex`/`compact`/`meilisearch` formats; **Civitai's query builder emits these today.** B3 has one query shape; the pluggable parser registry "vanished unadmitted" (audit P2 #8). Client-facing cutover rework on top of the sync gap.
3. **No cache subsystem.** A headline pillar of the design (`architecture.md`) has zero code and no milestone. v2's unified cache + BoundStore persistence + time-bucket cache interaction has no B3 counterpart.
4. **No time buckets** (sliding-window filter rewrites AND future-dated exclusion) — both undecided and unbuilt.
5. **Full-index RSS at scale is UNMEASURED.** Only the ~50MB query-working-set and the 12.5GB docstore are measured; separate-process resident memory is explicitly deferred (`benchmarks.md:90`). So B3 has **no number comparable to v2's proven 14.5GB RSS @104.6M.**
6. **Production `/rebuild` working-set surge** — the 114.8M streaming rebuild's managed heap is flat ~6GB but working set climbs to ~28GB (page-cache of 23GB docs.data). On a RAM-constrained serving box this can evict hot query pages mid-rebuild. Lever (windowed re-read / madvise) is a tracked follow-up, not built (`benchmarks.md:84`).
7. **No metrics/observability/HA/deploy** — no OpenTelemetry, no `/metrics`, no K8s. v2's whole Prometheus/Grafana/HAProxy stack has no counterpart.

## THE SCHEDULED-PUBLISH / sortAt / deferred-activation QUESTION
**B3 has NO implemented design AND no decided paper design for this. Decisive finding.**

What exists is all LOAD-TIME:
- `config/civitai.bdx:69`: `sort sortAt = max(existedAt, publishedAt)`, publishedAt resolved cross-silo from the `posts` silo, materialized at bulk-load via `MaterializedCrossSiloResolver`. **No future-dated sortAt, no pending bitmap, no exclude-until-now.**
- `config/civitai.bdx:52`: `index isPublished = images.posts.publishedAt != null` — load-time derived cross-silo index. Its *incremental* maintenance when a Post flips is the unbuilt path.

What's designed but undecided/unbuilt:
- `spec/05-time-buckets.md:67-82` §C "future-dated content that shouldn't be visible yet": **three options, all undecided** — C1 separate future bitmap AND-NOT'd on the hot path, C2 deferred-alive promoter tick, C3 hold writes in a sidecar queue. Open question line 117: *"Do we want 'scheduled content' semantics (C3) or strict 'alive only when visible' (C1/C2)?"* — the exact question v2 is fighting, unanswered in B3.
- **B3's C1 = the same shape as Justin's own sortAt+pending-bitmap redesign** (derive pending, AND-NOT on query). B3 is a clean greenfield to build that correctly — but it is NOT built, benched, or decided there.
- **No `queryOpSet` / per-image fan-out concept anywhere** in B3 code or docs. No deferred-activation, no verifier, no orphan class — because there's no sync layer to produce them.

Implication: a rebuild does NOT inherit a solution to the scheduled-publish problem. That problem is unsolved in BOTH systems. Worse, the incremental cross-silo maintenance that *would* handle a Post's publishedAt flipping is exactly audit P0 #2 (ReverseIndexMaintainer hardcodes the field list; /ddl-added indexes not maintained) AND depends on the unbuilt doc-level /ops AND the unbuilt sync surface.

## Perf numbers vs v2
| Metric | B3 | v2 |
|---|---|---|
| Scale validated | 114.8M (87% live, real export) | 104.6M / 105.3M |
| Query p99 | **70.6ms @114.8M** frozen-mmap (p50 48.3ms feed; selective 0.2ms; user-filtered ~310ms tail) | known-good sub-100ms common shapes |
| Bulk load | 8.5M docs/s | ~5.5M docs/s |
| Query working set | ~50MB frozen resident (10× less than in-heap) | — |
| Docstore | 12.5GB single data.dat, u64 offsets | — |
| Bitmap memory / RSS @scale | **NOT MEASURED separately** (deferred) | 6.51GB bitmaps / **14.51GB RSS** @104.6M, ~62 B/rec |
| Mutation throughput | ~1.3M ops/s, reader p99 0.1-0.4µs flat | — |
| Re-freeze fold burn | 50K trig/500K ceiling, zero 503s @22% util, self-heal 4.9s | — |
| Fold (single-thread) | 727s @0.16M/s, rebuild 342s, heap flat 5.1→6.0GB | — |

B3 query latency is in the same ballpark; **B3's memory footprint at scale is not yet comparable** (the one number that matters for a serving box — RSS — is unmeasured).

## Architecture deltas vs v2 (load-bearing decisions)
- **Zero-heap frozen-mmap CRoaring** via raw DllImport (`RawRoaringBitmap`/`FrozenBitmapSilo`), not in-heap roaring-rs.
- **Reader-epoch RCU + immutable-snapshot publish** (`OpsOverlay` ImmutableDictionary, release/acquire) — same "readers never block, single writer publishes" as v2's ArcSwap, different mechanism.
- **Overlay re-freeze fold** — writes land in an ops overlay applied on-read; soft-trigger inline fold re-freezes a generation-numbered `BaseGeneration` with per-gen reclaim + hard-ceiling backpressure. B3's compaction/merge equivalent, folded into the read path.
- **Delta-fold ops-on-read** (`7cc4fc9`) — op-size inline delta, not bitmap-population clone (was the P0bis 6-8× inflation bug, now fixed for 2 of 3 sites).
- **Multi-silo + auto cross-silo indexes** — the biggest departure. Per-table silos keyed by own source PK; `CrossSiloIndexBuilder` auto-materializes `(field,value)→bitmap` in image-slot space by walking FK dot-paths declared in `.bdx`. v2 flattens in the sidecar; B3 hides joins behind the DDL.
- **Batch-framed durable ops-log + fsync ack gate**, one logical mutation = one atomic op-batch (alive LAST), `WaitForApply` watermark.
- **`.bdx` config-as-source-of-truth DSL** (hand-written parser, `POST /ddl` validate+append).

## Timeline / cutover statements
**None.** No date, no committed cutover anywhere in docs. What the docs DO say:
- Framed as the V3 rebuild for the same Civitai ~115M workload.
- **"Rust V2 numbers are not C# targets"** (`roadmap.md:189`) — deliberately an independent rebuild, not a port; every ADR records its own .NET measurement.
- Reads v2's data + config as ground truth (`bitdex-v2/data/load_stage/`, names `prod-sync-config-civitai.yaml` "authoritative").
- Human-gated, slow, correctness-first: "STOP and surface to Justin" on any no-clean-winner gate; every chunk design-reviewed with Ivanna before commit. Not deadline-driven.

## Synthesis
B3 is a genuinely well-engineered query/storage engine — proven at full scale, with a cleaner cross-silo architecture than v2's flatten-in-sidecar model, and a natural greenfield home for the sortAt+pending-bitmap redesign (its own spec already proposes the AND-NOT-future shape). But it is **~half a system**: sync/ingestion, incremental cross-silo maintenance, cache, query-format parsers, observability, HA, and auth are absent or paper-only — and **the scheduled-publish problem causing today's pain is unsolved in B3 too.** Patching v2 keeps a working sync layer and fixes sortAt in place. Adopting B3 means rebuilding the sync layer from scratch AND solving scheduled-publish for the first time there. The rebuild buys a better engine and a clean date-model slate; it does not buy a shortcut past the problem in front of you.
