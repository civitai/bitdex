# Learnings

Things we tried that didn't work, or where a simpler approach beat a more sophisticated design. Organized by topic.

**Purpose:** Prevent future agents from re-trying approaches we've already evaluated. Before proposing a new design in any of these areas, read the relevant learnings file first.

## Files

- `write-pipeline.md` — Backpressure, persist thread, bulk accumulator
- `storage.md` — Tiered caching, redb, filesystem persistence
- `ingestion.md` — NDJSON parsing, simd-json, parallel loading
