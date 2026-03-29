# The BitDex Deployment Saga

A 105-million-record bitmap index engine meets production Kubernetes. What could go wrong?

## The Setup

BitDex is a roaring bitmap index engine. It holds 107M image records in ~7GB of bitmaps and answers filter+sort queries in microseconds. The challenge: get all that data loaded from Postgres into the engine running on a K8s node shared with an 80GB Meilisearch instance.

## Round 1: Just Stream It

First attempt: `COPY TO STDOUT` from Postgres, 5 parallel streams, build bitmaps on the fly. Tags alone is 4.5 billion rows.

**Problem:** The `Image + Post` LEFT JOIN query ran at 8K rows/sec. Postgres was serving 281 concurrent queries, all stuck on I/O. We killed them. Speed jumped to 568K/sec. Then they came back.

## Round 2: Fix Everything

Over 13 bulk load attempts, we hit and fixed:

- **INT4 vs INT8 mismatch** in sqlx (v1.0.3)
- **Trigger function using `NEW.id`** on join tables that have `imageId` (crashed 32 CDC sink tasks)
- **`statement_timeout = 120s`** on the Postgres role killing our COPY streams at exactly 2 minutes
- **ORDER BY on COPY queries** forcing Postgres to sort 4.5B rows before streaming
- **Slot arena out-of-bounds** when new images were inserted during the load
- **FluxCD auto-healing** the StatefulSet back to 1 replica every time we scaled to 0

Tags hit 2.6M rows/sec with ORDER BY removed. But connections kept dropping.

## Round 3: Two-Phase Loader

Streaming from Postgres was fundamentally unstable. We split the loader: Phase 1 downloads CSV files to disk, Phase 2 builds bitmaps from local files.

**Problem:** `kubectl cp` can't copy between two pods. The 63GB tags CSV broke the K8s API server pipe. Solution: spin up a `python3 -m http.server` on the Postgres pod, `wget` from the BitDex pod. 63GB transferred at 240MB/sec in 5 minutes.

## Round 4: OOM

Local file loading worked. Images at 250K/sec, tags at 2.6M/sec. Then: `OOMKilled`.

The 60GB memory-mapped slot arena (512 bytes x 124M slots) was the culprit. Kubernetes cgroups counts dirty mmap pages against the container memory limit. Random tag writes across 124M slots at 2.5M/sec forced pages into resident memory faster than the kernel could flush them. Even 48GB wasn't enough.

## The Breakthrough

The arena exists to store per-image data for the docstore (url, hash, tag lists). But every multi-value field (tags, tools, techniques, modelVersionIds) is already fully represented in the bitmaps. You can reconstruct "which tags does image X have?" by iterating the tag bitmaps.

Iterating 31K bitmaps per image would take 18 days. But roaring bitmaps have internal containers aligned at 65,536-slot boundaries. Process in 65K-slot chunks: for each chunk, call `bitmap.range(start..end)` on each tag bitmap. This jumps directly to the relevant container and iterates only set bits. Total work equals total associations (4.5B), not bitmaps x slots (3.3T).

**Benchmarked: 60M associations/sec, 75 seconds projected for 4.5B tags, 16MB working memory.**

## The Architecture

1. Build bitmaps from CSV (tags, tools, techniques, resources) -- pure bitmap construction, ~7GB peak
2. Process images CSV sequentially -- for each 65K-slot chunk, iterate all bitmaps to reconstruct multi-value fields, combine with scalar fields from the CSV row, write directly to docstore shards
3. No arena. No 60GB mmap. Peak ~8GB. Build phase under 5 minutes.

## Lessons

- **Kubernetes cgroups counts mmap pages.** A 60GB mmap that "should be fine because it's file-backed" will OOMKill you.
- **Postgres under load is a different database.** 281 concurrent queries turned 568K/sec into 8K/sec.
- **Pod-to-pod HTTP beats kubectl cp.** Python's `http.server` + `wget` transferred 63GB in 5 minutes. `kubectl cp` through the API server couldn't handle 14GB.
- **The data you need is already in the bitmaps.** You built the index -- use it to build the documents.
- **Roaring's container alignment is a superpower.** 65K-slot chunks turn an O(bitmaps x slots) problem into O(associations).
