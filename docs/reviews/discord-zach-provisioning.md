# Discord Message — BitDex Provisioning Request

**To:** Zach
**From:** Justin
**Re:** Replacing Meilisearch with BitDex — need a machine provisioned for shadow testing

---

Hey Zach,

I've been building a replacement for Meilisearch called BitDex. It's a single-binary bitmap index engine in Rust — no JVM, no cluster, no proxy layer. One process handles filtering, sorting, and document serving for our full 105M image dataset. I want to run it in shadow mode alongside Meilisearch so we can validate it at production scale before cutting over.

## What it replaces

BitDex replaces both Meilisearch **and** the Meilisearch proxy. One binary, one process. No more multi-node coordination.

## Resource requirements

### Memory: 32 GB

- **Loading (startup):** peaks at ~20 GB while building indexes
- **Stable state (105M records):** 14.5 GB RSS
- **Headroom for queries + cache:** ~5-10 GB

Bitmap memory is 6.5 GB. The rest is the document store and allocator overhead. Memory scales linearly — roughly 62 bytes per record in bitmaps.

### Disk: 200 GB, NVMe strongly preferred

- **NDJSON input file (one-time load):** ~70 GB
- **On-disk document store:** ~7 GB
- **Bitmap snapshots:** ~3 GB
- **Working space:** ~20 GB

NVMe matters because the document store does random reads for upsert diffing. On spinning disk, writes slow down significantly. Reads are fine either way (everything's in memory).

### CPU: 4-8 cores

- **Loading (startup, ~5-10 min):** high — saturates available threads parsing NDJSON + building bitmaps
- **Serving queries:** low — sub-millisecond queries, lock-free reads, minimal CPU even at 20K QPS

More cores speed up the initial data load. After that, CPU is nearly idle. A single core handles thousands of queries per second. For K8s: request 2 cores, limit 8. The burst matters only during startup.

## Performance comparison

### Query latency (105M records, single node)

- **Sparse filter (userId lookup):** Meilisearch 50-200ms → BitDex **0.03ms**
- **Dense filter + sort:** Meilisearch 100-500ms → BitDex **0.08-0.25ms**
- **Throughput (1 connection):** Meilisearch ~50 QPS → BitDex **1,461 QPS**
- **Throughput (16 connections):** Meilisearch N/A (multi-node) → BitDex **19,223 QPS**

### Write performance

Meilisearch collapsed at scale — 300 documents in 4 minutes (0.075 docs/sec). BitDex loads 105M records in about 5-6 minutes at 320K docs/sec.

### Resource efficiency

- **RAM:** Meilisearch needs 64+ GB per node across multiple nodes. BitDex uses 14.5 GB on a single node.
- **Disk:** Meilisearch indexes are large. BitDex needs ~10 GB (docstore + snapshots).
- **CPU (steady state):** Meilisearch moderate. BitDex near-zero.
- **Nodes:** Meilisearch requires multiple. BitDex runs on 1.
- **Proxy layer:** Meilisearch requires one. BitDex eliminates it.

## What I need

1. **A machine or pod** with 32 GB RAM, 200 GB NVMe, 4-8 cores
2. **Network access** to receive traffic from the API layer (port 3000, configurable)
3. **Ability to load the NDJSON file** — either mount a volume with the data or transfer it in

The plan is to run BitDex alongside Meilisearch in shadow mode: production queries hit both, we compare results and latency, then cut over once validated. This also eliminates the Meilisearch proxy entirely.

Do we already have a machine that fits this profile, or do we need to spin something up? Happy to hop on a call if it's easier to talk through the K8s config.
