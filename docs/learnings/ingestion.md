# Ingestion Learnings

- **Parsing is NOT the bottleneck**: Measured single-threaded parse+convert at 287K/s, parallel at 2.58M/s (16 threads). The write path (bitmap apply + docstore) is 50-60x slower than available parsing capacity. Effort to parallelize parsing yields diminishing returns. The fused parse+bitmap loader with rayon fold+reduce achieves 345K/s sustained, which is bound by bitmap operations, not parsing.

- **simd-json and rkyv not needed**: Evaluated simd-json for faster JSON parsing and rkyv for zero-copy deserialization. Neither implemented because parsing headroom is already 50-60x above the write path ceiling. Standard serde_json is sufficient.

- **85% of bulk load time is single-threaded NDJSON parsing**: At 104M records with 8 threads, 85% of wall time was the sequential NDJSON line-splitting pass, only 12% was bitmap operations. The fused loader with memmap2 + parallel fold resolved this.
