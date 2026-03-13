# Write Pipeline Learnings

- **Loading mode vs adaptive pressure levels**: We designed a 4-tier pressure system (idle/warm/hot/critical) with adaptive publish cadence based on channel fill ratio. Never implemented — the simpler binary loading_mode toggle (enter_loading_mode/exit_loading_mode) achieves the same throughput for bulk loads without the complexity. Loading mode skips snapshot publishing during bulk inserts, avoiding the Arc::make_mut() clone cascade. For steady-state mixed workloads, the current approach is adequate. Adaptive pressure would only help if sustained mixed read/write at high volume becomes a bottleneck.

- **Persist thread decoupling**: Designed a dedicated persist thread separate from the flush thread to prevent docstore I/O from blocking snapshot publishing. Not implemented — bulk loads use loading mode which skips docstore writes anyway, and steady-state write volume (100-1000/s) doesn't bottleneck the flush thread.

- **BulkAccumulator per-thread**: Designed per-thread accumulators that batch (field, value) → Vec<slot_id> mappings before converting to bitmaps. Partially implemented in the fused parse+bitmap loader (rayon fold+reduce pattern), but not exposed as a public API. The fold+reduce approach achieves 345K/s which exceeds the 300K/s target.
