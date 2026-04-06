//! Query execution methods for ConcurrentEngine.
//!
//! Extracted from concurrent_engine/mod.rs. Contains the public query entry
//! points and the private helpers they rely on.

use std::sync::Arc;
use std::time::Instant;
use parking_lot::MutexGuard;
use super::ConcurrentEngine;
use crate::silos::cache;
use crate::silos::cache_silo::UnifiedKey;
use crate::error::Result;
use crate::engine::executor::QueryExecutor;
use crate::query::planner;
use crate::query::{BitdexQuery, FilterClause, SortClause};
use crate::query::metrics::{QueryTrace, QueryTraceCollector, SortTrace};
use crate::time_buckets::TimeBucketManager;
use crate::types::QueryResult;

impl ConcurrentEngine {
    /// Execute a query from individual filter/sort/limit components.
    pub fn query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<QueryResult> {
        let slots_r = self.slots.read();
        let filters_r = self.filters.read();
        let sorts_r = self.sorts.read();
        let silo_guard = self.bitmap_silo.as_ref().map(|s| s.read());
        let tb_guard: Option<MutexGuard<TimeBucketManager>> = self.time_buckets.as_ref().map(|tb| tb.lock());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let dicts = if self.dictionaries.is_empty() { None } else { Some(&*self.dictionaries) };
        let executor = QueryExecutor::new_full(
            &*slots_r,
            &*filters_r,
            &*sorts_r,
            self.config.max_page_size,
            silo_guard.as_deref(),
            self.string_maps.as_ref().map(|m| &**m),
            self.case_sensitive_fields.as_ref().map(|c| &**c),
            dicts,
            tb_guard.as_deref().map(|tb| (tb, now_unix)),
        );
        let (filter_arc, use_simple_sort) =
            self.resolve_filters(&executor, filters, tb_guard.as_deref(), now_unix, silo_guard.as_deref())?;
        let result =
            executor.execute_from_bitmap(&filter_arc, sort, limit, None, use_simple_sort)?;
        Ok(result)
    }

    pub fn execute_query(&self, query: &BitdexQuery) -> Result<QueryResult> {
        self.execute_query_impl(query, None)
    }

    /// Core query implementation used by both execute_query and execute_query_with_collector.
    /// When `collector` is Some, per-clause timings and cache hit/miss are recorded.
    fn execute_query_impl(
        &self,
        query: &BitdexQuery,
        collector: Option<&mut QueryTraceCollector>,
    ) -> Result<QueryResult> {
        let slots_r = self.slots.read();
        let filters_r = self.filters.read();
        let sorts_r = self.sorts.read();
        let silo_guard = self.bitmap_silo.as_ref().map(|s| s.read());
        let tb_guard: Option<MutexGuard<TimeBucketManager>> = self.time_buckets.as_ref().map(|tb| tb.lock());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let dicts = if self.dictionaries.is_empty() { None } else { Some(&*self.dictionaries) };
        let executor = QueryExecutor::new_full(
            &*slots_r,
            &*filters_r,
            &*sorts_r,
            self.config.max_page_size,
            silo_guard.as_deref(),
            self.string_maps.as_ref().map(|m| &**m),
            self.case_sensitive_fields.as_ref().map(|c| &**c),
            dicts,
            tb_guard.as_deref().map(|tb| (tb, now_unix)),
        );
        // ── Snap range filters to bucket bitmaps BEFORE cache key ──
        // This ensures cache keys use stable bucket names ("7d") instead of
        // moving timestamps, so all queries within the same bucket window share
        // a single cache entry.
        let snapped_filters;
        let effective_filters = if let Some(ref tb) = tb_guard {
            let mut managers = std::collections::HashMap::new();
            managers.insert(tb.field_name().to_string(), &**tb);
            let ctx = crate::query::BucketSnapContext {
                managers: &managers,
                now_secs: now_unix,
                tolerance_pct: 0.10,
                always_snap: true,
                bitmap_silo: silo_guard.as_deref(),
            };
            snapped_filters = crate::query::snap_range_clauses(&query.filters, &ctx);
            &snapped_filters[..]
        } else {
            &query.filters[..]
        };

        // ── Fast path: CacheSilo hit ──
        // Check the silo BEFORE computing filters. On hit we skip the expensive
        // filter bitmap computation entirely (~2ms saved at 105M scale).
        let cache_disabled = self.config.cache.max_entries == 0 || self.config.cache.max_bytes == 0;
        let use_cache = !cache_disabled && !query.skip_cache && query.sort.is_some();
        let cache_key_opt = if use_cache {
            if let Some(sort_clause) = query.sort.as_ref() {
                cache::canonicalize(effective_filters).map(|clauses| {
                    let ukey = UnifiedKey {
                        filter_clauses: clauses,
                        sort_field: sort_clause.field.clone(),
                        direction: sort_clause.direction,
                    };
                    (crate::silos::cache_silo::hash_unified_key(&ukey), ukey)
                })
            } else {
                None
            }
        } else {
            None
        };

        if let Some((key_hash, ref _ukey)) = cache_key_opt {
            if let Some(ref silo_arc) = self.cache_silo {
                #[cfg(feature = "dump-timing")]
                let _t_cache_start = std::time::Instant::now();
                let cs = silo_arc.read();
                let entry_opt = cs.get_entry(key_hash);
                let field_ops_cursor = cs.field_ops_cursor();
                drop(cs);
                #[cfg(feature = "dump-timing")]
                let _t_cache_get = _t_cache_start.elapsed();
                if let Some(entry) = entry_opt {
                    // Meta-index registration is handled at startup (load_all → rebuild)
                    // and at seed time (query path below). No lazy registration needed.

                    // ── Field ops overlay: patch bitmap from pending mutations ──
                    // Instead of epoch-based invalidation, scan the field ops log
                    // from the entry's last_applied_offset to discover pending changes.
                    let has_pending_ops = field_ops_cursor > entry.last_applied_offset;
                    let mut patched_bm = entry.bitmap.clone();
                    let mut patched_keys = entry.sorted_keys.clone();
                    let mut overlay_applied = false;
                    let mut cache_stale = false;

                    if has_pending_ops {
                        // Scan pending field ops
                        let mut pending_ops: Vec<crate::silos::field_ops_log::FieldOp> = Vec::new();
                        let cs = silo_arc.read();
                        cs.scan_field_ops_from(entry.last_applied_offset, |op| pending_ops.push(op));
                        drop(cs);

                        if !pending_ops.is_empty() {
                            // Build field_idx → name map for clause matching
                            let field_map = silo_guard.as_ref()
                                .map(|s| s.field_idx_to_name_map())
                                .unwrap_or_default();
                            // Find sort field idx
                            let sort_field_idx = query.sort.as_ref().and_then(|sc| {
                                field_map.iter()
                                    .find(|(_, name)| name.as_str() == sc.field)
                                    .map(|(&idx, _)| idx)
                            });

                            let overlay_result = crate::silos::cache_overlay::apply_field_ops(
                                &entry.key.filter_clauses,
                                sort_field_idx,
                                &mut patched_bm,
                                &mut patched_keys,
                                &pending_ops,
                                &field_map,
                            );

                            if overlay_result.needs_epoch_fallback {
                                // Negation/range/compound clauses — fall back to epoch check
                                cache_stale = entry.is_stale(|field| self.field_epoch(field));
                            } else {
                                overlay_applied = true;
                            }

                            // Surgical sorted_keys update: remove/insert slots instead of
                            // nuking sorted_keys (which causes expensive bitmap traversal).
                            if overlay_applied && (!overlay_result.slots_removed.is_empty() || !overlay_result.slots_added.is_empty()) {
                                if let Some(ref mut keys) = patched_keys {
                                    // Remove slots (scan for matching slot_id in lower 32 bits)
                                    for &slot in &overlay_result.slots_removed {
                                        keys.retain(|&k| (k & 0xFFFF_FFFF) as u32 != slot);
                                    }
                                    // Insert slots with their sort values
                                    if !overlay_result.slots_added.is_empty() {
                                        if let Some(sort_clause) = query.sort.as_ref() {
                                            let direction = sort_clause.direction;
                                            for &slot in &overlay_result.slots_added {
                                                // Reconstruct sort value from BitmapSilo
                                                let sort_val = if let Some(ref silo) = silo_guard {
                                                    let num_bits = self.config.sort_fields.iter()
                                                        .find(|s| s.name == sort_clause.field)
                                                        .map(|s| s.bits as usize)
                                                        .unwrap_or(32);
                                                    crate::engine::frozen_sort::frozen_reconstruct_value(
                                                        silo, &sort_clause.field, num_bits, slot,
                                                    )
                                                } else {
                                                    sorts_r.get_field(&sort_clause.field)
                                                        .map(|f| f.reconstruct_value(slot))
                                                        .unwrap_or(0)
                                                };
                                                let packed = ((sort_val as u64) << 32) | (slot as u64);
                                                // Binary search insert at the right position
                                                let pos = match direction {
                                                    crate::query::SortDirection::Desc => {
                                                        // Desc: higher values first
                                                        keys.partition_point(|&k| k > packed)
                                                    }
                                                    crate::query::SortDirection::Asc => {
                                                        // Asc: lower values first
                                                        keys.partition_point(|&k| k < packed)
                                                    }
                                                };
                                                keys.insert(pos, packed);
                                            }
                                        }
                                    }
                                }
                            }

                            #[cfg(feature = "dump-timing")]
                            eprintln!(
                                "  [cache-overlay] ops={} applied={} fallback={} sort_changed={} added={} removed={}",
                                pending_ops.len(), overlay_result.applied_count,
                                overlay_result.needs_epoch_fallback, overlay_result.sort_changed,
                                overlay_result.slots_added.len(), overlay_result.slots_removed.len(),
                            );
                        }
                    }

                    // Bucket drift is handled by the flush thread's incremental
                    // time bucket refresh — expired slots are removed via BitmapSilo
                    // CLEARs which produce FieldOps caught by the overlay above.

                    if cache_stale {
                        tracing::debug!(
                            "cache_stale: entry epoch={} has stale fields (epoch fallback), forcing miss",
                            entry.epoch
                        );
                        // Fall through to slow path below (entry will be re-seeded)
                    } else {
                    let sort_clause = query.sort.as_ref().unwrap();
                    let has_more = entry.has_more;
                    let min_val = entry.min_tracked_value;
                    // Keep the original total_matched — the patched bitmap is bounded (top-K),
                    // not the full filter result. Overlay adjustments are local to the window.
                    let total = entry.total_matched;
                    let cached_bm = Arc::new(patched_bm);
                    let sorted_keys = patched_keys;

                    // Check if cursor is within the cached boundary
                    let needs_expansion = if let Some(cursor) = query.cursor.as_ref() {
                        let strictly_past = match sort_clause.direction {
                            crate::query::SortDirection::Desc => cursor.sort_value < min_val as u64,
                            crate::query::SortDirection::Asc => cursor.sort_value > min_val as u64,
                        };
                        if strictly_past {
                            true
                        } else if cursor.sort_value == min_val as u64 {
                            !cached_bm.contains(cursor.slot_id)
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if !needs_expansion {
                        // CACHE HIT: serve directly (with overlay patches applied)
                        let offset = if query.cursor.is_none() { query.offset.unwrap_or(0) } else { 0 };
                        let fetch_limit = query.limit.saturating_add(offset);
                        #[cfg(feature = "dump-timing")]
                        let _t_exec_start = std::time::Instant::now();
                        let mut result = if let Some(ref keys) = sorted_keys {
                            executor.execute_from_sorted_keys(
                                keys, &sort_clause.field, sort_clause.direction,
                                fetch_limit, query.cursor.as_ref(), total,
                            )?
                        } else {
                            let use_simple = cached_bm.len() < 10_000;
                            executor.execute_from_bitmap(
                                &cached_bm, query.sort.as_ref(), fetch_limit,
                                query.cursor.as_ref(), use_simple,
                            )?
                        };
                        #[cfg(feature = "dump-timing")]
                        let _t_exec = _t_exec_start.elapsed();
                        result.total_matched = total;
                        // Apply offset
                        if offset > 0 && !result.ids.is_empty() {
                            if offset >= result.ids.len() {
                                result.ids.clear();
                                result.cursor = None;
                            } else {
                                result.ids = result.ids.split_off(offset);
                                if let Some(&last_id) = result.ids.last() {
                                    let slot = last_id as u32;
                                    if let Some(sf) = sorts_r.get_field(&sort_clause.field) {
                                        result.cursor = Some(crate::query::CursorPosition {
                                            sort_value: sf.reconstruct_value(slot) as u64,
                                            slot_id: slot,
                                        });
                                    }
                                }
                            }
                        }
                        #[cfg(feature = "dump-timing")]
                        eprintln!(
                            "  [cache-hit] get={:.0}μs exec={:.0}μs total={:.0}μs bm_len={} keys={} overlay={}",
                            _t_cache_get.as_micros(),
                            _t_exec_start.elapsed().as_micros(),
                            _t_cache_start.elapsed().as_micros(),
                            cached_bm.len(),
                            sorted_keys.as_ref().map(|k| k.len()).unwrap_or(0),
                            overlay_applied,
                        );
                        return Ok(result);
                    }
                    // Cache boundary exceeded — fall through to full recompute below.
                    let _ = has_more;
                    } // end else (not stale)
                }
            }
        }

        // ── Cache miss (or skip_cache, or no sort) — full filter+sort path ──
        let filter_start = Instant::now();
        let (filter_arc, use_simple_sort) = if let Some(ref c) = collector {
            let _ = c;
            self.resolve_filters(&executor, effective_filters, tb_guard.as_deref(), now_unix, silo_guard.as_deref())?
        } else {
            self.resolve_filters(&executor, effective_filters, tb_guard.as_deref(), now_unix, silo_guard.as_deref())?
        };
        let filter_elapsed = filter_start.elapsed();
        let full_total_matched = filter_arc.len();
        tracing::debug!(
            "cache_miss: resolve_filters={:.1}ms matched={}",
            filter_elapsed.as_secs_f64() * 1000.0, full_total_matched
        );

        let offset = if query.cursor.is_none() { query.offset.unwrap_or(0) } else { 0 };
        let fetch_limit = query.limit.saturating_add(offset);

        // For sorted queries with a cache key, seed the cache with initial_capacity results.
        if let Some((key_hash, ref ukey)) = cache_key_opt {
            let sort_clause = query.sort.as_ref().unwrap();
            let initial_cap = self.config.cache.initial_capacity;
            let min_filter_size = self.config.cache.min_filter_size as u64;

            if full_total_matched >= min_filter_size && full_total_matched > 0 {
                let seed_result = executor.execute_from_bitmap_unclamped(
                    &filter_arc,
                    query.sort.as_ref(),
                    initial_cap,
                    None,
                    use_simple_sort,
                )?;
                let sort_field = sorts_r.get_field(&sort_clause.field);
                let sorted_slots: Vec<u32> = seed_result.ids.iter().map(|&id| id as u32).collect();
                let has_more = full_total_matched > sorted_slots.len() as u64;
                let value_fn = |slot: u32| -> u32 {
                    sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
                };
                let min_tracked_value = sorted_slots.last().map(|&s| value_fn(s)).unwrap_or(0);
                // Build sorted_keys packed as (sort_value << 32 | slot_id) in traversal order
                let sorted_keys: Vec<u64> = sorted_slots.iter()
                    .map(|&s| ((value_fn(s) as u64) << 32) | (s as u64))
                    .collect();
                // Build entry bitmap
                let mut bm = roaring::RoaringBitmap::new();
                for &slot in &sorted_slots { bm.insert(slot); }
                // Tag the entry with the current epoch so staleness can be detected.
                // Include __alive__ so inserts/deletes invalidate cached results that
                // implicitly depend on the alive set (e.g. negation queries, count queries).
                let current_epoch = self.mutation_epoch();
                let mut entry_field_epochs: Vec<(String, u64)> = ukey.filter_clauses.iter()
                    .map(|c| (c.field.clone(), self.field_epoch(&c.field)))
                    .collect();
                entry_field_epochs.push(("__alive__".to_string(), self.field_epoch("__alive__")));
                // Include the sort field so sort-value mutations (reactionCount etc.)
                // invalidate entries that depend on that sort order.
                entry_field_epochs.push((sort_clause.field.clone(), self.field_epoch(&sort_clause.field)));
                let entry_data = crate::silos::cache_silo::CacheEntryData {
                    key: ukey.clone(),
                    bitmap: bm,
                    min_tracked_value,
                    capacity: sorted_slots.len(),
                    max_capacity: self.config.cache.max_capacity,
                    has_more,
                    total_matched: full_total_matched,
                    direction: sort_clause.direction,
                    sorted_keys: if sorted_keys.is_empty() { None } else { Some(sorted_keys.clone()) },
                    epoch: current_epoch,
                    field_epochs: entry_field_epochs,
                    last_applied_offset: self.cache_silo.as_ref()
                        .map(|s| s.read().field_ops_cursor())
                        .unwrap_or(0),
                    bucket_cutoff_at_formation: 0, // Set by caller if entry uses bucket clause
                };
                // Save to silo and register in meta-index
                if let Some(ref silo_arc) = self.cache_silo {
                    let mut cs = silo_arc.write();
                    if let Err(e) = cs.save_entry(key_hash, &entry_data) {
                        eprintln!("CacheSilo: save_entry error: {e}");
                    }
                    // Register in meta-index for field ops routing
                    if let Some(ref bsilo) = silo_guard {
                        for clause in &ukey.filter_clauses {
                            if let Some(field_idx) = bsilo.field_id(&clause.field) {
                                match clause.op.as_str() {
                                    "eq" => {
                                        if let Ok(val) = clause.value_repr.parse::<u64>() {
                                            cs.meta_register_exact(key_hash, field_idx, val);
                                        }
                                    }
                                    "in" => {
                                        for v in clause.value_repr.split(',') {
                                            if let Ok(val) = v.parse::<u64>() {
                                                cs.meta_register_exact(key_hash, field_idx, val);
                                            }
                                        }
                                    }
                                    _ => {
                                        // Negation, range, compound, bucket → wildcard
                                        cs.meta_register_wildcard(key_hash, field_idx);
                                    }
                                }
                            }
                        }
                        // Register sort field as wildcard dependency
                        if let Some(sort_idx) = bsilo.field_id(&sort_clause.field) {
                            cs.meta_register_wildcard(key_hash, sort_idx);
                        }
                    }
                }
                // Serve from the freshly seeded entry
                let mut result = if !sorted_keys.is_empty() {
                    executor.execute_from_sorted_keys(
                        &sorted_keys, &sort_clause.field, sort_clause.direction,
                        fetch_limit, query.cursor.as_ref(), full_total_matched,
                    )?
                } else {
                    executor.execute_from_bitmap(
                        &filter_arc, query.sort.as_ref(), fetch_limit,
                        query.cursor.as_ref(), use_simple_sort,
                    )?
                };
                result.total_matched = full_total_matched;
                if offset > 0 && !result.ids.is_empty() {
                    if offset >= result.ids.len() {
                        result.ids.clear();
                        result.cursor = None;
                    } else {
                        result.ids = result.ids.split_off(offset);
                        if let Some(&last_id) = result.ids.last() {
                            let slot = last_id as u32;
                            if let Some(sf) = sorts_r.get_field(&sort_clause.field) {
                                result.cursor = Some(crate::query::CursorPosition {
                                    sort_value: sf.reconstruct_value(slot) as u64,
                                    slot_id: slot,
                                });
                            }
                        }
                    }
                }
                return Ok(result);
            }
        }

        // ── No cache (skip_cache, no sort, or too small) — plain execute ──
        let mut result = executor.execute_from_bitmap(
            &filter_arc, query.sort.as_ref(), fetch_limit,
            query.cursor.as_ref(), use_simple_sort,
        )?;
        result.total_matched = full_total_matched;
        if offset > 0 && !result.ids.is_empty() {
            if offset >= result.ids.len() {
                result.ids.clear();
                result.cursor = None;
            } else {
                result.ids = result.ids.split_off(offset);
                if let Some(sort_clause) = query.sort.as_ref() {
                    if let Some(&last_id) = result.ids.last() {
                        let slot = last_id as u32;
                        if let Some(sf) = sorts_r.get_field(&sort_clause.field) {
                            result.cursor = Some(crate::query::CursorPosition {
                                sort_value: sf.reconstruct_value(slot) as u64,
                                slot_id: slot,
                            });
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    /// Execute a query and produce a trace alongside the result.
    /// The trace captures overall timing, per-clause filter metrics (on cache miss),
    /// sort timing, and cache hit/miss status.
    ///
    /// Unlike the previous implementation which ran filters twice (once for tracing,
    /// once for the real result), this threads the trace collector through the real
    /// query path so timings reflect actual execution.
    pub fn execute_query_traced(&self, query: &BitdexQuery, index_name: &str) -> Result<(QueryResult, QueryTrace)> {
        let mut collector = QueryTraceCollector::new();
        let result = self.execute_query_with_collector(query, &mut collector)?;
        if let Some(sort_clause) = query.sort.as_ref() {
            collector.record_sort(SortTrace {
                field: sort_clause.field.clone(),
                dir: format!("{:?}", sort_clause.direction),
                input: result.total_matched,
                output: result.ids.len() as u64,
                time_us: collector.sort_us,
            });
        }
        let trace = collector.finalize(index_name, result.total_matched as u64);
        Ok((result, trace))
    }

    /// Execute a query while recording trace metrics into the collector.
    /// Mirrors `execute_query` but threads the collector through the real
    /// cache-aware path so timings are accurate.
    fn execute_query_with_collector(
        &self,
        query: &BitdexQuery,
        collector: &mut QueryTraceCollector,
    ) -> Result<QueryResult> {
        collector.lazy_load_us = 0;
        let filter_start = Instant::now();
        // Run the same unified path; trace fields are populated after the fact
        // from the result (total_matched, sort field). Per-clause tracing can be
        // re-added here in the future by threading the collector into resolve_filters.
        let result = self.execute_query_impl(query, None)?;
        collector.filter_us = filter_start.elapsed().as_micros() as u64;
        Ok(result)
    }

    /// Resolve filter clauses to a bitmap.
    ///
    /// Snaps range filters to time bucket bitmaps, plans clause ordering,
    /// and computes the filter intersection.
    fn resolve_filters(
        &self,
        executor: &QueryExecutor,
        filters: &[FilterClause],
        time_buckets: Option<&TimeBucketManager>,
        now_unix: u64,
        silo: Option<&crate::silos::bitmap_silo::BitmapSilo>,
    ) -> Result<(Arc<roaring::RoaringBitmap>, bool)> {
        // Snap range filters to pre-computed time bucket bitmaps (C3).
        // This must happen BEFORE canonicalization so cache keys use stable
        // bucket names ("7d") instead of moving timestamps.
        let snapped;
        let effective_filters = if let Some(tb) = time_buckets {
            let mut managers = std::collections::HashMap::new();
            managers.insert(tb.field_name().to_string(), tb);
            let ctx = crate::query::BucketSnapContext {
                managers: &managers,
                now_secs: now_unix,
                tolerance_pct: 0.10,
                always_snap: true,
                bitmap_silo: silo,
            };
            snapped = crate::query::snap_range_clauses(filters, &ctx);
            &snapped[..]
        } else {
            filters
        };
        let planner_ctx = planner::PlannerContext {
            string_maps: executor.string_maps(),
            dictionaries: executor.dictionaries(),
            bitmap_silo: executor.bitmap_silo(),
        };
        let plan = planner::plan_query_with_context(effective_filters, executor.filter_index(), executor.slot_allocator(), Some(&planner_ctx));
        let filter_bitmap = Arc::new(executor.compute_filters(&plan.ordered_clauses)?);
        Ok((filter_bitmap, plan.use_simple_sort))
    }

}
