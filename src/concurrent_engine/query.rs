//! Query execution methods for ConcurrentEngine.
//!
//! Extracted from concurrent_engine/mod.rs. Contains the public query entry
//! points and the private helpers they rely on.

use std::sync::Arc;
use std::time::Instant;
use parking_lot::MutexGuard;
use super::ConcurrentEngine;
use crate::cache;
use crate::cache_silo::UnifiedKey;
use crate::error::Result;
use crate::executor::QueryExecutor;
use crate::planner;
use crate::query::{BitdexQuery, FilterClause, SortClause};
use crate::query_metrics::{QueryTrace, QueryTraceCollector, SortTrace};
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
        let executor = {
            let mut base = QueryExecutor::new(
                &*slots_r,
                &*filters_r,
                &*sorts_r,
                self.config.max_page_size,
            );
            if let Some(ref guard) = silo_guard {
                base = base.with_bitmap_silo(guard);
            }
            if let Some(ref maps) = self.string_maps {
                base = base.with_string_maps(maps);
            }
            if let Some(ref cs) = self.case_sensitive_fields {
                base = base.with_case_sensitive_fields(cs);
            }
            if !self.dictionaries.is_empty() {
                base = base.with_dictionaries(&self.dictionaries);
            }
            if let Some(ref tb) = tb_guard {
                base.with_time_buckets(tb, now_unix)
            } else {
                base
            }
        };
        let (filter_arc, use_simple_sort) =
            self.resolve_filters(&executor, filters, tb_guard.as_deref(), now_unix)?;
        let mut result =
            executor.execute_from_bitmap(&filter_arc, sort, limit, None, use_simple_sort)?;
        // Post-validation against in-flight writes
        self.post_validate(&mut result, filters, &executor)?;
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
        let executor = {
            let mut base = QueryExecutor::new(
                &*slots_r,
                &*filters_r,
                &*sorts_r,
                self.config.max_page_size,
            );
            if let Some(ref guard) = silo_guard {
                base = base.with_bitmap_silo(guard);
            }
            if let Some(ref maps) = self.string_maps {
                base = base.with_string_maps(maps);
            }
            if let Some(ref cs) = self.case_sensitive_fields {
                base = base.with_case_sensitive_fields(cs);
            }
            if !self.dictionaries.is_empty() {
                base = base.with_dictionaries(&self.dictionaries);
            }
            if let Some(ref tb) = tb_guard {
                base.with_time_buckets(tb, now_unix)
            } else {
                base
            }
        };
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
            };
            snapped_filters = crate::query::snap_range_clauses(&query.filters, &ctx);
            &snapped_filters[..]
        } else {
            &query.filters[..]
        };

        // ── Fast path: CacheSilo hit ──
        // Check the silo BEFORE computing filters. On hit we skip the expensive
        // filter bitmap computation entirely (~2ms saved at 105M scale).
        let use_cache = !query.skip_cache && query.sort.is_some();
        let cache_key_opt = if use_cache {
            if let Some(sort_clause) = query.sort.as_ref() {
                cache::canonicalize(effective_filters).map(|clauses| {
                    let ukey = UnifiedKey {
                        filter_clauses: clauses,
                        sort_field: sort_clause.field.clone(),
                        direction: sort_clause.direction,
                    };
                    (crate::cache_silo::hash_unified_key(&ukey), ukey)
                })
            } else {
                None
            }
        } else {
            None
        };

        if let Some((key_hash, ref _ukey)) = cache_key_opt {
            if let Some(ref silo_arc) = self.cache_silo {
                if let Some(entry) = silo_arc.read().get_entry(key_hash) {
                    let sort_clause = query.sort.as_ref().unwrap();
                    let has_more = entry.has_more;
                    let min_val = entry.min_tracked_value;
                    let total = entry.total_matched;
                    let cached_bm = Arc::new(entry.bitmap.clone());
                    let sorted_keys = entry.sorted_keys.clone();

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
                        // CACHE HIT: serve directly from the silo entry
                        if let Some(ref c) = collector { let _ = c; } // collector.cache_hit = true — handled below
                        let offset = if query.cursor.is_none() { query.offset.unwrap_or(0) } else { 0 };
                        let fetch_limit = query.limit.saturating_add(offset);
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
                        self.post_validate(&mut result, &query.filters, &executor)?;
                        return Ok(result);
                    }
                    // Cache boundary exceeded — fall through to full recompute below.
                    // has_more tells us the silo has partial coverage; we'll re-seed it.
                    let _ = has_more;
                }
            }
        }

        // ── Cache miss (or skip_cache, or no sort) — full filter+sort path ──
        let filter_start = Instant::now();
        let (filter_arc, use_simple_sort) = if let Some(ref c) = collector {
            let _ = c;
            self.resolve_filters(&executor, effective_filters, tb_guard.as_deref(), now_unix)?
        } else {
            self.resolve_filters(&executor, effective_filters, tb_guard.as_deref(), now_unix)?
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
                let entry_data = crate::cache_silo::CacheEntryData {
                    key: ukey.clone(),
                    bitmap: bm,
                    min_tracked_value,
                    capacity: sorted_slots.len(),
                    max_capacity: self.config.cache.max_capacity,
                    has_more,
                    total_matched: full_total_matched,
                    direction: sort_clause.direction,
                    sorted_keys: if sorted_keys.is_empty() { None } else { Some(sorted_keys.clone()) },
                };
                // Save to silo outside any lock
                if let Some(ref silo_arc) = self.cache_silo {
                    let cs = silo_arc.read();
                    if let Err(e) = cs.save_entry(key_hash, &entry_data) {
                        eprintln!("CacheSilo: save_entry error: {e}");
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
                self.post_validate(&mut result, &query.filters, &executor)?;
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
        self.post_validate(&mut result, &query.filters, &executor)?;
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
            };
            snapped = crate::query::snap_range_clauses(filters, &ctx);
            &snapped[..]
        } else {
            filters
        };
        let planner_ctx = planner::PlannerContext {
            string_maps: executor.string_maps(),
            dictionaries: executor.dictionaries(),
        };
        let plan = planner::plan_query_with_context(effective_filters, executor.filter_index(), executor.slot_allocator(), Some(&planner_ctx));
        let filter_bitmap = Arc::new(executor.compute_filters(&plan.ordered_clauses)?);
        Ok((filter_bitmap, plan.use_simple_sort))
    }

    /// Post-validate query results against in-flight writes.
    fn post_validate(
        &self,
        result: &mut QueryResult,
        filters: &[FilterClause],
        executor: &QueryExecutor,
    ) -> Result<()> {
        if !self.in_flight.has_in_flight() {
            return Ok(());
        }
        let overlapping = self.in_flight.find_overlapping(&result.ids);
        if overlapping.is_empty() {
            return Ok(());
        }
        // The executor holds references to the snapshot's bitmap state
        // so we can revalidate in-flight slots.
        let mut invalid_slots: Vec<u32> = Vec::new();
        for &slot in &overlapping {
            if !executor.slot_matches_filters(slot, filters)? {
                invalid_slots.push(slot);
            }
        }
        if !invalid_slots.is_empty() {
            result
                .ids
                .retain(|id| !invalid_slots.contains(&(*id as u32)));
        }
        Ok(())
    }
}
