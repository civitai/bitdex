use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::dictionary::FieldDictionary;
use crate::error::{BitdexError, Result};
use crate::filter::FilterIndex;
use crate::planner;
use crate::query::{FilterClause, SortClause, SortDirection, Value};
use crate::slot::SlotAllocator;
use crate::sort::SortIndex;
use crate::types::QueryResult;

/// Convert a Value to a u64 bitmap key for filter indexing.
/// For MappedString fields, call `resolve_value_key` instead which consults the string_map.
fn value_to_bitmap_key(val: &Value) -> Option<u64> {
    match val {
        Value::Bool(b) => Some(if *b { 1 } else { 0 }),
        Value::Integer(v) => Some(*v as u64),
        Value::Float(_) | Value::String(_) => None,
    }
}

/// Pre-computed reverse string maps: field_name → (string_value → integer_key).
/// Built from the DataSchema's FieldMapping string_map entries.
/// For case-insensitive fields (the default), keys are stored lowercase.
pub type StringMaps = HashMap<String, HashMap<String, i64>>;

/// Set of field names where string matching is case-sensitive.
/// Fields not in this set use case-insensitive matching (lowercase normalization).
pub type CaseSensitiveFields = std::collections::HashSet<String>;

/// Query executor: computes filter intersections and sort traversals.
/// Uses the query planner for cardinality-based clause ordering.
pub struct QueryExecutor<'a> {
    slots: &'a SlotAllocator,
    filters: &'a FilterIndex,
    sorts: &'a SortIndex,
    max_page_size: usize,
    time_buckets: Option<&'a crate::time_buckets::TimeBucketManager>,
    now_unix: u64,
    string_maps: Option<&'a StringMaps>,
    case_sensitive_fields: Option<&'a CaseSensitiveFields>,
    /// Live dictionaries for LowCardinalityString fields — used as fallback
    /// when string_maps snapshot doesn't have a recently-added value.
    dictionaries: Option<&'a HashMap<String, FieldDictionary>>,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(
        slots: &'a SlotAllocator,
        filters: &'a FilterIndex,
        sorts: &'a SortIndex,
        max_page_size: usize,
    ) -> Self {
        Self {
            slots,
            filters,
            sorts,
            max_page_size,
            time_buckets: None,
            now_unix: 0,
            string_maps: None,
            case_sensitive_fields: None,
            dictionaries: None,
        }
    }

    /// Attach string maps for MappedString field reverse lookup.
    /// Enables querying with `Value::String("SD 1.5")` on MappedString fields.
    pub fn with_string_maps(mut self, maps: &'a StringMaps) -> Self {
        self.string_maps = Some(maps);
        self
    }

    /// Attach case-sensitive field set for string matching control.
    pub fn with_case_sensitive_fields(mut self, fields: &'a CaseSensitiveFields) -> Self {
        self.case_sensitive_fields = Some(fields);
        self
    }

    /// Attach live dictionaries for LowCardinalityString field query resolution.
    /// Used as fallback when the string_maps snapshot doesn't have a recently-added value.
    pub fn with_dictionaries(mut self, dicts: &'a HashMap<String, FieldDictionary>) -> Self {
        self.dictionaries = Some(dicts);
        self
    }

    /// Attach a time bucket manager for in-executor bucket snapping (C3).
    /// Range filters on the bucketed field will be snapped to pre-computed bitmaps.
    pub fn with_time_buckets(mut self, tb: &'a crate::time_buckets::TimeBucketManager, now: u64) -> Self {
        self.time_buckets = Some(tb);
        self.now_unix = now;
        self
    }

    /// Get a reference to the filter index.
    pub fn filter_index(&self) -> &'a FilterIndex {
        self.filters
    }

    /// Get a reference to the slot allocator.
    pub fn slot_allocator(&self) -> &'a SlotAllocator {
        self.slots
    }

    /// Resolve a Value to a bitmap key, consulting string_maps for MappedString fields
    /// and live dictionaries for LowCardinalityString fields.
    /// Applies case-insensitive normalization (lowercase) unless the field is in case_sensitive_fields.
    fn resolve_value_key(&self, field: &str, val: &Value) -> Option<u64> {
        // Try direct conversion first (Integer, Bool)
        if let Some(key) = value_to_bitmap_key(val) {
            return Some(key);
        }
        // For String values, try the string_map reverse lookup
        if let Value::String(s) = val {
            if let Some(maps) = self.string_maps {
                if let Some(field_map) = maps.get(field) {
                    let is_case_sensitive = self.case_sensitive_fields
                        .map_or(false, |cs| cs.contains(field));
                    let lookup = if is_case_sensitive {
                        std::borrow::Cow::Borrowed(s.as_str())
                    } else {
                        std::borrow::Cow::Owned(s.to_lowercase())
                    };
                    if let Some(&v) = field_map.get(lookup.as_ref()) {
                        return Some(v as u64);
                    }
                    // Don't return None — fall through to dictionary check.
                    // LowCardinalityString fields may have values added after the
                    // string_maps snapshot was built (or the snapshot may be empty
                    // for a freshly-created index).
                }
            }
            // Fallback: check live dictionaries for LowCardinalityString fields.
            // This catches values added via upsert after the string_maps snapshot was built.
            if let Some(dicts) = self.dictionaries {
                if let Some(dict) = dicts.get(field) {
                    return dict.get(s).map(|v| v as u64);
                }
            }
        }
        None
    }

    /// Build a bitmap for a single id = N filter (intersected with alive).
    fn id_bitmap_single(&self, value: &Value) -> Result<RoaringBitmap> {
        let slot = match value {
            Value::Integer(v) => *v as u32,
            _ => return Err(BitdexError::InvalidValue {
                field: "id".to_string(),
                reason: "id must be an integer".to_string(),
            }),
        };
        let alive = self.slots.alive_bitmap();
        let mut bm = RoaringBitmap::new();
        if alive.contains(slot) {
            bm.insert(slot);
        }
        Ok(bm)
    }

    /// Build a bitmap for id IN [N1, N2, ...] filter (intersected with alive).
    fn id_bitmap_multi(&self, values: &[Value]) -> Result<RoaringBitmap> {
        let alive = self.slots.alive_bitmap();
        let mut bm = RoaringBitmap::new();
        for v in values {
            if let Value::Integer(id) = v {
                let slot = *id as u32;
                if alive.contains(slot) {
                    bm.insert(slot);
                }
            }
        }
        Ok(bm)
    }

    /// Execute a full query: plan -> filter -> sort -> paginate -> return IDs.
    pub fn execute(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> Result<QueryResult> {
        let limit = limit.min(self.max_page_size);

        // Step 1: Plan the query (reorder clauses by cardinality)
        let plan = planner::plan_query(filters, self.filters, self.slots);

        // Step 2: Compute filter bitmap using planned clause order
        let filter_bitmap = self.compute_filters(&plan.ordered_clauses)?;

        // Filter bitmaps are kept clean (no stale bits from deleted docs),
        // so no alive AND is needed here.
        let total_matched = filter_bitmap.len();

        // Step 3: Sort and paginate
        let (ids, next_cursor) = if let Some(sort_clause) = sort {
            if plan.use_simple_sort {
                self.simple_sort_and_paginate(&filter_bitmap, sort_clause, limit, cursor)?
            } else {
                self.sort_and_paginate(&filter_bitmap, sort_clause, limit, cursor)?
            }
        } else {
            // No sort: return in descending slot order (newest first)
            self.slot_order_paginate(&filter_bitmap, limit, cursor)
        };

        Ok(QueryResult {
            ids,
            cursor: next_cursor,
            total_matched,
        })
    }

    /// Check if a single slot matches all the given filter clauses.
    /// Used by post-validation to revalidate slots that overlap with in-flight writes.
    pub fn slot_matches_filters(&self, slot: u32, clauses: &[FilterClause]) -> Result<bool> {
        for clause in clauses {
            let bitmap = self.evaluate_clause(clause)?;
            if !bitmap.contains(slot) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Execute from a pre-computed filter bitmap: alive AND + sort + paginate.
    /// Used when the caller handles cache interaction separately.
    pub fn execute_from_bitmap(
        &self,
        filter_bitmap: &RoaringBitmap,
        sort: Option<&SortClause>,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
        use_simple_sort: bool,
    ) -> Result<QueryResult> {
        self.execute_from_bitmap_inner(filter_bitmap, sort, limit.min(self.max_page_size), cursor, use_simple_sort)
    }

    /// Like execute_from_bitmap but without the max_page_size clamp.
    /// Used internally for bound cache seeding where we need 10K+ results.
    pub fn execute_from_bitmap_unclamped(
        &self,
        filter_bitmap: &RoaringBitmap,
        sort: Option<&SortClause>,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
        use_simple_sort: bool,
    ) -> Result<QueryResult> {
        self.execute_from_bitmap_inner(filter_bitmap, sort, limit, cursor, use_simple_sort)
    }

    fn execute_from_bitmap_inner(
        &self,
        filter_bitmap: &RoaringBitmap,
        sort: Option<&SortClause>,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
        use_simple_sort: bool,
    ) -> Result<QueryResult> {
        let limit = limit;

        // Filter bitmaps are kept clean (no stale bits from deleted docs),
        // so no alive AND is needed.
        let total_matched = filter_bitmap.len();

        // Sort and paginate
        let (ids, next_cursor) = if let Some(sort_clause) = sort {
            if use_simple_sort {
                self.simple_sort_and_paginate(filter_bitmap, sort_clause, limit, cursor)?
            } else {
                self.sort_and_paginate(filter_bitmap, sort_clause, limit, cursor)?
            }
        } else {
            // No sort: return in descending slot order (newest first)
            self.slot_order_paginate(filter_bitmap, limit, cursor)
        };

        Ok(QueryResult {
            ids,
            cursor: next_cursor,
            total_matched,
        })
    }

    /// Compute the combined filter bitmap from a list of filter clauses.
    /// Top-level clauses are implicitly ANDed together.
    /// Clauses are expected to be pre-ordered by the planner for optimal evaluation.
    pub(crate) fn compute_filters(&self, clauses: &[FilterClause]) -> Result<RoaringBitmap> {
        if clauses.is_empty() {
            return Ok(self.slots.alive_bitmap().clone());
        }

        let mut result: Option<RoaringBitmap> = None;

        for clause in clauses {
            let bitmap = self.evaluate_clause(clause)?;
            result = Some(match result {
                Some(existing) => existing & &bitmap,
                None => bitmap,
            });
        }

        Ok(result.unwrap_or_default())
    }

    /// Evaluate a single filter clause to a bitmap.
    pub(crate) fn evaluate_clause(&self, clause: &FilterClause) -> Result<RoaringBitmap> {
        match clause {
            FilterClause::Eq(field, value) => {
                // Special case: "id" means slot ID — construct bitmap directly
                if field == "id" {
                    return self.id_bitmap_single(value);
                }
                // Try Tier 1 (snapshot FilterIndex) first — diff-aware read
                if let Some(filter_field) = self.filters.get_field(field) {
                    let key = match self.resolve_value_key(field, value) {
                        Some(k) => k,
                        // Unknown string value (e.g. LCS value never inserted).
                        // Return empty bitmap — the value simply doesn't match anything.
                        None => return Ok(RoaringBitmap::new()),
                    };
                    return Ok(filter_field
                        .get_versioned(key)
                        .map(|vb| vb.fused())
                        .unwrap_or_default());
                }
                Err(BitdexError::FieldNotFound(field.clone()))
            }

            FilterClause::NotEq(field, value) => {
                // Use andnot optimization: compute the small negated bitmap
                // and subtract from alive, instead of computing the large complement
                let eq_bitmap = self.evaluate_clause(&FilterClause::Eq(field.clone(), value.clone()))?;
                let alive = self.slots.alive_bitmap();
                let mut result = alive.clone();
                result -= &eq_bitmap;
                Ok(result)
            }

            FilterClause::In(field, values) => {
                // Special case: "id" means slot ID — construct bitmap directly
                if field == "id" {
                    return self.id_bitmap_multi(values);
                }
                // Try Tier 1 first — diff-aware union
                if let Some(filter_field) = self.filters.get_field(field) {
                    let keys: Vec<u64> = values
                        .iter()
                        .filter_map(|v| self.resolve_value_key(field, v))
                        .collect();
                    let mut result = RoaringBitmap::new();
                    for &key in &keys {
                        if let Some(vb) = filter_field.get_versioned(key) {
                            result |= vb.fused();
                        }
                    }
                    return Ok(result);
                }
                Err(BitdexError::FieldNotFound(field.clone()))
            }

            FilterClause::NotIn(field, values) => {
                // NotIn = alive - In(field, values)
                let in_bitmap = self.evaluate_clause(&FilterClause::In(field.clone(), values.clone()))?;
                let alive = self.slots.alive_bitmap();
                let mut result = alive.clone();
                result -= &in_bitmap;
                Ok(result)
            }

            FilterClause::Not(inner) => {
                // NOT uses andnot: compute inner bitmap and subtract from alive
                let inner_bitmap = self.evaluate_clause(inner)?;
                let alive = self.slots.alive_bitmap();
                let mut result = alive.clone();
                result -= &inner_bitmap;
                Ok(result)
            }

            FilterClause::And(clauses) => {
                // Optimize And sub-clauses by reordering by cardinality
                let optimized = planner::optimize_and_clause(
                    clauses,
                    self.filters,
                    self.slots.alive_count(),
                );
                let mut result: Option<RoaringBitmap> = None;
                for clause in &optimized {
                    let bitmap = self.evaluate_clause(clause)?;
                    result = Some(match result {
                        Some(existing) => existing & &bitmap,
                        None => bitmap,
                    });
                }
                Ok(result.unwrap_or_default())
            }

            FilterClause::Or(clauses) => {
                let mut result = RoaringBitmap::new();
                for clause in clauses {
                    let bitmap = self.evaluate_clause(clause)?;
                    result |= &bitmap;
                }
                Ok(result)
            }

            // C3: Try bucket snapping for Gt/Gte on timestamp fields before falling back to range_scan.
            FilterClause::Gt(field, value) | FilterClause::Gte(field, value) => {
                if let Some(tb) = &self.time_buckets {
                    if field == tb.field_name() {
                        if let Some(threshold) = value_to_bitmap_key(value) {
                            let duration = self.now_unix.saturating_sub(threshold);
                            if let Some(bucket_name) = tb.snap_duration(duration, 0.10) {
                                if let Some(bucket) = tb.get_bucket(bucket_name) {
                                    return Ok(RoaringBitmap::clone(bucket.bitmap()));
                                }
                            }
                        }
                    }
                }
                match clause {
                    FilterClause::Gt(..) => self.range_scan(field, value, |k, t| k > t),
                    FilterClause::Gte(..) => self.range_scan(field, value, |k, t| k >= t),
                    _ => unreachable!(),
                }
            }

            FilterClause::Lt(field, value) | FilterClause::Lte(field, value) => {
                match clause {
                    FilterClause::Lt(..) => self.range_scan(field, value, |k, t| k < t),
                    FilterClause::Lte(..) => self.range_scan(field, value, |k, t| k <= t),
                    _ => unreachable!(),
                }
            }

            // Pre-computed bucket bitmap from range snapping (C3): use the bitmap directly.
            FilterClause::BucketBitmap { bitmap, .. } => Ok(bitmap.as_ref().clone()),
        }
    }

    /// Evaluate a range filter by scanning the filter field's bitmaps.
    /// Uses diff-aware iteration to handle dirty VersionedBitmaps.
    fn range_scan<F>(
        &self,
        field: &str,
        value: &Value,
        predicate: F,
    ) -> Result<RoaringBitmap>
    where
        F: Fn(u64, u64) -> bool,
    {
        let filter_field = self
            .filters
            .get_field(field)
            .ok_or_else(|| BitdexError::FieldNotFound(field.to_string()))?;
        let target = value_to_bitmap_key(value)
            .ok_or_else(|| BitdexError::InvalidValue {
                field: field.to_string(),
                reason: "cannot convert to bitmap key for range filter".to_string(),
            })?;

        let mut result = RoaringBitmap::new();
        for (&key, vb) in filter_field.iter_versioned() {
            if predicate(key, target) {
                if vb.is_dirty() {
                    result |= vb.fused();
                } else {
                    result |= vb.base().as_ref();
                }
            }
        }
        Ok(result)
    }

    /// Paginate by descending slot order (newest-first) for no-sort queries.
    /// Highest slot IDs come first since slots are monotonically assigned.
    fn slot_order_paginate(
        &self,
        candidates: &RoaringBitmap,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> (Vec<i64>, Option<crate::query::CursorPosition>) {
        self.slot_order_paginate_dir(candidates, limit, cursor, false)
    }

    /// Slot-order pagination with direction control.
    /// `ascending`: if true, returns oldest-first (lowest slot IDs first).
    /// If false (default), returns newest-first (highest slot IDs first).
    fn slot_order_paginate_dir(
        &self,
        candidates: &RoaringBitmap,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
        ascending: bool,
    ) -> (Vec<i64>, Option<crate::query::CursorPosition>) {
        if let Some(cursor) = cursor {
            // Cursor path: clone and narrow
            let mut narrowed = candidates.clone();
            if ascending {
                narrowed.remove_range(0..=cursor.slot_id);
            } else {
                narrowed.remove_range(cursor.slot_id..=u32::MAX);
            }
            if narrowed.is_empty() {
                return (Vec::new(), None);
            }
            let ids: Vec<i64> = if ascending {
                narrowed.iter().take(limit).map(|s| s as i64).collect()
            } else {
                narrowed.iter().rev().take(limit).map(|s| s as i64).collect()
            };
            let next_cursor = ids.last().map(|&last_id| crate::query::CursorPosition {
                // Set sort_value = slot ID for cursor-based pagination
                sort_value: last_id as u64,
                slot_id: last_id as u32,
            });
            (ids, next_cursor)
        } else {
            // No cursor: iterate candidates directly (no clone needed)
            if candidates.is_empty() {
                return (Vec::new(), None);
            }
            let ids: Vec<i64> = if ascending {
                candidates.iter().take(limit).map(|s| s as i64).collect()
            } else {
                // O(limit) via DoubleEndedIterator instead of O(N) skip
                candidates.iter().rev().take(limit).map(|s| s as i64).collect()
            };
            let next_cursor = ids.last().map(|&last_id| crate::query::CursorPosition {
                // Set sort_value = slot ID for cursor-based pagination
                sort_value: last_id as u64,
                slot_id: last_id as u32,
            });
            (ids, next_cursor)
        }
    }

    /// Sort candidates using bitmap sort layer traversal.
    fn sort_and_paginate(
        &self,
        candidates: &RoaringBitmap,
        sort: &SortClause,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> Result<(Vec<i64>, Option<crate::query::CursorPosition>)> {
        let sort_field = self
            .sorts
            .get_field(&sort.field)
            .ok_or_else(|| BitdexError::FieldNotFound(sort.field.clone()))?;

        let descending = sort.direction == SortDirection::Desc;
        let cursor_param = cursor.map(|c| (c.sort_value, c.slot_id));

        let sorted_slots = sort_field.top_n(candidates, limit, descending, cursor_param);

        let ids: Vec<i64> = sorted_slots.iter().map(|&s| s as i64).collect();

        let next_cursor = sorted_slots.last().map(|&last_slot| {
            let sort_value = sort_field.reconstruct_value(last_slot) as u64;
            crate::query::CursorPosition {
                sort_value,
                slot_id: last_slot,
            }
        });

        Ok((ids, next_cursor))
    }

    /// Paginate using pre-sorted packed keys (binary search fast path for initial-capacity entries).
    ///
    /// Each key is `(sort_value << 32) | slot_id`, pre-sorted in traversal order.
    /// Binary search for cursor position, then take N items. ~55ns at 4K entries.
    pub fn execute_from_sorted_keys(
        &self,
        sorted_keys: &[u64],
        _sort_field_name: &str,
        direction: SortDirection,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
        total_matched: u64,
    ) -> Result<QueryResult> {
        let limit = limit.min(self.max_page_size);

        let start_idx = if let Some(cursor) = cursor {
            let cursor_key = (cursor.sort_value << 32) | (cursor.slot_id as u64);
            // Find position past the cursor
            match direction {
                SortDirection::Desc => {
                    // Keys sorted descending — find first key strictly less than cursor_key
                    sorted_keys.partition_point(|&k| k >= cursor_key)
                }
                SortDirection::Asc => {
                    // Keys sorted ascending — find first key strictly greater than cursor_key
                    sorted_keys.partition_point(|&k| k <= cursor_key)
                }
            }
        } else {
            0
        };

        let end_idx = (start_idx + limit).min(sorted_keys.len());
        let ids: Vec<i64> = sorted_keys[start_idx..end_idx]
            .iter()
            .map(|&key| (key & 0xFFFF_FFFF) as i64)
            .collect();

        let cursor = if end_idx < sorted_keys.len() {
            let last_key = sorted_keys[end_idx - 1];
            Some(crate::query::CursorPosition {
                sort_value: last_key >> 32,
                slot_id: (last_key & 0xFFFF_FFFF) as u32,
            })
        } else {
            None
        };

        Ok(QueryResult {
            ids,
            total_matched,
            cursor,
        })
    }

    /// Paginate using a RadixSortIndex (bucket-based fast path for expanded entries).
    ///
    /// Instead of traversing 32 bit layers on the full bitmap, this:
    /// 1. Uses cumulative rank arrays to skip directly to the target bucket (O(1) for offset)
    /// 2. Calls top_n on a small bucket bitmap (~250 items at 64K uniform) instead of 64K
    /// 3. Collects results across buckets until limit is reached
    pub fn execute_from_radix(
        &self,
        radix: &crate::radix_sort::RadixSortIndex,
        sort_clause: &SortClause,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
        total_matched: u64,
    ) -> Result<QueryResult> {
        let sort_field = self
            .sorts
            .get_field(&sort_clause.field)
            .ok_or_else(|| BitdexError::FieldNotFound(sort_clause.field.clone()))?;

        let descending = sort_clause.direction == SortDirection::Desc;
        let limit = limit.min(self.max_page_size);

        let cursor_prefix = cursor.map(|c| (c.sort_value >> 24) as u8);
        let cursor_param = cursor.map(|c| (c.sort_value, c.slot_id));

        let mut result_ids: Vec<i64> = Vec::with_capacity(limit);
        let mut remaining = limit;
        let mut last_slot: Option<u32> = None;

        for (prefix, bucket_bm) in radix.iter_buckets(sort_clause.direction) {
            if remaining == 0 {
                break;
            }

            // Skip buckets that are entirely before the cursor
            if let Some(cp) = cursor_prefix {
                match sort_clause.direction {
                    SortDirection::Desc => {
                        if prefix > cp {
                            // This bucket has higher prefix than cursor — all slots are before cursor
                            continue;
                        }
                    }
                    SortDirection::Asc => {
                        if prefix < cp {
                            continue;
                        }
                    }
                }
            }

            // For the cursor bucket, pass the cursor. For subsequent buckets, no cursor needed.
            let bucket_cursor = if cursor_prefix == Some(prefix) {
                cursor_param
            } else {
                None
            };

            let sorted_slots = sort_field.top_n(bucket_bm, remaining, descending, bucket_cursor);

            for &slot in &sorted_slots {
                result_ids.push(slot as i64);
                last_slot = Some(slot);
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }

        let next_cursor = last_slot.map(|slot| {
            let sort_value = sort_field.reconstruct_value(slot) as u64;
            crate::query::CursorPosition {
                sort_value,
                slot_id: slot,
            }
        });

        Ok(QueryResult {
            ids: result_ids,
            cursor: next_cursor,
            total_matched,
        })
    }

    /// Simple in-memory sort for small result sets.
    /// When the planner estimates the result set is small, this avoids walking 32 bit layers.
    fn simple_sort_and_paginate(
        &self,
        candidates: &RoaringBitmap,
        sort: &SortClause,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> Result<(Vec<i64>, Option<crate::query::CursorPosition>)> {
        let sort_field = self
            .sorts
            .get_field(&sort.field)
            .ok_or_else(|| BitdexError::FieldNotFound(sort.field.clone()))?;

        let descending = sort.direction == SortDirection::Desc;

        // Reconstruct values and collect into Vec
        let mut entries: Vec<(u32, u32)> = candidates
            .iter()
            .map(|slot| (slot, sort_field.reconstruct_value(slot)))
            .collect();

        // Sort by value, tiebreak by slot ID
        if descending {
            entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        } else {
            entries.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        }

        // Apply cursor filtering
        if let Some(cursor) = cursor {
            let cursor_value = cursor.sort_value as u32;
            let cursor_slot = cursor.slot_id;
            entries.retain(|&(slot, value)| {
                if descending {
                    value < cursor_value || (value == cursor_value && slot < cursor_slot)
                } else {
                    value > cursor_value || (value == cursor_value && slot > cursor_slot)
                }
            });
        }

        // Take limit
        let result_slots: Vec<u32> = entries.iter().take(limit).map(|&(slot, _)| slot).collect();

        let ids: Vec<i64> = result_slots.iter().map(|&s| s as i64).collect();

        let next_cursor = result_slots.last().map(|&last_slot| {
            let sort_value = sort_field.reconstruct_value(last_slot) as u64;
            crate::query::CursorPosition {
                sort_value,
                slot_id: last_slot,
            }
        });

        Ok((ids, next_cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BucketConfig, Config, FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::mutation::{Document, FieldValue, MutationEngine};
    use crate::time_buckets::TimeBucketManager;

    fn test_config() -> Config {
        Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
                FilterFieldConfig {
                    name: "userId".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
            ],
            sort_fields: vec![
                SortFieldConfig {
                    name: "reactionCount".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                },
            ],
            max_page_size: 100,
            ..Default::default()
        }
    }

    struct TestHarness {
        slots: SlotAllocator,
        filters: FilterIndex,
        sorts: SortIndex,
        config: Config,
        docstore: crate::docstore::DocStore,
    }

    impl TestHarness {
        fn new() -> Self {
            let config = test_config();
            let slots = SlotAllocator::new();
            let mut filters = FilterIndex::new();
            let mut sorts = SortIndex::new();
            let docstore = crate::docstore::DocStore::open_temp().unwrap();

            for fc in &config.filter_fields {
                filters.add_field(fc.clone());
            }
            for sc in &config.sort_fields {
                sorts.add_field(sc.clone());
            }

            Self { slots, filters, sorts, config, docstore }
        }

        fn put(&mut self, id: u32, doc: &Document) {
            let mut engine = MutationEngine::new(
                &mut self.slots,
                &mut self.filters,
                &mut self.sorts,
                &self.config,
                &mut self.docstore,
            );
            engine.put(id, doc).unwrap();
            // Eager merge: mirror Engine::put() behavior
            for (_name, field) in self.sorts.fields_mut() {
                field.merge_dirty();
            }
            for (_name, field) in self.filters.fields_mut() {
                field.merge_dirty();
            }
            self.slots.merge_alive();
        }

        fn query(
            &self,
            filters: &[FilterClause],
            sort: Option<&SortClause>,
            limit: usize,
            cursor: Option<&crate::query::CursorPosition>,
        ) -> QueryResult {
            let executor = QueryExecutor::new(
                &self.slots,
                &self.filters,
                &self.sorts,
                self.config.max_page_size,
            );
            executor.execute(filters, sort, limit, cursor).unwrap()
        }
    }

    fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
        Document {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    fn make_time_bucket_manager(now: u64) -> TimeBucketManager {
        let configs = vec![
            BucketConfig { name: "24h".to_string(), duration_secs: 86400, refresh_interval_secs: 300 },
            BucketConfig { name: "7d".to_string(), duration_secs: 604800, refresh_interval_secs: 3600 },
        ];
        let mut mgr = TimeBucketManager::new("sortAt".to_string(), configs);
        // Slots 1-3 within 24h, slots 4-6 within 7d but outside 24h, slot 7 outside both
        let data: Vec<(u32, u64)> = vec![
            (1, now - 3600),
            (2, now - 7200),
            (3, now - 43200),
            (4, now - 90000),
            (5, now - 200000),
            (6, now - 500000),
            (7, now - 700000),
        ];
        mgr.rebuild_bucket("24h", data.iter().copied(), now);
        mgr.rebuild_bucket("7d", data.iter().copied(), now);
        mgr
    }

    #[test]
    fn test_basic_eq_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]));
        h.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("reactionCount", FieldValue::Single(Value::Integer(200))),
        ]));
        h.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(300))),
        ]));

        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 2);
        assert!(result.ids.contains(&1));
        assert!(result.ids.contains(&3));
    }

    #[test]
    fn test_not_eq_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(28)))]));
        h.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        h.put(3, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));

        let result = h.query(
            &[FilterClause::NotEq("nsfwLevel".to_string(), Value::Integer(28))],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 2);
        assert!(result.ids.contains(&2));
        assert!(result.ids.contains(&3));
    }

    #[test]
    fn test_in_filter() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3) as i64))),
            ]));
        }

        let result = h.query(
            &[FilterClause::In("nsfwLevel".to_string(), vec![Value::Integer(0), Value::Integer(1)])],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 7);
    }

    #[test]
    fn test_and_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("onSite", FieldValue::Single(Value::Bool(true))),
        ]));
        h.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("onSite", FieldValue::Single(Value::Bool(false))),
        ]));
        h.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("onSite", FieldValue::Single(Value::Bool(true))),
        ]));

        let result = h.query(
            &[FilterClause::And(vec![
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                FilterClause::Eq("onSite".to_string(), Value::Bool(true)),
            ])],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 1);
        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_or_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        h.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(2)))]));
        h.put(3, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(3)))]));

        let result = h.query(
            &[FilterClause::Or(vec![
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(3)),
            ])],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 2);
        assert!(result.ids.contains(&1));
        assert!(result.ids.contains(&3));
    }

    #[test]
    fn test_sort_descending() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]));
        h.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(500))),
        ]));
        h.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(200))),
        ]));

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
            None,
        );

        assert_eq!(result.ids, vec![2, 3, 1]); // 500, 200, 100
    }

    #[test]
    fn test_cursor_pagination() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer((i * 10) as i64))),
            ]));
        }

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };

        let page1 = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
            None,
        );
        assert_eq!(page1.ids, vec![10, 9, 8]);
        assert!(page1.cursor.is_some());

        let page2 = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
            page1.cursor.as_ref(),
        );
        assert_eq!(page2.ids, vec![7, 6, 5]);
    }

    #[test]
    fn test_deleted_invisible() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        h.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));

        {
            let mut engine = MutationEngine::new(
                &mut h.slots,
                &mut h.filters,
                &mut h.sorts,
                &h.config,
                &mut h.docstore,
            );
            engine.delete(1).unwrap();
        }

        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 1);
        assert_eq!(result.ids, vec![2]);
    }

    #[test]
    fn test_no_filters_returns_all_alive() {
        let mut h = TestHarness::new();

        for i in 1..=5u32 {
            h.put(i, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        }

        let result = h.query(&[], None, 100, None);
        assert_eq!(result.total_matched, 5);
    }

    #[test]
    fn test_max_page_size_enforced() {
        let mut h = TestHarness::new();
        h.config.max_page_size = 5;

        for i in 1..=20u32 {
            h.put(i, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        }

        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        );
        let result = executor.execute(&[], None, 1000, None).unwrap();
        assert_eq!(result.ids.len(), 5);
        assert_eq!(result.total_matched, 20);
    }

    #[test]
    fn test_no_sort_returns_descending_slot_order() {
        let mut h = TestHarness::new();

        for i in 1..=5u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ]));
        }

        let result = h.query(&[], None, 100, None);
        assert_eq!(result.ids, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_no_sort_cursor_pagination() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ]));
        }

        // First page: top 5 descending
        let page1 = h.query(&[], None, 5, None);
        assert_eq!(page1.ids, vec![10, 9, 8, 7, 6]);
        assert!(page1.cursor.is_some());

        // Second page: next 5 descending using cursor
        let page2 = h.query(&[], None, 5, page1.cursor.as_ref());
        assert_eq!(page2.ids, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_no_sort_with_filter() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            let level = if i % 2 == 0 { 1 } else { 2 };
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(level))),
            ]));
        }

        // Filter for nsfwLevel=1 (even IDs: 2,4,6,8,10), no sort
        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
            None,
        );
        assert_eq!(result.ids, vec![10, 8, 6, 4, 2]);
    }

    #[test]
    fn test_c3_bucket_snapping_gte_snaps_to_24h() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);

        let h = TestHarness::new();
        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // Gte("sortAt", now - 86400) — exact 24h match, not in FilterIndex, but bucket snapping
        // should return the 24h bucket bitmap (slots 1, 2, 3 from make_time_bucket_manager)
        let ts = (now - 86400) as i64;
        let clause = FilterClause::Gte("sortAt".to_string(), Value::Integer(ts));
        let bitmap = executor.evaluate_clause(&clause).unwrap();
        // Slots 1, 2, 3 are within 24h in make_time_bucket_manager
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
        // Slot 4 is at 90000s (25h) — outside 24h bucket
        assert!(!bitmap.contains(4));
    }

    #[test]
    fn test_c3_bucket_snapping_gt_snaps_to_7d() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);

        let h = TestHarness::new();
        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // Gt("sortAt", now - 590000) — duration=590000, within 10% of 7d (604800)
        let ts = (now - 590000) as i64;
        let clause = FilterClause::Gt("sortAt".to_string(), Value::Integer(ts));
        let bitmap = executor.evaluate_clause(&clause).unwrap();
        // Slots 1-6 are within 7d
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(6));
        // Slot 7 at 700000s is outside 7d
        assert!(!bitmap.contains(7));
    }

    #[test]
    fn test_c3_no_snap_outside_tolerance() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);

        let h = TestHarness::new();
        // Register "sortAt" as a filter field so range_scan won't error
        let mut filters = FilterIndex::new();
        filters.add_field(FilterFieldConfig {
            name: "sortAt".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
        });
        let executor = QueryExecutor::new(
            &h.slots,
            &filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // Duration = 200000s — outside tolerance of both 24h and 7d, falls through to range_scan
        let ts = (now - 200000) as i64;
        let clause = FilterClause::Gte("sortAt".to_string(), Value::Integer(ts));
        // range_scan on empty filter index returns empty bitmap (no values stored)
        let bitmap = executor.evaluate_clause(&clause).unwrap();
        assert!(bitmap.is_empty());
    }

    #[test]
    fn test_c3_no_snap_for_non_bucketed_field() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);

        let h = TestHarness::new();
        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // nsfwLevel is not the bucketed field (sortAt), so should fall through to range_scan
        // which will return an error since nsfwLevel has no stored range values matching Gt
        let ts = (now - 86400) as i64;
        let clause = FilterClause::Gte("nsfwLevel".to_string(), Value::Integer(ts));
        // This should succeed (nsfwLevel is in FilterIndex), returning empty (no values >= ts)
        let bitmap = executor.evaluate_clause(&clause).unwrap();
        assert!(bitmap.is_empty());
    }

    #[test]
    fn test_with_time_buckets_builder() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);
        let h = TestHarness::new();

        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // Verify field_name from the manager is accessible via bucket snapping
        assert_eq!(mgr.field_name(), "sortAt");
        // Verify executor was constructed successfully (indirectly)
        let _ = executor.filter_index();
    }


    // --- S4.5 / S4.6: Ascending slot-order and edge-case tests ---

    #[test]
    fn test_slot_order_ascending() {
        let h = TestHarness::new();
        let mut bm = RoaringBitmap::new();
        for i in 1..=10u32 {
            bm.insert(i);
        }
        let executor = QueryExecutor::new(&h.slots, &h.filters, &h.sorts, 100);

        // Descending (default): highest IDs first
        let (desc_ids, _) = executor.slot_order_paginate(&bm, 5, None);
        assert_eq!(desc_ids, vec![10, 9, 8, 7, 6]);

        // Ascending: lowest IDs first
        let (asc_ids, _) = executor.slot_order_paginate_dir(&bm, 5, None, true);
        assert_eq!(asc_ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_slot_order_ascending_cursor() {
        let h = TestHarness::new();
        let mut bm = RoaringBitmap::new();
        for i in 1..=10u32 {
            bm.insert(i);
        }
        let executor = QueryExecutor::new(&h.slots, &h.filters, &h.sorts, 100);

        // Page 1 ascending
        let (page1, cursor1) = executor.slot_order_paginate_dir(&bm, 3, None, true);
        assert_eq!(page1, vec![1, 2, 3]);
        assert!(cursor1.is_some());

        // Page 2 ascending (cursor past slot 3)
        let (page2, cursor2) = executor.slot_order_paginate_dir(&bm, 3, cursor1.as_ref(), true);
        assert_eq!(page2, vec![4, 5, 6]);
        assert!(cursor2.is_some());

        // Page 3 ascending
        let (page3, cursor3) = executor.slot_order_paginate_dir(&bm, 3, cursor2.as_ref(), true);
        assert_eq!(page3, vec![7, 8, 9]);
        assert!(cursor3.is_some());

        // Page 4 ascending (only 1 left)
        let (page4, _) = executor.slot_order_paginate_dir(&bm, 3, cursor3.as_ref(), true);
        assert_eq!(page4, vec![10]);
    }

    #[test]
    fn test_slot_order_empty_page() {
        let h = TestHarness::new();
        let bm = RoaringBitmap::new(); // empty
        let executor = QueryExecutor::new(&h.slots, &h.filters, &h.sorts, 100);

        let (ids, cursor) = executor.slot_order_paginate(&bm, 10, None);
        assert!(ids.is_empty());
        assert!(cursor.is_none());
    }

    #[test]
    fn test_slot_order_cursor_beyond_all() {
        let h = TestHarness::new();
        let mut bm = RoaringBitmap::new();
        for i in 1..=5u32 {
            bm.insert(i);
        }
        let executor = QueryExecutor::new(&h.slots, &h.filters, &h.sorts, 100);

        // Descending cursor at slot 1 — nothing below it
        let cursor = crate::query::CursorPosition { sort_value: 0, slot_id: 1 };
        let (ids, next) = executor.slot_order_paginate(&bm, 10, Some(&cursor));
        assert!(ids.is_empty());
        assert!(next.is_none());
    }

    #[test]
    fn test_slot_order_single_result() {
        let h = TestHarness::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(42);
        let executor = QueryExecutor::new(&h.slots, &h.filters, &h.sorts, 100);

        let (ids, cursor) = executor.slot_order_paginate(&bm, 10, None);
        assert_eq!(ids, vec![42]);
        assert!(cursor.is_some());
        assert_eq!(cursor.unwrap().slot_id, 42);
    }
}
