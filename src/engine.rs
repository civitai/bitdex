use std::path::Path;

use crate::concurrency::InFlightTracker;
use crate::config::Config;
use crate::docstore::DocStore;
use crate::error::Result;
use crate::executor::QueryExecutor;
use crate::filter::FilterIndex;
use crate::mutation::{Document, MutationEngine, PatchPayload};
use crate::query::{BitdexQuery, FilterClause, SortClause};
use crate::slot::SlotAllocator;
use crate::sort::SortIndex;
use crate::types::QueryResult;

/// The top-level Bitdex engine tying all components together.
///
/// This struct owns all bitmap state and provides the public API
/// for mutations and queries. Includes in-flight write tracking
/// for optimistic concurrency.
pub struct Engine {
    slots: SlotAllocator,
    filters: FilterIndex,
    sorts: SortIndex,
    in_flight: InFlightTracker,
    docstore: DocStore,
    config: Config,
}

impl Engine {
    /// Create a new engine with an on-disk docstore at the given path.
    pub fn new_with_path(config: Config, docstore_path: &Path) -> Result<Self> {
        config.validate()?;

        let slots = SlotAllocator::new();
        let mut filters = FilterIndex::new();
        let mut sorts = SortIndex::new();
        let docstore = DocStore::open(docstore_path)?;

        for fc in &config.filter_fields {
            filters.add_field(fc.clone());
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }

        Ok(Self {
            slots,
            filters,
            sorts,
            in_flight: InFlightTracker::new(),
            docstore,
            config,
        })
    }

    /// Create a new engine with an in-memory docstore (for testing).
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;

        let slots = SlotAllocator::new();
        let mut filters = FilterIndex::new();
        let mut sorts = SortIndex::new();
        let docstore = DocStore::open_temp()?;

        for fc in &config.filter_fields {
            filters.add_field(fc.clone());
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }

        Ok(Self {
            slots,
            filters,
            sorts,
            in_flight: InFlightTracker::new(),
            docstore,
            config,
        })
    }

    /// PUT(id, document) -- full replace with upsert semantics.
    /// Marks the slot as in-flight during the mutation.
    pub fn put(&mut self, id: u32, doc: &Document) -> Result<()> {
        // Mark in-flight before mutation
        self.in_flight.mark_in_flight(id);

        let result = {
            let mut engine = MutationEngine::new(
                &mut self.slots,
                &mut self.filters,
                &mut self.sorts,
                &self.config,
                &mut self.docstore,
            );
            engine.put(id, doc)
        };

        // Eager merge: sort diffs and alive must be compacted before readers see them
        for (_name, field) in self.sorts.fields_mut() {
            field.merge_dirty();
        }
        // Eager merge: filter diffs must be compacted before readers see them
        for (_name, field) in self.filters.fields_mut() {
            field.merge_dirty();
        }
        self.slots.merge_alive();

        // Clear in-flight after mutation
        self.in_flight.clear_in_flight(id);
        result
    }

    /// PATCH(id, partial_fields) -- merge only provided fields.
    /// Marks the slot as in-flight during the mutation.
    pub fn patch(&mut self, id: u32, patch: &PatchPayload) -> Result<()> {
        // Mark in-flight before mutation
        self.in_flight.mark_in_flight(id);

        let result = {
            let mut engine = MutationEngine::new(
                &mut self.slots,
                &mut self.filters,
                &mut self.sorts,
                &self.config,
                &mut self.docstore,
            );
            engine.patch(id, patch)
        };

        // Eager merge: sort diffs and alive must be compacted before readers see them
        for (_name, field) in self.sorts.fields_mut() {
            field.merge_dirty();
        }
        // Eager merge: filter diffs must be compacted before readers see them
        for (_name, field) in self.filters.fields_mut() {
            field.merge_dirty();
        }
        self.slots.merge_alive();

        // Clear in-flight after mutation
        self.in_flight.clear_in_flight(id);
        result
    }

    /// DELETE(id) -- clean delete: clear filter/sort bitmaps then alive bit.
    /// Marks the slot as in-flight during the mutation.
    pub fn delete(&mut self, id: u32) -> Result<()> {
        self.in_flight.mark_in_flight(id);

        let result = {
            let mut engine = MutationEngine::new(
                &mut self.slots,
                &mut self.filters,
                &mut self.sorts,
                &self.config,
                &mut self.docstore,
            );
            engine.delete(id)
        };
        // Eager merge: filter/sort diffs and alive must be compacted before readers see them
        for (_name, field) in self.filters.fields_mut() {
            field.merge_dirty();
        }
        for (_name, field) in self.sorts.fields_mut() {
            field.merge_dirty();
        }
        self.slots.merge_alive();
        self.in_flight.clear_in_flight(id);
        result
    }

    /// DELETE WHERE(query) -- resolve query, clean-delete all matches.
    pub fn delete_where(&mut self, filters: &[FilterClause]) -> Result<u64> {
        // First, resolve the filter to get matching slot IDs
        let executor = QueryExecutor::new(
            &self.slots,
            &self.filters,
            &self.sorts,
            u32::MAX as usize,
        );
        let result = executor.execute(
            filters,
            None,
            u32::MAX as usize,
            None,
        )?;

        // Build a bitmap of matching slots
        let mut matching = roaring::RoaringBitmap::new();
        for id in &result.ids {
            matching.insert(*id as u32);
        }

        // Now delete them
        let result = {
            let mut engine = MutationEngine::new(
                &mut self.slots,
                &mut self.filters,
                &mut self.sorts,
                &self.config,
                &mut self.docstore,
            );
            engine.delete_where(&matching)
        };
        // Eager merge: filter/sort diffs and alive must be compacted before readers see them
        for (_name, field) in self.filters.fields_mut() {
            field.merge_dirty();
        }
        for (_name, field) in self.sorts.fields_mut() {
            field.merge_dirty();
        }
        self.slots.merge_alive();
        result
    }

    /// Execute a parsed query.
    pub fn execute_query(&self, query: &BitdexQuery) -> Result<QueryResult> {
        let executor = QueryExecutor::new(
            &self.slots,
            &self.filters,
            &self.sorts,
            self.config.max_page_size,
        );

        // Offset pagination: fetch offset+limit results, then drop first offset
        let offset = if query.cursor.is_none() {
            query.offset.unwrap_or(0)
        } else {
            0
        };
        let fetch_limit = query.limit.saturating_add(offset);

        let mut result = executor.execute(
            &query.filters,
            query.sort.as_ref(),
            fetch_limit,
            query.cursor.as_ref(),
        )?;

        // Apply offset: drop the first N results
        if offset > 0 && !result.ids.is_empty() {
            if offset >= result.ids.len() {
                result.ids.clear();
                result.cursor = None;
            } else {
                result.ids = result.ids.split_off(offset);
            }
        }

        // Post-validation: check for in-flight write overlap and revalidate
        self.post_validate(&mut result, &query.filters, &executor)?;
        Ok(result)
    }

    /// Execute a query from individual components.
    pub fn query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<QueryResult> {
        let executor = QueryExecutor::new(
            &self.slots,
            &self.filters,
            &self.sorts,
            self.config.max_page_size,
        );
        let mut result = executor.execute(filters, sort, limit, None)?;

        // Post-validation: check for in-flight write overlap and revalidate
        self.post_validate(&mut result, filters, &executor)?;
        Ok(result)
    }

    /// Post-validate query results against in-flight writes.
    ///
    /// After computing results, checks if any result IDs overlap with the
    /// in-flight set. For overlapping IDs, re-checks if they still match
    /// all filter predicates and are still alive. Removes any that no longer qualify.
    fn post_validate(
        &self,
        result: &mut QueryResult,
        filters: &[FilterClause],
        executor: &QueryExecutor,
    ) -> Result<()> {
        // Fast path: no in-flight writes means nothing to revalidate
        if !self.in_flight.has_in_flight() {
            return Ok(());
        }

        let overlapping = self.in_flight.find_overlapping(&result.ids);
        if overlapping.is_empty() {
            return Ok(());
        }

        // Revalidate each overlapping slot: must be alive AND match all filters
        let alive = self.slots.alive_bitmap();
        let mut invalid_slots: Vec<u32> = Vec::new();

        for slot in &overlapping {
            // Check alive first (cheapest check)
            if !alive.contains(*slot) {
                invalid_slots.push(*slot);
                continue;
            }

            // Check all filter predicates
            if !executor.slot_matches_filters(*slot, filters)? {
                invalid_slots.push(*slot);
            }
        }

        // Remove invalid slots from results
        if !invalid_slots.is_empty() {
            result.ids.retain(|id| !invalid_slots.contains(&(*id as u32)));
        }

        Ok(())
    }

    /// Get the number of alive documents.
    pub fn alive_count(&self) -> u64 {
        self.slots.alive_count()
    }

    /// Get the number of dead (deleted but not cleaned) slots.
    pub fn dead_count(&self) -> u64 {
        self.slots.dead_count()
    }

    /// Get the high-water mark slot counter.
    pub fn slot_counter(&self) -> u32 {
        self.slots.slot_counter()
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get a reference to the slot allocator.
    pub fn slots(&self) -> &SlotAllocator {
        &self.slots
    }

    /// Get a mutable reference to the slot allocator (for autovac).
    pub fn slots_mut(&mut self) -> &mut SlotAllocator {
        &mut self.slots
    }

    /// Get a reference to the filter index.
    pub fn filters(&self) -> &FilterIndex {
        &self.filters
    }

    /// Get a mutable reference to the filter index (for autovac).
    pub fn filters_mut(&mut self) -> &mut FilterIndex {
        &mut self.filters
    }

    /// Get a reference to the sort index.
    pub fn sorts(&self) -> &SortIndex {
        &self.sorts
    }

    /// Get a mutable reference to the sort index (for autovac).
    pub fn sorts_mut(&mut self) -> &mut SortIndex {
        &mut self.sorts
    }

    /// Get a reference to the in-flight tracker (for concurrent access).
    pub fn in_flight(&self) -> &InFlightTracker {
        &self.in_flight
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::mutation::FieldValue;
    use crate::query::{SortDirection, Value};

    fn test_config() -> Config {
        Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                },
            ],
            sort_fields: vec![SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: false,
                computed: None,
            }],
            max_page_size: 100,
            ..Default::default()
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

    #[test]
    fn test_engine_put_and_query() {
        let mut engine = Engine::new(test_config()).unwrap();

        engine
            .put(1, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(42))),
            ]))
            .unwrap();

        assert_eq!(engine.alive_count(), 1);

        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_engine_delete_and_query() {
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))])).unwrap();
        engine.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))])).unwrap();

        engine.delete(1).unwrap();

        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.ids, vec![2]);
    }

    #[test]
    fn test_engine_delete_where() {
        let mut engine = Engine::new(test_config()).unwrap();

        for i in 1..=10u32 {
            engine.put(
                i,
                &make_doc(vec![(
                    "nsfwLevel",
                    FieldValue::Single(Value::Integer(if i <= 5 { 1 } else { 2 })),
                )]),
            ).unwrap();
        }

        let deleted = engine
            .delete_where(&[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))])
            .unwrap();

        assert_eq!(deleted, 5);
        assert_eq!(engine.alive_count(), 5);
    }

    #[test]
    fn test_engine_sorted_query() {
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ])).unwrap();
        engine.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(500))),
        ])).unwrap();
        engine.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(300))),
        ])).unwrap();

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                Some(&sort),
                10,
            )
            .unwrap();

        assert_eq!(result.ids, vec![2, 3, 1]); // 500, 300, 100
    }

    #[test]
    fn test_engine_full_workflow() {
        let mut engine = Engine::new(test_config()).unwrap();

        for i in 1..=5u32 {
            engine.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("tagIds", FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)])),
                ("onSite", FieldValue::Single(Value::Bool(true))),
                ("reactionCount", FieldValue::Single(Value::Integer((i * 10) as i64))),
            ])).unwrap();
        }

        assert_eq!(engine.alive_count(), 5);

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine.query(
            &[
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                FilterClause::Eq("tagIds".to_string(), Value::Integer(100)),
                FilterClause::Eq("onSite".to_string(), Value::Bool(true)),
            ],
            Some(&sort),
            3,
        ).unwrap();

        assert_eq!(result.total_matched, 5);
        assert_eq!(result.ids, vec![5, 4, 3]);

        engine.delete(5).unwrap();
        assert_eq!(engine.alive_count(), 4);

        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
        ).unwrap();

        assert_eq!(result.ids, vec![4, 3, 2]);
    }

    #[test]
    fn test_execute_parsed_query() {
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(42))),
        ])).unwrap();

        let query = BitdexQuery {
            filters: vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            sort: Some(SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            limit: 50,
            cursor: None,
            offset: None,
            skip_cache: false,
        };

        let result = engine.execute_query(&query).unwrap();
        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_offset_pagination() {
        let mut engine = Engine::new(test_config()).unwrap();

        // Insert 5 docs with different reactionCounts
        for i in 1..=5u32 {
            engine.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 10))),
            ])).unwrap();
        }

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };

        // Page 1: limit=2, offset=0 → [5, 4]
        let q1 = BitdexQuery {
            filters: vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            sort: Some(sort.clone()),
            limit: 2,
            cursor: None,
            offset: None,
            skip_cache: false,
        };
        let r1 = engine.execute_query(&q1).unwrap();
        assert_eq!(r1.ids, vec![5, 4]);

        // Page 2: limit=2, offset=2 → [3, 2]
        let q2 = BitdexQuery {
            filters: vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            sort: Some(sort.clone()),
            limit: 2,
            cursor: None,
            offset: Some(2),
            skip_cache: false,
        };
        let r2 = engine.execute_query(&q2).unwrap();
        assert_eq!(r2.ids, vec![3, 2]);

        // Page 3: limit=2, offset=4 → [1]
        let q3 = BitdexQuery {
            filters: vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            sort: Some(sort.clone()),
            limit: 2,
            cursor: None,
            offset: Some(4),
            skip_cache: false,
        };
        let r3 = engine.execute_query(&q3).unwrap();
        assert_eq!(r3.ids, vec![1]);

        // Offset past end → empty
        let q4 = BitdexQuery {
            filters: vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            sort: Some(sort.clone()),
            limit: 2,
            cursor: None,
            offset: Some(10),
            skip_cache: false,
        };
        let r4 = engine.execute_query(&q4).unwrap();
        assert!(r4.ids.is_empty());
    }

    #[test]
    fn test_post_validation_removes_in_flight_slot_that_no_longer_matches() {
        // Set up engine with 3 documents, all matching nsfwLevel=1
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
        ])).unwrap();
        engine.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
        ])).unwrap();
        engine.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
        ])).unwrap();

        // Simulate a concurrent writer changing slot 2's nsfwLevel from 1 to 2:
        // 1. Mark slot 2 as in-flight (writer does this before mutation)
        engine.in_flight.mark_in_flight(2);

        // 2. Mutate the filter bitmaps directly (simulating the write in progress)
        //    Move slot 2 from nsfwLevel=1 bitmap to nsfwLevel=2 bitmap
        let filter_field = engine.filters.get_field_mut("nsfwLevel").unwrap();
        filter_field.remove(1, 2);  // remove from old value
        filter_field.insert(2, 2);  // add to new value
        filter_field.merge_dirty();

        // Now query for nsfwLevel=1. Without post-validation, slot 2 might
        // still appear in results due to bitmap state during write.
        // With post-validation, the reader should detect slot 2 is in-flight,
        // revalidate it, find it no longer matches nsfwLevel=1, and remove it.
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        ).unwrap();

        // Slot 2 should NOT appear in results (it no longer matches nsfwLevel=1)
        assert!(!result.ids.contains(&2), "in-flight slot that no longer matches should be removed");
        // Slots 1 and 3 should still be present
        assert!(result.ids.contains(&1));
        assert!(result.ids.contains(&3));

        // Clean up: clear the in-flight mark (writer would do this after mutation)
        engine.in_flight.clear_in_flight(2);
    }

    #[test]
    fn test_post_validation_keeps_in_flight_slot_that_still_matches() {
        // Verify that post-validation does NOT remove an in-flight slot
        // that still matches the filter predicates.
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ])).unwrap();
        engine.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(200))),
        ])).unwrap();

        // Mark slot 2 as in-flight (simulating a write to its sort field,
        // which doesn't affect the filter predicate)
        engine.in_flight.mark_in_flight(2);

        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        ).unwrap();

        // Slot 2 still matches nsfwLevel=1, so it should remain in results
        assert!(result.ids.contains(&1));
        assert!(result.ids.contains(&2));

        engine.in_flight.clear_in_flight(2);
    }

    #[test]
    fn test_post_validation_removes_deleted_in_flight_slot() {
        // If a slot is being deleted (alive bit cleared) while in-flight,
        // post-validation should detect it's no longer alive and remove it.
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
        ])).unwrap();
        engine.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
        ])).unwrap();

        // Simulate a concurrent delete of slot 2:
        // Mark in-flight, then clear the alive bit directly
        engine.in_flight.mark_in_flight(2);
        engine.slots_mut().delete(2).unwrap();
        engine.slots_mut().merge_alive();

        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        ).unwrap();

        // Slot 2 is dead — should not appear even if filter bitmaps still have it
        assert_eq!(result.ids, vec![1]);

        engine.in_flight.clear_in_flight(2);
    }

    #[test]
    fn test_post_validation_no_overhead_when_no_in_flight() {
        // When there are no in-flight writes, post-validation is a no-op
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
        ])).unwrap();

        assert!(!engine.in_flight().has_in_flight());

        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        ).unwrap();

        assert_eq!(result.ids, vec![1]);
    }
}
