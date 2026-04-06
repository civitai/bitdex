//! Meta-Index: Bitmaps Indexing Bitmaps (Phase E)
//!
//! The meta-index maps discrete filter clause components and sort specifications
//! to sets of cache/bound entry IDs via tiny roaring bitmaps. This replaces
//! linear scans over all cache entries during both writes (finding relevant
//! bounds to maintain) and queries (finding matching bounds to apply).
//!
//! Each cache/bound entry gets a sequential integer ID. For each clause component
//! (field + op + value) and sort specification (field + direction) that appears
//! in any entry's definition, a meta-bitmap tracks which entry IDs reference it.
//!
//! On write: intersect meta-bitmaps for the mutated field to find affected entries.
//! On query: intersect meta-bitmaps for the query clauses to find matching entries.
//! Both are O(1) vs cache count — tiny bitmap intersections on ~32-bit IDs.

use ahash::AHashMap as HashMap;

use roaring::RoaringBitmap;

use crate::cache::CanonicalClause;
use crate::query::SortDirection;

/// A cache/bound entry ID. Sequential allocation, recycled on eviction.
pub type CacheEntryId = u32;

/// Key for a meta-bitmap: a discrete filter clause component.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ClauseKey {
    field: String,
    op: String,
    value_repr: String,
}

impl ClauseKey {
    fn from_canonical(clause: &CanonicalClause) -> Self {
        Self {
            field: clause.field.clone(),
            op: clause.op.clone(),
            value_repr: clause.value_repr.clone(),
        }
    }
}

/// Key for sort-field meta-bitmaps.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SortKey {
    field: String,
    direction: SortDirection,
}

/// Key for field-level meta-bitmaps (used for write-path: find all entries
/// that reference a given filter field, regardless of op/value).
/// This is broader than ClauseKey — used for filter field invalidation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FieldKey(String);

/// Tracks what an entry is registered with, for clean deregistration.
struct EntryRegistration {
    clause_keys: Vec<ClauseKey>,
    field_keys: Vec<FieldKey>,
    sort_key: Option<SortKey>,
}

/// Meta-index: maps filter/sort components to sets of cache entry IDs.
pub struct MetaIndex {
    /// Next ID to allocate.
    next_id: CacheEntryId,
    /// Recycled IDs available for reuse.
    free_ids: Vec<CacheEntryId>,

    /// Maps each discrete clause (field+op+value) to the set of entry IDs using it.
    clause_bitmaps: HashMap<ClauseKey, RoaringBitmap>,

    /// Maps each filter field name to ALL entry IDs that reference it (any op/value).
    /// Used on write path: when field X is mutated, find all entries mentioning X.
    field_bitmaps: HashMap<FieldKey, RoaringBitmap>,

    /// Maps each sort spec (field+direction) to entry IDs that sort by it.
    sort_bitmaps: HashMap<SortKey, RoaringBitmap>,

    /// Registration records for clean deregistration.
    registrations: HashMap<CacheEntryId, EntryRegistration>,

    /// Tombstoned entry IDs — entries that can't be maintained because their
    /// shard isn't loaded. Persisted in meta.bin, cleaned up on shard rewrite.
    tombstoned: RoaringBitmap,
}

impl MetaIndex {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            free_ids: Vec::new(),
            clause_bitmaps: HashMap::new(),
            field_bitmaps: HashMap::new(),
            sort_bitmaps: HashMap::new(),
            registrations: HashMap::new(),
            tombstoned: RoaringBitmap::new(),
        }
    }

    /// Allocate a new cache entry ID.
    fn allocate_id(&mut self) -> CacheEntryId {
        if let Some(id) = self.free_ids.pop() {
            id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            id
        }
    }

    /// Register a cache/bound entry with the meta-index.
    ///
    /// `filter_clauses` are the canonical filter key components.
    /// `sort_field` and `direction` are the sort specification (if any).
    ///
    /// Returns the allocated entry ID.
    pub fn register(
        &mut self,
        filter_clauses: &[CanonicalClause],
        sort_field: Option<&str>,
        sort_direction: Option<SortDirection>,
    ) -> CacheEntryId {
        let id = self.allocate_id();

        let mut clause_keys = Vec::with_capacity(filter_clauses.len());
        let mut field_keys = Vec::new();
        let mut seen_fields = std::collections::HashSet::new();

        for clause in filter_clauses {
            let ck = ClauseKey::from_canonical(clause);
            self.clause_bitmaps
                .entry(ck.clone())
                .or_default()
                .insert(id);
            clause_keys.push(ck);

            // Also register at the field level (deduped)
            if seen_fields.insert(clause.field.clone()) {
                let fk = FieldKey(clause.field.clone());
                self.field_bitmaps
                    .entry(fk.clone())
                    .or_default()
                    .insert(id);
                field_keys.push(fk);
            }
        }

        let sort_key = match (sort_field, sort_direction) {
            (Some(field), Some(dir)) => {
                let sk = SortKey {
                    field: field.to_string(),
                    direction: dir,
                };
                self.sort_bitmaps.entry(sk.clone()).or_default().insert(id);
                Some(sk)
            }
            _ => None,
        };

        self.registrations.insert(
            id,
            EntryRegistration {
                clause_keys,
                field_keys,
                sort_key,
            },
        );

        id
    }

    /// Deregister a cache/bound entry, freeing its ID for reuse.
    pub fn deregister(&mut self, id: CacheEntryId) {
        let Some(reg) = self.registrations.remove(&id) else {
            return;
        };

        for ck in &reg.clause_keys {
            if let Some(bm) = self.clause_bitmaps.get_mut(ck) {
                bm.remove(id);
                if bm.is_empty() {
                    self.clause_bitmaps.remove(ck);
                }
            }
        }

        for fk in &reg.field_keys {
            if let Some(bm) = self.field_bitmaps.get_mut(fk) {
                bm.remove(id);
                if bm.is_empty() {
                    self.field_bitmaps.remove(fk);
                }
            }
        }

        if let Some(ref sk) = reg.sort_key {
            if let Some(bm) = self.sort_bitmaps.get_mut(sk) {
                bm.remove(id);
                if bm.is_empty() {
                    self.sort_bitmaps.remove(sk);
                }
            }
        }

        self.free_ids.push(id);
    }

    /// Find all entry IDs that reference a given filter field (any op/value).
    ///
    /// Used on write path: when a filter field is mutated, find all bounds
    /// whose filter definition mentions that field. O(1) — returns a bitmap.
    pub fn entries_for_filter_field(&self, field: &str) -> Option<&RoaringBitmap> {
        self.field_bitmaps.get(&FieldKey(field.to_string()))
    }

    /// Find all entry IDs that sort by a given field+direction.
    ///
    /// Used on write path: when a sort field is mutated, find all bounds
    /// that sort by that field. O(1) — returns a bitmap.
    pub fn entries_for_sort(&self, field: &str, direction: SortDirection) -> Option<&RoaringBitmap> {
        self.sort_bitmaps.get(&SortKey {
            field: field.to_string(),
            direction,
        })
    }

    /// Find all entry IDs that sort by a given field (any direction).
    ///
    /// Used on write path: when a sort field is mutated, find all bounds
    /// that sort by that field regardless of direction.
    pub fn entries_for_sort_field(&self, field: &str) -> RoaringBitmap {
        let asc = self
            .entries_for_sort(field, SortDirection::Asc)
            .cloned()
            .unwrap_or_default();
        let desc = self
            .entries_for_sort(field, SortDirection::Desc)
            .cloned()
            .unwrap_or_default();
        asc | desc
    }

    /// Find entry IDs matching a query's filter+sort specification.
    ///
    /// Intersects the meta-bitmaps for each clause in the filter key,
    /// then intersects with the sort meta-bitmap. Returns the set of
    /// entry IDs that match ALL clauses AND the sort spec.
    pub fn find_matching_entries(
        &self,
        filter_clauses: &[CanonicalClause],
        sort_field: Option<&str>,
        sort_direction: Option<SortDirection>,
    ) -> RoaringBitmap {
        if filter_clauses.is_empty() {
            return RoaringBitmap::new();
        }

        // Intersect clause meta-bitmaps
        let mut result: Option<RoaringBitmap> = None;
        for clause in filter_clauses {
            let ck = ClauseKey::from_canonical(clause);
            match self.clause_bitmaps.get(&ck) {
                Some(bm) => {
                    result = Some(match result {
                        Some(r) => r & bm,
                        None => bm.clone(),
                    });
                }
                None => return RoaringBitmap::new(), // No entries match this clause
            }
        }

        let mut result = result.unwrap_or_default();

        // Intersect with sort meta-bitmap if specified
        if let (Some(field), Some(dir)) = (sort_field, sort_direction) {
            let sk = SortKey {
                field: field.to_string(),
                direction: dir,
            };
            match self.sort_bitmaps.get(&sk) {
                Some(bm) => result &= bm,
                None => return RoaringBitmap::new(),
            }
        }

        result
    }

    /// Find all entry IDs that reference a specific clause (field+op+value).
    ///
    /// Used by trie cache live updates: when (field, eq, value) is mutated, find
    /// all cache entries whose filter key includes that exact clause.
    pub fn entries_for_clause(&self, field: &str, op: &str, value_repr: &str) -> Option<&RoaringBitmap> {
        self.clause_bitmaps.get(&ClauseKey {
            field: field.to_string(),
            op: op.to_string(),
            value_repr: value_repr.to_string(),
        })
    }

    // ── Persistence Support ──────────────────────────────────────────────

    /// Register an entry with a specific ID (for restoring from disk).
    /// Updates next_id if needed. Does NOT allocate from free_ids.
    pub fn register_with_id(
        &mut self,
        id: CacheEntryId,
        filter_clauses: &[CanonicalClause],
        sort_field: Option<&str>,
        sort_direction: Option<SortDirection>,
    ) {
        // Ensure next_id stays ahead of any restored ID
        if id >= self.next_id {
            self.next_id = id + 1;
        }

        let mut clause_keys = Vec::with_capacity(filter_clauses.len());
        let mut field_keys = Vec::new();
        let mut seen_fields = std::collections::HashSet::new();

        for clause in filter_clauses {
            let ck = ClauseKey::from_canonical(clause);
            self.clause_bitmaps
                .entry(ck.clone())
                .or_default()
                .insert(id);
            clause_keys.push(ck);

            if seen_fields.insert(clause.field.clone()) {
                let fk = FieldKey(clause.field.clone());
                self.field_bitmaps
                    .entry(fk.clone())
                    .or_default()
                    .insert(id);
                field_keys.push(fk);
            }
        }

        let sort_key = match (sort_field, sort_direction) {
            (Some(field), Some(dir)) => {
                let sk = SortKey {
                    field: field.to_string(),
                    direction: dir,
                };
                self.sort_bitmaps.entry(sk.clone()).or_default().insert(id);
                Some(sk)
            }
            _ => None,
        };

        self.registrations.insert(
            id,
            EntryRegistration {
                clause_keys,
                field_keys,
                sort_key,
            },
        );
    }

    /// Set the next_entry_id counter (for restoring from disk).
    pub fn set_next_id(&mut self, id: CacheEntryId) {
        self.next_id = id;
    }

    /// Get the next_entry_id counter (for persistence).
    pub fn next_id(&self) -> CacheEntryId {
        self.next_id
    }

    // ── Tombstone Support ──────────────────────────────────────────────

    /// Mark an entry as tombstoned (stale, can't be maintained).
    /// The entry stays registered in the meta-index but is skipped on shard load.
    pub fn tombstone(&mut self, id: CacheEntryId) {
        self.tombstoned.insert(id);
    }

    /// Check if an entry is tombstoned.
    pub fn is_tombstoned(&self, id: CacheEntryId) -> bool {
        self.tombstoned.contains(id)
    }

    /// Get the tombstone bitmap (for persistence).
    pub fn tombstones(&self) -> &RoaringBitmap {
        &self.tombstoned
    }

    /// Set the tombstone bitmap (for restoring from disk).
    pub fn set_tombstones(&mut self, tombstones: RoaringBitmap) {
        self.tombstoned = tombstones;
    }

    /// Remove a tombstone (entry cleaned up on shard rewrite → transition to Free).
    pub fn clear_tombstone(&mut self, id: CacheEntryId) {
        self.tombstoned.remove(id);
    }

    /// Number of tombstoned entries.
    pub fn tombstone_count(&self) -> u64 {
        self.tombstoned.len()
    }

    /// Check if an entry is registered (regardless of tombstone state).
    pub fn is_registered(&self, id: CacheEntryId) -> bool {
        self.registrations.contains_key(&id)
    }

    /// Iterator over all registered entry IDs (for tombstoning all unloaded).
    pub fn all_registered_ids(&self) -> impl Iterator<Item = CacheEntryId> + '_ {
        self.registrations.keys().copied()
    }

    /// Number of registered entries.
    pub fn entry_count(&self) -> usize {
        self.registrations.len()
    }

    /// Number of clause meta-bitmaps.
    pub fn clause_bitmap_count(&self) -> usize {
        self.clause_bitmaps.len()
    }

    /// Number of sort meta-bitmaps.
    pub fn sort_bitmap_count(&self) -> usize {
        self.sort_bitmaps.len()
    }

    /// Total memory usage of all meta-bitmaps (approximate).
    pub fn memory_bytes(&self) -> usize {
        let clause_bytes: usize = self
            .clause_bitmaps
            .values()
            .map(|bm| bm.serialized_size())
            .sum();
        let field_bytes: usize = self
            .field_bitmaps
            .values()
            .map(|bm| bm.serialized_size())
            .sum();
        let sort_bytes: usize = self
            .sort_bitmaps
            .values()
            .map(|bm| bm.serialized_size())
            .sum();
        clause_bytes + field_bytes + sort_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clause(field: &str, value: &str) -> CanonicalClause {
        CanonicalClause {
            field: field.to_string(),
            op: "eq".to_string(),
            value_repr: value.to_string(),
        }
    }

    #[test]
    fn test_register_and_lookup() {
        let mut mi = MetaIndex::new();

        let id = mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );
        assert_eq!(id, 0);
        assert_eq!(mi.entry_count(), 1);

        // Should find entry via filter field
        let entries = mi.entries_for_filter_field("nsfwLevel").unwrap();
        assert!(entries.contains(id));

        // Should find entry via sort spec
        let entries = mi
            .entries_for_sort("reactionCount", SortDirection::Desc)
            .unwrap();
        assert!(entries.contains(id));

        // Should NOT find via wrong sort direction
        assert!(mi
            .entries_for_sort("reactionCount", SortDirection::Asc)
            .is_none());
    }

    #[test]
    fn test_deregister_frees_id() {
        let mut mi = MetaIndex::new();

        let id0 = mi.register(&[clause("nsfwLevel", "1")], None, None);
        let id1 = mi.register(&[clause("nsfwLevel", "2")], None, None);
        assert_eq!(id0, 0);
        assert_eq!(id1, 1);

        mi.deregister(id0);
        assert_eq!(mi.entry_count(), 1);

        // Recycled ID should be reused
        let id2 = mi.register(&[clause("onSite", "true")], None, None);
        assert_eq!(id2, 0); // recycled
    }

    #[test]
    fn test_deregister_cleans_up_bitmaps() {
        let mut mi = MetaIndex::new();

        let id = mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );

        mi.deregister(id);

        // All meta-bitmaps should be cleaned up
        assert!(mi.entries_for_filter_field("nsfwLevel").is_none());
        assert!(mi
            .entries_for_sort("reactionCount", SortDirection::Desc)
            .is_none());
        assert_eq!(mi.clause_bitmap_count(), 0);
    }

    #[test]
    fn test_entries_for_sort_field_both_directions() {
        let mut mi = MetaIndex::new();

        let id_desc = mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );
        let id_asc = mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Asc),
        );

        let all = mi.entries_for_sort_field("reactionCount");
        assert!(all.contains(id_desc));
        assert!(all.contains(id_asc));
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_find_matching_entries_intersection() {
        let mut mi = MetaIndex::new();

        // Entry 0: nsfwLevel=1, sort=reactionCount Desc
        let id0 = mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );

        // Entry 1: nsfwLevel=1 + onSite=true, sort=reactionCount Desc
        let id1 = mi.register(
            &[clause("nsfwLevel", "1"), clause("onSite", "true")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );

        // Entry 2: nsfwLevel=2, sort=reactionCount Desc
        let _id2 = mi.register(
            &[clause("nsfwLevel", "2")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );

        // Query: nsfwLevel=1, sort=reactionCount Desc → should match id0 and id1
        let matches = mi.find_matching_entries(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );
        assert!(matches.contains(id0));
        assert!(matches.contains(id1));
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_find_matching_entries_narrows_with_more_clauses() {
        let mut mi = MetaIndex::new();

        // Entry 0: nsfwLevel=1 only
        let id0 = mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );

        // Entry 1: nsfwLevel=1 + onSite=true
        let id1 = mi.register(
            &[clause("nsfwLevel", "1"), clause("onSite", "true")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );

        // Query with both clauses: nsfwLevel=1 + onSite=true → only id1 matches BOTH
        let matches = mi.find_matching_entries(
            &[clause("nsfwLevel", "1"), clause("onSite", "true")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );
        // id0 has nsfwLevel=1 but NOT onSite=true, so it shouldn't match
        // Wait — id0 registered with only nsfwLevel=1. The query asks for entries
        // that have BOTH nsfwLevel=1 AND onSite=true. id0 doesn't have onSite=true
        // in its registration, so the intersection of meta-bitmaps for onSite=true
        // won't include id0.
        assert!(!matches.contains(id0));
        assert!(matches.contains(id1));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_find_matching_entries_no_match() {
        let mut mi = MetaIndex::new();

        mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );

        // Query for a value no entry has
        let matches = mi.find_matching_entries(
            &[clause("nsfwLevel", "99")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );
        assert!(matches.is_empty());
    }

    #[test]
    fn test_find_matching_entries_wrong_sort() {
        let mut mi = MetaIndex::new();

        mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );

        // Query with matching filter but wrong sort
        let matches = mi.find_matching_entries(
            &[clause("nsfwLevel", "1")],
            Some("commentCount"),
            Some(SortDirection::Desc),
        );
        assert!(matches.is_empty());
    }

    #[test]
    fn test_multiple_entries_same_clause() {
        let mut mi = MetaIndex::new();

        // Three entries all have nsfwLevel=1 but different sort fields
        let id0 = mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );
        let id1 = mi.register(
            &[clause("nsfwLevel", "1")],
            Some("commentCount"),
            Some(SortDirection::Desc),
        );
        let id2 = mi.register(
            &[clause("nsfwLevel", "1")],
            Some("reactionCount"),
            Some(SortDirection::Asc),
        );

        // Field-level lookup should find all three
        let field_entries = mi.entries_for_filter_field("nsfwLevel").unwrap();
        assert_eq!(field_entries.len(), 3);

        // Sort-specific lookup
        let sort_entries = mi
            .entries_for_sort("reactionCount", SortDirection::Desc)
            .unwrap();
        assert!(sort_entries.contains(id0));
        assert!(!sort_entries.contains(id1));
        assert!(!sort_entries.contains(id2));
    }

    #[test]
    fn test_memory_bytes_nonzero() {
        let mut mi = MetaIndex::new();
        mi.register(
            &[clause("nsfwLevel", "1"), clause("onSite", "true")],
            Some("reactionCount"),
            Some(SortDirection::Desc),
        );
        assert!(mi.memory_bytes() > 0);
    }

    #[test]
    fn test_id_recycling_order() {
        let mut mi = MetaIndex::new();

        let id0 = mi.register(&[clause("a", "1")], None, None);
        let id1 = mi.register(&[clause("b", "2")], None, None);
        let id2 = mi.register(&[clause("c", "3")], None, None);

        // Deregister id1 and id0
        mi.deregister(id1);
        mi.deregister(id0);

        // Next allocations should reuse freed IDs (LIFO from free_ids)
        let id3 = mi.register(&[clause("d", "4")], None, None);
        let id4 = mi.register(&[clause("e", "5")], None, None);
        assert_eq!(id3, id0); // id0 was pushed last, popped first
        assert_eq!(id4, id1);

        // Next allocation should be fresh
        let id5 = mi.register(&[clause("f", "6")], None, None);
        assert_eq!(id5, 3); // next_id was 3 after id0,id1,id2
        let _ = id2; // suppress warning
    }
}
