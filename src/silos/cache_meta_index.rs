//! CacheMetaIndex — maps (field_idx, value) → affected cache entry hashes.
//!
//! Used by the janitor checkpoint to route field ops to the cache entries
//! they affect in O(affected) time instead of scanning all entries.
//!
//! Two lookup modes:
//! - **Exact**: (field_idx, value) → entries with an `Eq` or `In` clause matching
//!   that exact (field, value) pair.
//! - **Wildcard**: field_idx → entries with negation (`NotEq`, `NotIn`), range
//!   (`Gt`, `Gte`, `Lt`, `Lte`), or sort field dependency on that field. These
//!   entries are affected by *any* value change for the field.
//!
//! The cache read path also uses this to quickly check if a field op *could*
//! affect a specific entry without scanning all clauses.

use std::collections::{HashMap, HashSet};

/// Maps (field_idx, value) pairs to cache entry key hashes for efficient
/// field-op → cache-entry routing.
pub struct CacheMetaIndex {
    /// Exact match: (field_idx, value) → set of cache key hashes.
    /// Populated for Eq and In clauses.
    exact: HashMap<(u16, u64), HashSet<u64>>,
    /// Wildcard match: field_idx → set of cache key hashes.
    /// Populated for NotEq, NotIn, Gt, Gte, Lt, Lte, and sort field deps.
    wildcard: HashMap<u16, HashSet<u64>>,
    /// Reverse index: cache_key_hash → all (field_idx, value) exact keys registered.
    /// Used for efficient unregister without scanning all exact entries.
    reverse_exact: HashMap<u64, Vec<(u16, u64)>>,
    /// Reverse index: cache_key_hash → all field_idx wildcard keys registered.
    reverse_wildcard: HashMap<u64, Vec<u16>>,
}

/// Which type of clause causes the registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClauseKind {
    /// Exact value match (Eq, In) — register under (field_idx, value).
    Exact,
    /// Any-value match (NotEq, NotIn, Gt, Gte, Lt, Lte, sort) — register under field_idx wildcard.
    Wildcard,
}

impl CacheMetaIndex {
    /// Create an empty meta-index.
    pub fn new() -> Self {
        Self {
            exact: HashMap::new(),
            wildcard: HashMap::new(),
            reverse_exact: HashMap::new(),
            reverse_wildcard: HashMap::new(),
        }
    }

    /// Create with pre-allocated capacity.
    pub fn with_capacity(exact_cap: usize, wildcard_cap: usize) -> Self {
        Self {
            exact: HashMap::with_capacity(exact_cap),
            wildcard: HashMap::with_capacity(wildcard_cap),
            reverse_exact: HashMap::new(),
            reverse_wildcard: HashMap::new(),
        }
    }

    /// Register a cache entry under an exact (field_idx, value) pair.
    pub fn register_exact(&mut self, key_hash: u64, field_idx: u16, value: u64) {
        self.exact
            .entry((field_idx, value))
            .or_default()
            .insert(key_hash);
        self.reverse_exact
            .entry(key_hash)
            .or_default()
            .push((field_idx, value));
    }

    /// Register a cache entry under a wildcard field_idx.
    pub fn register_wildcard(&mut self, key_hash: u64, field_idx: u16) {
        self.wildcard
            .entry(field_idx)
            .or_default()
            .insert(key_hash);
        self.reverse_wildcard
            .entry(key_hash)
            .or_default()
            .push(field_idx);
    }

    /// Unregister a cache entry (on eviction or rebuild).
    /// Removes all exact and wildcard registrations for this key_hash.
    pub fn unregister(&mut self, key_hash: u64) {
        // Remove exact registrations
        if let Some(exact_keys) = self.reverse_exact.remove(&key_hash) {
            for (field_idx, value) in exact_keys {
                if let Some(set) = self.exact.get_mut(&(field_idx, value)) {
                    set.remove(&key_hash);
                    if set.is_empty() {
                        self.exact.remove(&(field_idx, value));
                    }
                }
            }
        }
        // Remove wildcard registrations
        if let Some(wildcard_keys) = self.reverse_wildcard.remove(&key_hash) {
            for field_idx in wildcard_keys {
                if let Some(set) = self.wildcard.get_mut(&field_idx) {
                    set.remove(&key_hash);
                    if set.is_empty() {
                        self.wildcard.remove(&field_idx);
                    }
                }
            }
        }
    }

    /// Look up all cache entries affected by a field op at (field_idx, value).
    /// Returns the union of exact matches and wildcard matches for this field.
    ///
    /// The caller receives a deduplicated iterator. For checkpoint processing,
    /// collect into a Vec; for read-time checks, use `.contains()` or `.any()`.
    pub fn lookup(&self, field_idx: u16, value: u64) -> Vec<u64> {
        let exact_iter = self.exact
            .get(&(field_idx, value))
            .into_iter()
            .flat_map(|s| s.iter().copied());
        let wildcard_iter = self.wildcard
            .get(&field_idx)
            .into_iter()
            .flat_map(|s| s.iter().copied());

        // Deduplicate: a cache entry might appear in both exact and wildcard
        let mut result: HashSet<u64> = exact_iter.collect();
        result.extend(wildcard_iter);
        result.into_iter().collect()
    }

    /// Look up cache entries affected by a sort field change.
    /// Sort dependencies are registered as wildcards.
    pub fn lookup_sort(&self, field_idx: u16) -> Vec<u64> {
        self.wildcard
            .get(&field_idx)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Check if a specific cache entry is affected by a (field_idx, value) op.
    /// Faster than `lookup()` when checking a single entry.
    pub fn is_affected(&self, key_hash: u64, field_idx: u16, value: u64) -> bool {
        if let Some(set) = self.exact.get(&(field_idx, value)) {
            if set.contains(&key_hash) {
                return true;
            }
        }
        if let Some(set) = self.wildcard.get(&field_idx) {
            if set.contains(&key_hash) {
                return true;
            }
        }
        false
    }

    /// Number of exact (field_idx, value) keys tracked.
    pub fn exact_key_count(&self) -> usize {
        self.exact.len()
    }

    /// Number of wildcard field_idx keys tracked.
    pub fn wildcard_key_count(&self) -> usize {
        self.wildcard.len()
    }

    /// Total cache entries registered (unique key_hashes).
    pub fn entry_count(&self) -> usize {
        let mut all: HashSet<u64> = HashSet::new();
        for set in self.exact.values() {
            all.extend(set);
        }
        for set in self.wildcard.values() {
            all.extend(set);
        }
        all.len()
    }

    /// Clear the entire index.
    pub fn clear(&mut self) {
        self.exact.clear();
        self.wildcard.clear();
        self.reverse_exact.clear();
        self.reverse_wildcard.clear();
    }
}

impl Default for CacheMetaIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_lookup_exact() {
        let mut idx = CacheMetaIndex::new();
        idx.register_exact(100, 1, 42); // cache entry 100 has clause field=1, value=42
        idx.register_exact(200, 1, 42); // cache entry 200 also matches
        idx.register_exact(300, 1, 99); // cache entry 300 matches field=1, value=99

        let affected = idx.lookup(1, 42);
        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&100));
        assert!(affected.contains(&200));

        let affected2 = idx.lookup(1, 99);
        assert_eq!(affected2.len(), 1);
        assert!(affected2.contains(&300));

        // Unrelated value returns empty
        assert!(idx.lookup(1, 0).is_empty());
        // Unrelated field returns empty
        assert!(idx.lookup(2, 42).is_empty());
    }

    #[test]
    fn register_and_lookup_wildcard() {
        let mut idx = CacheMetaIndex::new();
        idx.register_wildcard(100, 1); // cache entry 100 has NotEq on field 1
        idx.register_wildcard(200, 1); // entry 200 also depends on field 1

        // Any value change for field 1 affects these entries
        let affected = idx.lookup(1, 42);
        assert_eq!(affected.len(), 2);

        let affected2 = idx.lookup(1, 999);
        assert_eq!(affected2.len(), 2);
    }

    #[test]
    fn mixed_exact_and_wildcard() {
        let mut idx = CacheMetaIndex::new();
        // Entry 100: Eq(field=1, value=42)
        idx.register_exact(100, 1, 42);
        // Entry 200: NotEq(field=1, value=42) — wildcard on field 1
        idx.register_wildcard(200, 1);
        // Entry 300: Eq(field=1, value=42) AND has range on field 1
        idx.register_exact(300, 1, 42);
        idx.register_wildcard(300, 1);

        let affected = idx.lookup(1, 42);
        assert_eq!(affected.len(), 3); // 100, 200, 300 (deduped)

        let affected2 = idx.lookup(1, 99);
        assert_eq!(affected2.len(), 2); // 200 (wildcard) + 300 (wildcard)
    }

    #[test]
    fn unregister_removes_all_refs() {
        let mut idx = CacheMetaIndex::new();
        idx.register_exact(100, 1, 42);
        idx.register_exact(100, 2, 10);
        idx.register_wildcard(100, 3);
        idx.register_exact(200, 1, 42); // different entry, same field/value

        idx.unregister(100);

        // Entry 100 gone from exact lookups
        let affected = idx.lookup(1, 42);
        assert_eq!(affected.len(), 1);
        assert!(affected.contains(&200));

        // Entry 100 gone from wildcard lookups
        assert!(idx.lookup(3, 0).is_empty());

        // Entry 100 gone from field 2
        assert!(idx.lookup(2, 10).is_empty());
    }

    #[test]
    fn unregister_nonexistent_is_noop() {
        let mut idx = CacheMetaIndex::new();
        idx.register_exact(100, 1, 42);
        idx.unregister(999); // doesn't exist — no panic
        assert_eq!(idx.lookup(1, 42).len(), 1);
    }

    #[test]
    fn is_affected_checks_both() {
        let mut idx = CacheMetaIndex::new();
        idx.register_exact(100, 1, 42);
        idx.register_wildcard(200, 1);

        assert!(idx.is_affected(100, 1, 42));  // exact match
        assert!(!idx.is_affected(100, 1, 99)); // wrong value, no wildcard
        assert!(idx.is_affected(200, 1, 42));  // wildcard match
        assert!(idx.is_affected(200, 1, 99));  // wildcard matches any value
        assert!(!idx.is_affected(300, 1, 42)); // unregistered entry
    }

    #[test]
    fn lookup_sort_uses_wildcard() {
        let mut idx = CacheMetaIndex::new();
        // Sort field dependency registered as wildcard
        idx.register_wildcard(100, 5); // entry 100 sorts by field 5
        idx.register_wildcard(200, 5);

        let sort_affected = idx.lookup_sort(5);
        assert_eq!(sort_affected.len(), 2);
        assert!(sort_affected.contains(&100));
        assert!(sort_affected.contains(&200));
    }

    #[test]
    fn clear_removes_everything() {
        let mut idx = CacheMetaIndex::new();
        idx.register_exact(100, 1, 42);
        idx.register_wildcard(200, 2);
        assert_eq!(idx.entry_count(), 2);

        idx.clear();
        assert_eq!(idx.entry_count(), 0);
        assert_eq!(idx.exact_key_count(), 0);
        assert_eq!(idx.wildcard_key_count(), 0);
    }

    #[test]
    fn entry_count_dedupes() {
        let mut idx = CacheMetaIndex::new();
        // Same entry registered in multiple places
        idx.register_exact(100, 1, 42);
        idx.register_exact(100, 2, 10);
        idx.register_wildcard(100, 3);
        assert_eq!(idx.entry_count(), 1); // only one unique key_hash
    }

    #[test]
    fn duplicate_register_is_idempotent() {
        let mut idx = CacheMetaIndex::new();
        idx.register_exact(100, 1, 42);
        idx.register_exact(100, 1, 42); // duplicate
        assert_eq!(idx.lookup(1, 42).len(), 1); // still just one entry in the HashSet

        // But reverse_exact accumulates (non-critical — unregister still works)
        idx.unregister(100);
        assert!(idx.lookup(1, 42).is_empty());
    }

    #[test]
    fn many_entries_per_field_value() {
        let mut idx = CacheMetaIndex::new();
        // 1000 cache entries all filtering on nsfwLevel=1
        for i in 0..1000u64 {
            idx.register_exact(i, 1, 1);
        }
        let affected = idx.lookup(1, 1);
        assert_eq!(affected.len(), 1000);
    }
}
