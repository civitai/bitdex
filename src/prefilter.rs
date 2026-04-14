//! Prefilter Registry — named, precomputed filter bitmaps that the query
//! planner can substitute into matching queries.
//!
//! See `docs/_in/design-prefilter-registry.md` for the full design.
//!
//! A prefilter is a named clause set (e.g. the 7-clause Civitai safety prefix)
//! whose bitmap we compute once and cache. When a query's canonicalized clause
//! set is a superset of a registered prefilter's clauses, those clauses are
//! elided from the query and replaced with a single `BucketBitmap` AND against
//! the cached bitmap — turning ~900 ms of filter work into a single intersect.
//!
//! The registry lives on `ConcurrentEngine` (outside the `ArcSwap<InnerEngine>`
//! snapshot) so registrations don't force a snapshot publish. This is a
//! deliberate deviation from the design doc — see
//! `docs/_in/design-prefilter-registry.md#implementation-deviations`. Each
//! entry's bitmap is an `ArcSwap<RoaringBitmap>` so a refresh thread can
//! publish a fresh version without disrupting in-flight queries.
//!
//! **Delete safety.** The engine uses clean-delete: on delete, a doc's bits
//! are cleared from every filter/sort bitmap and from `alive`. The cached
//! prefilter bitmap is NOT maintained by clean-delete — it's a frozen
//! snapshot. Between refreshes, deleted docs still have bits set in the
//! cached prefilter. `substitute()` therefore intersects the cached bitmap
//! with the live `alive` bitmap at substitute time to avoid resurrecting
//! deleted docs through a superset-only query.

use arc_swap::ArcSwap;
use dashmap::DashMap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::query::{FilterClause, Value};

pub const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 300;
pub const MIN_REFRESH_INTERVAL_SECS: u64 = 30;
pub const MAX_REFRESH_INTERVAL_SECS: u64 = 86_400;
pub const MAX_REGISTERED_PREFILTERS: usize = 32;

/// A named precomputed filter result. Shared across query threads via `Arc`.
pub struct PrefilterEntry {
    pub name: String,
    /// Canonical clause set (sorted, deduped, And-flattened). Queries whose
    /// canonical clause set is a superset of this will have these clauses
    /// elided and replaced by a single AND against `bitmap`.
    pub clauses: Vec<FilterClause>,
    /// The precomputed bitmap — the conjunction of `clauses`.
    pub bitmap: ArcSwap<RoaringBitmap>,
    pub refresh_interval_secs: AtomicU64,
    /// Unix seconds of last successful refresh.
    pub last_refreshed: AtomicI64,
    /// Nanoseconds spent on the last compute.
    pub compute_duration_nanos: AtomicU64,
    /// Cached bitmap cardinality so listing doesn't pay `bitmap.len()`.
    pub cardinality: AtomicU64,
    pub refresh_errors: AtomicU64,
    /// Cumulative number of queries that substituted this prefilter.
    pub substitutions: AtomicU64,
}

impl PrefilterEntry {
    pub fn cardinality(&self) -> u64 { self.cardinality.load(Ordering::Relaxed) }
    pub fn last_refreshed(&self) -> i64 { self.last_refreshed.load(Ordering::Relaxed) }
    pub fn refresh_interval_secs(&self) -> u64 { self.refresh_interval_secs.load(Ordering::Relaxed) }
    pub fn compute_ms(&self) -> u64 { self.compute_duration_nanos.load(Ordering::Relaxed) / 1_000_000 }
    pub fn refresh_errors(&self) -> u64 { self.refresh_errors.load(Ordering::Relaxed) }
    pub fn substitutions(&self) -> u64 { self.substitutions.load(Ordering::Relaxed) }

    pub fn is_stale(&self, now_unix: i64) -> bool {
        let interval = self.refresh_interval_secs() as i64;
        now_unix.saturating_sub(self.last_refreshed()) >= interval
    }

    /// Atomically publish a fresh bitmap. In-flight queries holding the old
    /// `Arc` continue reading the old version.
    pub fn publish_refresh(&self, bitmap: RoaringBitmap, compute_duration_nanos: u64) {
        let card = bitmap.len();
        self.bitmap.store(Arc::new(bitmap));
        self.cardinality.store(card, Ordering::Relaxed);
        self.compute_duration_nanos.store(compute_duration_nanos, Ordering::Relaxed);
        self.last_refreshed.store(now_unix_secs(), Ordering::Relaxed);
    }
}

pub struct PrefilterRegistry {
    entries: DashMap<String, Arc<PrefilterEntry>>,
    max_entries: usize,
}

impl PrefilterRegistry {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            max_entries: MAX_REGISTERED_PREFILTERS,
        }
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    pub fn get(&self, name: &str) -> Option<Arc<PrefilterEntry>> {
        self.entries.get(name).map(|r| Arc::clone(r.value()))
    }

    pub fn remove(&self, name: &str) -> bool {
        self.entries.remove(name).is_some()
    }

    /// Register a prefilter. If `name` already exists, the existing entry is
    /// replaced. Clauses are canonicalized before storage so future matching
    /// is a pure structural equality check.
    pub fn insert(
        &self,
        name: String,
        clauses: Vec<FilterClause>,
        bitmap: RoaringBitmap,
        refresh_interval_secs: u64,
        compute_duration_nanos: u64,
    ) -> Result<Arc<PrefilterEntry>, RegistryError> {
        let interval = refresh_interval_secs.clamp(MIN_REFRESH_INTERVAL_SECS, MAX_REFRESH_INTERVAL_SECS);

        if clauses.is_empty() {
            return Err(RegistryError::InvalidClauses("clause list is empty".into()));
        }

        let canonical = canonicalize_clauses(&clauses);
        if canonical.is_empty() {
            return Err(RegistryError::InvalidClauses("clauses reduced to empty after canonicalization".into()));
        }

        if !self.entries.contains_key(&name) && self.entries.len() >= self.max_entries {
            return Err(RegistryError::RegistryFull(self.max_entries));
        }

        let card = bitmap.len();
        let entry = Arc::new(PrefilterEntry {
            name: name.clone(),
            clauses: canonical,
            bitmap: ArcSwap::from(Arc::new(bitmap)),
            refresh_interval_secs: AtomicU64::new(interval),
            last_refreshed: AtomicI64::new(now_unix_secs()),
            compute_duration_nanos: AtomicU64::new(compute_duration_nanos),
            cardinality: AtomicU64::new(card),
            refresh_errors: AtomicU64::new(0),
            substitutions: AtomicU64::new(0),
        });

        self.entries.insert(name, Arc::clone(&entry));
        Ok(entry)
    }

    pub fn entries(&self) -> Vec<Arc<PrefilterEntry>> {
        self.entries.iter().map(|r| Arc::clone(r.value())).collect()
    }

    pub fn list(&self) -> Vec<PrefilterInfo> {
        self.entries()
            .iter()
            .map(|e| PrefilterInfo {
                name: e.name.clone(),
                cardinality: e.cardinality(),
                last_refreshed: e.last_refreshed(),
                refresh_interval_secs: e.refresh_interval_secs(),
                last_compute_ms: e.compute_ms(),
                refresh_errors: e.refresh_errors(),
                substitutions: e.substitutions(),
                clauses: e.clauses.clone(),
            })
            .collect()
    }

    /// Find a registered prefilter whose canonical clause set is a subset of
    /// `canonical_query_clauses`. Prefers the prefilter with the most matched
    /// clauses (largest work saved).
    ///
    /// Returns the entry and the sorted indices (in `canonical_query_clauses`)
    /// that were matched — callers should remove those indices from the query
    /// clause list and prepend a `BucketBitmap` clause.
    pub fn find_substitution(
        &self,
        canonical_query_clauses: &[FilterClause],
    ) -> Option<(Arc<PrefilterEntry>, Vec<usize>)> {
        if canonical_query_clauses.is_empty() || self.entries.is_empty() {
            return None;
        }
        let mut best: Option<(Arc<PrefilterEntry>, Vec<usize>)> = None;
        for r in self.entries.iter() {
            let entry = r.value();
            if let Some(matched) = subset_match(&entry.clauses, canonical_query_clauses) {
                let better = match &best {
                    None => true,
                    Some((_, prev)) => matched.len() > prev.len(),
                };
                if better {
                    best = Some((Arc::clone(entry), matched));
                }
            }
        }
        best
    }
}

impl Default for PrefilterRegistry {
    fn default() -> Self { Self::new() }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefilterInfo {
    pub name: String,
    pub cardinality: u64,
    pub last_refreshed: i64,
    pub refresh_interval_secs: u64,
    pub last_compute_ms: u64,
    pub refresh_errors: u64,
    pub substitutions: u64,
    pub clauses: Vec<FilterClause>,
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("prefilter registry full (max={0})")]
    RegistryFull(usize),
    #[error("invalid clause set: {0}")]
    InvalidClauses(String),
}

// ─── Canonicalization ──────────────────────────────────────────────────────

/// Canonicalize a clause set for structural matching:
/// - Flatten nested `And` at the top level.
/// - Sort `In`/`NotIn` value lists.
/// - Collapse `Not(Not(X))` to `X`.
/// - Canonicalize children of `And`/`Or`/`Not` recursively.
/// - Sort top-level clauses into a deterministic order and dedupe.
///
/// Does NOT try to prove `Or` ↔ `In` equivalence or reorder `Or` children
/// (handled at a higher level if needed).
pub fn canonicalize_clauses(clauses: &[FilterClause]) -> Vec<FilterClause> {
    let mut flat: Vec<FilterClause> = Vec::with_capacity(clauses.len());
    for c in clauses {
        flatten_and_into(c.clone(), &mut flat);
    }
    let mut out: Vec<FilterClause> = flat.into_iter().map(canonicalize_clause).collect();
    out.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    out.dedup();
    out
}

fn flatten_and_into(clause: FilterClause, out: &mut Vec<FilterClause>) {
    match clause {
        FilterClause::And(children) => {
            for c in children {
                flatten_and_into(c, out);
            }
        }
        other => out.push(other),
    }
}

fn canonicalize_clause(clause: FilterClause) -> FilterClause {
    match clause {
        FilterClause::In(field, mut values) => {
            sort_values(&mut values);
            values.dedup();
            FilterClause::In(field, values)
        }
        FilterClause::NotIn(field, mut values) => {
            sort_values(&mut values);
            values.dedup();
            FilterClause::NotIn(field, values)
        }
        FilterClause::Not(inner) => {
            if let FilterClause::Not(inner2) = *inner {
                return canonicalize_clause(*inner2);
            }
            FilterClause::Not(Box::new(canonicalize_clause(*inner)))
        }
        FilterClause::And(children) => {
            let mut flat = Vec::new();
            for c in children {
                flatten_and_into(c, &mut flat);
            }
            let mut canon: Vec<FilterClause> = flat.into_iter().map(canonicalize_clause).collect();
            canon.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            canon.dedup();
            FilterClause::And(canon)
        }
        FilterClause::Or(children) => {
            let canon: Vec<FilterClause> = children.into_iter().map(canonicalize_clause).collect();
            FilterClause::Or(canon)
        }
        other => other,
    }
}

fn sort_values(values: &mut [Value]) {
    values.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
}

/// Check whether every clause in `prefilter_clauses` appears in
/// `query_clauses`. Returns the matched query-side indices (sorted) on
/// success. Both inputs MUST already be canonicalized.
fn subset_match(
    prefilter_clauses: &[FilterClause],
    query_clauses: &[FilterClause],
) -> Option<Vec<usize>> {
    if prefilter_clauses.is_empty() || prefilter_clauses.len() > query_clauses.len() {
        return None;
    }
    let mut used = vec![false; query_clauses.len()];
    let mut matched = Vec::with_capacity(prefilter_clauses.len());
    for pc in prefilter_clauses {
        let mut found = None;
        for (i, qc) in query_clauses.iter().enumerate() {
            if !used[i] && qc == pc {
                found = Some(i);
                break;
            }
        }
        match found {
            Some(i) => { used[i] = true; matched.push(i); }
            None => return None,
        }
    }
    matched.sort_unstable();
    Some(matched)
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Apply a registered prefilter to a query clause list.
///
/// Input is the raw (post-bucket-snap) clause list. Returns a new clause list
/// where the matched prefilter clauses have been removed and a single
/// `BucketBitmap` clause prepended. If no prefilter matches, returns
/// `(original_clauses, None)`.
///
/// This is the core substitution routine used by the query planner.
pub fn substitute<'a>(
    registry: &PrefilterRegistry,
    query_clauses: &'a [FilterClause],
    alive: &RoaringBitmap,
) -> (std::borrow::Cow<'a, [FilterClause]>, Option<Arc<PrefilterEntry>>) {
    use std::borrow::Cow;
    if registry.is_empty() {
        return (Cow::Borrowed(query_clauses), None);
    }
    let canonical_query = canonicalize_clauses(query_clauses);
    let (entry, matched_in_canonical) = match registry.find_substitution(&canonical_query) {
        Some(x) => x,
        None => return (Cow::Borrowed(query_clauses), None),
    };

    // `matched_in_canonical` refers to indices in `canonical_query`, not in
    // the input `query_clauses`. Map by structural equality: remove each
    // matched canonical clause from a canonical copy of the query, then
    // rebuild the resulting clause list.
    //
    // Since we drop the matched clauses and evaluate against the prefilter
    // bitmap instead, the remaining clause order doesn't need to match the
    // caller's input — the planner will re-order anyway.
    let mut remaining: Vec<FilterClause> = Vec::with_capacity(canonical_query.len() + 1);
    for (i, c) in canonical_query.into_iter().enumerate() {
        if !matched_in_canonical.contains(&i) {
            remaining.push(c);
        }
    }

    // Intersect with alive at substitute time.
    //
    // The engine uses clean-delete: on delete, the doc's bits are cleared
    // from every filter/sort bitmap AND from alive. Filter bitmaps are thus
    // kept clean and there is no `alive AND filter` in the query hot path.
    //
    // The cached prefilter bitmap is NOT maintained by clean-delete — it's
    // a frozen snapshot. Between refreshes, bits for deleted docs remain
    // set. If a query's only constraint is the prefilter (no extra clauses
    // to narrow against a clean filter bitmap), the deleted doc would
    // re-surface in results. Guard against that by intersecting with the
    // current alive bitmap.
    //
    // Cost: one AND against alive per substituted query. At 107M records
    // this is ~20-50 ms — still a large win vs the 300-900 ms we save by
    // eliding the original clauses, and cheap insurance against drift.
    let stale = entry.bitmap.load_full();
    let live = Arc::new(stale.as_ref() & alive);
    let bucket_clause = FilterClause::BucketBitmap {
        field: "__prefilter".to_string(),
        bucket_name: entry.name.clone(),
        bitmap: live,
    };
    let mut out = Vec::with_capacity(remaining.len() + 1);
    out.push(bucket_clause);
    out.extend(remaining);

    entry.substitutions.fetch_add(1, Ordering::Relaxed);
    (Cow::Owned(out), Some(entry))
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{FilterClause, Value};

    fn eq(field: &str, val: i64) -> FilterClause {
        FilterClause::Eq(field.into(), Value::Integer(val))
    }
    fn in_(field: &str, vals: &[i64]) -> FilterClause {
        FilterClause::In(field.into(), vals.iter().map(|v| Value::Integer(*v)).collect())
    }

    #[test]
    fn canonicalize_sorts_in_values() {
        let c = canonicalize_clause(in_("x", &[3, 1, 2]));
        assert_eq!(c, in_("x", &[1, 2, 3]));
    }

    #[test]
    fn canonicalize_flattens_and() {
        let c = vec![FilterClause::And(vec![
            eq("a", 1),
            FilterClause::And(vec![eq("b", 2)]),
        ])];
        let canon = canonicalize_clauses(&c);
        assert_eq!(canon, vec![eq("a", 1), eq("b", 2)]);
    }

    #[test]
    fn canonicalize_collapses_double_not() {
        let c = FilterClause::Not(Box::new(FilterClause::Not(Box::new(eq("a", 1)))));
        assert_eq!(canonicalize_clause(c), eq("a", 1));
    }

    #[test]
    fn canonicalize_sorts_top_level_and_dedupes() {
        let c = vec![eq("b", 2), eq("a", 1), eq("a", 1)];
        let canon = canonicalize_clauses(&c);
        assert_eq!(canon.len(), 2);
        assert_eq!(canon[0].to_string(), "a = 1");
        assert_eq!(canon[1].to_string(), "b = 2");
    }

    #[test]
    fn subset_match_detects_matching_subset() {
        let prefilter = canonicalize_clauses(&[eq("a", 1), eq("b", 2)]);
        let query = canonicalize_clauses(&[eq("a", 1), eq("b", 2), eq("c", 3)]);
        let m = subset_match(&prefilter, &query).unwrap();
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn subset_match_rejects_partial_overlap() {
        let prefilter = canonicalize_clauses(&[eq("a", 1), eq("b", 2)]);
        let query = canonicalize_clauses(&[eq("a", 1), eq("c", 3)]);
        assert!(subset_match(&prefilter, &query).is_none());
    }

    #[test]
    fn registry_insert_and_get() {
        let reg = PrefilterRegistry::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(42);
        let entry = reg.insert("test".into(), vec![eq("a", 1)], bm, 300, 1_000_000).unwrap();
        assert_eq!(entry.cardinality(), 1);
        let got = reg.get("test").unwrap();
        assert_eq!(got.cardinality(), 1);
        assert_eq!(got.refresh_interval_secs(), 300);
    }

    #[test]
    fn registry_replaces_existing_name() {
        let reg = PrefilterRegistry::new();
        let mut bm1 = RoaringBitmap::new(); bm1.insert(1);
        let mut bm2 = RoaringBitmap::new(); bm2.insert(2); bm2.insert(3);
        reg.insert("x".into(), vec![eq("a", 1)], bm1, 300, 0).unwrap();
        reg.insert("x".into(), vec![eq("a", 1)], bm2, 300, 0).unwrap();
        assert_eq!(reg.get("x").unwrap().cardinality(), 2);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn registry_enforces_max() {
        let mut reg = PrefilterRegistry::new();
        reg.max_entries = 2;
        reg.insert("a".into(), vec![eq("x", 1)], RoaringBitmap::new(), 300, 0).unwrap();
        reg.insert("b".into(), vec![eq("x", 2)], RoaringBitmap::new(), 300, 0).unwrap();
        let err = reg.insert("c".into(), vec![eq("x", 3)], RoaringBitmap::new(), 300, 0);
        assert!(matches!(err, Err(RegistryError::RegistryFull(_))));
    }

    #[test]
    fn registry_rejects_empty_clauses() {
        let reg = PrefilterRegistry::new();
        let err = reg.insert("x".into(), vec![], RoaringBitmap::new(), 300, 0);
        assert!(matches!(err, Err(RegistryError::InvalidClauses(_))));
    }

    #[test]
    fn registry_clamps_interval() {
        let reg = PrefilterRegistry::new();
        let e1 = reg.insert("a".into(), vec![eq("x", 1)], RoaringBitmap::new(), 5, 0).unwrap();
        assert_eq!(e1.refresh_interval_secs(), MIN_REFRESH_INTERVAL_SECS);
        let e2 = reg.insert("b".into(), vec![eq("x", 1)], RoaringBitmap::new(), 999_999, 0).unwrap();
        assert_eq!(e2.refresh_interval_secs(), MAX_REFRESH_INTERVAL_SECS);
    }

    #[test]
    fn find_substitution_prefers_largest_match() {
        let reg = PrefilterRegistry::new();
        reg.insert("small".into(), vec![eq("a", 1)], RoaringBitmap::new(), 300, 0).unwrap();
        reg.insert("big".into(), vec![eq("a", 1), eq("b", 2)], RoaringBitmap::new(), 300, 0).unwrap();
        let query = canonicalize_clauses(&[eq("a", 1), eq("b", 2), eq("c", 3)]);
        let (entry, matched) = reg.find_substitution(&query).unwrap();
        assert_eq!(entry.name, "big");
        assert_eq!(matched.len(), 2);
    }

    fn alive_all() -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        bm.insert_range(0..100u32);
        bm
    }

    #[test]
    fn substitute_replaces_matched_clauses_with_bucket_bitmap() {
        let reg = PrefilterRegistry::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(10); bm.insert(20);
        reg.insert("safe".into(), vec![eq("a", 1), eq("b", 2)], bm, 300, 0).unwrap();
        let query = vec![eq("c", 3), eq("a", 1), eq("b", 2)];
        let alive = alive_all();
        let (out, entry) = substitute(&reg, &query, &alive);
        let entry = entry.unwrap();
        assert_eq!(entry.name, "safe");
        assert_eq!(out.len(), 2);
        // First clause must be the BucketBitmap substitution.
        match &out[0] {
            FilterClause::BucketBitmap { bucket_name, bitmap, .. } => {
                assert_eq!(bucket_name, "safe");
                assert_eq!(bitmap.len(), 2);
            }
            other => panic!("expected BucketBitmap first, got {other:?}"),
        }
        // Remaining clause should be `c = 3` (the un-matched clause).
        match &out[1] {
            FilterClause::Eq(f, _) => assert_eq!(f, "c"),
            other => panic!("expected Eq(c,..), got {other:?}"),
        }
    }

    #[test]
    fn substitute_excludes_dead_slots_from_stale_prefilter() {
        // Regression: cached prefilter has bits for slots 10, 20, 30, but
        // slot 20 has since been deleted (not in alive). Substitution must
        // NOT leak slot 20 through the returned BucketBitmap.
        let reg = PrefilterRegistry::new();
        let mut stale = RoaringBitmap::new();
        stale.insert(10); stale.insert(20); stale.insert(30);
        reg.insert("safe".into(), vec![eq("a", 1)], stale, 300, 0).unwrap();

        let mut alive = RoaringBitmap::new();
        alive.insert(10); alive.insert(30); // 20 was deleted

        let query = vec![eq("a", 1)];
        let (out, _) = substitute(&reg, &query, &alive);
        match &out[0] {
            FilterClause::BucketBitmap { bitmap, .. } => {
                assert_eq!(bitmap.len(), 2, "dead slot 20 must not leak through");
                assert!(bitmap.contains(10));
                assert!(!bitmap.contains(20), "slot 20 was deleted, must not appear");
                assert!(bitmap.contains(30));
            }
            other => panic!("expected BucketBitmap, got {other:?}"),
        }
    }

    #[test]
    fn substitute_is_noop_when_no_match() {
        let reg = PrefilterRegistry::new();
        reg.insert("safe".into(), vec![eq("a", 1)], RoaringBitmap::new(), 300, 0).unwrap();
        let query = vec![eq("b", 2)];
        let alive = alive_all();
        let (out, entry) = substitute(&reg, &query, &alive);
        assert!(entry.is_none());
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn substitute_is_noop_on_empty_registry() {
        let reg = PrefilterRegistry::new();
        let query = vec![eq("a", 1)];
        let alive = alive_all();
        let (out, entry) = substitute(&reg, &query, &alive);
        assert!(entry.is_none());
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn canonicalize_does_not_reorder_or_children() {
        // Design open question #1: Or(A, B) and Or(B, A) currently canonicalize
        // distinctly. This regression test pins that behavior — a future
        // change adding Or reordering must update it explicitly (and consider
        // the test coverage implied by reordering).
        let c1 = FilterClause::Or(vec![eq("a", 1), eq("b", 2)]);
        let c2 = FilterClause::Or(vec![eq("b", 2), eq("a", 1)]);
        assert_ne!(canonicalize_clause(c1), canonicalize_clause(c2),
            "Or canonicalization changed — update substitution matching logic or this test");
    }
}
