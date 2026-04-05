//! Standalone sort traversal from BitmapSilo frozen layers.
//!
//! Performs the same MSB-to-LSB bifurcation algorithm as `SortField::top_n_frozen`,
//! but reads ALL bit-layers directly from `BitmapSilo` without requiring an
//! in-memory `SortField`. This is the primary sort path when sort layers are
//! fully backed by the silo (e.g., after a restore or during incremental loading).

use roaring::RoaringBitmap;

use crate::silos::bitmap_silo::BitmapSilo;

/// Sort traversal using only frozen BitmapSilo layers. No in-memory SortField needed.
///
/// Performs MSB-to-LSB bifurcation across `num_bits` sort layers, reading each
/// bit-layer directly from the silo's mmap via `get_frozen_sort_layer`.
///
/// # Arguments
/// - `silo` — bitmap silo holding the frozen sort layers
/// - `field_name` — name of the sort field (used to build the silo key)
/// - `num_bits` — number of bit layers for this field (from `SortFieldConfig.bits`)
/// - `candidates` — working set of slot IDs to sort (already filtered)
/// - `limit` — maximum number of results to return
/// - `descending` — if true, return highest values first
/// - `cursor` — optional pagination cursor as `(sort_value, slot_id)` pair
///
/// Returns up to `limit` slot IDs in sorted order.
pub fn frozen_top_n(
    silo: &BitmapSilo,
    field_name: &str,
    num_bits: usize,
    candidates: &RoaringBitmap,
    limit: usize,
    descending: bool,
    cursor: Option<(u64, u32)>,
) -> Vec<u32> {
    if candidates.is_empty() || limit == 0 {
        return Vec::new();
    }

    // Apply cursor filtering if present
    let effective_candidates;
    let candidates = if let Some((cursor_sort_value, cursor_slot_id)) = cursor {
        effective_candidates =
            apply_cursor_filter(silo, field_name, num_bits, candidates, descending, cursor_sort_value, cursor_slot_id);
        &effective_candidates
    } else {
        candidates
    };

    if candidates.is_empty() {
        return Vec::new();
    }

    // MSB-to-LSB bifurcation: collect top-N slots via bitmap AND operations
    let top_n_bitmap = bifurcate(silo, field_name, num_bits, candidates, limit, descending);

    // Reconstruct values ONLY for the final top-N slots and sort them
    order_results(silo, field_name, num_bits, &top_n_bitmap, descending)
}

/// Reconstruct the sort value for a single slot from frozen BitmapSilo layers.
///
/// Reads each bit-layer from the silo and assembles the value by OR-ing bits.
/// Layers not present in the silo are treated as all-zeros (the bit contributes 0).
///
/// # Arguments
/// - `silo` — bitmap silo holding the frozen sort layers
/// - `field_name` — sort field name
/// - `num_bits` — number of bit layers
/// - `slot` — slot ID whose value to reconstruct
pub fn frozen_reconstruct_value(
    silo: &BitmapSilo,
    field_name: &str,
    num_bits: usize,
    slot: u32,
) -> u32 {
    let mut value = 0u32;
    for bit in 0..num_bits {
        let contains = silo
            .get_frozen_sort_layer(field_name, bit)
            .map_or_else(
                || {
                    // Frozen not available (data not compacted yet) — fall back to ops log
                    silo.get_sort_layer_with_ops(field_name, bit)
                        .map_or(false, |bm| bm.contains(slot))
                },
                |frozen| frozen.contains(slot),
            );
        if contains {
            value |= 1 << bit;
        }
    }
    value
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// MSB-to-LSB bifurcation over silo frozen layers.
///
/// Identical in structure to `SortField::bifurcate_frozen`, but reads exclusively
/// from the silo. Missing layers (not stored in silo) are treated as all-zeros.
fn bifurcate(
    silo: &BitmapSilo,
    field_name: &str,
    num_bits: usize,
    candidates: &RoaringBitmap,
    limit: usize,
    descending: bool,
) -> RoaringBitmap {
    let total = candidates.len() as usize;
    if total <= limit {
        return candidates.clone();
    }

    let mut result = RoaringBitmap::new();
    let mut remaining = candidates.clone();
    let mut remaining_limit = limit;

    for bit in (0..num_bits).rev() {
        if remaining_limit == 0 || remaining.is_empty() {
            break;
        }

        // Try frozen data file first, fall back to ops log
        let layer_bitmap: RoaringBitmap;
        let preferred: RoaringBitmap = if let Some(frozen) = silo.get_frozen_sort_layer(field_name, bit) {
            if descending { &remaining & &frozen } else { &remaining - &frozen }
        } else if let Some(ops_bm) = silo.get_sort_layer_with_ops(field_name, bit) {
            layer_bitmap = ops_bm;
            if descending { &remaining & &layer_bitmap } else { &remaining - &layer_bitmap }
        } else {
            continue;
        };

        let preferred_count = preferred.len() as usize;

        if preferred_count == 0 {
            continue;
        } else if preferred_count >= remaining_limit {
            remaining = preferred;
        } else {
            result |= &preferred;
            remaining -= &preferred;
            remaining_limit -= preferred_count;
        }
    }

    // After all layers, take any still-needed slots from remaining
    if remaining_limit > 0 && !remaining.is_empty() {
        let mut taken = 0;
        for slot in remaining.iter() {
            if taken >= remaining_limit {
                break;
            }
            result.insert(slot);
            taken += 1;
        }
    }

    result
}

/// Reconstruct sort values for all slots in `result_bitmap`, then sort and return slot IDs.
fn order_results(
    silo: &BitmapSilo,
    field_name: &str,
    num_bits: usize,
    result_bitmap: &RoaringBitmap,
    descending: bool,
) -> Vec<u32> {
    let mut entries: Vec<(u32, u32)> = result_bitmap
        .iter()
        .map(|slot| (slot, frozen_reconstruct_value(silo, field_name, num_bits, slot)))
        .collect();

    if descending {
        entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
    } else {
        entries.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
    }

    entries.into_iter().map(|(slot, _)| slot).collect()
}

/// Cursor filter for frozen-only traversal.
///
/// Eliminates candidates that come before the cursor position in the sort order,
/// enabling correct pagination from a prior page's last item.
fn apply_cursor_filter(
    silo: &BitmapSilo,
    field_name: &str,
    num_bits: usize,
    candidates: &RoaringBitmap,
    descending: bool,
    cursor_sort_value: u64,
    cursor_slot_id: u32,
) -> RoaringBitmap {
    let cursor_value = cursor_sort_value as u32;

    let mut confirmed = RoaringBitmap::new();
    let mut equal = candidates.clone();

    for bit in (0..num_bits).rev() {
        if equal.is_empty() {
            break;
        }

        let cursor_bit_set = (cursor_value >> bit) & 1 == 1;

        // Try frozen data file first, fall back to ops log
        let ops_fallback: RoaringBitmap;
        let (equal_with_bit_set, equal_with_bit_clear) =
            if let Some(frozen) = silo.get_frozen_sort_layer(field_name, bit) {
                (&equal & &frozen, &equal - &frozen)
            } else if let Some(bm) = silo.get_sort_layer_with_ops(field_name, bit) {
                ops_fallback = bm;
                (&equal & &ops_fallback, &equal - &ops_fallback)
            } else {
                (RoaringBitmap::new(), equal.clone())
            };

        if descending {
            if cursor_bit_set {
                // Slots with bit clear are strictly less → confirmed winners
                confirmed |= &equal_with_bit_clear;
                equal = equal_with_bit_set;
            } else {
                // All set-bit slots are > cursor on this layer — exclude them
                equal = equal_with_bit_clear;
            }
        } else {
            // Ascending: lower values win
            if cursor_bit_set {
                // All clear-bit slots are < cursor on this layer — exclude them
                equal = equal_with_bit_set;
            } else {
                // Slots with bit set are strictly greater → confirmed winners
                confirmed |= &equal_with_bit_set;
                equal = equal_with_bit_clear;
            }
        }
    }

    // Slot ID tiebreaker for slots with identical sort value to cursor
    if !equal.is_empty() {
        if descending {
            equal.remove_range(cursor_slot_id..=u32::MAX);
        } else {
            equal.remove_range(0..=cursor_slot_id);
        }
        confirmed |= equal;
    }

    confirmed
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::filter::FilterIndex;
    use crate::engine::sort::SortIndex;
    use crate::engine::slot::SlotAllocator;
    use crate::config::SortFieldConfig;
    use std::collections::HashMap;

    /// Build a BitmapSilo populated with sort layers for `field_name` by encoding
    /// `values` (slot → value) into bit-layer bitmaps, saving to the silo, and
    /// returning the silo. Uses a temp directory that lives for the test.
    fn build_silo(
        field_name: &str,
        num_bits: usize,
        values: &[(u32, u32)], // (slot, value)
    ) -> (tempfile::TempDir, crate::silos::bitmap_silo::BitmapSilo) {
        let dir = tempfile::tempdir().unwrap();

        // Build bit-layer bitmaps
        let mut layers: Vec<RoaringBitmap> = (0..num_bits).map(|_| RoaringBitmap::new()).collect();
        for &(slot, value) in values {
            for bit in 0..num_bits {
                if (value >> bit) & 1 == 1 {
                    layers[bit].insert(slot);
                }
            }
        }

        // Write to silo via save_all
        let mut silo = crate::silos::bitmap_silo::BitmapSilo::open(dir.path()).unwrap();
        let mut slots = SlotAllocator::new();
        for &(slot, _) in values {
            slots.allocate(slot).unwrap();
        }
        slots.merge_alive();
        let mut filters = FilterIndex::new();
        let mut sorts = SortIndex::new();
        sorts.add_field(SortFieldConfig {
            name: field_name.to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: num_bits as u8,
            eager_load: false,
            computed: None,
        });
        // Load layers into sort field
        if let Some(field) = sorts.get_field_mut(field_name) {
            field.load_layers(layers);
        }
        silo.save_all(&filters, &sorts, &slots, &HashMap::new()).unwrap();
        (dir, silo)
    }

    #[test]
    fn test_frozen_reconstruct_value() {
        let values = vec![(0u32, 5u32), (1, 3), (2, 10), (3, 0)];
        let (_dir, silo) = build_silo("score", 8, &values);
        for (slot, expected) in &values {
            assert_eq!(
                frozen_reconstruct_value(&silo, "score", 8, *slot),
                *expected,
                "slot {slot} value mismatch"
            );
        }
    }

    #[test]
    fn test_frozen_top_n_descending() {
        let values = vec![(0u32, 10u32), (1, 7), (2, 15), (3, 3)];
        let (_dir, silo) = build_silo("score", 8, &values);
        let candidates: RoaringBitmap = values.iter().map(|&(s, _)| s).collect();

        let result = frozen_top_n(&silo, "score", 8, &candidates, 3, true, None);
        // Expect descending: slot 2 (15), slot 0 (10), slot 1 (7)
        assert_eq!(result, vec![2, 0, 1]);
    }

    #[test]
    fn test_frozen_top_n_ascending() {
        let values = vec![(0u32, 10u32), (1, 7), (2, 15), (3, 3)];
        let (_dir, silo) = build_silo("score", 8, &values);
        let candidates: RoaringBitmap = values.iter().map(|&(s, _)| s).collect();

        let result = frozen_top_n(&silo, "score", 8, &candidates, 3, false, None);
        // Expect ascending: slot 3 (3), slot 1 (7), slot 0 (10)
        assert_eq!(result, vec![3, 1, 0]);
    }

    #[test]
    fn test_frozen_top_n_empty_candidates() {
        let values = vec![(0u32, 5u32)];
        let (_dir, silo) = build_silo("score", 8, &values);
        let candidates = RoaringBitmap::new();
        let result = frozen_top_n(&silo, "score", 8, &candidates, 10, true, None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_frozen_top_n_zero_limit() {
        let values = vec![(0u32, 5u32), (1, 10u32)];
        let (_dir, silo) = build_silo("score", 8, &values);
        let candidates: RoaringBitmap = values.iter().map(|&(s, _)| s).collect();
        let result = frozen_top_n(&silo, "score", 8, &candidates, 0, true, None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_frozen_top_n_single_candidate() {
        let values = vec![(42u32, 99u32)];
        let (_dir, silo) = build_silo("score", 8, &values);
        let candidates: RoaringBitmap = [42u32].iter().cloned().collect();

        let result_desc = frozen_top_n(&silo, "score", 8, &candidates, 1, true, None);
        assert_eq!(result_desc, vec![42]);

        let result_asc = frozen_top_n(&silo, "score", 8, &candidates, 1, false, None);
        assert_eq!(result_asc, vec![42]);
    }

    #[test]
    fn test_frozen_top_n_with_cursor_descending() {
        // Values: slot→value: 0→20, 1→15, 2→10, 3→5
        let values = vec![(0u32, 20u32), (1, 15), (2, 10), (3, 5)];
        let (_dir, silo) = build_silo("score", 8, &values);
        let candidates: RoaringBitmap = values.iter().map(|&(s, _)| s).collect();

        // First page: top 2 descending → [0 (20), 1 (15)]
        let page1 = frozen_top_n(&silo, "score", 8, &candidates, 2, true, None);
        assert_eq!(page1, vec![0, 1]);

        // Cursor = last of page1: slot 1, value 15
        let cursor = Some((15u64, 1u32));
        let page2 = frozen_top_n(&silo, "score", 8, &candidates, 2, true, cursor);
        // After cursor (15, slot 1): remaining are slot 2 (10), slot 3 (5)
        assert_eq!(page2, vec![2, 3]);
    }

    #[test]
    fn test_frozen_top_n_with_cursor_ascending() {
        let values = vec![(0u32, 5u32), (1, 10), (2, 15), (3, 20)];
        let (_dir, silo) = build_silo("score", 8, &values);
        let candidates: RoaringBitmap = values.iter().map(|&(s, _)| s).collect();

        // First page ascending → [0 (5), 1 (10)]
        let page1 = frozen_top_n(&silo, "score", 8, &candidates, 2, false, None);
        assert_eq!(page1, vec![0, 1]);

        // Cursor = last of page1: slot 1, value 10
        let cursor = Some((10u64, 1u32));
        let page2 = frozen_top_n(&silo, "score", 8, &candidates, 2, false, cursor);
        assert_eq!(page2, vec![2, 3]);
    }

    #[test]
    fn test_frozen_top_n_tied_values_stable_by_slot() {
        // Two slots with the same value — lower slot ID wins tiebreaker in ascending,
        // higher slot ID wins in descending
        let values = vec![(10u32, 7u32), (20, 7), (30, 7)];
        let (_dir, silo) = build_silo("score", 8, &values);
        let candidates: RoaringBitmap = values.iter().map(|&(s, _)| s).collect();

        let asc = frozen_top_n(&silo, "score", 8, &candidates, 3, false, None);
        // Ascending: ties broken by slot ID ascending → 10, 20, 30
        assert_eq!(asc, vec![10, 20, 30]);

        let desc = frozen_top_n(&silo, "score", 8, &candidates, 3, true, None);
        // Descending: ties broken by slot ID descending → 30, 20, 10
        assert_eq!(desc, vec![30, 20, 10]);
    }

    #[test]
    fn test_frozen_top_n_limit_larger_than_candidates() {
        let values = vec![(0u32, 3u32), (1, 1), (2, 2)];
        let (_dir, silo) = build_silo("score", 8, &values);
        let candidates: RoaringBitmap = values.iter().map(|&(s, _)| s).collect();

        let result = frozen_top_n(&silo, "score", 8, &candidates, 100, true, None);
        // All 3 returned, descending
        assert_eq!(result, vec![0, 2, 1]);
    }
}
