//! Integration tests that run CSV parsers against real PG/CH fixture data.
//!
//! These fixtures are small samples (100 rows) dumped from production using
//! the exact same COPY queries as copy_queries.rs. They catch type mismatches,
//! column format changes, and parser regressions.

#[cfg(feature = "pg-sync")]
mod pg_sync_fixtures {
    use bitdex_v2::pg_sync::backfill::process_collection_items_csv;
    use std::path::Path;

    /// Verify collection_items.csv parses without error against real PG data.
    #[test]
    fn test_collection_items_fixture_parses() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/csv");
        // Symlink or copy the fixture so backfill finds it
        let stage = tempfile::tempdir().unwrap();
        std::fs::copy(
            fixtures.join("collection_items.csv"),
            stage.path().join("collection_items.csv"),
        )
        .unwrap();
        std::fs::write(stage.path().join("collection_items.csv.done"), b"ok").unwrap();

        let bitmaps = process_collection_items_csv(stage.path()).unwrap();

        // Should have parsed rows successfully
        assert!(!bitmaps.is_empty(), "Expected non-empty bitmaps from fixture");

        // Verify bitmap values are reasonable (collectionIds and imageIds are positive i32)
        for (&collection_id, bm) in &bitmaps {
            assert!(
                collection_id <= i32::MAX as u64,
                "collectionId {} exceeds i32 range",
                collection_id
            );
            for slot in bm.iter() {
                assert!(
                    slot <= i32::MAX as u32,
                    "imageId {} exceeds i32 range",
                    slot
                );
            }
        }

        eprintln!(
            "collection_items fixture: {} distinct collectionIds, {} total memberships",
            bitmaps.len(),
            bitmaps.values().map(|bm| bm.len()).sum::<u64>()
        );
    }
}
