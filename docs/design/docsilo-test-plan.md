# DocSilo / DataSilo Correctness Test Plan

> Tests must ALL pass before any code ships.
> Write tests first, then implement until green.

---

## Layer 1: DataSilo crate (`crates/datasilo/`)

### Basic CRUD

1. `test_put_get_roundtrip` — put a single entry, get it back, bytes match
2. `test_put_overwrite` — put same key twice, get returns latest value
3. `test_get_missing_key` — get non-existent key returns None
4. `test_put_many_get_many` — put 1000 entries, get_many returns all
5. `test_delete_removes_entry` — put then delete via ops, get returns None

### Bulk load

6. `test_bulk_load_parallel` — bulk_load 10K entries with rayon, all readable
7. `test_bulk_load_then_single_put` — bulk load, then put to existing key via ops, get sees new value
8. `test_bulk_load_capacity_overflow` — exceed estimated capacity, error returned (not silent data loss)

### In-place update (DumpMergeWriter)

9. `test_merge_put_combines_data` — write entry, merge_put with merge function that concatenates, get returns merged result
10. `test_merge_put_fits_in_buffer` — merged data fits in buffer_ratio allocation, in_place_count increments
11. `test_merge_put_overflow` — merged data exceeds buffer, overflow_count increments, returns false
12. `test_merge_put_empty_slot` — merge_put on key with length=0, writes new_bytes directly without calling merge_fn
13. `test_merge_put_concurrent` — 32 rayon threads merge_put different keys simultaneously, all results correct
14. `test_merge_put_same_key_serialized` — two threads merge_put same key, striped lock ensures no data race, final value is consistent

### Ops log

15. `test_ops_log_append_and_replay` — append ops, close, reopen, all ops replayed on read
16. `test_ops_log_crc_corruption` — corrupt one byte in ops log, replay skips corrupted entry, others survive
17. `test_get_with_ops_applies_pending` — put via ops log (no compact), get returns ops-applied value (NOT stale snapshot)
18. `test_ops_survive_reopen` — append ops, close, reopen, get_with_ops still returns correct value
19. `test_ab_swap_no_ops_lost` — ops written during compaction are not lost (A/B slot swap design)

### Compaction

20. `test_compact_merges_ops_into_snapshot` — write ops, compact, ops log cleared, get still returns correct data
21. `test_compact_preserves_all_data` — put N entries, compact, all N still readable with correct values
22. `test_compact_with_merge_function` — multiple ops on same key need union-merge, compaction produces correct merged result (not LWW)
23. `test_hot_compact_in_place` — update fits in buffer, hot compact reuses allocated space, no relocation
24. `test_hot_compact_concurrent_read_safe` — hot compact does not drop read mmap while readers are active
25. `test_lww_without_merge_fn` — when no merge function is set, compaction uses last-write-wins (default)

### HashIndex

26. `test_hash_index_build_and_lookup` — build from entries, all lookable
27. `test_hash_index_update_existing_concurrent` — update entry's length in-place, lookup returns new values
28. `test_hash_index_collision_handling` — keys that hash to same bucket, linear probing finds all
29. `test_hash_index_reopen` — build, persist, reopen from file, all entries present
30. `test_hash_index_reserved_keys` — KEY_EMPTY and KEY_TOMBSTONE are rejected

---

## Layer 2: DocSilo wrapper (`src/doc_silo.rs`)

### StoredDoc roundtrip

31. `test_doc_silo_put_get` — put StoredDoc with I/B/S/Mi fields, get returns identical fields
32. `test_doc_silo_put_batch` — put_batch 100 docs, get each, all correct
33. `test_doc_silo_get_many` — put 50, get_many returns all with correct fields

### Field dictionary

34. `test_field_dict_persistence` — ensure_field_index, save_field_dict, reopen, same name→index mapping
35. `test_field_dict_auto_register` — put doc with new field name, field auto-registered, subsequent get uses same index

### Ops path (steady-state writes) — P0 gap from audit

36. `test_ops_set_visible_on_read` — apply DocOp::Set via ops log, get returns updated value WITHOUT compaction
37. `test_ops_append_merges_multi_int` — apply DocOp::Append to Mi field, get returns union of original + appended values
38. `test_ops_remove_subtracts_multi_int` — apply DocOp::Remove from Mi field, get returns original minus removed values
39. `test_ops_survive_restart` — apply ops, close DocSilo, reopen, get still returns ops-applied values

### Multi-phase dump (the critical path) — P0 gap from audit

40. `test_multi_phase_images_then_tags` — phase 1: bulk write image docs (nsfwLevel=1, sortAt=100). Phase 2: DumpMergeWriter merge_put adds tagIds=[10,20]. Final get returns doc with BOTH nsfwLevel=1 AND tagIds=[10,20].
41. `test_multi_phase_three_phases` — phase 1: image fields, phase 2: tag fields, phase 3: resource fields. Final doc has ALL fields from all 3 phases.
42. `test_multi_phase_no_data_loss` — phase 1 writes 1000 docs, phase 2 merges into same 1000 slots. get_many after both phases returns 1000 docs, each with combined fields. Zero docs lost.
43. `test_multi_phase_no_field_loss` — verify every field written in phase 1 is still present after phase 2 merge. Field-by-field assertion.
44. `test_multi_phase_memory_bounded` — track peak layout Vec size during multi-phase dump. Assert it stays under LAYOUT_SPILL_THRESHOLD × num_threads × 24 bytes.

### DocWriter (steady-state sync-v2)

45. `test_doc_writer_set_visible` — DocWriter::new, write_set field, flush, DocSilo::get returns the value
46. `test_doc_writer_append_remove` — write_add tagId, write_remove tagId, flush, Mi field has correct values

### Cold restart

47. `test_cold_restart_preserves_docs` — put docs via bulk + ops, close, DocSilo::open, all docs readable
48. `test_cold_restart_replays_ops` — put base docs, apply ops, close, reopen, get returns fully ops-applied values
49. `test_cold_restart_field_dict_intact` — close, reopen, field_to_idx matches what was saved

---

## Test priority for current bugs

| Bug | Tests that catch it |
|-----|-------------------|
| data.bin truncated between phases | #40, #41, #42, #43 |
| No DumpMergeWriter | #9, #10, #11, #12, #13, #14 |
| get/get_many don't apply ops | #17, #36, #37, #38 |
| No mmap reload between phases | #40, #41 (would fail without reload) |
| Compaction LWW instead of union | #22 |
| Layout memory unbounded | #44 |

Total: **49 tests**
