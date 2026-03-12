//! Generic NDJSON loader — converts arbitrary NDJSON files to engine Documents
//! using a DataSchema definition.
//!
//! Three-stage pipeline:
//!   Stage 1 (reader thread): reads raw bytes from disk into blocks
//!   Stage 2 (parse thread):  rayon fold+reduce → bitmap maps + full docs (fused)
//!   Stage 3 (main thread):   apply bitmaps to staging + async docstore writes
//!
//! Key optimization: bitmaps are built directly from JSON during parse — no
//! intermediate Document allocation for the bitmap path. The old decompose/merge
//! pipeline in put_bulk_into is bypassed entirely.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::concurrent_engine::ConcurrentEngine;
use crate::config::{DataSchema, FieldMapping, FieldValueType};
use crate::mutation::{Document, FieldValue};
use crate::query::Value;
#[cfg(test)]
use crate::docstore::StoredDoc;

/// Statistics from a completed load operation.
#[derive(Debug, Clone)]
pub struct LoadStats {
    pub records_loaded: u64,
    pub elapsed: Duration,
    pub errors_skipped: u64,
}

/// Bitmap accumulator for rayon fold+reduce.
/// Each rayon task builds its own instance; reduce merges them with bitmap OR.
pub(crate) struct BitmapAccum {
    pub(crate) filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>>,
    pub(crate) sort_maps: HashMap<String, HashMap<usize, RoaringBitmap>>,
    pub(crate) alive: RoaringBitmap,
    /// Pre-encoded msgpack bytes — encoding happens in the rayon fold so
    /// BulkWriter does pure I/O with no rayon contention.
    pub(crate) encoded_docs: Vec<(u32, Vec<u8>)>,
    pub(crate) count: usize,
    pub(crate) errors: u64,
}

impl BitmapAccum {
    pub(crate) fn new(filter_names: &[String], sort_configs: &[(String, u8)]) -> Self {
        let mut filter_maps = HashMap::with_capacity(filter_names.len());
        for name in filter_names {
            filter_maps.insert(name.clone(), HashMap::new());
        }
        let mut sort_maps = HashMap::with_capacity(sort_configs.len());
        for (name, bits) in sort_configs {
            sort_maps.insert(name.clone(), HashMap::with_capacity(*bits as usize));
        }
        BitmapAccum {
            filter_maps,
            sort_maps,
            alive: RoaringBitmap::new(),
            encoded_docs: Vec::new(),
            count: 0,
            errors: 0,
        }
    }

    pub(crate) fn merge(mut self, other: Self) -> Self {
        self.alive |= &other.alive;
        for (field, value_map) in other.filter_maps {
            let target = self.filter_maps.entry(field).or_default();
            for (value, bm) in value_map {
                target
                    .entry(value)
                    .and_modify(|e| *e |= &bm)
                    .or_insert(bm);
            }
        }
        for (field, bit_map) in other.sort_maps {
            let target = self.sort_maps.entry(field).or_default();
            for (bit, bm) in bit_map {
                target
                    .entry(bit)
                    .and_modify(|e| *e |= &bm)
                    .or_insert(bm);
            }
        }
        self.encoded_docs.extend(other.encoded_docs);
        self.count += other.count;
        self.errors += other.errors;
        self
    }
}

/// Load an NDJSON file into an engine using the given data schema.
///
/// - `engine`: target ConcurrentEngine (must already be constructed with the right config)
/// - `schema`: field mapping rules for converting raw JSON → Documents
/// - `path`: path to the NDJSON file
/// - `limit`: optional max records to load
/// - `threads`: number of threads (unused — rayon manages parallelism)
/// - `chunk_size`: number of full docs to accumulate before flushing docstore
/// - `docstore_batch_size`: unused
/// - `progress`: atomic counter updated as records are loaded (for progress polling)
pub fn load_ndjson(
    engine: &ConcurrentEngine,
    schema: &DataSchema,
    path: &Path,
    limit: Option<usize>,
    _threads: usize,
    chunk_size: usize,
    _docstore_batch_size: usize,
    progress: Arc<AtomicU64>,
) -> Result<LoadStats, String> {
    let record_limit = limit.unwrap_or(usize::MAX);
    let _chunk_size = chunk_size; // kept for API compat; docstore flushes per block now
    let read_batch_size: usize = 500_000;
    let target_batch_bytes = read_batch_size * 600;

    // Pre-build field lookup tables for direct bitmap extraction
    let config = engine.config();
    let filter_names: Vec<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_configs: Vec<(String, u8)> = config
        .sort_fields
        .iter()
        .map(|f| (f.name.clone(), f.bits))
        .collect();
    let filter_set: HashSet<String> = filter_names.iter().cloned().collect();
    let sort_bits: HashMap<String, u8> = sort_configs.iter().cloned().collect();

    // ---- Stage 1: Reader thread ----
    // Reads raw bytes from disk in large blocks, split on newline boundaries.
    let data_path_owned = path.to_owned();
    let (block_tx, block_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(2);

    let reader_handle = thread::spawn(move || {
        let file = File::open(&data_path_owned).expect("Failed to open data file");
        let mut reader = std::io::BufReader::with_capacity(16 * 1024 * 1024, file);
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        let mut accum = Vec::<u8>::with_capacity(target_batch_bytes + 4 * 1024 * 1024);

        loop {
            let bytes_read = reader.read(&mut buf).unwrap_or(0);
            if bytes_read == 0 {
                if !accum.is_empty() {
                    let _ = block_tx.send(accum);
                }
                break;
            }
            accum.extend_from_slice(&buf[..bytes_read]);

            if accum.len() >= target_batch_bytes {
                if let Some(last_nl) = memrchr_newline(&accum) {
                    let remainder = accum[last_nl + 1..].to_vec();
                    accum.truncate(last_nl + 1);
                    let batch = std::mem::replace(
                        &mut accum,
                        Vec::with_capacity(target_batch_bytes + 4 * 1024 * 1024),
                    );
                    accum = remainder;
                    if block_tx.send(batch).is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Prepare BulkWriter before Stage 2 so encoding happens in the rayon fold.
    // This eliminates rayon contention — all CPU work in one pool pass.
    let all_field_names: Vec<String> = schema
        .fields
        .iter()
        .map(|f| f.target.clone())
        .chain(std::iter::once("id".to_string()))
        .collect();
    let bulk_writer = Arc::new(
        engine
            .prepare_bulk_writer(&all_field_names)
            .expect("prepare_bulk_writer"),
    );

    // ---- Stage 2: Fused parse + bitmap build + doc encode thread ----
    // Rayon fold+reduce: JSON → bitmap maps + pre-encoded msgpack bytes in one pass.
    // No intermediate Document for the bitmap path; encoding in-fold avoids rayon contention.
    let schema_ref = schema.clone();
    let filter_names_clone = filter_names.clone();
    let sort_configs_clone = sort_configs.clone();
    let filter_set_clone = filter_set;
    let sort_bits_clone = sort_bits;
    let parse_writer = Arc::clone(&bulk_writer);
    let (chunk_tx, chunk_rx) = std::sync::mpsc::sync_channel::<BitmapAccum>(2);

    let id_field = schema_ref.id_field.clone();
    let parse_handle = thread::spawn(move || {
        let mut total_parsed: usize = 0;

        while let Ok(raw_block) = block_rx.recv() {
            if total_parsed >= record_limit {
                break;
            }

            let block_str = match std::str::from_utf8(&raw_block) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let mut lines: Vec<&str> = block_str
                .split('\n')
                .map(|l| l.trim_end_matches('\r'))
                .filter(|l| !l.is_empty())
                .collect();

            // Respect limit
            let remaining = record_limit.saturating_sub(total_parsed);
            if lines.len() > remaining {
                lines.truncate(remaining);
            }

            let schema = &schema_ref;
            let f_names = &filter_names_clone;
            let s_configs = &sort_configs_clone;
            let f_set = &filter_set_clone;
            let s_bits = &sort_bits_clone;
            let writer = &parse_writer;
            let id_field_ref = &id_field;

            // Rayon fold+reduce: each worker builds thread-local bitmap maps
            // AND encodes docs to msgpack bytes — all CPU work in one pass.
            // Slot = document ID (Postgres ID), not a sequential counter.
            let accum = lines
                .into_par_iter()
                .fold(
                    || BitmapAccum::new(f_names, s_configs),
                    |mut acc, line| {
                        match serde_json::from_str::<serde_json::Value>(line) {
                            Ok(json) => {
                                // Extract the document ID to use as the slot
                                let slot = match json.get(id_field_ref).and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|n| n as u64))) {
                                    Some(id) => id as u32,
                                    None => {
                                        acc.errors += 1;
                                        return acc;
                                    }
                                };

                                // Encode doc directly from JSON — no StoredDoc allocation
                                let bytes = writer.encode_json(&json, schema);
                                acc.encoded_docs.push((slot, bytes));

                                // Build bitmaps directly from JSON
                                acc.alive.insert(slot);
                                extract_bitmaps(
                                    &json,
                                    schema,
                                    f_set,
                                    s_bits,
                                    slot,
                                    &mut acc.filter_maps,
                                    &mut acc.sort_maps,
                                );
                                acc.count += 1;
                            }
                            Err(_) => acc.errors += 1,
                        }
                        acc
                    },
                )
                .reduce(
                    || BitmapAccum::new(f_names, s_configs),
                    |a, b| a.merge(b),
                );

            total_parsed += accum.count;

            if chunk_tx.send(accum).is_err() {
                break;
            }
        }
    });

    // ---- Stage 3: Apply bitmaps + docstore (main thread) ----
    let mut staging = engine.clone_staging();
    let mut total_inserted: usize = 0;
    let mut total_errors: u64 = 0;
    let mut chunks_processed: usize = 0;
    let wall_start = Instant::now();

    let mut ds_handles: Vec<thread::JoinHandle<()>> = Vec::new();

    while let Ok(chunk) = chunk_rx.recv() {
        total_errors += chunk.errors;
        let chunk_count = chunk.count;

        // Apply pre-built bitmaps directly to staging — no decompose/merge needed
        let t0 = Instant::now();
        ConcurrentEngine::apply_bitmap_maps(
            &mut staging,
            chunk.filter_maps,
            chunk.sort_maps,
            chunk.alive,
        );
        let apply_ms = t0.elapsed().as_secs_f64() * 1000.0;

        total_inserted += chunk_count;
        progress.store(total_inserted as u64, Ordering::Release);
        chunks_processed += 1;

        let elapsed = wall_start.elapsed();
        let rate = total_inserted as f64 / elapsed.as_secs_f64();
        eprintln!(
            "  chunk {}: {} total ({:.0}/s) apply={:.1}ms",
            chunks_processed, total_inserted, rate, apply_ms
        );

        // Spawn docstore writer with pre-encoded bytes — pure I/O, no rayon contention.
        if !chunk.encoded_docs.is_empty() {
            let writer = Arc::clone(&bulk_writer);
            ds_handles.push(thread::spawn(move || {
                writer.write_batch_encoded(chunk.encoded_docs);
            }));
        }
    }

    // Wait for threads
    parse_handle.join().unwrap();
    reader_handle.join().unwrap();
    for h in ds_handles {
        h.join().unwrap();
    }

    // Publish staging snapshot
    engine.publish_staging(staging);

    let elapsed = wall_start.elapsed();
    let rate = total_inserted as f64 / elapsed.as_secs_f64();
    eprintln!(
        "Loaded {} records in {:.1}s ({:.0}/s), errors skipped: {}",
        total_inserted,
        elapsed.as_secs_f64(),
        rate,
        total_errors
    );

    Ok(LoadStats {
        records_loaded: total_inserted as u64,
        elapsed,
        errors_skipped: total_errors,
    })
}

/// Extract bitmap entries directly from JSON into accumulator maps.
/// Skips intermediate Document creation for indexed fields.
pub(crate) fn extract_bitmaps(
    json: &serde_json::Value,
    schema: &DataSchema,
    filter_set: &HashSet<String>,
    sort_bits: &HashMap<String, u8>,
    slot: u32,
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
    sort_maps: &mut HashMap<String, HashMap<usize, RoaringBitmap>>,
) {
    for mapping in &schema.fields {
        if mapping.doc_only {
            continue;
        }

        let is_filter = filter_set.contains(&mapping.target);
        let s_bits = sort_bits.get(&mapping.target).copied();

        if !is_filter && s_bits.is_none() {
            continue;
        }

        let raw = json
            .get(&mapping.source)
            .or_else(|| mapping.fallback.as_ref().and_then(|fb| json.get(fb)));

        let raw = match raw {
            Some(v) if !v.is_null() => v,
            _ => {
                // ExistsBoolean: field absent → false
                if is_filter && matches!(mapping.value_type, FieldValueType::ExistsBoolean) {
                    if let Some(fm) = filter_maps.get_mut(&mapping.target) {
                        fm.entry(0)
                            .or_insert_with(RoaringBitmap::new)
                            .insert(slot);
                    }
                }
                continue;
            }
        };

        if is_filter {
            if let Some(fm) = filter_maps.get_mut(&mapping.target) {
                extract_filter_value(raw, mapping, slot, fm);
            }
        }

        if let Some(bits) = s_bits {
            if let Some(sm) = sort_maps.get_mut(&mapping.target) {
                extract_sort_value(raw, mapping, slot, bits, sm);
            }
        }
    }
}

/// Extract a single filter value from JSON and insert into the field's bitmap map.
pub(crate) fn extract_filter_value(
    raw: &serde_json::Value,
    mapping: &FieldMapping,
    slot: u32,
    field_map: &mut HashMap<u64, RoaringBitmap>,
) {
    match mapping.value_type {
        FieldValueType::Integer => {
            if let Some(n) = extract_integer(raw, mapping.truncate_u32) {
                field_map
                    .entry(n as u64)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(slot);
            }
        }
        FieldValueType::Boolean => {
            if let Some(b) = raw.as_bool() {
                field_map
                    .entry(if b { 1 } else { 0 })
                    .or_insert_with(RoaringBitmap::new)
                    .insert(slot);
            }
        }
        FieldValueType::MappedString => {
            if let Some(s) = raw.as_str() {
                let n = mapping
                    .string_map
                    .as_ref()
                    .and_then(|m| m.get(s).copied())
                    .unwrap_or(0);
                field_map
                    .entry(n as u64)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(slot);
            }
        }
        FieldValueType::IntegerArray => {
            if let Some(arr) = raw.as_array() {
                for v in arr {
                    if let Some(n) = v.as_i64().or_else(|| v.as_u64().map(|n| n as i64)) {
                        field_map
                            .entry(n as u64)
                            .or_insert_with(RoaringBitmap::new)
                            .insert(slot);
                    }
                }
            }
        }
        FieldValueType::ExistsBoolean => {
            field_map
                .entry(1)
                .or_insert_with(RoaringBitmap::new)
                .insert(slot);
        }
        FieldValueType::String => {} // String filter fields not supported in bitmap index
    }
}

/// Extract sort value from JSON and insert into bit-layer bitmap maps.
pub(crate) fn extract_sort_value(
    raw: &serde_json::Value,
    mapping: &FieldMapping,
    slot: u32,
    bits: u8,
    bit_map: &mut HashMap<usize, RoaringBitmap>,
) {
    let value = match mapping.value_type {
        // Sort fields are stored as u32 — clamp negative values to 0 so they don't
        // wrap around to u32::MAX and sort incorrectly.
        FieldValueType::Integer => {
            extract_integer(raw, mapping.truncate_u32).map(|n| n.max(0) as u32)
        }
        _ => None,
    };
    if let Some(v) = value {
        for bit in 0..(bits as usize) {
            if (v >> bit) & 1 == 1 {
                bit_map
                    .entry(bit)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(slot);
            }
        }
    }
}

/// Extract an integer from a JSON value, optionally truncating to u32.
pub(crate) fn extract_integer(raw: &serde_json::Value, truncate_u32: bool) -> Option<i64> {
    let n = raw
        .as_i64()
        .or_else(|| raw.as_u64().map(|n| n as i64))
        .or_else(|| raw.as_f64().map(|n| n as i64))?;
    Some(if truncate_u32 {
        (n as u32) as i64
    } else {
        n
    })
}

/// Convert a raw JSON value to a StoredDoc using the DataSchema field mappings.
/// Used by tests to verify field mapping correctness.
#[cfg(test)]
fn json_to_stored_doc(json: &serde_json::Value, schema: &DataSchema) -> StoredDoc {
    let mut fields = HashMap::new();

    if let Some(id_val) = json.get(&schema.id_field) {
        if let Some(n) = id_val.as_i64() {
            fields.insert("id".to_string(), FieldValue::Single(Value::Integer(n)));
        } else if let Some(n) = id_val.as_u64() {
            fields.insert(
                "id".to_string(),
                FieldValue::Single(Value::Integer(n as i64)),
            );
        }
    }

    for mapping in &schema.fields {
        let raw = json
            .get(&mapping.source)
            .or_else(|| mapping.fallback.as_ref().and_then(|fb| json.get(fb)));

        let raw = match raw {
            Some(v) if !v.is_null() => v,
            _ => {
                match mapping.value_type {
                    FieldValueType::ExistsBoolean => {
                        fields.insert(
                            mapping.target.clone(),
                            FieldValue::Single(Value::Bool(false)),
                        );
                    }
                    _ => {}
                }
                continue;
            }
        };

        if let Some(fv) = convert_field(raw, mapping) {
            fields.insert(mapping.target.clone(), fv);
        }
    }

    StoredDoc { fields }
}

/// Convert a raw JSON object to a `Document` using the given `DataSchema`.
///
/// Extracts the ID from `schema.id_field` and builds the Document's field map
/// using the schema's field mappings. Returns `(slot_id, Document)` or an error
/// if the ID field is missing or not an integer.
pub fn json_to_document(
    json: &serde_json::Value,
    schema: &DataSchema,
) -> Result<(u32, Document), String> {
    // Extract ID
    let id_val = json
        .get(&schema.id_field)
        .ok_or_else(|| format!("Missing id field '{}'", schema.id_field))?;
    let id = id_val
        .as_u64()
        .or_else(|| id_val.as_i64().map(|n| n as u64))
        .ok_or_else(|| format!("id field '{}' is not an integer", schema.id_field))?;
    let slot = id as u32;

    let mut fields = HashMap::new();

    // Store the ID in the document fields
    fields.insert(
        "id".to_string(),
        FieldValue::Single(Value::Integer(id as i64)),
    );

    for mapping in &schema.fields {
        let raw = json
            .get(&mapping.source)
            .or_else(|| mapping.fallback.as_ref().and_then(|fb| json.get(fb)));

        let raw = match raw {
            Some(v) if !v.is_null() => v,
            _ => {
                if matches!(mapping.value_type, FieldValueType::ExistsBoolean) {
                    fields.insert(
                        mapping.target.clone(),
                        FieldValue::Single(Value::Bool(false)),
                    );
                }
                continue;
            }
        };

        if let Some(fv) = convert_field(raw, mapping) {
            fields.insert(mapping.target.clone(), fv);
        }
    }

    Ok((slot, Document { fields }))
}

/// Convert a raw serde_json Value field to a FieldValue.
fn convert_field(raw: &serde_json::Value, mapping: &FieldMapping) -> Option<FieldValue> {
    match mapping.value_type {
        FieldValueType::Integer => {
            let n = if let Some(n) = raw.as_i64() {
                n
            } else if let Some(n) = raw.as_u64() {
                n as i64
            } else if let Some(n) = raw.as_f64() {
                n as i64
            } else {
                return None;
            };
            let n = if mapping.truncate_u32 {
                (n as u32) as i64
            } else {
                n
            };
            Some(FieldValue::Single(Value::Integer(n)))
        }
        FieldValueType::Boolean => {
            let b = raw.as_bool()?;
            Some(FieldValue::Single(Value::Bool(b)))
        }
        FieldValueType::String => {
            let s = raw.as_str()?;
            Some(FieldValue::Single(Value::String(s.to_string())))
        }
        FieldValueType::MappedString => {
            let s = raw.as_str()?;
            let map = mapping.string_map.as_ref()?;
            let n = map.get(s).copied().unwrap_or(0);
            Some(FieldValue::Single(Value::Integer(n)))
        }
        FieldValueType::IntegerArray => {
            let arr = raw.as_array()?;
            if arr.is_empty() {
                return None;
            }
            let values: Vec<Value> = arr
                .iter()
                .filter_map(|v| {
                    v.as_i64()
                        .or_else(|| v.as_u64().map(|n| n as i64))
                        .map(Value::Integer)
                })
                .collect();
            if values.is_empty() {
                None
            } else {
                Some(FieldValue::Multi(values))
            }
        }
        FieldValueType::ExistsBoolean => Some(FieldValue::Single(Value::Bool(true))),
    }
}

fn memrchr_newline(data: &[u8]) -> Option<usize> {
    data.iter().rposition(|&b| b == b'\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_stored_doc_integer() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "count".into(),
                target: "count".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 42, "count": 100});
        let doc = json_to_stored_doc(&json, &schema);
        assert_eq!(
            doc.fields.get("id"),
            Some(&FieldValue::Single(Value::Integer(42)))
        );
        assert_eq!(
            doc.fields.get("count"),
            Some(&FieldValue::Single(Value::Integer(100)))
        );
    }

    #[test]
    fn test_json_to_stored_doc_fallback() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "primary".into(),
                target: "val".into(),
                value_type: FieldValueType::Integer,
                fallback: Some("secondary".into()),
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "secondary": 99});
        let doc = json_to_stored_doc(&json, &schema);
        assert_eq!(
            doc.fields.get("val"),
            Some(&FieldValue::Single(Value::Integer(99)))
        );
    }

    #[test]
    fn test_json_to_stored_doc_mapped_string() {
        let mut map = HashMap::new();
        map.insert("image".into(), 1);
        map.insert("video".into(), 2);

        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "type".into(),
                target: "type".into(),
                value_type: FieldValueType::MappedString,
                fallback: None,
                string_map: Some(map),
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "type": "image"});
        let doc = json_to_stored_doc(&json, &schema);
        assert_eq!(
            doc.fields.get("type"),
            Some(&FieldValue::Single(Value::Integer(1)))
        );
    }

    #[test]
    fn test_json_to_stored_doc_boolean() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "hasMeta".into(),
                target: "hasMeta".into(),
                value_type: FieldValueType::Boolean,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "hasMeta": true});
        let doc = json_to_stored_doc(&json, &schema);
        assert_eq!(
            doc.fields.get("hasMeta"),
            Some(&FieldValue::Single(Value::Bool(true)))
        );
    }

    #[test]
    fn test_json_to_stored_doc_integer_array() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "tagIds".into(),
                target: "tagIds".into(),
                value_type: FieldValueType::IntegerArray,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "tagIds": [10, 20, 30]});
        let doc = json_to_stored_doc(&json, &schema);
        assert_eq!(
            doc.fields.get("tagIds"),
            Some(&FieldValue::Multi(vec![
                Value::Integer(10),
                Value::Integer(20),
                Value::Integer(30),
            ]))
        );
    }

    #[test]
    fn test_json_to_stored_doc_truncate_u32() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "ts".into(),
                target: "ts".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: true,
            }],
        };
        let big_val: i64 = 5_000_000_000;
        let json: serde_json::Value = serde_json::json!({"id": 1, "ts": big_val});
        let doc = json_to_stored_doc(&json, &schema);
        let expected = (big_val as u32) as i64;
        assert_eq!(
            doc.fields.get("ts"),
            Some(&FieldValue::Single(Value::Integer(expected)))
        );
    }

    #[test]
    fn test_json_to_stored_doc_string() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "url".into(),
                target: "url".into(),
                value_type: FieldValueType::String,
                fallback: None,
                string_map: None,
                doc_only: true,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "url": "http://example.com"});
        let doc = json_to_stored_doc(&json, &schema);
        assert_eq!(
            doc.fields.get("url"),
            Some(&FieldValue::Single(Value::String(
                "http://example.com".into()
            )))
        );
    }

    #[test]
    fn test_json_to_stored_doc_missing_field_skipped() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "missing".into(),
                target: "val".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1});
        let doc = json_to_stored_doc(&json, &schema);
        assert!(doc.fields.get("val").is_none());
    }

    #[test]
    fn test_json_to_stored_doc_null_field_skipped() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "val".into(),
                target: "val".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "val": null});
        let doc = json_to_stored_doc(&json, &schema);
        assert!(doc.fields.get("val").is_none());
    }

    #[test]
    fn test_json_to_stored_doc_empty_array_skipped() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "tags".into(),
                target: "tags".into(),
                value_type: FieldValueType::IntegerArray,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "tags": []});
        let doc = json_to_stored_doc(&json, &schema);
        assert!(doc.fields.get("tags").is_none());
    }
}
