//! Enrichment system for the dump pipeline.
//!
//! Loads lookup CSVs into HashMaps, resolves enrichment chains (single-level
//! and nested), and provides a clean API for the dump processor to enrich
//! rows during CSV iteration.
//!
//! Design: D1 from sync-v2-final-implementation-plan.md
//!
//! ## Usage
//!
//! ```ignore
//! // 1. Parse enrichment config from dump request
//! let config = EnrichmentConfig::from_dump_request(&request.enrichment[0]);
//!
//! // 2. Load the enrichment table (and nested children)
//! let table = EnrichmentTable::load(&config, &stage_dir)?;
//!
//! // 3. For each row in the main CSV, resolve enrichment
//! let enriched = table.enrich(&main_row, &config);
//! // enriched contains fields + computed fields from the lookup chain
//!
//! // 4. After phase completes, drop the table to free memory
//! drop(table);
//! ```

use ahash::AHashMap as HashMap;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::dictionary::FieldDictionary;
use crate::dump_expression::{
    ColumnIndex, ComputedFieldDef, CsvRow, ExprValue, FilterExpression,
};

/// Configuration for a single enrichment level, parsed from the dump request body.
#[derive(Debug, Clone)]
pub struct EnrichmentConfig {
    /// Path to the lookup CSV file.
    pub csv_path: PathBuf,
    /// Column name in the lookup CSV that serves as the join key.
    pub key: String,
    /// Column name in the parent CSV to join on.
    pub join_on: String,
    /// Direct field mappings: (csv_column, target_field).
    pub fields: Vec<(String, String)>,
    /// Computed field definitions.
    pub computed_fields: Vec<ComputedFieldDef>,
    /// Optional filter expression on the lookup row.
    pub filter: Option<FilterExpression>,
    /// Nested enrichment (child lookup from this lookup's rows).
    pub child: Option<Box<EnrichmentConfig>>,
    /// Explicit column names for headerless CSVs (PG COPY output).
    /// When set, the CSV has no header row — columns are positional.
    /// When empty, the first row is treated as a header.
    pub columns: Vec<String>,
}

/// A row stored in the enrichment lookup table.
/// Compact representation: Vec indexed by column position with shared column name index.
/// At 22.8M rows, this saves ~7GB vs HashMap<String, String> per row.
#[derive(Debug, Clone)]
pub struct LookupRow {
    /// Column values indexed by position. None = null/empty.
    values: Vec<Option<String>>,
    /// Shared column name → index mapping (same across all rows in a table).
    col_index: Arc<HashMap<String, usize>>,
}

impl LookupRow {
    /// Get a column value by name.
    pub fn get(&self, column: &str) -> Option<&str> {
        let idx = self.col_index.get(column)?;
        self.values.get(*idx)?.as_deref()
    }

    /// Convert to CsvRow for expression evaluation.
    pub fn to_csv_row(&self) -> CsvRow<'_> {
        let mut row = CsvRow::new();
        for (name, &idx) in self.col_index.as_ref() {
            let val = self.values.get(idx).and_then(|v| v.as_deref());
            row.insert(name.as_str(), val);
        }
        row
    }

    /// Iterate over (column_name, value) pairs (non-null only).
    pub fn iter_columns(&self) -> impl Iterator<Item = (&str, &str)> {
        self.col_index.iter().filter_map(move |(name, &idx)| {
            self.values.get(idx)?.as_deref().map(|v| (name.as_str(), v))
        })
    }
}

/// Dense mmap-based enrichment index: byte offsets into an mmap'd CSV, indexed
/// by integer key. Lookups parse CSV on demand. 7.4x faster build, 5.1x less
/// memory than HashMap for large enrichment files (posts.csv, 23M rows).
struct MmapIndex {
    /// Dense offset index: offsets[key] = byte offset of the line in the mmap.
    /// u64::MAX = key not present.
    offsets: Vec<u64>,
    /// Memory-mapped CSV file (OS page cache, not heap).
    mmap: memmap2::Mmap,
    /// Shared column name → index mapping.
    col_index: Arc<HashMap<String, usize>>,
}

impl MmapIndex {
    /// Look up a key and parse the line into a reusable buffer.
    /// Returns true if found, false if not.
    fn lookup_into<'a>(&'a self, key: i64, buf: &mut Vec<Option<&'a str>>) -> bool {
        if key < 0 || (key as usize) >= self.offsets.len() { return false; }
        let offset = self.offsets[key as usize];
        if offset == u64::MAX { return false; }
        let line = mmap_line_at(&self.mmap, offset);
        buf.clear();
        let line_str = match std::str::from_utf8(line) {
            Ok(s) => s,
            Err(_) => return false,
        };
        for field in parse_csv_fields(line_str) {
            buf.push(if field.is_empty() { None } else { Some(field) });
        }
        true
    }

    fn col_index(&self) -> &HashMap<String, usize> {
        &self.col_index
    }
}

/// Extract a line from an mmap starting at byte offset, up to next newline.
fn mmap_line_at(mmap: &memmap2::Mmap, offset: u64) -> &[u8] {
    let start = offset as usize;
    if start >= mmap.len() { return &[]; }
    let rest = &mmap[start..];
    let nl = rest.iter().position(|&b| b == b'\n').unwrap_or(rest.len());
    let line = &rest[..nl];
    if line.last() == Some(&b'\r') { &line[..line.len() - 1] } else { line }
}

/// Fast column extraction: counts commas to find the key column without full CSV parse.
#[inline]
fn fast_extract_column_i64(line: &[u8], col: usize) -> Option<i64> {
    let mut current = 0usize;
    let mut start = 0usize;
    for i in 0..line.len() {
        if line[i] == b',' {
            if current == col {
                return fast_parse_i64_bytes(&line[start..i]);
            }
            current += 1;
            start = i + 1;
        }
    }
    if current == col {
        fast_parse_i64_bytes(&line[start..])
    } else {
        None
    }
}

/// Pure ASCII decimal parser — no UTF-8 validation overhead.
#[inline]
fn fast_parse_i64_bytes(s: &[u8]) -> Option<i64> {
    if s.is_empty() { return None; }
    let (neg, digits) = if s[0] == b'-' { (true, &s[1..]) } else { (false, s) };
    if digits.is_empty() { return None; }
    let mut v: i64 = 0;
    for &b in digits {
        if b < b'0' || b > b'9' { return None; }
        v = v.wrapping_mul(10).wrapping_add((b - b'0') as i64);
    }
    Some(if neg { -v } else { v })
}

/// Storage backend for an enrichment table.
enum EnrichmentStorage {
    /// Traditional HashMap — used for small files or negative/sparse keys.
    HashMap(HashMap<i64, LookupRow>),
    /// Mmap + dense Vec offset index — used for large files with dense positive integer keys.
    Mmap(MmapIndex),
}

/// A loaded enrichment lookup table.
///
/// Memory: loaded before the dependent dump phase, dropped after.
/// Uses HashMap for small files, MmapIndex for large files (>100MB).
pub struct EnrichmentTable {
    /// Storage backend.
    storage: EnrichmentStorage,
    /// Nested child table (loaded eagerly with parent).
    child: Option<Box<EnrichmentTable>>,
    /// Number of rows loaded.
    pub row_count: usize,
}

/// Result of enriching a single row. Contains resolved field values
/// that should be written to bitmaps/docstore.
#[derive(Debug, Default)]
pub struct EnrichedFields {
    /// Direct field values: (target_field, string_value).
    pub fields: Vec<(String, String)>,
    /// Computed field values: (target_field, evaluated_value).
    pub computed: Vec<(String, ExprValue)>,
}

impl EnrichedFields {
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty() && self.computed.is_empty()
    }
}

impl EnrichmentTable {
    /// Load an enrichment table from a CSV file.
    ///
    /// Reads the entire CSV into a HashMap keyed by the `key` column.
    /// If the config has a nested child, loads that too.
    ///
    /// Uses BufReader (not mmap) since enrichment CSVs are typically small
    /// (posts.csv ~600MB, model_versions.csv ~24MB, models.csv ~12MB).
    pub fn load(config: &EnrichmentConfig) -> io::Result<Self> {
        let file = std::fs::File::open(&config.csv_path)?;
        let reader = BufReader::with_capacity(64 * 1024, file);
        let mut data: HashMap<i64, LookupRow> = HashMap::new();

        let mut lines = reader.lines();

        // Read first line — could be a header or data
        let first_line = lines
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty CSV file"))??;
        let first_fields: Vec<&str> = parse_csv_fields(&first_line);

        // Determine column names and whether first line is data
        let (header_names, first_data_line): (Vec<String>, Option<String>) =
            if !config.columns.is_empty() {
                // Config provides column names — check if first row is a header to skip
                let is_header = first_fields.len() == config.columns.len()
                    && first_fields.iter().zip(&config.columns).all(|(a, b)| *a == b);
                if is_header {
                    (config.columns.clone(), None) // header consumed
                } else {
                    (config.columns.clone(), Some(first_line)) // first line is data
                }
            } else {
                // No config columns — first row IS the header
                (first_fields.iter().map(|h| h.to_string()).collect(), None)
            };

        // Find key column index
        let key_idx = header_names
            .iter()
            .position(|h| h == &config.key)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "key column '{}' not found in CSV headers: {:?}",
                        config.key, header_names
                    ),
                )
            })?;

        // Shared column index for compact LookupRow (saves ~7GB at 22.8M rows)
        let col_index_arc = Arc::new(
            header_names.iter().enumerate().map(|(i, name)| (name.clone(), i)).collect::<HashMap<String, usize>>()
        );

        let mut row_count = 0usize;
        // Chain first_data_line (if it was data, not a header) with remaining lines
        let first_iter = first_data_line.into_iter().map(Ok);
        for line_result in first_iter.chain(lines) {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = parse_csv_fields(&line);

            // Parse key value
            let key_str = fields.get(key_idx).copied().unwrap_or("");
            let key: i64 = match key_str.parse() {
                Ok(k) => k,
                Err(_) => continue, // Skip rows with non-integer keys
            };

            // Build compact row (Vec indexed by column position)
            let mut values: Vec<Option<String>> = Vec::with_capacity(header_names.len());
            for (i, value) in fields.iter().enumerate() {
                if i < header_names.len() {
                    if value.is_empty() {
                        values.push(None);
                    } else {
                        values.push(Some(value.to_string()));
                    }
                }
            }
            // Pad with None if fewer fields than headers
            while values.len() < header_names.len() {
                values.push(None);
            }

            data.insert(key, LookupRow { values, col_index: col_index_arc.clone() });
            row_count += 1;
        }

        // Load nested child if configured
        let child = if let Some(ref child_config) = config.child {
            Some(Box::new(EnrichmentTable::load(child_config)?))
        } else {
            None
        };

        Ok(Self {
            storage: EnrichmentStorage::HashMap(data),
            child,
            row_count,
        })
    }

    /// Load an enrichment table using mmap for large files (>100MB).
    /// Files >100MB use MmapIndex (dense Vec of byte offsets, parse-on-demand).
    /// Falls back to sequential BufReader for small files.
    pub fn load_fast(config: &EnrichmentConfig) -> io::Result<Self> {
        let file_size = std::fs::metadata(&config.csv_path)?.len();
        if file_size < 100 * 1024 * 1024 {
            return Self::load(config); // Small file — sequential is fine
        }

        let file = std::fs::File::open(&config.csv_path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("mmap: {e}")))?;
        #[cfg(unix)] let _ = mmap.advise(memmap2::Advice::Sequential);
        let raw = &mmap[..];

        // Column names from config or first line
        let (header_names, data_start) = if !config.columns.is_empty() {
            // Check if first row is actually a header matching config columns
            let first_nl = raw.iter().position(|&b| b == b'\n').unwrap_or(raw.len());
            let first_line = std::str::from_utf8(&raw[..first_nl]).unwrap_or("");
            let first_fields = parse_csv_fields(first_line);
            let is_header = first_fields.len() == config.columns.len()
                && first_fields.iter().zip(&config.columns).all(|(a, b)| *a == b);
            if is_header {
                (config.columns.clone(), first_nl + 1)
            } else {
                (config.columns.clone(), 0usize)
            }
        } else {
            let first_nl = raw.iter().position(|&b| b == b'\n').unwrap_or(raw.len());
            let header_line = std::str::from_utf8(&raw[..first_nl]).unwrap_or("");
            let names: Vec<String> = parse_csv_fields(header_line).iter().map(|s| s.to_string()).collect();
            (names, first_nl + 1)
        };

        let key_idx = header_names.iter().position(|h| h == &config.key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData,
                format!("key '{}' not in headers: {:?}", config.key, header_names)))?;

        let col_index_arc = Arc::new(
            header_names.iter().enumerate().map(|(i, name)| (name.clone(), i)).collect::<HashMap<String, usize>>()
        );

        // Two-pass MmapIndex build: first pass finds max key, second records offsets
        let body = &raw[data_start..];

        // Pass 1: find max key to size the dense Vec
        let mut max_key: i64 = 0;
        let mut row_count: usize = 0;
        {
            let mut pos = 0usize;
            while pos < body.len() {
                let slice = &body[pos..];
                let nl = slice.iter().position(|&b| b == b'\n').unwrap_or(slice.len());
                let line = {
                    let raw_line = &slice[..nl];
                    if raw_line.last() == Some(&b'\r') { &raw_line[..raw_line.len()-1] } else { raw_line }
                };
                if !line.is_empty() {
                    if let Some(key) = fast_extract_column_i64(line, key_idx) {
                        if key > max_key { max_key = key; }
                        row_count += 1;
                    }
                }
                pos += nl + 1;
            }
        }

        // Build dense offset Vec
        let capacity = (max_key as usize + 1).min(200_000_000);
        if max_key as usize >= 200_000_000 {
            eprintln!("WARN: enrichment max_key {} exceeds 200M cap — keys >= 200M will be dropped", max_key);
        }
        let mut offsets = vec![u64::MAX; capacity];

        // Pass 2: record byte offsets
        {
            let mut pos = 0usize;
            while pos < body.len() {
                let line_offset = (data_start + pos) as u64;
                let slice = &body[pos..];
                let nl = slice.iter().position(|&b| b == b'\n').unwrap_or(slice.len());
                let line = {
                    let raw_line = &slice[..nl];
                    if raw_line.last() == Some(&b'\r') { &raw_line[..raw_line.len()-1] } else { raw_line }
                };
                if !line.is_empty() {
                    if let Some(key) = fast_extract_column_i64(line, key_idx) {
                        if key >= 0 && (key as usize) < capacity {
                            offsets[key as usize] = line_offset;
                        }
                    }
                }
                pos += nl + 1;
            }
        }

        eprintln!("  MmapIndex: {} rows, max_key={}, vec_size={}MB, file={}MB",
            row_count, max_key,
            capacity * 8 / (1024 * 1024),
            file_size / (1024 * 1024));

        // Switch from Sequential (build scan) to Random (lookup phase)
        #[cfg(unix)] let _ = mmap.advise(memmap2::Advice::Random);

        // Load nested child
        let child = if let Some(ref child_config) = config.child {
            Some(Box::new(EnrichmentTable::load_fast(child_config)?))
        } else {
            None
        };

        Ok(Self {
            storage: EnrichmentStorage::Mmap(MmapIndex { offsets, mmap, col_index: col_index_arc }),
            child,
            row_count,
        })
    }

    /// Look up a row by key value (HashMap path only).
    pub fn get(&self, key: i64) -> Option<&LookupRow> {
        match &self.storage {
            EnrichmentStorage::HashMap(data) => data.get(&key),
            EnrichmentStorage::Mmap(_) => None, // Use enrich_indexed_into for mmap path
        }
    }

    /// Get the nested child table (if any).
    pub fn child(&self) -> Option<&EnrichmentTable> {
        self.child.as_deref()
    }

    /// Enrich a parent row using this lookup table and its config.
    ///
    /// This is the full enrichment resolution that handles the filter-on-nested pattern:
    /// Resources → MV (by modelVersionId) → Model (by modelId) → if type='Checkpoint', set baseModel
    pub fn enrich<'a>(
        &self,
        parent_row: &CsvRow<'a>,
        config: &EnrichmentConfig,
    ) -> EnrichedFields {
        let mut result = EnrichedFields::default();

        // Get join key from parent row
        let join_value = match parent_row.get(config.join_on.as_str()) {
            Some(Some(v)) if !v.is_empty() => *v,
            _ => return result,
        };

        let join_key: i64 = match join_value.parse() {
            Ok(k) => k,
            Err(_) => return result,
        };

        let lookup_row = match self.get(join_key) {
            Some(row) => row,
            None => return result,
        };

        let lookup_fields: Vec<Option<&str>> = lookup_row.values.iter()
            .map(|v| v.as_deref())
            .collect();
        let lookup_col_idx = lookup_row.col_index.as_ref();
        self.enrich_from_fields(&lookup_fields, lookup_col_idx, join_key, config, &mut result);
        result
    }

    /// Enrich using indexed parent row (zero-allocation hot path for 107M+ rows).
    ///
    /// The parent row is `&[Option<&str>]` + `ColumnIndex` — no HashMap per row.
    /// Works with both HashMap and MmapIndex storage backends.
    pub fn enrich_indexed(
        &self,
        parent_fields: &[Option<&str>],
        parent_col_idx: &ColumnIndex,
        config: &EnrichmentConfig,
    ) -> EnrichedFields {
        let mut result = EnrichedFields::default();
        let mut lookup_buf: Vec<Option<&str>> = Vec::new();
        self.enrich_indexed_into(parent_fields, parent_col_idx, config, &mut result, &mut lookup_buf);
        result
    }

    /// Enrich using indexed parent row with caller-supplied buffers (zero-allocation).
    pub fn enrich_indexed_into<'a>(
        &'a self,
        parent_fields: &[Option<&str>],
        parent_col_idx: &ColumnIndex,
        config: &EnrichmentConfig,
        result: &mut EnrichedFields,
        lookup_buf: &mut Vec<Option<&'a str>>,
    ) {
        let join_value = match parent_col_idx.get(&config.join_on) {
            Some(&idx) => match parent_fields.get(idx) {
                Some(Some(v)) if !v.is_empty() => *v,
                _ => return,
            },
            None => return,
        };

        let join_key: i64 = match join_value.parse() {
            Ok(k) => k,
            Err(_) => return,
        };

        match &self.storage {
            EnrichmentStorage::HashMap(data) => {
                let lookup_row = match data.get(&join_key) {
                    Some(row) => row,
                    None => return,
                };
                lookup_buf.clear();
                for v in &lookup_row.values {
                    lookup_buf.push(v.as_deref());
                }
                let lookup_col_idx = lookup_row.col_index.as_ref();
                self.enrich_from_fields(lookup_buf, lookup_col_idx, join_key, config, result);
            }
            EnrichmentStorage::Mmap(mmap_idx) => {
                if !mmap_idx.lookup_into(join_key, lookup_buf) {
                    return;
                }
                let lookup_col_idx = mmap_idx.col_index();
                self.enrich_from_fields(lookup_buf, lookup_col_idx, join_key, config, result);
            }
        }
    }

    /// Core enrichment: extract fields + eval computed from indexed field slices.
    /// Works with both HashMap (LookupRow converted to slices) and MmapIndex (parsed on demand).
    fn enrich_from_fields(
        &self,
        lookup_fields: &[Option<&str>],
        lookup_col_idx: &HashMap<String, usize>,
        join_key: i64,
        config: &EnrichmentConfig,
        result: &mut EnrichedFields,
    ) {
        // Check this level's filter
        if let Some(ref filter) = config.filter {
            if !filter.eval_indexed(lookup_fields, lookup_col_idx, Some(join_key)) {
                return;
            }
        }

        // Extract direct fields
        for (csv_col, target) in &config.fields {
            if let Some(&idx) = lookup_col_idx.get(csv_col.as_str()) {
                if let Some(Some(value)) = lookup_fields.get(idx) {
                    result.fields.push((target.clone(), value.to_string()));
                }
            }
        }

        // Evaluate computed fields via indexed path
        for cf in &config.computed_fields {
            if let Some(value) = cf.eval_indexed(lookup_fields, lookup_col_idx, Some(join_key)) {
                result.computed.push((cf.target.clone(), value));
            }
        }

        // Resolve nested enrichment (recursive)
        if let (Some(ref child_table), Some(ref child_config)) = (&self.child, &config.child) {
            let join_value = match lookup_col_idx.get(&child_config.join_on) {
                Some(&idx) => match lookup_fields.get(idx) {
                    Some(Some(v)) if !v.is_empty() => *v,
                    _ => return,
                },
                None => return,
            };
            let child_key: i64 = match join_value.parse() {
                Ok(k) => k,
                Err(_) => return,
            };
            // Child tables always use HashMap — they're small (models.csv ~12MB)
            if let Some(child_row) = child_table.get(child_key) {
                let child_fields: Vec<Option<&str>> = child_row.values.iter()
                    .map(|v| v.as_deref())
                    .collect();
                let child_col_idx = child_row.col_index.as_ref();
                child_table.enrich_from_fields(&child_fields, child_col_idx, child_key, child_config, result);
            }
        }
    }

    /// Memory usage estimate in bytes.
    pub fn estimated_memory(&self) -> usize {
        let self_mem = match &self.storage {
            EnrichmentStorage::HashMap(data) => {
                let row_size_estimate = data
                    .values()
                    .take(100)
                    .map(|r| {
                        r.values
                            .iter()
                            .map(|v| v.as_ref().map_or(8, |s| s.len() + 24))
                            .sum::<usize>()
                            + 24 // Vec overhead
                    })
                    .sum::<usize>()
                    / 100.max(1);
                data.len() * (row_size_estimate + 16) // +16 for HashMap bucket
            }
            EnrichmentStorage::Mmap(idx) => {
                // MmapIndex: only the offset Vec is on the heap, mmap is OS page cache
                idx.offsets.len() * 8
            }
        };
        let child_mem = self
            .child
            .as_ref()
            .map(|c| c.estimated_memory())
            .unwrap_or(0);
        self_mem + child_mem
    }
}

/// Manages enrichment tables for a dump phase.
///
/// Handles lazy loading (load before dependent phase) and explicit dropping
/// (free memory after phase completes).
pub struct EnrichmentManager {
    /// Loaded tables, keyed by join_on field name.
    tables: HashMap<String, (EnrichmentTable, EnrichmentConfig)>,
}

impl EnrichmentManager {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Load an enrichment table for a phase.
    /// Call this before processing the phase's CSV.
    pub fn load(&mut self, config: EnrichmentConfig) -> io::Result<()> {
        let join_on = config.join_on.clone();
        let table = EnrichmentTable::load_fast(&config)?;
        self.tables.insert(join_on, (table, config));
        Ok(())
    }

    /// Enrich a row using all loaded tables.
    /// Returns combined enriched fields from all enrichment sources.
    pub fn enrich_row<'a>(&self, row: &CsvRow<'a>) -> EnrichedFields {
        let mut combined = EnrichedFields::default();
        for (table, config) in self.tables.values() {
            let enriched = table.enrich(row, config);
            combined.fields.extend(enriched.fields);
            combined.computed.extend(enriched.computed);
        }
        combined
    }

    /// Enrich a row using indexed fields (zero-allocation hot path).
    pub fn enrich_row_indexed(&self, fields: &[Option<&str>], col_idx: &crate::dump_expression::ColumnIndex) -> EnrichedFields {
        let mut combined = EnrichedFields::default();
        for (table, config) in self.tables.values() {
            let enriched = table.enrich_indexed(fields, col_idx, config);
            combined.fields.extend(enriched.fields);
            combined.computed.extend(enriched.computed);
        }
        combined
    }

    /// Enrich a row using indexed fields with caller-supplied buffers (zero-allocation).
    /// Reuses `out` and `lookup_buf` across rows — clears them before each enrichment.
    pub fn enrich_row_indexed_into<'a>(
        &'a self,
        fields: &[Option<&str>],
        col_idx: &crate::dump_expression::ColumnIndex,
        out: &mut EnrichedFields,
        lookup_buf: &mut Vec<Option<&'a str>>,
    ) {
        out.fields.clear();
        out.computed.clear();
        for (table, config) in self.tables.values() {
            table.enrich_indexed_into(fields, col_idx, config, out, lookup_buf);
        }
    }

    /// Drop all tables to free memory. Call after the phase completes.
    pub fn clear(&mut self) {
        self.tables.clear();
    }

    /// Drop a specific table by join_on key.
    pub fn drop_table(&mut self, join_on: &str) {
        self.tables.remove(join_on);
    }

    /// Total estimated memory across all loaded tables.
    pub fn total_memory(&self) -> usize {
        self.tables.values().map(|(t, _)| t.estimated_memory()).sum()
    }

    /// Number of loaded tables.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

// ---- Dictionary helpers ----

/// Resolve a string value through a FieldDictionary, returning the integer key.
///
/// This is the clean API for 1.10/1.15#7: pass individual `&FieldDictionary` refs,
/// not a full `HashMap<String, FieldDictionary>`.
pub fn resolve_dictionary_value(dict: &FieldDictionary, value: &str) -> i64 {
    dict.get_or_insert(value)
}

/// Resolve an ExprValue through a dictionary if it's a string.
/// Returns the bitmap key (i64) for the value.
pub fn resolve_expr_to_bitmap_key(
    value: &ExprValue,
    dict: Option<&FieldDictionary>,
) -> Option<u64> {
    match value {
        ExprValue::Int(n) => Some(*n as u64),
        ExprValue::Bool(b) => Some(if *b { 1 } else { 0 }),
        ExprValue::Str(s) => {
            if let Some(d) = dict {
                Some(d.get_or_insert(s) as u64)
            } else {
                // Try parsing as integer
                s.parse::<u64>().ok()
            }
        }
        ExprValue::Null => None,
    }
}

/// Collection of field dictionaries for LCS fields, keyed by field name.
///
/// Thread-safe: FieldDictionary uses DashMap internally.
/// Share via `Arc<DictionarySet>` across threads.
pub struct DictionarySet {
    dicts: HashMap<String, Arc<FieldDictionary>>,
}

impl DictionarySet {
    /// Create a new set with dictionaries for the given field names.
    pub fn new(field_names: &[&str]) -> Self {
        let mut dicts = HashMap::new();
        for name in field_names {
            dicts.insert(name.to_string(), Arc::new(FieldDictionary::new()));
        }
        Self { dicts }
    }

    /// Create from existing dictionaries (e.g., loaded from disk).
    pub fn from_existing(dicts: HashMap<String, Arc<FieldDictionary>>) -> Self {
        Self { dicts }
    }

    /// Get a dictionary by field name.
    pub fn get(&self, field: &str) -> Option<&Arc<FieldDictionary>> {
        self.dicts.get(field)
    }

    /// Resolve a string value for a field, returning the bitmap key.
    /// Returns None if the field has no dictionary (not an LCS field).
    pub fn resolve(&self, field: &str, value: &str) -> Option<i64> {
        self.dicts.get(field).map(|d| d.get_or_insert(value))
    }

    /// Persist all dirty dictionaries to disk.
    pub fn persist_all(&self, dict_dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dict_dir)?;
        for (name, dict) in &self.dicts {
            let snapshot = dict.snapshot();
            let path = dict_dir.join(format!("{}.dict", name));
            let json = serde_json::to_string_pretty(&snapshot)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            // Atomic write
            let tmp = path.with_extension("dict.tmp");
            std::fs::write(&tmp, &json)?;
            std::fs::rename(&tmp, &path)?;
        }
        Ok(())
    }

    /// Get all dictionary names.
    pub fn names(&self) -> Vec<&str> {
        self.dicts.keys().map(|s| s.as_str()).collect()
    }

    /// Iterate over all dictionaries.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Arc<FieldDictionary>)> {
        self.dicts.iter().map(|(k, v)| (k.as_str(), v))
    }
}

// ---- CSV parsing helpers ----

/// Parse a CSV line into fields, handling quoted values.
/// Returns borrowed slices into the input line.
fn parse_csv_fields(line: &str) -> Vec<&str> {
    let mut fields = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    let len = bytes.len();

    while i <= len {
        if i == len {
            // Trailing empty field after comma
            if !fields.is_empty() && i > 0 && bytes[i - 1] == b',' {
                fields.push("");
            }
            break;
        }

        if bytes[i] == b'"' {
            // Quoted field
            let start = i + 1;
            i += 1;
            while i < len {
                if bytes[i] == b'"' {
                    if i + 1 < len && bytes[i + 1] == b'"' {
                        i += 2; // Escaped quote
                    } else {
                        break; // End of quoted field
                    }
                } else {
                    i += 1;
                }
            }
            let end = i;
            // Note: doesn't handle escaped quotes in the returned slice
            // For enrichment CSVs this is fine — keys/values rarely contain quotes
            let field = &line[start..end];
            fields.push(field);
            i += 1; // Skip closing quote
            if i < len && bytes[i] == b',' {
                i += 1; // Skip comma
            }
        } else {
            // Unquoted field
            let start = i;
            while i < len && bytes[i] != b',' {
                i += 1;
            }
            fields.push(&line[start..i]);
            if i < len {
                i += 1; // Skip comma
            }
        }
    }

    fields
}

/// Parse a TSV line (tab-separated). Simpler than CSV — no quoting.
pub fn parse_tsv_fields(line: &str) -> Vec<&str> {
    line.split('\t').collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_csv(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    // ---- CSV parser tests ----

    #[test]
    fn test_parse_csv_simple() {
        let fields = parse_csv_fields("1,hello,42");
        assert_eq!(fields, vec!["1", "hello", "42"]);
    }

    #[test]
    fn test_parse_csv_quoted() {
        let fields = parse_csv_fields("1,\"hello world\",42");
        assert_eq!(fields, vec!["1", "hello world", "42"]);
    }

    #[test]
    fn test_parse_csv_empty_fields() {
        let fields = parse_csv_fields("1,,42");
        assert_eq!(fields, vec!["1", "", "42"]);
    }

    #[test]
    fn test_parse_tsv() {
        let fields = parse_tsv_fields("1\thello\t42");
        assert_eq!(fields, vec!["1", "hello", "42"]);
    }

    // ---- EnrichmentTable tests ----

    #[test]
    fn test_load_simple_table() {
        let dir = TempDir::new().unwrap();
        let csv = write_csv(
            dir.path(),
            "posts.csv",
            "id,publishedAtSecs,availability\n100,1700000000,Public\n200,,Private\n300,1700001000,Public\n",
        );

        let config = EnrichmentConfig {
            csv_path: csv,
            key: "id".into(),
            join_on: "postId".into(),
            fields: vec![
                ("publishedAtSecs".into(), "publishedAt".into()),
                ("availability".into(), "availability".into()),
            ],
            computed_fields: vec![],
            filter: None,
            child: None,
            columns: vec![],
        };

        let table = EnrichmentTable::load(&config).unwrap();
        assert_eq!(table.row_count, 3);

        // Check row 100
        let row = table.get(100).unwrap();
        assert_eq!(row.get("publishedAtSecs").unwrap(), "1700000000");
        assert_eq!(row.get("availability").unwrap(), "Public");

        // Check row 200 (null publishedAtSecs)
        let row200 = table.get(200).unwrap();
        assert!(row200.get("publishedAtSecs").is_none()); // empty → absent
        assert_eq!(row200.get("availability").unwrap(), "Private");
    }

    #[test]
    fn test_single_level_enrichment() {
        let dir = TempDir::new().unwrap();
        let csv = write_csv(
            dir.path(),
            "posts.csv",
            "id,publishedAtSecs,availability\n100,1700000000,Public\n",
        );

        let config = EnrichmentConfig {
            csv_path: csv,
            key: "id".into(),
            join_on: "postId".into(),
            fields: vec![
                ("publishedAtSecs".into(), "publishedAt".into()),
                ("availability".into(), "availability".into()),
            ],
            computed_fields: vec![
                ComputedFieldDef::parse("isPublished", "publishedAtSecs != null", None).unwrap(),
                ComputedFieldDef::parse("postedToId", "lookup_key", None).unwrap(),
            ],
            filter: None,
            child: None,
            columns: vec![],
        };

        let table = EnrichmentTable::load(&config).unwrap();

        // Simulate a parent row with postId=100
        let parent: CsvRow = vec![("postId", Some("100"))].into_iter().collect();

        let enriched = table.enrich(&parent, &config);

        // Direct fields
        assert_eq!(enriched.fields.len(), 2);
        assert!(enriched.fields.contains(&("publishedAt".into(), "1700000000".into())));
        assert!(enriched.fields.contains(&("availability".into(), "Public".into())));

        // Computed fields
        assert_eq!(enriched.computed.len(), 2);
        assert!(enriched
            .computed
            .contains(&("isPublished".into(), ExprValue::Bool(true))));
        assert!(enriched
            .computed
            .contains(&("postedToId".into(), ExprValue::Int(100))));
    }

    #[test]
    fn test_enrichment_no_match() {
        let dir = TempDir::new().unwrap();
        let csv = write_csv(dir.path(), "posts.csv", "id,name\n100,hello\n");

        let config = EnrichmentConfig {
            csv_path: csv,
            key: "id".into(),
            join_on: "postId".into(),
            fields: vec![("name".into(), "name".into())],
            computed_fields: vec![],
            filter: None,
            child: None,
            columns: vec![],
        };

        let table = EnrichmentTable::load(&config).unwrap();
        let parent: CsvRow = vec![("postId", Some("999"))].into_iter().collect();
        let enriched = table.enrich(&parent, &config);
        assert!(enriched.is_empty());
    }

    #[test]
    fn test_enrichment_null_join_key() {
        let dir = TempDir::new().unwrap();
        let csv = write_csv(dir.path(), "posts.csv", "id,name\n100,hello\n");

        let config = EnrichmentConfig {
            csv_path: csv,
            key: "id".into(),
            join_on: "postId".into(),
            fields: vec![("name".into(), "name".into())],
            computed_fields: vec![],
            filter: None,
            child: None,
            columns: vec![],
        };

        let table = EnrichmentTable::load(&config).unwrap();

        // Missing join key
        let parent: CsvRow = HashMap::new();
        let enriched = table.enrich(&parent, &config);
        assert!(enriched.is_empty());

        // Null join key
        let parent2: CsvRow = vec![("postId", None)].into_iter().collect();
        let enriched2 = table.enrich(&parent2, &config);
        assert!(enriched2.is_empty());
    }

    #[test]
    fn test_nested_enrichment_with_filter() {
        let dir = TempDir::new().unwrap();

        // Model versions CSV
        let mv_csv = write_csv(
            dir.path(),
            "model_versions.csv",
            "id,baseModel,modelId\n10,SDXL,1000\n20,SD 1.5,2000\n",
        );

        // Models CSV
        let models_csv = write_csv(
            dir.path(),
            "models.csv",
            "id,poi,type\n1000,false,Checkpoint\n2000,true,LORA\n",
        );

        // Config: Resources → MV (by modelVersionId) → Model (by modelId, filter: Checkpoint)
        let config = EnrichmentConfig {
            csv_path: mv_csv,
            key: "id".into(),
            join_on: "modelVersionId".into(),
            fields: vec![("baseModel".into(), "baseModel".into())],
            computed_fields: vec![],
            filter: None,
            columns: vec![],
            child: Some(Box::new(EnrichmentConfig {
                csv_path: models_csv,
                key: "id".into(),
                join_on: "modelId".into(),
                fields: vec![("poi".into(), "poi".into())],
                computed_fields: vec![],
                filter: Some(FilterExpression::parse("type = 'Checkpoint'").unwrap()),
                child: None,
            columns: vec![],
            })),
        };

        let table = EnrichmentTable::load(&config).unwrap();
        assert_eq!(table.row_count, 2);
        assert!(table.child().is_some());
        assert_eq!(table.child().unwrap().row_count, 2);

        // Resource row with MV id=10 (Checkpoint model → filter passes)
        let row1: CsvRow = vec![("modelVersionId", Some("10"))].into_iter().collect();
        let enriched1 = table.enrich(&row1, &config);
        // baseModel from MV level
        assert!(enriched1.fields.contains(&("baseModel".into(), "SDXL".into())));
        // poi from Model level (Checkpoint, filter passed)
        assert!(enriched1.fields.contains(&("poi".into(), "false".into())));

        // Resource row with MV id=20 (LORA model → filter fails)
        let row2: CsvRow = vec![("modelVersionId", Some("20"))].into_iter().collect();
        let enriched2 = table.enrich(&row2, &config);
        // baseModel from MV level (no filter on MV)
        assert!(enriched2.fields.contains(&("baseModel".into(), "SD 1.5".into())));
        // poi NOT present — Model filter (type=Checkpoint) failed for LORA
        assert!(!enriched2.fields.iter().any(|(k, _)| k == "poi"));
    }

    // ---- EnrichmentManager tests ----

    #[test]
    fn test_manager_load_and_clear() {
        let dir = TempDir::new().unwrap();
        let csv = write_csv(dir.path(), "posts.csv", "id,name\n100,hello\n");

        let mut mgr = EnrichmentManager::new();
        assert_eq!(mgr.table_count(), 0);

        mgr.load(EnrichmentConfig {
            csv_path: csv,
            key: "id".into(),
            join_on: "postId".into(),
            fields: vec![("name".into(), "name".into())],
            computed_fields: vec![],
            filter: None,
            child: None,
            columns: vec![],
        })
        .unwrap();

        assert_eq!(mgr.table_count(), 1);
        assert!(mgr.total_memory() > 0);

        mgr.clear();
        assert_eq!(mgr.table_count(), 0);
    }

    #[test]
    fn test_manager_enrich_row() {
        let dir = TempDir::new().unwrap();
        let csv = write_csv(
            dir.path(),
            "posts.csv",
            "id,availability\n100,Public\n200,Private\n",
        );

        let mut mgr = EnrichmentManager::new();
        mgr.load(EnrichmentConfig {
            csv_path: csv,
            key: "id".into(),
            join_on: "postId".into(),
            fields: vec![("availability".into(), "availability".into())],
            computed_fields: vec![],
            filter: None,
            child: None,
            columns: vec![],
        })
        .unwrap();

        let row: CsvRow = vec![("postId", Some("100"))].into_iter().collect();
        let enriched = mgr.enrich_row(&row);
        assert_eq!(enriched.fields.len(), 1);
        assert!(enriched.fields.contains(&("availability".into(), "Public".into())));
    }

    // ---- Dictionary tests ----

    #[test]
    fn test_resolve_dictionary_value() {
        let dict = FieldDictionary::new();
        let key1 = resolve_dictionary_value(&dict, "Checkpoint");
        let key2 = resolve_dictionary_value(&dict, "LORA");
        let key3 = resolve_dictionary_value(&dict, "Checkpoint"); // same as key1
        assert_ne!(key1, key2);
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_resolve_expr_to_bitmap_key() {
        let dict = FieldDictionary::new();

        // Integer → direct
        assert_eq!(
            resolve_expr_to_bitmap_key(&ExprValue::Int(42), None),
            Some(42)
        );

        // Bool → 0/1
        assert_eq!(
            resolve_expr_to_bitmap_key(&ExprValue::Bool(true), None),
            Some(1)
        );

        // String with dict → dictionary key
        let key = resolve_expr_to_bitmap_key(&ExprValue::Str("Public".into()), Some(&dict));
        assert!(key.is_some());

        // String without dict → try parse
        assert_eq!(
            resolve_expr_to_bitmap_key(&ExprValue::Str("42".into()), None),
            Some(42)
        );

        // Null → None
        assert_eq!(resolve_expr_to_bitmap_key(&ExprValue::Null, None), None);
    }

    #[test]
    fn test_dictionary_set() {
        let set = DictionarySet::new(&["type", "availability", "baseModel"]);
        assert_eq!(set.names().len(), 3);

        let key1 = set.resolve("type", "Checkpoint").unwrap();
        let key2 = set.resolve("type", "LORA").unwrap();
        assert_ne!(key1, key2);

        // Unknown field → None
        assert!(set.resolve("unknown", "value").is_none());
    }

    #[test]
    fn test_dictionary_set_persist() {
        let dir = TempDir::new().unwrap();
        let dict_dir = dir.path().join("dictionaries");

        let set = DictionarySet::new(&["type", "availability"]);
        set.resolve("type", "Checkpoint");
        set.resolve("type", "LORA");
        set.resolve("availability", "Public");

        set.persist_all(&dict_dir).unwrap();

        // Check files exist
        assert!(dict_dir.join("type.dict").exists());
        assert!(dict_dir.join("availability.dict").exists());
    }
}
