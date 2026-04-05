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
use std::path::PathBuf;
use std::sync::Arc;

use super::dump_expression::{
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
    // No public methods needed — LookupRow is internal to EnrichmentTable.
    // Accessed via indexed path (enrich_indexed_into_with_buf) only.
}

/// Mmap-backed dense offset index for enrichment lookups.
/// Replaces HashMap for large files: 7.6x faster build, 5.2x less memory, 1.6x faster lookups.
/// Keys must be non-negative integers that fit in a reasonable range (up to ~100M).
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
    /// Returns the column index if found, None if not.
    fn lookup_into<'a>(&'a self, key: i64, buf: &mut Vec<Option<&'a str>>) -> bool {
        if key < 0 || (key as usize) >= self.offsets.len() { return false; }
        let offset = self.offsets[key as usize];
        if offset == u64::MAX { return false; }
        let line = mmap_line_at(&self.mmap, offset);
        buf.clear();
        // Parse CSV line into Option<&str> fields
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

/// Read the line at a byte offset from a mmap. Returns bytes excluding newline/CR.
#[inline]
fn mmap_line_at(mmap: &memmap2::Mmap, offset: u64) -> &[u8] {
    let start = offset as usize;
    let data = &mmap[start..];
    let end = data.iter().position(|&b| b == b'\n').unwrap_or(data.len());
    let slice = &data[..end];
    if slice.last() == Some(&b'\r') { &slice[..slice.len() - 1] } else { slice }
}

/// Storage backend for enrichment tables.
enum EnrichmentStorage {
    /// Traditional HashMap — used for small files or negative/sparse keys.
    HashMap(HashMap<i64, LookupRow>),
    /// Mmap + dense Vec offset index — used for large files with dense positive integer keys.
    Mmap(MmapIndex),
}

/// A loaded enrichment lookup table.
///
/// Memory: loaded before the dependent dump phase, dropped after.
/// Large files (>100MB) use mmap + dense Vec offset index for 7.6x faster build
/// and 5.2x less memory. Small files use HashMap.
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

    /// Load an enrichment table using mmap + dense Vec offset index for large files.
    /// Falls back to sequential BufReader for small files.
    ///
    /// For large files (>100MB): builds a dense Vec<u64> where offsets[key] = byte offset
    /// into the mmap'd CSV. Lookups parse the CSV line on demand from the mmap.
    /// 7.6x faster build, 5.2x less memory, 1.6x faster lookups vs HashMap.
    pub fn load_fast(config: &EnrichmentConfig) -> io::Result<Self> {
        let file_size = std::fs::metadata(&config.csv_path)?.len();
        if file_size < 100 * 1024 * 1024 {
            return Self::load(config); // Small file — HashMap is fine
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

        // First pass: find max key to size the dense Vec
        let body = &raw[data_start..];
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
                    // Fast parse: extract key column without full CSV parse
                    if let Some(key) = fast_extract_column_i64(line, key_idx) {
                        if key > max_key { max_key = key; }
                        row_count += 1;
                    }
                }
                pos += nl + 1;
            }
        }

        // Build dense offset Vec
        let capacity = (max_key as usize + 1).min(200_000_000); // Cap at 200M to prevent OOM
        if max_key as usize >= 200_000_000 {
            eprintln!("WARN: enrichment max_key {} exceeds 200M cap — keys >= 200M will be dropped", max_key);
        }
        let mut offsets = vec![u64::MAX; capacity];

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

    /// Enrich into a pre-allocated buffer (avoids Vec reallocation across rows).
    pub fn enrich_indexed_into(
        &self,
        parent_fields: &[Option<&str>],
        parent_col_idx: &ColumnIndex,
        config: &EnrichmentConfig,
        result: &mut EnrichedFields,
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

        // Resolve lookup fields based on storage backend
        match &self.storage {
            EnrichmentStorage::HashMap(data) => {
                let lookup_row = match data.get(&join_key) {
                    Some(row) => row,
                    None => return,
                };
                let lookup_fields: Vec<Option<&str>> = lookup_row.values.iter()
                    .map(|v| v.as_deref())
                    .collect();
                let lookup_col_idx = lookup_row.col_index.as_ref();
                self.enrich_from_fields(&lookup_fields, lookup_col_idx, join_key, config, result);
            }
            EnrichmentStorage::Mmap(mmap_idx) => {
                let mut lookup_fields: Vec<Option<&str>> = Vec::new();
                if !mmap_idx.lookup_into(join_key, &mut lookup_fields) {
                    return;
                }
                let lookup_col_idx = mmap_idx.col_index();
                self.enrich_from_fields(&lookup_fields, lookup_col_idx, join_key, config, result);
            }
        }
    }

    /// Enrich with a reusable lookup buffer (avoids per-row Vec alloc for Mmap tables).
    pub fn enrich_indexed_into_with_buf<'a>(
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

    /// Core enrichment: extract fields + eval computed from lookup fields.
    /// Works with both HashMap (LookupRow) and Mmap (parsed on demand) backends.
    fn enrich_from_fields(
        &self,
        lookup_fields: &[Option<&str>],
        lookup_col_idx: &ColumnIndex,
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

        // Extract direct fields by column index
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
            // Recursive: child table resolves its own storage type
            child_table.enrich_key_into(child_key, child_config, result);
        }
    }

    /// Look up a key and enrich into the result buffer.
    /// Handles both HashMap and Mmap storage transparently.
    fn enrich_key_into(
        &self,
        join_key: i64,
        config: &EnrichmentConfig,
        result: &mut EnrichedFields,
    ) {
        match &self.storage {
            EnrichmentStorage::HashMap(data) => {
                let lookup_row = match data.get(&join_key) {
                    Some(row) => row,
                    None => return,
                };
                let lookup_fields: Vec<Option<&str>> = lookup_row.values.iter()
                    .map(|v| v.as_deref())
                    .collect();
                let lookup_col_idx = lookup_row.col_index.as_ref();
                self.enrich_from_fields(&lookup_fields, lookup_col_idx, join_key, config, result);
            }
            EnrichmentStorage::Mmap(mmap_idx) => {
                let mut lookup_fields: Vec<Option<&str>> = Vec::new();
                if !mmap_idx.lookup_into(join_key, &mut lookup_fields) {
                    return;
                }
                let lookup_col_idx = mmap_idx.col_index();
                self.enrich_from_fields(&lookup_fields, lookup_col_idx, join_key, config, result);
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
                data.len() * (row_size_estimate + 16)
            }
            EnrichmentStorage::Mmap(mmap_idx) => {
                mmap_idx.offsets.len() * 8 // Dense Vec heap (mmap is page cache, not counted)
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

    /// Enrich a row into a pre-allocated buffer (reuse across rows).
    /// Avoids Vec reallocation — clear + refill. String allocs still per-row.
    /// `lookup_buf` is a reusable buffer for mmap-backed table lookups (avoids Vec alloc per row).
    pub fn enrich_row_indexed_into<'a>(&'a self, fields: &[Option<&str>], col_idx: &super::dump_expression::ColumnIndex, out: &mut EnrichedFields, lookup_buf: &mut Vec<Option<&'a str>>) {
        out.fields.clear();
        out.computed.clear();
        for (table, config) in self.tables.values() {
            table.enrich_indexed_into_with_buf(fields, col_idx, config, out, lookup_buf);
        }
    }

    /// Number of loaded tables.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

// ---- CSV parsing helpers ----

/// Fast extract of a specific column as i64 from a comma-delimited byte line.
/// Avoids full CSV parse — just counts commas to find the target column.
/// Does NOT handle quoted fields (enrichment keys are always unquoted integers).
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
    // Last column (no trailing comma)
    if current == col {
        fast_parse_i64_bytes(&line[start..])
    } else {
        None
    }
}

/// Fast ASCII decimal i64 parser from bytes — avoids UTF-8 validation.
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

}
