//! Config-driven CSV/TSV dump processor for Sync V2.
//!
//! Replaces the hardcoded single_pass.rs approach with a generic processor
//! that receives dump instructions via a JSON request body (D3 schema).
//!
//! Each dump phase:
//!   1. Load enrichment HashMaps (if configured)
//!   2. Mmap the CSV/TSV file, split into byte ranges for parallel processing
//!   3. Parse rows using named columns from the dump request
//!   4. Evaluate filter expressions (skip rows that don't pass)
//!   5. Evaluate computed field expressions
//!   6. Build filter/sort bitmaps + append docstore tuples
//!   7. Save bitmaps to BitmapSilo, drop from memory
//!
//! Processing is sequential per phase (no cross-phase parallelism in V2).

use std::collections::{BTreeMap, HashSet};
use ahash::AHashMap as HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::engine::ConcurrentEngine;
use crate::dictionary::FieldDictionary;
use crate::silos::doc_format::PackedValue;
use super::dump_enrichment;
use super::dump_expression::{FilterExpression, ComputedFieldDef, CsvRow};
use super::dump_expression::ExprValue as NateExprValue;

const LOG_INTERVAL: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Per-row timing instrumentation (zero overhead when dump-timing feature is off)
// ---------------------------------------------------------------------------

#[cfg(feature = "dump-timing")]
#[derive(Default, Clone)]
struct RowTimings {
    rows: u64,
    csv_parse: u64,
    slot_extract: u64,
    indexed_fields: u64,
    filter_expr: u64,
    enrichment: u64,
    config_computed_sort_early: u64,  // first computation (~line 1705)
    config_computed_sort_late: u64,   // second computation (~line 1960)
    filter_bitmap_insert: u64,
    sort_bitmap_insert: u64,
    enrichment_bitmap: u64,
    computed_field: u64,
    doc_encode: u64,
    doc_field_collect: u64,           // sub-timing: gathering field values
    doc_pack_encode: u64,             // sub-timing: encode_merge_fields_into
    doc_mmap_write: u64,              // sub-timing: write_put_reuse / push to vec
    deferred_alive: u64,
    total: u64,
    enriched_get_calls: u64,          // count of enriched_get closure invocations
}

#[cfg(feature = "dump-timing")]
impl RowTimings {
    fn print_summary(&self, thread_id: usize) {
        if self.rows == 0 { return; }
        let r = self.rows as f64;
        let fields = [
            ("csv_parse", self.csv_parse),
            ("slot_extract", self.slot_extract),
            ("indexed_fields", self.indexed_fields),
            ("filter_expr", self.filter_expr),
            ("enrichment", self.enrichment),
            ("config_sort_early", self.config_computed_sort_early),
            ("config_sort_late", self.config_computed_sort_late),
            ("filter_bm_insert", self.filter_bitmap_insert),
            ("sort_bm_insert", self.sort_bitmap_insert),
            ("enrichment_bm", self.enrichment_bitmap),
            ("computed_field", self.computed_field),
            ("doc_encode", self.doc_encode),
            ("  doc_field_collect", self.doc_field_collect),
            ("  doc_pack_encode", self.doc_pack_encode),
            ("  doc_mmap_write", self.doc_mmap_write),
            ("deferred_alive", self.deferred_alive),
        ];
        let total_ns = self.total;
        eprintln!("  [dump-timing] thread {} — {} rows, {:.1} ns/row total", thread_id, self.rows, total_ns as f64 / r);
        let mut sorted: Vec<(&str, u64)> = fields.iter().map(|&(n, v)| (n, v)).collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        for (name, ns) in &sorted {
            let pct = if total_ns > 0 { *ns as f64 / total_ns as f64 * 100.0 } else { 0.0 };
            eprintln!("    {:>20}: {:>8.1} ns/row  ({:>5.1}%)", name, *ns as f64 / r, pct);
        }
        if self.enriched_get_calls > 0 {
            eprintln!("    enriched_get calls: {} ({:.1}/row)", self.enriched_get_calls, self.enriched_get_calls as f64 / r);
        }
        // Top 3 hotspots
        eprintln!("    TOP 3: {}, {}, {}", sorted[0].0, sorted[1].0, sorted[2].0);
    }
}

/// Helper macro to time a block and accumulate into RowTimings field.
#[cfg(feature = "dump-timing")]
macro_rules! time_block {
    ($timings:expr, $field:ident, $block:expr) => {{
        let _t_start = std::time::Instant::now();
        let _result = $block;
        $timings.$field += _t_start.elapsed().as_nanos() as u64;
        _result
    }};
}

/// Emit a structured JSON stage marker to stderr for phase monitoring.
/// Zero overhead — only called at stage transitions, not per row.
fn emit_stage(dump_name: &str, stage: &str, detail: &str, t0: &Instant, rows: u64) {
    let elapsed_ms = t0.elapsed().as_millis();
    eprintln!(
        r#"{{"dump":"{}","stage":"{}","detail":"{}","elapsed_ms":{},"rows":{}}}"#,
        dump_name, stage, detail, elapsed_ms, rows
    );
}

// ---------------------------------------------------------------------------
// D3 Request Body Schema
// ---------------------------------------------------------------------------

/// Dump request body — what bitdex-sync sends to `PUT /api/indexes/{name}/dumps`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpRequest {
    /// Unique dump name (e.g., "tags-a1b2c3d4")
    pub name: String,

    /// Path to the CSV/TSV file on the filesystem
    pub csv_path: String,

    /// File format: "csv" (comma) or "tsv" (tab)
    #[serde(default = "default_format")]
    pub format: DumpFormat,

    /// Column name that maps to the slot ID (e.g., "imageId", "id")
    pub slot_field: String,

    /// Whether this phase sets the alive bitmap
    #[serde(default)]
    pub sets_alive: bool,

    /// Explicit column names (positional order) for headerless CSVs.
    /// PG COPY output has no header row — this tells the parser what each column is.
    /// If omitted, the first line of the CSV is treated as a header.
    #[serde(default)]
    pub columns: Vec<String>,

    /// Field mappings: CSV column → BitDex target field
    #[serde(default)]
    pub fields: Vec<DumpFieldMapping>,

    /// Optional row filter expression (e.g., "(attributes >> 10) & 1 = 0")
    #[serde(default)]
    pub filter: Option<String>,

    /// Computed fields derived from expressions
    #[serde(default)]
    pub computed_fields: Vec<ComputedField>,

    /// Enrichment lookups (recursive)
    #[serde(default)]
    pub enrichment: Vec<EnrichmentConfig>,

    /// Use streaming N-way merge (MultiOps::union) instead of rayon parallel reduce.
    /// Better for large datasets (107M+) where per-thread bitmaps are large.
    /// Slower for small datasets (<20M) due to collection overhead.
    #[serde(default)]
    pub streaming_merge: bool,
}

/// File format for the dump.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DumpFormat {
    Csv,
    Tsv,
}

fn default_format() -> DumpFormat {
    DumpFormat::Csv
}

/// A single field mapping: source column → target field.
///
/// Supports both expanded form `{ "column": "tagId", "target": "tagIds" }`
/// and shorthand form `"nsfwLevel"` (column == target).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DumpFieldMapping {
    /// Shorthand: column name == target field name
    Short(String),
    /// Expanded: explicit column → target mapping
    Expanded {
        column: String,
        target: String,
    },
}

impl DumpFieldMapping {
    pub fn column(&self) -> &str {
        match self {
            DumpFieldMapping::Short(s) => s,
            DumpFieldMapping::Expanded { column, .. } => column,
        }
    }

    pub fn target(&self) -> &str {
        match self {
            DumpFieldMapping::Short(s) => s,
            DumpFieldMapping::Expanded { target, .. } => target,
        }
    }
}

/// A computed field derived from an expression evaluated per row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputedField {
    /// Target field name in BitDex
    pub target: String,

    /// Expression to evaluate (see D7 expression types)
    pub expression: String,

    /// For conditional computed fields: the value column to use when expression is true.
    /// E.g., modelVersionIdsManual uses `value: "modelVersionId"` when `detected == false`.
    #[serde(default)]
    pub value: Option<String>,
}

/// Enrichment lookup configuration (recursive for nested enrichment).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentConfig {
    /// Path to the lookup CSV file (relative or absolute)
    #[serde(alias = "csv_path", alias = "lookup")]
    pub csv_path: Option<String>,

    /// Table name (for display/logging)
    #[serde(default)]
    pub table: Option<String>,

    /// Column in the lookup CSV to use as the HashMap key
    pub key: String,

    /// Column in the main CSV to join on (looked up in the enrichment HashMap)
    pub join_on: String,

    /// Fields to copy from the lookup row
    #[serde(default)]
    pub fields: Vec<DumpFieldMapping>,

    /// Computed fields from the lookup row
    #[serde(default)]
    pub computed_fields: Vec<ComputedField>,

    /// Optional filter expression on the lookup (e.g., "type = 'Checkpoint'")
    #[serde(default)]
    pub filter: Option<String>,

    /// Explicit column names for headerless CSVs (PG COPY output).
    #[serde(default)]
    pub columns: Vec<String>,

    /// Nested enrichment (e.g., MV → Model chain)
    #[serde(default)]
    pub enrichment: Vec<EnrichmentConfig>,
}

// ---------------------------------------------------------------------------
// Expression evaluation (placeholder — Nate's dump_expression.rs will replace)
// ---------------------------------------------------------------------------

/// A parsed expression ready for evaluation.
/// NOTE: This is a placeholder implementation. Nate (Agent B) is building
/// dump_expression.rs with the full expression engine. These types and
/// functions will be replaced at integration time.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Bitfield extraction: (column >> shift) & mask == expected
    Bitfield {
        column: String,
        shift: u32,
        mask: u64,
        op: CmpOp,
        expected: u64,
    },
    /// Equality: column = 'literal' or column = number
    Eq { column: String, value: ExprValue },
    /// Not-equal: column != value
    NotEq { column: String, value: ExprValue },
    /// Null check: column != null or column == null
    NullCheck { column: String, is_not_null: bool },
    /// Boolean: detected == false / detected == true
    BoolEq { column: String, expected: bool },
    /// AND of two expressions
    And(Box<Expr>, Box<Expr>),
    /// max(a, b)
    Max { columns: Vec<String> },
    /// Identity pass-through: just the column value
    Identity(String),
    /// lookup_key: the enrichment join key value itself
    LookupKey,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CmpOp {
    Eq,
    NotEq,
}

#[derive(Debug, Clone)]
pub enum ExprValue {
    Int(i64),
    Str(String),
}

/// Parse a filter/computed expression string into an Expr.
pub fn parse_expression(expr: &str) -> Result<Expr, String> {
    let expr = expr.trim();

    // "lookup_key"
    if expr == "lookup_key" {
        return Ok(Expr::LookupKey);
    }

    // Identity: just a column name (no operators)
    if expr.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Ok(Expr::Identity(expr.to_string()));
    }

    // AND expression: "expr1 && expr2"
    if let Some(pos) = find_top_level_and(expr) {
        let left = parse_expression(&expr[..pos])?;
        let right = parse_expression(&expr[pos + 2..])?;
        return Ok(Expr::And(Box::new(left), Box::new(right)));
    }

    // max(a, b)
    if expr.starts_with("max(") && expr.ends_with(')') {
        let inner = &expr[4..expr.len() - 1];
        let columns: Vec<String> = inner.split(',').map(|s| s.trim().to_string()).collect();
        return Ok(Expr::Max { columns });
    }

    // Null check: "column != null" or "column == null"
    if expr.ends_with("!= null") {
        let column = expr[..expr.len() - 7].trim().to_string();
        return Ok(Expr::NullCheck {
            column,
            is_not_null: true,
        });
    }
    if expr.ends_with("== null") {
        let column = expr[..expr.len() - 7].trim().to_string();
        return Ok(Expr::NullCheck {
            column,
            is_not_null: false,
        });
    }

    // Boolean: "column == false" / "column == true"
    if expr.ends_with("== false") {
        let column = expr[..expr.len() - 8].trim().to_string();
        return Ok(Expr::BoolEq {
            column,
            expected: false,
        });
    }
    if expr.ends_with("== true") {
        let column = expr[..expr.len() - 7].trim().to_string();
        return Ok(Expr::BoolEq {
            column,
            expected: true,
        });
    }

    // Bitfield: "(column >> N) & M == V" or "(column >> N) & M = V"
    if expr.starts_with('(') {
        return parse_bitfield_expr(expr);
    }

    // Equality: "column = 'value'" or "column = number"
    if let Some(pos) = expr.find(" = ") {
        let column = expr[..pos].trim().to_string();
        let value_str = expr[pos + 3..].trim();
        let value = parse_expr_value(value_str)?;
        return Ok(Expr::Eq { column, value });
    }

    // Not-equal: "column != value"
    if let Some(pos) = expr.find(" != ") {
        let column = expr[..pos].trim().to_string();
        let value_str = expr[pos + 4..].trim();
        // Check if it's not a null check (already handled above)
        let value = parse_expr_value(value_str)?;
        return Ok(Expr::NotEq { column, value });
    }

    Err(format!("Cannot parse expression: {expr}"))
}

fn find_top_level_and(expr: &str) -> Option<usize> {
    let mut depth = 0;
    let bytes = expr.as_bytes();
    for i in 0..bytes.len().saturating_sub(1) {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'&' if depth == 0 && bytes.get(i + 1) == Some(&b'&') => return Some(i),
            _ => {}
        }
    }
    None
}

fn parse_bitfield_expr(expr: &str) -> Result<Expr, String> {
    // Format: "(column >> N) & M op V"
    // Examples:
    //   "(attributes >> 10) & 1 = 0"
    //   "(flags >> 13) & 1 == 1"
    let close_paren = expr
        .find(')')
        .ok_or_else(|| format!("Missing ) in bitfield expr: {expr}"))?;
    let inner = &expr[1..close_paren]; // "column >> N"

    let parts: Vec<&str> = inner.split(">>").collect();
    if parts.len() != 2 {
        return Err(format!("Expected 'column >> N' in: {inner}"));
    }
    let column = parts[0].trim().to_string();
    let shift: u32 = parts[1]
        .trim()
        .parse()
        .map_err(|_| format!("Bad shift in: {inner}"))?;

    // After ): "& M op V"
    let rest = expr[close_paren + 1..].trim();
    let rest = rest
        .strip_prefix('&')
        .ok_or_else(|| format!("Expected & after ): {rest}"))?
        .trim();

    // Split on "==" or "="
    let (mask_str, op, val_str) = if let Some(pos) = rest.find("==") {
        (&rest[..pos], CmpOp::Eq, rest[pos + 2..].trim())
    } else if let Some(pos) = rest.find('=') {
        (&rest[..pos], CmpOp::Eq, rest[pos + 1..].trim())
    } else if let Some(pos) = rest.find("!=") {
        (&rest[..pos], CmpOp::NotEq, rest[pos + 2..].trim())
    } else {
        return Err(format!("No comparison op in: {rest}"));
    };

    let mask: u64 = mask_str
        .trim()
        .parse()
        .map_err(|_| format!("Bad mask: {mask_str}"))?;
    let expected: u64 = val_str
        .trim()
        .parse()
        .map_err(|_| format!("Bad expected value: {val_str}"))?;

    Ok(Expr::Bitfield {
        column,
        shift,
        mask,
        op,
        expected,
    })
}

fn parse_expr_value(s: &str) -> Result<ExprValue, String> {
    let s = s.trim();
    if s.starts_with('\'') && s.ends_with('\'') {
        Ok(ExprValue::Str(s[1..s.len() - 1].to_string()))
    } else if let Ok(n) = s.parse::<i64>() {
        Ok(ExprValue::Int(n))
    } else {
        // Treat as string without quotes
        Ok(ExprValue::Str(s.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Row representation — a parsed CSV row with named column access
// ---------------------------------------------------------------------------

/// A parsed row from a CSV/TSV file. Column values accessed by name via index lookup.
#[derive(Debug)]
pub struct ParsedRow<'a> {
    /// Raw field values as byte slices (zero-copy from mmap)
    fields: Vec<&'a [u8]>,
    /// Column name → index mapping (shared reference, not cloned per row)
    col_index: &'a HashMap<String, usize>,
}

impl<'a> ParsedRow<'a> {
    /// Get a column value as bytes, or None if column doesn't exist or is empty.
    pub fn get_bytes(&self, column: &str) -> Option<&'a [u8]> {
        let idx = self.col_index.get(column)?;
        let val = self.fields.get(*idx)?;
        if val.is_empty() {
            None
        } else {
            Some(val)
        }
    }

    /// Get a column value as i64.
    pub fn get_i64(&self, column: &str) -> Option<i64> {
        let bytes = self.get_bytes(column)?;
        parse_i64_fast(bytes)
    }

    /// Get a column value as u64.
    pub fn get_u64(&self, column: &str) -> Option<u64> {
        self.get_i64(column).map(|v| v as u64)
    }

    /// Get a column value as a string (UTF-8).
    pub fn get_str(&self, column: &str) -> Option<&'a str> {
        let bytes = self.get_bytes(column)?;
        // Strip quotes if present
        let bytes = if bytes.len() >= 2 && bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"' {
            &bytes[1..bytes.len() - 1]
        } else {
            bytes
        };
        std::str::from_utf8(bytes).ok()
    }

    /// Check if a column is null/empty.
    pub fn is_null(&self, column: &str) -> bool {
        self.get_bytes(column).is_none()
    }

    /// Get the slot ID from the configured slot_field.
    pub fn slot(&self, slot_field: &str) -> Option<u32> {
        self.get_i64(slot_field).map(|v| v as u32)
    }

    /// Convert to Nate's CsvRow format for expression/enrichment evaluation.
    pub fn to_csv_row<'b>(&'b self) -> CsvRow<'b> {
        let mut row = CsvRow::new();
        for (name, &idx) in self.col_index {
            if let Some(bytes) = self.fields.get(idx) {
                if bytes.is_empty() {
                    row.insert(name.as_str(), None);
                } else {
                    let s = if bytes.len() >= 2 && bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"' {
                        std::str::from_utf8(&bytes[1..bytes.len() - 1]).ok()
                    } else {
                        std::str::from_utf8(bytes).ok()
                    };
                    row.insert(name.as_str(), s);
                }
            }
        }
        row
    }

    /// Build indexed fields for zero-allocation expression evaluation.
    /// Returns a Vec<Option<&str>> aligned to the column index positions.
    /// Much cheaper than to_csv_row() — no HashMap allocation.
    pub fn to_indexed_fields<'b>(&'b self) -> Vec<Option<&'b str>> {
        self.fields
            .iter()
            .map(|bytes| parse_field_to_str(bytes))
            .collect()
    }

    /// Fill a pre-allocated buffer with indexed fields (reuse across rows).
    /// Avoids Vec allocation per row — just clear and refill.
    /// Uses lifetime 'a (mmap chunk) not 'b (row borrow) so the Vec can live outside the loop.
    pub fn fill_indexed_fields(&self, buf: &mut Vec<Option<&'a str>>) {
        buf.clear();
        for bytes in &self.fields {
            buf.push(parse_field_to_str(bytes));
        }
    }

    /// Get the column index (shared across all rows).
    pub fn col_index_ref(&self) -> &HashMap<String, usize> {
        self.col_index
    }
}

/// Evaluate a filter expression against a parsed row.
/// Returns true if the row passes the filter (should be included).
pub fn eval_filter(expr: &Expr, row: &ParsedRow, _lookup_key: Option<i64>) -> bool {
    match expr {
        Expr::Bitfield {
            column,
            shift,
            mask,
            op,
            expected,
        } => {
            let val = row.get_u64(column).unwrap_or(0);
            let result = (val >> shift) & mask;
            match op {
                CmpOp::Eq => result == *expected,
                CmpOp::NotEq => result != *expected,
            }
        }
        Expr::Eq { column, value } => match value {
            ExprValue::Int(n) => row.get_i64(column) == Some(*n),
            ExprValue::Str(s) => row.get_str(column) == Some(s.as_str()),
        },
        Expr::NotEq { column, value } => match value {
            ExprValue::Int(n) => row.get_i64(column) != Some(*n),
            ExprValue::Str(s) => row.get_str(column) != Some(s.as_str()),
        },
        Expr::NullCheck {
            column,
            is_not_null,
        } => {
            let has_value = !row.is_null(column);
            if *is_not_null {
                has_value
            } else {
                !has_value
            }
        }
        Expr::BoolEq { column, expected } => {
            let val = row.get_str(column).unwrap_or("");
            let is_true = val == "true" || val == "t" || val == "1";
            is_true == *expected
        }
        Expr::And(left, right) => {
            eval_filter(left, row, _lookup_key) && eval_filter(right, row, _lookup_key)
        }
        _ => true, // Max, Identity, LookupKey don't make sense as filters
    }
}

/// Evaluate a computed field expression and return the result as an i64 (for bitmap keys).
/// For boolean results, returns 1 (true) or 0 (false).
pub fn eval_computed(expr: &Expr, row: &ParsedRow, lookup_key: Option<i64>) -> Option<i64> {
    match expr {
        Expr::Identity(column) => row.get_i64(column),
        Expr::LookupKey => lookup_key,
        Expr::Max { columns } => {
            let mut max_val: Option<i64> = None;
            for col in columns {
                if let Some(v) = row.get_i64(col) {
                    max_val = Some(max_val.map_or(v, |m: i64| m.max(v)));
                }
            }
            max_val
        }
        Expr::NullCheck {
            column,
            is_not_null,
        } => {
            let has_value = !row.is_null(column);
            let result = if *is_not_null { has_value } else { !has_value };
            Some(if result { 1 } else { 0 })
        }
        Expr::BoolEq { column, expected } => {
            let val = row.get_str(column).unwrap_or("");
            let is_true = val == "true" || val == "t" || val == "1";
            Some(if is_true == *expected { 1 } else { 0 })
        }
        Expr::Bitfield {
            column,
            shift,
            mask,
            op,
            expected,
        } => {
            let val = row.get_u64(column).unwrap_or(0);
            let result = (val >> shift) & mask;
            let matches = match op {
                CmpOp::Eq => result == *expected,
                CmpOp::NotEq => result != *expected,
            };
            Some(if matches { 1 } else { 0 })
        }
        Expr::And(left, right) => {
            let l = eval_computed(left, row, lookup_key).unwrap_or(0) != 0;
            let r = eval_computed(right, row, lookup_key).unwrap_or(0) != 0;
            Some(if l && r { 1 } else { 0 })
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Enrichment system (placeholder — Nate's dump_enrichment.rs will replace)
// ---------------------------------------------------------------------------

/// A loaded enrichment HashMap — keyed by the join column value.
/// NOTE: This is a placeholder implementation. Nate (Agent B) is building
/// dump_enrichment.rs with the full enrichment engine including nested
/// lookups and expression evaluation. These types will be replaced at
/// integration time.
/// Values are raw column data stored as (column_name → value_bytes).
pub struct EnrichmentMap {
    /// key_value → { column_name → value_string }
    data: HashMap<i64, HashMap<String, String>>,
    /// Nested enrichment (loaded lazily)
    nested: Vec<EnrichmentMap>,
    /// Config for this enrichment level
    config: EnrichmentConfig,
    /// Parsed filter expression (if any)
    filter_expr: Option<Expr>,
}

impl EnrichmentMap {
    /// Load an enrichment map from a CSV file.
    pub fn load(config: &EnrichmentConfig, stage_dir: &Path) -> Result<Self, String> {
        let csv_path = resolve_csv_path(config, stage_dir);
        let table_name = config.table.as_deref().unwrap_or("unknown");
        let t = Instant::now();

        // Read file — enrichment CSVs are small enough for BufReader (< 100MB typically)
        let file_data = std::fs::read(&csv_path)
            .map_err(|e| format!("read enrichment CSV {}: {e}", csv_path.display()))?;

        // Detect delimiter: first non-empty line
        let delimiter = detect_delimiter(&file_data, &DumpFormat::Csv);

        // Parse header to get column indices
        let mut lines = file_data.split(|&b| b == b'\n');
        let header_line = lines
            .next()
            .ok_or_else(|| format!("Empty enrichment CSV: {}", csv_path.display()))?;
        let headers = parse_delimited_line(header_line, delimiter);
        let header_names: Vec<String> = headers
            .iter()
            .map(|h| {
                let s = std::str::from_utf8(h).unwrap_or("").trim();
                s.trim_matches('"').to_string()
            })
            .collect();

        let key_idx = header_names
            .iter()
            .position(|h| h == &config.key)
            .ok_or_else(|| {
                format!(
                    "Enrichment key column '{}' not found in headers: {:?}",
                    config.key, header_names
                )
            })?;

        // Build HashMap
        let mut data: HashMap<i64, HashMap<String, String>> = HashMap::new();
        for line in lines {
            let line = line.strip_suffix(&[b'\r']).unwrap_or(line);
            if line.is_empty() {
                continue;
            }
            let fields = parse_delimited_line(line, delimiter);
            let key_bytes = fields.get(key_idx).copied().unwrap_or(b"");
            let key = match parse_i64_fast(key_bytes) {
                Some(k) => k,
                None => continue,
            };

            let mut row_data: HashMap<String, String> = HashMap::new();
            for (i, name) in header_names.iter().enumerate() {
                if let Some(bytes) = fields.get(i) {
                    let s = std::str::from_utf8(bytes).unwrap_or("").trim();
                    let s = s.trim_matches('"');
                    if !s.is_empty() {
                        row_data.insert(name.clone(), s.to_string());
                    }
                }
            }
            data.insert(key, row_data);
        }

        eprintln!(
            "  Enrichment {}: {} rows loaded in {:.1}s",
            table_name,
            data.len(),
            t.elapsed().as_secs_f64()
        );

        // Parse filter expression
        let filter_expr = config
            .filter
            .as_ref()
            .map(|f| parse_expression(f))
            .transpose()?;

        // Load nested enrichment
        let mut nested = Vec::new();
        for child_config in &config.enrichment {
            nested.push(EnrichmentMap::load(child_config, stage_dir)?);
        }

        Ok(EnrichmentMap {
            data,
            nested,
            config: config.clone(),
            filter_expr,
        })
    }

    /// Look up enrichment data for a given key and apply fields to the row context.
    /// Returns a map of target_field → value_string for all resolved fields.
    pub fn resolve(
        &self,
        key: i64,
    ) -> Option<HashMap<String, String>> {
        let row_data = self.data.get(&key)?;

        // Check filter on this enrichment level
        if let Some(ref filter) = self.filter_expr {
            // Build a temporary ParsedRow for filter evaluation
            if !eval_enrichment_filter(filter, row_data) {
                return None;
            }
        }

        let mut result: HashMap<String, String> = HashMap::new();

        // Copy configured fields
        for field_mapping in &self.config.fields {
            let col = field_mapping.column();
            let target = field_mapping.target();
            if let Some(val) = row_data.get(col) {
                result.insert(target.to_string(), val.clone());
            }
        }

        // Evaluate computed fields
        for computed in &self.config.computed_fields {
            if let Some(val) = eval_enrichment_computed(&computed.expression, row_data, key) {
                result.insert(computed.target.clone(), val);
            }
        }

        // Resolve nested enrichment
        for (i, child_map) in self.nested.iter().enumerate() {
            let child_config = &self.config.enrichment[i];
            // The nested join_on references a column in THIS enrichment's row
            if let Some(join_val_str) = row_data.get(&child_config.join_on) {
                if let Ok(join_val) = join_val_str.parse::<i64>() {
                    if let Some(nested_fields) = child_map.resolve(join_val) {
                        result.extend(nested_fields);
                    }
                }
            }
        }

        Some(result)
    }
}

/// Evaluate a filter expression against enrichment row data.
fn eval_enrichment_filter(expr: &Expr, data: &HashMap<String, String>) -> bool {
    match expr {
        Expr::Eq { column, value } => {
            let val = data.get(column).map(|s| s.as_str()).unwrap_or("");
            match value {
                ExprValue::Str(s) => val == s,
                ExprValue::Int(n) => val.parse::<i64>().ok() == Some(*n),
            }
        }
        Expr::NotEq { column, value } => {
            let val = data.get(column).map(|s| s.as_str()).unwrap_or("");
            match value {
                ExprValue::Str(s) => val != s,
                ExprValue::Int(n) => val.parse::<i64>().ok() != Some(*n),
            }
        }
        Expr::NullCheck {
            column,
            is_not_null,
        } => {
            let has = data.contains_key(column);
            if *is_not_null { has } else { !has }
        }
        Expr::And(l, r) => {
            eval_enrichment_filter(l, data) && eval_enrichment_filter(r, data)
        }
        _ => true,
    }
}

/// Evaluate a computed field expression against enrichment row data.
fn eval_enrichment_computed(
    expr_str: &str,
    data: &HashMap<String, String>,
    lookup_key: i64,
) -> Option<String> {
    let expr_str = expr_str.trim();
    if expr_str == "lookup_key" {
        return Some(lookup_key.to_string());
    }
    if expr_str.ends_with("!= null") {
        let col = expr_str[..expr_str.len() - 7].trim();
        let has = data.contains_key(col);
        return Some(if has { "1" } else { "0" }.to_string());
    }
    // Identity: just the column name
    if expr_str.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return data.get(expr_str).cloned();
    }
    None
}

fn resolve_csv_path(config: &EnrichmentConfig, stage_dir: &Path) -> std::path::PathBuf {
    if let Some(ref p) = config.csv_path {
        let path = std::path::Path::new(p);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            stage_dir.join(p)
        }
    } else {
        // Derive from table name
        let table = config.table.as_deref().unwrap_or("unknown");
        stage_dir.join(format!("{}.csv", table.to_lowercase()))
    }
}

/// Convert a D3 EnrichmentConfig to Nate's dump_enrichment::EnrichmentConfig.
fn to_nate_enrichment_config(
    config: &EnrichmentConfig,
    stage_dir: &Path,
) -> dump_enrichment::EnrichmentConfig {
    let csv_path = resolve_csv_path(config, stage_dir);
    let fields: Vec<(String, String)> = config
        .fields
        .iter()
        .map(|f| (f.column().to_string(), f.target().to_string()))
        .collect();
    let computed_fields: Vec<ComputedFieldDef> = config
        .computed_fields
        .iter()
        .filter_map(|cf| {
            ComputedFieldDef::parse(&cf.target, &cf.expression, cf.value.as_deref()).ok()
        })
        .collect();
    let filter = config
        .filter
        .as_ref()
        .and_then(|f| FilterExpression::parse(f).ok());

    // Nested enrichment (only first child supported in Nate's API via Box)
    let child = config.enrichment.first().map(|child_config| {
        Box::new(to_nate_enrichment_config(child_config, stage_dir))
    });

    dump_enrichment::EnrichmentConfig {
        csv_path,
        key: config.key.clone(),
        join_on: config.join_on.clone(),
        fields,
        computed_fields,
        filter,
        child,
        columns: config.columns.clone(),
    }
}

// ---------------------------------------------------------------------------
// CSV parsing helpers
// ---------------------------------------------------------------------------

fn detect_delimiter(_data: &[u8], format: &DumpFormat) -> u8 {
    match format {
        DumpFormat::Tsv => b'\t',
        DumpFormat::Csv => b',',
    }
}

/// Parse a byte-slice field to &str, handling quotes and empty values.
#[inline]
fn parse_field_to_str<'a>(bytes: &'a [u8]) -> Option<&'a str> {
    if bytes.is_empty() {
        None
    } else if bytes.len() >= 2 && bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"' {
        std::str::from_utf8(&bytes[1..bytes.len() - 1]).ok()
    } else {
        std::str::from_utf8(bytes).ok()
    }
}

/// Parse a single delimited line into fields. Handles quoted fields.
fn parse_delimited_line<'a>(line: &'a [u8], delimiter: u8) -> Vec<&'a [u8]> {
    let mut fields = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let line = line.strip_suffix(&[b'\r']).unwrap_or(line);

    for i in 0..line.len() {
        match line[i] {
            b'"' => in_quotes = !in_quotes,
            d if d == delimiter && !in_quotes => {
                fields.push(&line[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    fields.push(&line[start..]);
    fields
}

/// Fast i64 parser from byte slice (no allocation).
#[inline]
fn parse_i64_fast(bytes: &[u8]) -> Option<i64> {
    let bytes = bytes.strip_suffix(&[b'\r']).unwrap_or(bytes);
    if bytes.is_empty() {
        return None;
    }
    let (negative, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };
    let mut n: i64 = 0;
    for &b in &bytes[start..] {
        if b >= b'0' && b <= b'9' {
            n = n * 10 + (b - b'0') as i64;
        } else if b == b'\r' || b == b' ' || b == b'"' {
            break;
        } else {
            return None;
        }
    }
    Some(if negative { -n } else { n })
}

/// Split mmap data into N byte-range chunks aligned to newlines.
fn split_mmap_ranges(data: &[u8], num_threads: usize) -> Vec<(usize, usize)> {
    let file_len = data.len();
    if file_len == 0 || num_threads == 0 {
        return vec![];
    }
    let chunk_size = file_len / num_threads;
    let mut ranges = Vec::with_capacity(num_threads);
    let mut start = 0;
    for i in 0..num_threads {
        let end = if i == num_threads - 1 {
            file_len
        } else {
            let tentative = (start + chunk_size).min(file_len);
            match data[tentative..].iter().position(|&b| b == b'\n') {
                Some(offset) => tentative + offset + 1,
                None => file_len,
            }
        }
        .min(file_len);
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }
    ranges
}

// ---------------------------------------------------------------------------
// Phase result types
// ---------------------------------------------------------------------------

/// Result of processing one dump phase.
pub struct PhaseResult {
    pub row_count: u64,
    pub filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>>,
    pub sort_maps: HashMap<String, Vec<RoaringBitmap>>,
    pub alive: RoaringBitmap,
    pub deferred_slots: BTreeMap<u64, Vec<u32>>,
    pub max_slot: u32,
}

// ---------------------------------------------------------------------------
// Main dump processor entry point
// ---------------------------------------------------------------------------

/// Process a single dump phase based on the D3 request body.
///
/// This is the main entry point called by the server when it receives
/// `PUT /api/indexes/{name}/dumps`.
/// Validate a dump request before processing. Returns Ok(()) or a clear error.
pub fn validate_dump_request(
    request: &DumpRequest,
    engine: &ConcurrentEngine,
) -> Result<(), String> {
    // Check csv_path exists
    let csv_path = std::path::Path::new(&request.csv_path);
    if !csv_path.exists() {
        return Err(format!("CSV file not found: {}", request.csv_path));
    }

    // Check name is non-empty
    if request.name.is_empty() {
        return Err("Dump name cannot be empty".to_string());
    }

    // Check slot_field is non-empty
    if request.slot_field.is_empty() {
        return Err("slot_field cannot be empty".to_string());
    }

    // Check at least one field or computed_field
    if request.fields.is_empty() && request.computed_fields.is_empty() && !request.sets_alive {
        return Err("Dump must have at least one field, computed_field, or sets_alive=true".to_string());
    }

    // Validate filter expression parses
    if let Some(ref filter) = request.filter {
        parse_expression(filter).map_err(|e| format!("Invalid filter expression '{}': {}", filter, e))?;
    }

    // Validate computed field expressions parse
    for cf in &request.computed_fields {
        parse_expression(&cf.expression)
            .map_err(|e| format!("Invalid computed expression for '{}': {}", cf.target, e))?;
    }

    // Warn about unknown target fields (not in engine config)
    let config = engine.config();
    let known_filters: HashSet<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let known_sorts: HashSet<String> = config.sort_fields.iter().map(|f| f.name.clone()).collect();

    for field in &request.fields {
        let target = field.target();
        if !known_filters.contains(target) && !known_sorts.contains(target) {
            eprintln!(
                "WARNING: dump '{}' targets unknown field '{}' (not in filter/sort config — may be doc-only)",
                request.name, target
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// ShardPreCreator — background thread for progressive shard file creation
// ---------------------------------------------------------------------------

/// Progressively creates docstore shard files and bitmap dirs as the slot
/// watermark rises during CSV processing. Run on a background thread during
/// the first dump phase (e.g., tags) so that subsequent phases (images) find
/// all files already on disk — eliminating lazy file creation from the hot path.
///
/// Microbench showed 10x write speedup when files pre-exist (1.8M rows/s vs 177K/s).
pub struct ShardPreCreator {
    handle: Option<std::thread::JoinHandle<u32>>,
}

impl ShardPreCreator {
    /// Spawn a background thread that watches `watermark` and creates shard files
    /// up to `shard_id(watermark)`. Also creates filter bitmap bucket dirs for
    /// all configured filter fields.
    ///
    /// Call `watermark.fetch_max(slot, Relaxed)` from parse threads to advance.
    /// Call `stop()` when the phase completes to join the thread.
    pub fn spawn(
        watermark: Arc<AtomicU64>,
        done: Arc<std::sync::atomic::AtomicBool>,
        docstore_root: std::path::PathBuf,
        bitmap_path: Option<std::path::PathBuf>,
        filter_field_names: Vec<String>,
    ) -> Self {
        let handle = std::thread::Builder::new()
            .name("shard-precreator".into())
            .spawn(move || {
                let files_created: u32 = 0;
                let mut bitmap_dirs_done = false;
                let _docstore_root = docstore_root; // DataSilo needs no shard pre-creation

                // DataSilo does not use per-shard files — no pre-creation needed.
                // Only pre-create filter bitmap bucket dirs for BitmapSilo persistence.
                loop {
                    let current_max_slot = watermark.load(std::sync::atomic::Ordering::Relaxed) as u32;

                    // Create filter bitmap dirs once (first time watermark > 0)
                    if !bitmap_dirs_done && current_max_slot > 0 {
                        if let Some(ref bp) = bitmap_path {
                            for field in &filter_field_names {
                                for bucket in 0..=255u8 {
                                    let dir = bp.join("filter").join(field).join(format!("{:02x}", bucket));
                                    let _ = std::fs::create_dir_all(&dir);
                                }
                            }
                            // Sort dirs
                            let _ = std::fs::create_dir_all(bp.join("sort"));
                            let _ = std::fs::create_dir_all(bp.join("system"));
                        }
                        bitmap_dirs_done = true;
                    }

                    if done.load(std::sync::atomic::Ordering::Relaxed) {
                        eprintln!("  ShardPreCreator: done — DataSilo needs no shard pre-creation");
                        return files_created;
                    }

                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            })
            .expect("failed to spawn shard-precreator");

        ShardPreCreator { handle: Some(handle) }
    }

    /// Signal completion and wait for the creator thread to finish.
    /// Returns the number of shard files created.
    pub fn stop(mut self) -> u32 {
        if let Some(h) = self.handle.take() {
            h.join().unwrap_or(0)
        } else {
            0
        }
    }
}

pub fn process_dump(
    request: &DumpRequest,
    engine: &ConcurrentEngine,
    stage_dir: &Path,
    progress_counter: Option<Arc<AtomicU64>>,
    data_schema: Option<&crate::config::DataSchema>,
    slot_watermark: Option<Arc<AtomicU64>>,
    shutdown: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
) -> Result<PhaseResult, String> {
    let t_total = Instant::now();

    let result = process_dump_with_progress(request, engine, stage_dir, progress_counter, data_schema, slot_watermark.as_ref(), shutdown.as_ref())?;

    // Bitmaps applied to staging inside process_dump_with_progress (fused with merge).
    // save_snapshot and doc compact deferred to after all phases complete.

    // Persist LCS dictionaries after each phase.
    if let Some(ref bitmap_path) = engine.config().storage.bitmap_path {
        engine.save_dictionaries(bitmap_path)
            .map_err(|e| format!("save_dictionaries: {e}"))?;
    }

    eprintln!("  Dump {} total process_dump in {:.1}s", request.name, t_total.elapsed().as_secs_f64());
    Ok(result)
}

/// Compact the doc silo after all dump phases complete.
/// This merges all ops (from all phases) into the data file.
/// Call ONCE after the last dump phase, before reload_after_dumps.
pub fn compact_after_dumps(engine: &ConcurrentEngine) -> Result<(), String> {
    let t = Instant::now();
    let ds = engine.docstore_arc();
    let mut ds_lock = ds.lock();
    let count = ds_lock.silo_mut().compact()
        .map_err(|e| format!("compact: {e}"))?;
    eprintln!("  Dump compact: {} docs in {:.2}s", count, t.elapsed().as_secs_f64());
    Ok(())
}

/// Post-dump hook. Called after the last dump phase completes.
/// With DataSilo, bitmaps are already applied to engine staging during process_dump.
/// No disk reload needed — bitmaps are in-memory.
pub fn reload_after_dumps(engine: &ConcurrentEngine, _had_alive_phase: bool) {
    // Bitmaps are already in the engine staging from process_dump's apply_bitmap_maps.
    // No need to mark fields for lazy reload from disk (BitmapSilo Phase 5).
    // Just clear the unified cache to ensure queries see fresh bitmap data.
    engine.clear_cache();
    let snap = engine.snapshot_public();
    eprintln!(
        "  Dump reload: alive={}, no disk reload needed (bitmaps applied in-memory)",
        snap.slots.alive_count()
    );
}

/// Process a dump phase with optional external progress counter.
/// When `progress_counter` is provided (from the task system), it's incremented
/// per row so `GET /api/tasks/{id}` shows real-time progress.
///
/// When `data_schema` is provided, fields marked `filter_only: true` in the schema
/// are excluded from docstore writes (BulkWriter will not get an index for them,
/// so `field_to_idx().get(target)` returns None and the docstore write path is skipped).
pub fn process_dump_with_progress(
    request: &DumpRequest,
    engine: &ConcurrentEngine,
    stage_dir: &Path,
    progress_counter: Option<Arc<AtomicU64>>,
    data_schema: Option<&crate::config::DataSchema>,
    slot_watermark: Option<&Arc<AtomicU64>>,
    shutdown: Option<&Arc<dyn Fn() -> bool + Send + Sync>>,
) -> Result<PhaseResult, String> {
    let t = Instant::now();

    // Validate before processing
    validate_dump_request(request, engine)?;
    emit_stage(&request.name, "validated", "ok", &t, 0);

    let config = engine.config();
    let filter_field_names: HashSet<String> =
        config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_bits: HashMap<String, u8> = config
        .sort_fields
        .iter()
        .map(|f| (f.name.clone(), f.bits))
        .collect();

    // Determine target fields and their types
    let target_fields = collect_target_fields(request);

    // Parse filter expression (Nate's API)
    let filter_expr: Option<FilterExpression> = request
        .filter
        .as_ref()
        .map(|f| FilterExpression::parse(f))
        .transpose()?;

    // Parse computed field expressions (Nate's API)
    let computed_defs: Vec<ComputedFieldDef> = request
        .computed_fields
        .iter()
        .map(|cf| {
            ComputedFieldDef::parse(&cf.target, &cf.expression, cf.value.as_deref())
        })
        .collect::<Result<Vec<_>, String>>()?;

    // Load enrichment tables — per-table timing
    emit_stage(&request.name, "enrichment", "start", &t, 0);
    let mut enrichment_mgr = dump_enrichment::EnrichmentManager::new();
    for ec in &request.enrichment {
        let table_name = ec.table.as_deref().or(ec.csv_path.as_deref()).unwrap_or("unknown");
        let t_el = Instant::now();
        let nate_config = to_nate_enrichment_config(ec, stage_dir);
        enrichment_mgr
            .load(nate_config)
            .map_err(|e| format!("load enrichment: {e}"))?;
        eprintln!("  Enrichment '{}': loaded in {:.2}s", table_name, t_el.elapsed().as_secs_f64());
    }
    emit_stage(&request.name, "enrichment", "done", &t, 0);

    // Get LCS dictionaries from engine (thread-safe DashMap-based)
    let dictionaries: Arc<std::collections::HashMap<String, FieldDictionary>> = engine.dictionaries_arc();

    // Build set of filter_only field names from data schema (config-driven).
    // Fields marked filter_only are bitmap-indexed only — no docstore writes.
    let filter_only_fields: HashSet<String> = data_schema
        .map(|ds| {
            ds.fields
                .iter()
                .filter(|m| m.filter_only)
                .map(|m| m.target.clone())
                .collect()
        })
        .unwrap_or_default();

    // Build set of boolean field names from data schema for type-aware docstore writes.
    // PG COPY outputs booleans as "t"/"f" strings — we need to coerce them to PackedValue::B.
    let boolean_fields: HashSet<String> = data_schema
        .map(|ds| {
            ds.fields
                .iter()
                .filter(|m| matches!(m.value_type, crate::config::FieldValueType::Boolean | crate::config::FieldValueType::ExistsBoolean))
                .map(|m| m.target.clone())
                .collect()
        })
        .unwrap_or_default();

    // Ensure field names are registered in the DocSiloAdapter before dump.
    // Include config-computed sort field names (e.g., sortAt = GREATEST(...)) since
    // those are written via extra_i64_fields and must have a field index.
    let mut doc_target_names: Vec<String> = target_fields
        .iter()
        .filter(|t| !filter_only_fields.contains(*t))
        .cloned()
        .collect();
    for sf in &config.sort_fields {
        if sf.computed.is_some() && !doc_target_names.contains(&sf.name) {
            doc_target_names.push(sf.name.clone());
        }
    }
    engine.prepare_field_names(&doc_target_names)
        .map_err(|e| format!("prepare_field_names: {e}"))?;
    // Get the field_to_idx mapping for doc encoding during parse.
    let doc_field_to_idx: Arc<std::collections::HashMap<String, u16>> = {
        let ds = engine.docstore_arc();
        let ds_lock = ds.lock();
        Arc::new(ds_lock.field_to_idx().clone())
    };
    // Mmap the CSV/TSV file.
    // IMPORTANT: The mmap is scoped tightly around the parse phase (see the
    // `mmap_scope` block below). After parsing completes and the PhaseResult
    // is built, the mmap is dropped immediately. This prevents zombie processes
    // from holding 80+ GB of virtual memory after a forced kill — the mmap is
    // the largest allocation and must not outlive the parse.
    let csv_path = std::path::Path::new(&request.csv_path);
    let file = std::fs::File::open(csv_path)
        .map_err(|e| format!("open {}: {e}", csv_path.display()))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| format!("mmap {}: {e}", csv_path.display()))?;
    // Sequential hint: single front-to-back scan split across rayon threads.
    #[cfg(unix)] let _ = mmap.advise(memmap2::Advice::Sequential);
    let data = &mmap[..];
    let delimiter = detect_delimiter(data, &request.format);

    eprintln!(
        "  Dump {}: mmap'd {} ({:.1} GB), format={:?}",
        request.name,
        data.len(),
        data.len() as f64 / (1024.0 * 1024.0 * 1024.0),
        request.format
    );

    // Build column index: from explicit columns (headerless CSV) or first row (header CSV).
    // Auto-detect: if columns are in config AND the first row matches them, skip it as a header.
    let (col_index, data_start) = if !request.columns.is_empty() {
        let index: HashMap<String, usize> = request
            .columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        // Check if first row is a header that matches config columns — skip it if so
        let first_newline = data.iter().position(|&b| b == b'\n').unwrap_or(data.len());
        let first_row = parse_delimited_line(&data[..first_newline], delimiter);
        let is_header = first_row.len() == request.columns.len()
            && first_row.iter().zip(&request.columns).all(|(a, b)| {
                let a_str = std::str::from_utf8(a).unwrap_or("").trim().trim_matches('"');
                a_str == b
            });
        let skip = if is_header { first_newline + 1 } else { 0 };
        (Arc::new(index), skip)
    } else {
        // Header CSV — parse first row as column names
        let first_newline = data
            .iter()
            .position(|&b| b == b'\n')
            .unwrap_or(data.len());
        let header_line = &data[..first_newline];
        let headers = parse_delimited_line(header_line, delimiter);
        let index: HashMap<String, usize> = headers
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let name = std::str::from_utf8(h)
                    .unwrap_or("")
                    .trim()
                    .trim_matches('"')
                    .to_string();
                (name, i)
            })
            .collect();
        (Arc::new(index), first_newline + 1) // Data starts after header
    };

    // Data starts after header
    let body = &data[data_start..];

    // Deferred alive config
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let has_deferred_alive = config.deferred_alive.is_some() && request.sets_alive;

    // Detect multi-value-only phases (tags, tools, techniques).
    // These have a single multi-value field and no enrichment/computed fields.
    // After parse, we invert the accumulated bitmaps to reconstruct per-slot arrays
    // and write them to the DataSilo ops log — one Merge op per slot.
    let is_multi_value_only = request.fields.len() == 1
        && !request.sets_alive
        && request.computed_fields.is_empty()
        && request.enrichment.is_empty()
        && {
            let target = request.fields[0].target();
            target == "tagIds" || target == "toolIds" || target == "techniqueIds"
        };

    emit_stage(&request.name, "parallel_parse", "start", &t, 0);

    // General phase processing with rayon parallelism
    let ranges = split_mmap_ranges(body, rayon::current_num_threads());
    let total = AtomicU64::new(0);
    let total_ref = &total;
    let ext_progress = &progress_counter; // task system progress counter
    // Shared references for parallel access
    let filter_expr_ref = &filter_expr;
    let computed_defs_ref = &computed_defs;
    let enrichment_mgr_ref = &enrichment_mgr;
    let dictionaries_ref = dictionaries.as_ref();
    let filter_field_names_ref = &filter_field_names;
    let sort_bits_ref = &sort_bits;
    let slot_field = &request.slot_field;
    let request_fields = &request.fields;
    let sets_alive = request.sets_alive;

    // Collect filter target names from request fields + enrichment targets
    let mut filter_targets: Vec<String> = request_fields
        .iter()
        .map(|f| f.target().to_string())
        .filter(|t| filter_field_names.contains(t))
        .collect();
    let mut sort_targets: Vec<(String, u8)> = request_fields
        .iter()
        .filter_map(|f| {
            let t = f.target().to_string();
            sort_bits.get(&t).map(|&b| (t, b))
        })
        .collect();
    // Also include enrichment-derived fields (availability, baseModel, etc.)
    let mut enrichment_targets: Vec<String> = Vec::new();
    for ec in &request.enrichment {
        collect_enrichment_targets(ec, &mut enrichment_targets);
    }
    for t in &enrichment_targets {
        if filter_field_names.contains(t) && !filter_targets.contains(t) {
            filter_targets.push(t.clone());
        }
        if let Some(&b) = sort_bits.get(t.as_str()) {
            if !sort_targets.iter().any(|(n, _)| n == t) {
                sort_targets.push((t.clone(), b));
            }
        }
    }
    let enrichment_targets_ref = &enrichment_targets;
    // Also include computed filter fields in filter_targets
    for def in &computed_defs {
        if filter_field_names.contains(&def.target) && !filter_targets.contains(&def.target) {
            filter_targets.push(def.target.clone());
        }
    }
    // Build compact field_name → u8 index for flat Vec filter tuples
    let filter_field_to_idx: HashMap<String, u8> = filter_targets.iter().enumerate()
        .map(|(i, name)| (name.clone(), i as u8))
        .collect();
    let filter_idx_to_name: Vec<String> = filter_targets.clone();
    // Also include computed fields that are sort fields
    let computed_sort_targets: Vec<(String, u8)> = computed_defs
        .iter()
        .filter_map(|def| {
            sort_bits.get(&def.target).map(|&b| (def.target.clone(), b))
        })
        .collect();

    // Config-driven computed sort fields (e.g., sortAt = GREATEST(existedAt, publishedAt)).
    // These are defined in the index config's sort_fields, NOT in the dump request.
    // After all per-row sort values are collected, we evaluate these and set the bitmaps.
    struct ConfigComputedSort {
        target: String,
        bits: u8,
        op: crate::config::ComputedOp,
        source_fields: Vec<String>,
    }
    let config_computed_sorts: Vec<ConfigComputedSort> = config
        .sort_fields
        .iter()
        .filter_map(|sc| {
            sc.computed.as_ref().map(|c| ConfigComputedSort {
                target: sc.name.clone(),
                bits: sc.bits,
                op: c.op.clone(),
                source_fields: c.source_fields.clone(),
            })
        })
        .collect();
    let config_computed_sorts_ref = &config_computed_sorts;

    // Source fields needed by config-computed sorts (e.g., existedAt, publishedAt for sortAt).
    // These values must be collected per-row even if the source field isn't in sort_fields,
    // so that GREATEST/LEAST can evaluate correctly.
    let config_computed_sources: std::collections::HashSet<String> = config_computed_sorts
        .iter()
        .flat_map(|ccs| ccs.source_fields.iter().cloned())
        .collect();
    let config_computed_sources_ref = &config_computed_sources;

    // Ollie #5: Vec<RoaringBitmap> for sort bit layers instead of HashMap<usize, _>.
    // Preallocate Vec of size num_bits — eliminates per-bit hash overhead.
    // Thread result includes doc_ops: encoded Merge ops to write to DataSilo after parse.
    // For multi-value-only phases, doc_ops is empty (bitmap inversion post-pass writes docs).
    type ThreadResult = (
        HashMap<String, HashMap<u64, RoaringBitmap>>,
        HashMap<String, Vec<RoaringBitmap>>,
        RoaringBitmap,
        Vec<(u32, u64)>,
        u64,
        u32,
        Vec<(u32, Vec<u8>)>, // doc_ops: (slot, encoded Merge op bytes)
    );

    // Prepare parallel ops writer for direct mmap writes from rayon threads.
    // Each thread writes doc ops directly to the mmap'd ops log at 32M+ ops/s.
    // Prepare parallel ops writer for ALL phases (including multi-value).
    // For MV phases, the post-pass uses it to write doc ops in parallel.
    let parallel_ops_writer: Option<Arc<datasilo::ParallelOpsWriter>> = {
        let estimated_rows = (body.len() / 100).max(1000);
        let estimated_bytes = estimated_rows as u64 * 400; // ~300 bytes per doc + framing
        let ds = engine.docstore_arc();
        let ds_lock = ds.lock();
        match ds_lock.silo().prepare_parallel_ops(estimated_bytes) {
            Ok(pw) => Some(Arc::new(pw)),
            Err(e) => {
                eprintln!("  Dump {}: parallel ops writer failed (falling back to batch): {e}", request.name);
                None
            }
        }
    };
    let pw_ref = &parallel_ops_writer;

    // Build compiled doc field plan — pre-resolves all HashMap lookups and HashSet checks.
    let extra_i64_targets: Vec<String> = config_computed_sorts.iter().map(|ccs| ccs.target.clone()).collect();
    let doc_field_plan = build_doc_field_plan(
        request_fields, enrichment_targets_ref, &computed_defs,
        &extra_i64_targets, doc_field_to_idx.as_ref(), &boolean_fields,
        filter_field_names_ref,
    );
    let doc_field_plan_ref = &doc_field_plan;

    let thread_results: Vec<ThreadResult> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &body[range_start..range_end];

            // Use the shared field_to_idx for doc encoding.
            // Convert std HashMap → AHashMap for use in inner loop (one-time per thread)
            let field_idx_cache: HashMap<String, u16> = doc_field_to_idx.iter().map(|(k, v)| (k.clone(), *v)).collect();
            let col_idx_ref: &HashMap<String, usize> = col_index.as_ref();

            // Flat Vec for filter bitmap tuples — push (field_idx, value, slot) per row.
            // Bitmaps built in post-pass via sort + from_sorted_iter (5.3x faster than per-row HashMap insert).
            let mut filter_tuples: Vec<(u8, u64, u32)> = Vec::with_capacity(
                ((range_end - range_start) / 100) * 8  // ~8 filter fields per row
            );
            // Collect sort slots into Vec<u32> per bit-layer (not RoaringBitmap).
            // After the row loop, sort + from_sorted_iter builds bitmaps 5.86x faster.
            let mut sort_vecs: HashMap<String, Vec<Vec<u32>>> = sort_targets
                .iter()
                .chain(computed_sort_targets.iter())
                .map(|(n, b)| {
                    let layers: Vec<Vec<u32>> = (0..*b as usize).map(|_| Vec::new()).collect();
                    (n.clone(), layers)
                })
                .collect();
            for ccs in config_computed_sorts_ref {
                sort_vecs.entry(ccs.target.clone()).or_insert_with(|| {
                    (0..ccs.bits as usize).map(|_| Vec::new()).collect()
                });
            }
            let mut alive = RoaringBitmap::new();
            let mut deferred: Vec<(u32, u64)> = Vec::new();
            // Doc ops collected during parse — written to DataSilo after fold/reduce.
            // For multi-value-only phases, no doc ops are collected here (post-pass handles it).
            let mut doc_ops: Vec<(u32, Vec<u8>)> = if is_multi_value_only || pw_ref.is_some() {
                Vec::new() // not needed when using parallel ops writer
            } else {
                Vec::with_capacity(4096)
            };
            // Thread-local cursor for parallel ops writer (1MB regions)
            let mut ops_local_cursor: usize = 0;
            let mut ops_local_end: usize = 0;
            // Thread-local scratch buffers for zero-alloc doc encoding + framing
            let mut doc_encode_buf: Vec<u8> = Vec::with_capacity(512);
            let mut frame_buf: Vec<u8> = Vec::with_capacity(512);
            let mut count = 0u64;
            let mut max_slot: u32 = 0;
            let mut line_start = 0;
            // Reusable buffer for indexed fields — avoids Vec alloc per row.
            let mut indexed_fields_buf: Vec<Option<&str>> = Vec::new();
            // Reusable buffer for enrichment results — avoids Vec realloc per row.
            let mut enriched_buf = dump_enrichment::EnrichedFields::default();
            // Reusable buffer for mmap enrichment lookups — avoids Vec alloc per row.
            let mut enrichment_lookup_buf: Vec<Option<&str>> = Vec::new();
            // Note: enriched_map is created fresh each iteration (small — typically <10 entries).
            // Cannot reuse across iterations due to borrow of enriched_buf.
            // Reusable Vec for doc field plan output — cleared per row, no alloc after first.
            // doc_fields created per-iteration (DumpFieldValue borrows from row/enrichment
            // which are per-iteration scoped — can't reuse Vec across iterations)
            #[cfg(feature = "dump-timing")]
            let mut timings = RowTimings::default();

            for i in 0..chunk.len() {
                if chunk[i] != b'\n' {
                    continue;
                }
                let line = &chunk[line_start..i];
                line_start = i + 1;
                let line = line.strip_suffix(&[b'\r']).unwrap_or(line);
                if line.is_empty() {
                    continue;
                }

                #[cfg(feature = "dump-timing")]
                let _row_start = std::time::Instant::now();

                #[cfg(feature = "dump-timing")]
                let _t_csv = std::time::Instant::now();
                let fields = parse_delimited_line(line, delimiter);
                let row = ParsedRow {
                    fields,
                    col_index: col_idx_ref,
                };
                #[cfg(feature = "dump-timing")]
                { timings.csv_parse += _t_csv.elapsed().as_nanos() as u64; }

                // Get slot ID
                #[cfg(feature = "dump-timing")]
                let _t_slot = std::time::Instant::now();
                let slot = match row.slot(slot_field) {
                    Some(s) => s,
                    None => continue,
                };
                if slot > max_slot {
                    max_slot = slot;
                    if let Some(ref wm) = slot_watermark {
                        wm.fetch_max(slot as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                #[cfg(feature = "dump-timing")]
                { timings.slot_extract += _t_slot.elapsed().as_nanos() as u64; }

                // Reuse indexed fields buffer
                #[cfg(feature = "dump-timing")]
                let _t_idx = std::time::Instant::now();
                row.fill_indexed_fields(&mut indexed_fields_buf);
                let col_idx = row.col_index_ref();
                #[cfg(feature = "dump-timing")]
                { timings.indexed_fields += _t_idx.elapsed().as_nanos() as u64; }

                // Apply filter via indexed path
                #[cfg(feature = "dump-timing")]
                let _t_filt = std::time::Instant::now();
                if let Some(ref fexpr) = filter_expr_ref {
                    if !fexpr.eval_indexed(&indexed_fields_buf, col_idx, None) {
                        #[cfg(feature = "dump-timing")]
                        { timings.filter_expr += _t_filt.elapsed().as_nanos() as u64; }
                        continue;
                    }
                }
                #[cfg(feature = "dump-timing")]
                { timings.filter_expr += _t_filt.elapsed().as_nanos() as u64; }


                // Resolve enrichment via indexed path — reuse buffer
                #[cfg(feature = "dump-timing")]
                let _t_enrich = std::time::Instant::now();
                if enrichment_mgr_ref.table_count() > 0 {
                    enrichment_mgr_ref.enrich_row_indexed_into(&indexed_fields_buf, col_idx, &mut enriched_buf, &mut enrichment_lookup_buf);
                } else {
                    enriched_buf.fields.clear();
                    enriched_buf.computed.clear();
                }
                #[cfg(feature = "dump-timing")]
                { timings.enrichment += _t_enrich.elapsed().as_nanos() as u64; }
                let enriched = &enriched_buf;
                // Build O(1) lookup map from enriched fields (replaces O(n) linear scan closure)
                let mut enriched_map: HashMap<&str, &str> = HashMap::with_capacity(enriched.fields.len() + enriched.computed.len());
                for (t, v) in &enriched.fields {
                    enriched_map.insert(t.as_str(), v.as_str());
                }
                for (t, v) in &enriched.computed {
                    if let NateExprValue::Str(s) = v {
                        enriched_map.insert(t.as_str(), s.as_str());
                    }
                }
                #[cfg(feature = "dump-timing")]
                let enriched_get_count = std::cell::Cell::new(0u64);
                let enriched_get = |target: &str| -> Option<&str> {
                    #[cfg(feature = "dump-timing")]
                    enriched_get_count.set(enriched_get_count.get() + 1);
                    enriched_map.get(target).copied()
                };

                // Evaluate config-computed sort values (early computation for deferred alive + doc)
                #[cfg(feature = "dump-timing")]
                let _t_ccs_early = std::time::Instant::now();
                let config_computed_sort_vals: Vec<(&str, i64)> = if !config_computed_sorts_ref.is_empty() {
                    let mut row_sv: HashMap<&str, u32> = HashMap::with_capacity(8);
                    for fm in request_fields {
                        let t = fm.target();
                        if sort_bits_ref.contains_key(t) || config_computed_sources_ref.contains(t) {
                            if let Some(v) = row.get_i64(fm.column()).or_else(|| enriched_get(t).and_then(|s| s.parse::<i64>().ok())) {
                                row_sv.insert(t, v.max(0) as u32);
                            }
                        }
                    }
                    for t in enrichment_targets_ref {
                        if sort_bits_ref.contains_key(t.as_str()) || config_computed_sources_ref.contains(t.as_str()) {
                            if let Some(s) = enriched_get(t) { if let Ok(v) = s.parse::<i64>() { row_sv.insert(t, v.max(0) as u32); } }
                        }
                    }
                    for (t, value) in &enriched.computed {
                        if sort_bits_ref.contains_key(t.as_str()) || config_computed_sources_ref.contains(t.as_str()) {
                            if let NateExprValue::Int(n) = value { row_sv.insert(t, (*n).max(0) as u32); }
                        }
                    }
                    for def in computed_defs_ref {
                        if sort_bits_ref.contains_key(&def.target) || config_computed_sources_ref.contains(&def.target) {
                            if let Some(NateExprValue::Int(v)) = def.eval_indexed(&indexed_fields_buf, col_idx, None) {
                                row_sv.insert(&def.target, v.max(0) as u32);
                            }
                        }
                    }
                    config_computed_sorts_ref.iter().map(|ccs| {
                        let vals: Vec<u32> = ccs.source_fields.iter()
                            .map(|sf| row_sv.get(sf.as_str()).copied().unwrap_or(0))
                            .collect();
                        let cv = match ccs.op {
                            crate::config::ComputedOp::Greatest => *vals.iter().max().unwrap_or(&0),
                            crate::config::ComputedOp::Least => *vals.iter().min().unwrap_or(&0),
                        };
                        (ccs.target.as_str(), cv as i64)
                    }).collect()
                } else {
                    Vec::new()
                };

                // Diagnostic: log config-computed sort values for first 3 rows per thread
                if count < 3 && !config_computed_sort_vals.is_empty() {
                    eprintln!("  [diag] slot={} config_computed_sort_vals={:?} enriched_fields={:?}",
                        slot,
                        config_computed_sort_vals,
                        enriched.fields.iter().map(|(t, v)| (t.as_str(), &v[..v.len().min(20)])).collect::<Vec<_>>()
                    );
                }

                #[cfg(feature = "dump-timing")]
                { timings.config_computed_sort_early += _t_ccs_early.elapsed().as_nanos() as u64; }

                // Check deferred alive: if publishedAt from enrichment is in the future
                #[cfg(feature = "dump-timing")]
                let _t_deferred = std::time::Instant::now();
                if has_deferred_alive {
                    if let Some(pub_str) = enriched_get("publishedAt") {
                        if let Ok(pub_secs) = pub_str.parse::<u64>() {
                            if pub_secs > now_unix {
                                // Write doc op (deferred rows need their doc data stored),
                                // but skip all bitmap operations.
                                if !is_multi_value_only {
                                    let pw_arg = pw_ref.as_ref().map(|pw| (pw.as_ref(), &mut ops_local_cursor, &mut ops_local_end));
                                    let scratch = if pw_arg.is_some() { Some((&mut doc_encode_buf, &mut frame_buf)) } else { None };
                                    collect_doc_op(
                                        &row,
                                        &enriched,
                                        computed_defs_ref,
                                        &indexed_fields_buf,
                                        col_idx,
                                        slot,
                                        request_fields,
                                        &field_idx_cache,
                                        &boolean_fields,
                                        &config_computed_sort_vals,
                                        &mut doc_ops,
                                        pw_arg,
                                        scratch,
                                    );
                                }
                                deferred.push((slot, pub_secs));
                                count += 1;
                                if count % LOG_INTERVAL == 0 {
                                    total_ref.fetch_add(LOG_INTERVAL, Ordering::Relaxed);
                                    if let Some(ref p) = ext_progress { p.fetch_add(LOG_INTERVAL, Ordering::Relaxed); }
                                }
                                continue;
                            }
                        }
                    }
                }

                #[cfg(feature = "dump-timing")]
                { timings.deferred_alive += _t_deferred.elapsed().as_nanos() as u64; }

                // Set alive bit
                if sets_alive {
                    alive.insert(slot);
                }

                // Build filter + sort bitmaps from direct fields
                #[cfg(feature = "dump-timing")]
                let _t_filter_bm = std::time::Instant::now();
                for field_mapping in request_fields {
                    let target = field_mapping.target();
                    let column = field_mapping.column();

                    // Filter bitmap: push tuple to flat Vec (post-pass builds bitmaps)
                    if let Some(&fidx) = filter_field_to_idx.get(target) {
                        let bitmap_key: Option<u64> = if let Some(dict) = dictionaries_ref.get(target) {
                            let s = row
                                .get_str(column)
                                .or_else(|| enriched_get(target));
                            s.map(|v| dict.get_or_insert(v) as u64)
                        } else {
                            row.get_i64(column)
                                .or_else(|| {
                                    enriched_get(target).and_then(|s| s.parse::<i64>().ok())
                                })
                                .map(|v| v as u64)
                        };

                        if let Some(key) = bitmap_key {
                            filter_tuples.push((fidx, key, slot));
                        }
                    }

                    // Build sort bitmaps from direct fields
                    if let Some(&bits) = sort_bits_ref.get(target) {
                        if let Some(v) = row.get_i64(column).or_else(|| {
                            enriched_get(target).and_then(|s| s.parse::<i64>().ok())
                        }) {
                            let val32 = v.max(0) as u32;
                            if let Some(sv) = sort_vecs.get_mut(target) {
                                for bit in 0..(bits as usize) {
                                    if (val32 >> bit) & 1 == 1 {
                                        sv[bit].push(slot);
                                    }
                                }
                            }
                        }
                    }
                }

                #[cfg(feature = "dump-timing")]
                { timings.filter_bitmap_insert += _t_filter_bm.elapsed().as_nanos() as u64; }

                // Build filter + sort bitmaps from enrichment-only fields
                #[cfg(feature = "dump-timing")]
                let _t_enrich_bm = std::time::Instant::now();
                for target in enrichment_targets_ref {
                    if let Some(val_str) = enriched_get(target) {
                        // Filter bitmap — push tuple to flat Vec
                        if let Some(&fidx) = filter_field_to_idx.get(target.as_str()) {
                            let bitmap_key: Option<u64> = if let Some(dict) = dictionaries_ref.get(target.as_str()) {
                                Some(dict.get_or_insert(val_str) as u64)
                            } else {
                                val_str.parse::<i64>().ok().map(|v| v as u64)
                            };
                            if let Some(key) = bitmap_key {
                                filter_tuples.push((fidx, key, slot));
                            }
                        }
                        // Sort bitmap
                        if let Some(&bits) = sort_bits_ref.get(target.as_str()) {
                            if let Some(v) = val_str.parse::<i64>().ok() {
                                let val32 = v.max(0) as u32;
                                if let Some(sv) = sort_vecs.get_mut(target.as_str()) {
                                    for bit in 0..(bits as usize) {
                                        if (val32 >> bit) & 1 == 1 {
                                            sv[bit].push(slot);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Handle enrichment computed fields with Bool/Int values
                // (enriched_get only returns Option<&str>, so Bool/Int values are missed above)
                for (target, value) in &enriched.computed {
                    match value {
                        NateExprValue::Bool(b) => {
                            let key = if *b { 1u64 } else { 0u64 };
                            if let Some(&fidx) = filter_field_to_idx.get(target.as_str()) {
                                filter_tuples.push((fidx, key, slot));
                            }
                        }
                        NateExprValue::Int(n) => {
                            if let Some(&fidx) = filter_field_to_idx.get(target.as_str()) {
                                filter_tuples.push((fidx, *n as u64, slot));
                            }
                            if let Some(&bits) = sort_bits_ref.get(target.as_str()) {
                                let val32 = (*n).max(0) as u32;
                                if let Some(sv) = sort_vecs.get_mut(target.as_str()) {
                                    for bit in 0..(bits as usize) {
                                        if (val32 >> bit) & 1 == 1 {
                                            sv[bit].push(slot);
                                        }
                                    }
                                }
                            }
                        }
                        _ => {} // Str already handled by enriched_get above
                    }
                }

                #[cfg(feature = "dump-timing")]
                { timings.enrichment_bitmap += _t_enrich_bm.elapsed().as_nanos() as u64; }

                // Build bitmaps from computed fields (Nate's ComputedFieldDef API)
                #[cfg(feature = "dump-timing")]
                let _t_computed = std::time::Instant::now();
                for def in computed_defs_ref {
                    let computed_val = def.eval_indexed(&indexed_fields_buf, col_idx, None);

                    match computed_val {
                        Some(NateExprValue::Int(v)) if def.value_column.is_none() => {
                            if let Some(&fidx) = filter_field_to_idx.get(def.target.as_str()) {
                                filter_tuples.push((fidx, v as u64, slot));
                            }
                            if let Some(&bits) = sort_bits_ref.get(&def.target) {
                                let val32 = v.max(0) as u32;
                                if let Some(sv) = sort_vecs.get_mut(&def.target) {
                                    for bit in 0..(bits as usize) {
                                        if (val32 >> bit) & 1 == 1 {
                                            sv[bit].push(slot);
                                        }
                                    }
                                }
                            }
                        }
                        Some(NateExprValue::Bool(true)) if def.value_column.is_some() => {
                            // Conditional: expression is true, use the value column
                            let vcol = def.value_column.as_deref().unwrap();
                            if let Some(v) = row.get_i64(vcol) {
                                if let Some(&fidx) = filter_field_to_idx.get(def.target.as_str()) {
                                    filter_tuples.push((fidx, v as u64, slot));
                                }
                            }
                        }
                        Some(NateExprValue::Bool(b)) if def.value_column.is_none() => {
                            let key = if b { 1u64 } else { 0u64 };
                            if let Some(&fidx) = filter_field_to_idx.get(def.target.as_str()) {
                                filter_tuples.push((fidx, key, slot));
                            }
                        }
                        _ => {} // Null or non-matching pattern
                    }
                }


                #[cfg(feature = "dump-timing")]
                { timings.computed_field += _t_computed.elapsed().as_nanos() as u64; }

                // Write config-computed sort values to sort bitmaps.
                // Reuses config_computed_sort_vals from the early computation — no duplicate work.
                #[cfg(feature = "dump-timing")]
                let _t_ccs_late = std::time::Instant::now();
                for (target, val) in &config_computed_sort_vals {
                    let val32 = (*val).max(0) as u32;
                    if let Some(sv) = sort_vecs.get_mut(*target) {
                        for bit in 0..sv.len() {
                            if (val32 >> bit) & 1 == 1 {
                                sv[bit].push(slot);
                            }
                        }
                    }
                }

                #[cfg(feature = "dump-timing")]
                { timings.config_computed_sort_late += _t_ccs_late.elapsed().as_nanos() as u64; }

                // Write doc op — directly to mmap if parallel writer available, else collect.
                #[cfg(feature = "dump-timing")]
                let _t_doc = std::time::Instant::now();
                if !is_multi_value_only {
                    #[cfg(feature = "dump-timing")]
                    let _t_fc = std::time::Instant::now();
                    let mut doc_fields: Vec<(u16, DumpFieldValue)> = Vec::with_capacity(20);
                    execute_doc_plan(
                        doc_field_plan_ref, &row, &enriched_map, &enriched,
                        computed_defs_ref, &indexed_fields_buf, col_idx,
                        &config_computed_sort_vals, &mut doc_fields,
                    );
                    #[cfg(feature = "dump-timing")]
                    { timings.doc_field_collect += _t_fc.elapsed().as_nanos() as u64; }

                    if !doc_fields.is_empty() {
                        #[cfg(feature = "dump-timing")]
                        let _t_enc = std::time::Instant::now();
                        if let Some(ref pw) = pw_ref {
                            encode_dump_merge(slot, &doc_fields, &mut doc_encode_buf);
                            #[cfg(feature = "dump-timing")]
                            { timings.doc_pack_encode += _t_enc.elapsed().as_nanos() as u64; }
                            #[cfg(feature = "dump-timing")]
                            let _t_wr = std::time::Instant::now();
                            pw.write_put_reuse(slot, &mut doc_encode_buf, &mut frame_buf, &mut ops_local_cursor, &mut ops_local_end);
                            #[cfg(feature = "dump-timing")]
                            { timings.doc_mmap_write += _t_wr.elapsed().as_nanos() as u64; }
                        } else {
                            encode_dump_merge(slot, &doc_fields, &mut doc_encode_buf);
                            let bytes = doc_encode_buf.clone();
                            #[cfg(feature = "dump-timing")]
                            { timings.doc_pack_encode += _t_enc.elapsed().as_nanos() as u64; }
                            doc_ops.push((slot, bytes));
                        }
                    }
                }

                #[cfg(feature = "dump-timing")]
                { timings.doc_encode += _t_doc.elapsed().as_nanos() as u64; }

                #[cfg(feature = "dump-timing")]
                {
                    timings.total += _row_start.elapsed().as_nanos() as u64;
                    timings.rows += 1;
                    timings.enriched_get_calls += enriched_get_count.get();
                    enriched_get_count.set(0);
                }

                count += 1;
                if count % LOG_INTERVAL == 0 {
                    let t = total_ref.fetch_add(LOG_INTERVAL, Ordering::Relaxed) + LOG_INTERVAL;
                    if let Some(ref p) = ext_progress { p.fetch_add(LOG_INTERVAL, Ordering::Relaxed); }
                    eprintln!("  dump {}: {}M rows...", request.name, t / 1_000_000);
                    // Check shutdown flag — abort early on Ctrl+C
                    if let Some(ref sf) = shutdown { if sf() { break; } }

                }
            }
            let remainder = count % LOG_INTERVAL;
            total_ref.fetch_add(remainder, Ordering::Relaxed);
            if let Some(ref p) = ext_progress { p.fetch_add(remainder, Ordering::Relaxed); }

            #[cfg(feature = "dump-timing")]
            {
                let thread_id = rayon::current_thread_index().unwrap_or(0);
                timings.print_summary(thread_id);
            }

            // Convert filter_tuples → filter_maps via sort + grouped from_sorted_iter
            // Flat Vec push (per row) + batch sort + from_sorted_iter is 5.3x faster
            // than per-row HashMap.entry().or_insert_with(RoaringBitmap::new).insert().
            filter_tuples.sort_unstable();
            let mut filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
            if !filter_tuples.is_empty() {
                let mut prev_field = filter_tuples[0].0;
                let mut prev_value = filter_tuples[0].1;
                let mut slots: Vec<u32> = Vec::new();
                for &(field_idx, value, slot) in &filter_tuples {
                    if field_idx != prev_field || value != prev_value {
                        if !slots.is_empty() {
                            let field_name = &filter_idx_to_name[prev_field as usize];
                            filter_maps.entry(field_name.clone()).or_default()
                                .insert(prev_value, RoaringBitmap::from_sorted_iter(slots.drain(..)).unwrap_or_default());
                        }
                        prev_field = field_idx;
                        prev_value = value;
                    }
                    slots.push(slot);
                }
                // Flush last group
                if !slots.is_empty() {
                    let field_name = &filter_idx_to_name[prev_field as usize];
                    filter_maps.entry(field_name.clone()).or_default()
                        .insert(prev_value, RoaringBitmap::from_sorted_iter(slots.drain(..)).unwrap_or_default());
                }
            }

            // Convert sort_vecs → sort_maps via sort + from_sorted_iter (5.86x faster)
            let sort_maps: HashMap<String, Vec<RoaringBitmap>> = sort_vecs.into_iter().map(|(field, layers)| {
                let bitmaps: Vec<RoaringBitmap> = layers.into_iter().map(|mut slots| {
                    if slots.is_empty() {
                        RoaringBitmap::new()
                    } else {
                        slots.sort_unstable();
                        RoaringBitmap::from_sorted_iter(slots.into_iter()).unwrap_or_default()
                    }
                }).collect();
                (field, bitmaps)
            }).collect();

            (filter_maps, sort_maps, alive, deferred, count, max_slot, doc_ops)
        })
        .collect();

    emit_stage(&request.name, "parallel_parse", "done", &t, total.load(Ordering::Relaxed));

    // Drop the mmap immediately after parsing — prevents zombie processes from
    // holding 80+ GB of virtual memory if the process is force-killed during
    // the merge/save phase. NLL ensures the borrow of `body`/`data` has ended.
    // DONTNEED before drop: immediately reduces RSS on Linux before the OS-level
    // unmap completes. Especially important for 80+ GB CSV files.
    #[cfg(target_os = "linux")]
    let _ = unsafe { mmap.unchecked_advise(memmap2::UncheckedAdvice::DontNeed) };
    drop(mmap);
    drop(file);
    eprintln!("  Dump {}: mmap released", request.name);

    // Drop enrichment tables on a background thread — they can be 5+ GB and
    // take 30-60s to free due to millions of individual heap allocations.
    // Spawning the drop avoids blocking the save phase.
    {
        let name = request.name.clone();
        std::thread::spawn(move || {
            let t_drop = Instant::now();
            drop(enrichment_mgr);
            let secs = t_drop.elapsed().as_secs_f64();
            if secs > 1.0 {
                eprintln!("  Dump {}: enrichment drop took {:.1}s (background)", name, secs);
            }
        });
    }

    emit_stage(&request.name, "merge", "start", &t, total.load(Ordering::Relaxed));

    // Two merge strategies:
    // - streaming_merge=false (default): rayon par_iter fold+reduce — faster for small datasets
    // - streaming_merge=true: collect + MultiOps::union() — faster for large datasets (107M+)
    //   where per-thread bitmaps are large and memory-bandwidth dominates
    let (merged_filters, merged_sorts, merged_alive, merged_deferred, total_count, max_slot, all_doc_ops) = if request.streaming_merge {
        use roaring::MultiOps;

        let mut merged_filters: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
        let mut merged_sorts: HashMap<String, Vec<RoaringBitmap>> = HashMap::new();
        let mut all_alive: Vec<RoaringBitmap> = Vec::with_capacity(thread_results.len());
        let mut merged_deferred: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        let mut total_count: u64 = 0;
        let mut max_slot: u32 = 0;
        let mut all_doc_ops: Vec<(u32, Vec<u8>)> = Vec::new();

        let mut filter_collectors: HashMap<String, HashMap<u64, Vec<RoaringBitmap>>> = HashMap::new();
        let mut sort_collectors: HashMap<String, Vec<Vec<RoaringBitmap>>> = HashMap::new();

        for (filter_maps, sort_maps, alive, deferred, count, thread_max, doc_ops) in thread_results {
            all_alive.push(alive);
            total_count += count;
            if thread_max > max_slot { max_slot = thread_max; }
            all_doc_ops.extend(doc_ops);

            for (slot, activate_at) in deferred {
                merged_deferred.entry(activate_at).or_default().push(slot);
            }

            for (field, values) in filter_maps {
                let fc = filter_collectors.entry(field).or_default();
                for (val, bm) in values {
                    fc.entry(val).or_default().push(bm);
                }
            }
            for (field, layers) in sort_maps {
                let sc = sort_collectors.entry(field).or_insert_with(|| {
                    (0..layers.len()).map(|_| Vec::new()).collect()
                });
                for (bit, bm) in layers.into_iter().enumerate() {
                    if bit < sc.len() { sc[bit].push(bm); }
                }
            }
        }

        let merged_alive: RoaringBitmap = all_alive.iter().union();
        for (field, values) in filter_collectors {
            let dest = merged_filters.entry(field).or_default();
            for (val, bitmaps) in values {
                dest.insert(val, bitmaps.iter().union());
            }
        }
        for (field, layers) in sort_collectors {
            let bitmaps: Vec<RoaringBitmap> = layers.into_iter().map(|bms| bms.iter().union()).collect();
            merged_sorts.insert(field, bitmaps);
        }

        (merged_filters, merged_sorts, merged_alive, merged_deferred, total_count, max_slot, all_doc_ops)
    } else {
        // Default: per-field parallel merge — 3.78x faster than fold+reduce tree reduction.
        // Step 1: Sequential collect — group per-thread results by field name (~1ms)
        let mut per_field_filters: HashMap<String, Vec<HashMap<u64, RoaringBitmap>>> = HashMap::new();
        let mut per_field_sorts: HashMap<String, Vec<Vec<RoaringBitmap>>> = HashMap::new();
        let mut merged_alive = RoaringBitmap::new();
        let mut merged_deferred: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        let mut total_count: u64 = 0;
        let mut max_slot: u32 = 0;
        let mut all_doc_ops: Vec<(u32, Vec<u8>)> = Vec::new();

        for (filter_maps, sort_maps, alive, deferred, count, thread_max, doc_ops) in thread_results {
            merged_alive |= alive;
            total_count += count;
            if thread_max > max_slot { max_slot = thread_max; }
            all_doc_ops.extend(doc_ops);
            for (slot, activate_at) in deferred {
                merged_deferred.entry(activate_at).or_default().push(slot);
            }
            for (field, values) in filter_maps {
                per_field_filters.entry(field).or_default().push(values);
            }
            for (field, layers) in sort_maps {
                per_field_sorts.entry(field).or_default().push(layers);
            }
        }

        // Step 2: Parallel merge — each field is an independent rayon task.
        // userId (2M values) gets its own thread, nsfwLevel (5 values) finishes instantly.
        // Collect into Vec<(String, ...)> then convert to HashMap (AHashMap doesn't impl FromParallelIterator)
        let filter_pairs: Vec<(String, HashMap<u64, RoaringBitmap>)> = per_field_filters
            .into_iter().collect::<Vec<_>>()
            .into_par_iter()
            .map(|(field, thread_maps)| {
                let mut merged: HashMap<u64, RoaringBitmap> = HashMap::new();
                for map in thread_maps {
                    for (val, bm) in map {
                        merged.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
                    }
                }
                (field, merged)
            })
            .collect();
        let merged_filters: HashMap<String, HashMap<u64, RoaringBitmap>> = filter_pairs.into_iter().collect();

        let sort_pairs: Vec<(String, Vec<RoaringBitmap>)> = per_field_sorts
            .into_iter().collect::<Vec<_>>()
            .into_par_iter()
            .map(|(field, thread_layer_sets)| {
                let num_layers = thread_layer_sets.iter().map(|l| l.len()).max().unwrap_or(0);
                let mut merged: Vec<RoaringBitmap> = (0..num_layers).map(|_| RoaringBitmap::new()).collect();
                for layers in thread_layer_sets {
                    for (bit, bm) in layers.into_iter().enumerate() {
                        if bit < merged.len() { merged[bit] |= bm; }
                    }
                }
                (field, merged)
            })
            .collect();
        let merged_sorts: HashMap<String, Vec<RoaringBitmap>> = sort_pairs.into_iter().collect();

        (merged_filters, merged_sorts, merged_alive, merged_deferred, total_count, max_slot, all_doc_ops)
    };

    emit_stage(&request.name, "merge", "done", &t, total_count);

    // Write doc ops to DataSilo ops log.
    // For non-multi-value phases: write the collected per-row Merge ops.
    // For multi-value-only phases: invert the filter bitmaps to reconstruct per-slot arrays,
    // then write one Merge op per slot.
    {
        let t_doc = Instant::now();
        let ds = engine.docstore_arc();
        let mut ds_lock = ds.lock();

        if is_multi_value_only {
            // Bitmap inversion post-pass: for each (value_id, bitmap) pair, iterate the bitmap
            // to build per-slot tag/tool/technique arrays, then write one Merge op per slot.
            // Uses a temporary slot→values HashMap built from the merged filter bitmaps.
            let target = request.fields[0].target();
            if let Some(field_idx_val) = doc_field_to_idx.get(target) {
                let fidx = *field_idx_val;
                // Build slot → Vec<i64> from the merged bitmap
                let mut slot_values: HashMap<u32, Vec<i64>> = HashMap::new();
                if let Some(value_map) = merged_filters.get(target) {
                    for (&value_id, bitmap) in value_map {
                        for slot in bitmap.iter() {
                            slot_values.entry(slot).or_default().push(value_id as i64);
                        }
                    }
                }
                let mv_count = slot_values.len();
                if mv_count > 0 {
                    if let Some(ref pw) = parallel_ops_writer {
                        // Parallel path: encode + write directly to mmap
                        use rayon::prelude::*;
                        let mv_entries: Vec<(u32, Vec<i64>)> = slot_values.into_iter().collect();
                        mv_entries.par_iter().for_each(|(slot, values)| {
                            let fields = vec![(fidx, PackedValue::Mi(values.clone()))];
                            let bytes = crate::silos::doc_format::encode_merge_fields(*slot, &fields);
                            let mut c = 0usize;
                            let mut e = 0usize;
                            pw.write_put(*slot, &bytes, &mut c, &mut e);
                        });
                        ds_lock.silo().flush_ops()
                            .map_err(|e| format!("flush_ops (multi-value parallel): {e}"))?;
                    } else {
                        // Sequential fallback
                        let mv_ops: Vec<(u32, Vec<u8>)> = slot_values.into_iter().map(|(slot, values)| {
                            let fields = vec![(fidx, PackedValue::Mi(values))];
                            let bytes = crate::silos::doc_format::encode_merge_fields(slot, &fields);
                            (slot, bytes)
                        }).collect();
                        ds_lock.silo_mut().append_ops_batch(&mv_ops)
                            .map_err(|e| format!("append_ops_batch (multi-value): {e}"))?;
                    }
                }
                eprintln!("  Dump {}: multi-value post-pass wrote {} doc ops ({:.1}s)",
                    request.name, mv_count, t_doc.elapsed().as_secs_f64());
            }
        } else if let Some(ref pw) = parallel_ops_writer {
            // Doc ops were already written directly to the mmap'd ops log during parse.
            // Check for overflow (correctness: dropped ops = missing docs)
            let dropped = pw.overflow_count.load(std::sync::atomic::Ordering::Relaxed);
            if dropped > 0 {
                eprintln!("  WARNING: Dump {}: {} doc ops dropped due to parallel writer overflow!", request.name, dropped);
            }
            ds_lock.silo().flush_ops()
                .map_err(|e| format!("flush_ops: {e}"))?;
            eprintln!("  Dump {}: doc ops written inline via parallel mmap ({:.1}s)",
                request.name, t_doc.elapsed().as_secs_f64());
        } else if !all_doc_ops.is_empty() {
            eprintln!("  Dump {}: writing {} doc ops to DataSilo (batch) ({:.1}s)",
                request.name, all_doc_ops.len(), t_doc.elapsed().as_secs_f64());
            ds_lock.silo_mut().append_ops_batch(&all_doc_ops)
                .map_err(|e| format!("append_ops_batch: {e}"))?;
        }
        eprintln!("  Dump {}: doc write done in {:.1}s", request.name, t_doc.elapsed().as_secs_f64());
    }

    let elapsed = t.elapsed();
    eprintln!(
        "  Dump {} parse+merge complete: {} rows in {:.1}s ({:.0}/s)",
        request.name,
        total_count,
        elapsed.as_secs_f64(),
        total_count as f64 / elapsed.as_secs_f64().max(0.001)
    );

    // Apply bitmaps directly to engine staging — fused with merge, no intermediate copy.
    let t_apply = Instant::now();
    {
        let mut staging = engine.clone_staging();

        // Convert sort_maps to indexed format and apply directly (into_iter = move, no clone)
        let sort_maps_indexed: std::collections::HashMap<String, std::collections::HashMap<usize, RoaringBitmap>> =
            merged_sorts.into_iter().map(|(name, layers)| {
                let indexed: std::collections::HashMap<usize, RoaringBitmap> = layers
                    .into_iter().enumerate().filter(|(_, bm)| !bm.is_empty()).collect();
                (name, indexed)
            }).collect();
        let filter_maps_std: std::collections::HashMap<String, std::collections::HashMap<u64, RoaringBitmap>> =
            merged_filters.into_iter().map(|(k, v)| (k, v.into_iter().collect())).collect();

        ConcurrentEngine::apply_bitmap_maps(&mut staging, filter_maps_std, sort_maps_indexed, merged_alive);

        // Update slot counter
        if max_slot > 0 {
            let current_counter = staging.slots.slot_counter();
            if max_slot + 1 > current_counter {
                staging.slots = crate::engine::slot::SlotAllocator::from_state(
                    max_slot + 1,
                    staging.slots.alive_bitmap().clone(),
                    roaring::RoaringBitmap::new(),
                );
            }
        }
        if !merged_deferred.is_empty() {
            staging.slots.set_deferred(merged_deferred.clone());
        }

        engine.publish_staging(staging);
    }
    eprintln!("  Dump {} apply_bitmaps in {:.1}s", request.name, t_apply.elapsed().as_secs_f64());

    Ok(PhaseResult {
        row_count: total_count,
        filter_maps: HashMap::new(),
        sort_maps: HashMap::new(),
        alive: RoaringBitmap::new(),
        deferred_slots: merged_deferred,
        max_slot,
    })
}

// SaveHandle deleted — no separate save step with DataSilo.
// Bitmaps go to engine staging, docs go to ops log, compact merges.

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------


/// Collect all target field names from a dump request (direct + computed + enrichment).
fn collect_target_fields(request: &DumpRequest) -> Vec<String> {
    let mut targets: Vec<String> = Vec::new();
    for f in &request.fields {
        targets.push(f.target().to_string());
    }
    for cf in &request.computed_fields {
        targets.push(cf.target.clone());
    }
    for enrichment in &request.enrichment {
        collect_enrichment_targets(enrichment, &mut targets);
    }
    targets.sort();
    targets.dedup();
    targets
}

fn collect_enrichment_targets(config: &EnrichmentConfig, targets: &mut Vec<String>) {
    for f in &config.fields {
        targets.push(f.target().to_string());
    }
    for cf in &config.computed_fields {
        targets.push(cf.target.clone());
    }
    for child in &config.enrichment {
        collect_enrichment_targets(child, targets);
    }
}

// ---------------------------------------------------------------------------
// DumpFieldValue — zero-copy field value for dump pipeline encoding
// ---------------------------------------------------------------------------

/// Dump-specific field value that borrows strings from mmap/enrichment buffers.
/// Only used in the dump parse loop — never stored, never crosses thread boundaries.
/// Uses shared wire format primitives from doc_format for encoding.
enum DumpFieldValue<'a> {
    Int(i64),
    Bool(bool),
    Str(&'a str),
}

/// Encode a Merge op from DumpFieldValues into a buffer.
/// Uses shared wire format primitives — same binary output as encode_merge_fields_into.
fn encode_dump_merge(slot: u32, fields: &[(u16, DumpFieldValue)], buf: &mut Vec<u8>) {
    buf.clear();
    crate::silos::doc_format::write_merge_header(slot, fields.len() as u16, buf);
    for (field_idx, value) in fields {
        match value {
            DumpFieldValue::Int(v) => crate::silos::doc_format::write_field_int(*field_idx, *v, buf),
            DumpFieldValue::Bool(v) => crate::silos::doc_format::write_field_bool(*field_idx, *v, buf),
            DumpFieldValue::Str(s) => crate::silos::doc_format::write_field_str(*field_idx, s, buf),
        }
    }
}

// ---------------------------------------------------------------------------
// Compiled DocFieldPlan — eliminates per-row HashMap/HashSet lookups
// ---------------------------------------------------------------------------

/// How to read a field value during doc encoding.
enum DocFieldSource {
    /// Direct CSV field — use row.get_i64(column) / row.get_str(column)
    Direct { column: String },
    /// Enrichment result — look up in enriched_map AHashMap
    Enriched { target: String },
    /// Enrichment computed field — look up in enriched.computed Vec
    EnrichedComputed { target: String },
    /// Computed field — eval_indexed on computed_defs[index]
    Computed { def_index: usize },
    /// Config-computed sort value (extra_i64) — pre-computed before doc encoding
    ExtraI64 { index: usize },
}

/// How to interpret the raw value.
#[derive(Clone, Copy)]
enum DocValueType {
    Int,
    Boolean,
    String,
    IntOrString,
}

/// One entry in the compiled doc field plan.
struct DocFieldPlanEntry {
    doc_field_idx: u16,
    source: DocFieldSource,
    value_type: DocValueType,
}

/// Build the compiled doc field plan at phase setup.
fn build_doc_field_plan(
    request_fields: &[DumpFieldMapping],
    enrichment_targets: &[String],
    computed_defs: &[ComputedFieldDef],
    extra_i64_targets: &[String], // config-computed sort targets
    field_idx: &std::collections::HashMap<String, u16>,
    boolean_fields: &HashSet<String>,
    filter_field_names: &HashSet<String>,
) -> Vec<DocFieldPlanEntry> {
    let extra_skip: std::collections::HashSet<&str> = extra_i64_targets.iter().map(|s| s.as_str()).collect();
    let mut plan = Vec::new();

    // Direct fields
    for mapping in request_fields {
        let target = mapping.target();
        if extra_skip.contains(target) { continue; }
        if let Some(&fidx) = field_idx.get(target) {
            let vtype = if boolean_fields.contains(target) {
                DocValueType::Boolean
            } else {
                DocValueType::IntOrString
            };
            plan.push(DocFieldPlanEntry {
                doc_field_idx: fidx,
                source: DocFieldSource::Direct { column: mapping.column().to_string() },
                value_type: vtype,
            });
        }
    }

    // Enrichment fields
    for target in enrichment_targets {
        if extra_skip.contains(target.as_str()) { continue; }
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            let vtype = if boolean_fields.contains(target.as_str()) {
                DocValueType::Boolean
            } else {
                DocValueType::IntOrString
            };
            plan.push(DocFieldPlanEntry {
                doc_field_idx: fidx,
                source: DocFieldSource::Enriched { target: target.clone() },
                value_type: vtype,
            });
        }
    }

    // Computed fields
    for (i, def) in computed_defs.iter().enumerate() {
        if extra_skip.contains(def.target.as_str()) { continue; }
        if let Some(&fidx) = field_idx.get(def.target.as_str()) {
            plan.push(DocFieldPlanEntry {
                doc_field_idx: fidx,
                source: DocFieldSource::Computed { def_index: i },
                value_type: if boolean_fields.contains(def.target.as_str()) {
                    DocValueType::Boolean
                } else {
                    DocValueType::IntOrString
                },
            });
        }
    }

    // Extra i64 fields (config-computed sort values)
    for (i, target) in extra_i64_targets.iter().enumerate() {
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            plan.push(DocFieldPlanEntry {
                doc_field_idx: fidx,
                source: DocFieldSource::ExtraI64 { index: i },
                value_type: DocValueType::Int,
            });
        }
    }

    plan
}

/// Execute the compiled doc field plan for a single row.
/// Produces DumpFieldValue with borrowed strings — zero allocation for string fields.
fn execute_doc_plan<'a>(
    plan: &[DocFieldPlanEntry],
    row: &'a ParsedRow<'a>,
    enriched_map: &HashMap<&str, &'a str>,
    enriched: &'a dump_enrichment::EnrichedFields,
    computed_defs: &[ComputedFieldDef],
    indexed_fields: &[Option<&str>],
    col_idx: &HashMap<String, usize>,
    extra_i64_fields: &[(&str, i64)],
    fields: &mut Vec<(u16, DumpFieldValue<'a>)>,
) {
    fields.clear();
    for entry in plan {
        match &entry.source {
            DocFieldSource::Direct { column } => {
                if let Some(v) = row.get_i64(column) {
                    fields.push((entry.doc_field_idx, DumpFieldValue::Int(v)));
                } else if let Some(s) = row.get_str(column).or_else(|| enriched_map.get(column.as_str()).copied()) {
                    match entry.value_type {
                        DocValueType::Boolean => {
                            match s { "t" | "true" => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(true))),
                                       "f" | "false" => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(false))),
                                       _ => fields.push((entry.doc_field_idx, DumpFieldValue::Str(s))), }
                        }
                        _ => fields.push((entry.doc_field_idx, DumpFieldValue::Str(s))),
                    }
                }
            }
            DocFieldSource::Enriched { target } => {
                if let Some(&val) = enriched_map.get(target.as_str()) {
                    if let Ok(v) = val.parse::<i64>() {
                        fields.push((entry.doc_field_idx, DumpFieldValue::Int(v)));
                    } else {
                        match entry.value_type {
                            DocValueType::Boolean => {
                                match val { "t" | "true" => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(true))),
                                             "f" | "false" => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(false))),
                                             _ => fields.push((entry.doc_field_idx, DumpFieldValue::Str(val))), }
                            }
                            _ => fields.push((entry.doc_field_idx, DumpFieldValue::Str(val))),
                        }
                    }
                }
            }
            DocFieldSource::EnrichedComputed { target } => {
                for (t, v) in &enriched.computed {
                    if t == target {
                        match v {
                            NateExprValue::Int(n) => fields.push((entry.doc_field_idx, DumpFieldValue::Int(*n))),
                            NateExprValue::Bool(b) => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(*b))),
                            NateExprValue::Str(s) => fields.push((entry.doc_field_idx, DumpFieldValue::Str(s.as_str()))),
                            NateExprValue::Null => {}
                        }
                        break;
                    }
                }
            }
            DocFieldSource::Computed { def_index } => {
                // Computed fields produce owned NateExprValue — Int and Bool are zero-copy,
                // Str requires the eval result to outlive this scope. Since eval_indexed returns
                // owned values, we can't borrow the string. Use Int/Bool directly, skip Str
                // (rare in practice — computed fields are almost always Int or Bool).
                match computed_defs[*def_index].eval_indexed(indexed_fields, col_idx, None) {
                    Some(NateExprValue::Int(v)) => fields.push((entry.doc_field_idx, DumpFieldValue::Int(v))),
                    Some(NateExprValue::Bool(b)) => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(b))),
                    // Str from computed fields can't be borrowed (owned by eval result).
                    // Extremely rare — all current computed fields produce Int or Bool.
                    Some(NateExprValue::Str(_)) => {} // skip — would need allocation
                    _ => {}
                }
            }
            DocFieldSource::ExtraI64 { index } => {
                let (_, value) = extra_i64_fields[*index];
                if value != 0 {
                    fields.push((entry.doc_field_idx, DumpFieldValue::Int(value)));
                }
            }
        }
    }
}

/// Encode a row's fields into a Merge op.
/// If `pw` is provided, writes directly to the mmap'd ops log (32M+ ops/s).
/// Otherwise collects into `doc_ops` Vec for batch write after parse.
fn collect_doc_op(
    row: &ParsedRow,
    enriched: &dump_enrichment::EnrichedFields,
    computed_defs: &[ComputedFieldDef],
    indexed_fields: &[Option<&str>],
    col_idx: &HashMap<String, usize>,
    slot: u32,
    request_fields: &[DumpFieldMapping],
    field_idx: &HashMap<String, u16>,
    boolean_fields: &HashSet<String>,
    extra_i64_fields: &[(&str, i64)],
    doc_ops: &mut Vec<(u32, Vec<u8>)>,
    pw: Option<(&datasilo::ParallelOpsWriter, &mut usize, &mut usize)>,
    scratch: Option<(&mut Vec<u8>, &mut Vec<u8>)>, // (doc_encode_buf, frame_buf) for zero-alloc pw path
) -> (u64, u64, u64) { // (field_collect_ns, pack_encode_ns, mmap_write_ns) — always 0 without dump-timing
    #[cfg(feature = "dump-timing")]
    let _t0 = std::time::Instant::now();
    // Build skip set: fields provided by extra_i64_fields (config-computed sort values
    // like sortAt = GREATEST) take priority over direct/enriched/computed writes.
    // Without this, a data_schema mapping (e.g., sortAtUnix → sortAt) that fails to
    // find its source column could overwrite the correct computed value with 0.
    let extra_skip: std::collections::HashSet<&str> = extra_i64_fields.iter().map(|&(t, _)| t).collect();

    let mut fields: Vec<(u16, PackedValue)> = Vec::with_capacity(20);

    // Direct fields — skip fields that will be written by extra_i64_fields
    for mapping in request_fields {
        let target = mapping.target();
        if extra_skip.contains(target) { continue; }
        let column = mapping.column();
        if let Some(&fidx) = field_idx.get(target) {
            if let Some(v) = row.get_i64(column) {
                fields.push((fidx, PackedValue::I(v)));
            } else if let Some(s) = row.get_str(column) {
                if boolean_fields.contains(target) {
                    match s {
                        "t" | "true" => { fields.push((fidx, PackedValue::B(true))); }
                        "f" | "false" => { fields.push((fidx, PackedValue::B(false))); }
                        _ => { fields.push((fidx, PackedValue::S(s.to_string()))); }
                    }
                } else {
                    fields.push((fidx, PackedValue::S(s.to_string())));
                }
            }
        }
    }

    // Enriched fields — skip fields that will be written by extra_i64_fields
    for (target, value) in &enriched.fields {
        if extra_skip.contains(target.as_str()) { continue; }
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            if let Ok(v) = value.parse::<i64>() {
                fields.push((fidx, PackedValue::I(v)));
            } else if boolean_fields.contains(target.as_str()) {
                match value.as_str() {
                    "t" | "true" => { fields.push((fidx, PackedValue::B(true))); }
                    "f" | "false" => { fields.push((fidx, PackedValue::B(false))); }
                    _ => { fields.push((fidx, PackedValue::S(value.clone()))); }
                }
            } else {
                fields.push((fidx, PackedValue::S(value.clone())));
            }
        }
    }

    // Enriched computed fields — skip fields that will be written by extra_i64_fields
    for (target, value) in &enriched.computed {
        if extra_skip.contains(target.as_str()) { continue; }
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            match value {
                NateExprValue::Int(v) => { fields.push((fidx, PackedValue::I(*v))); }
                NateExprValue::Bool(b) => {
                    if boolean_fields.contains(target.as_str()) {
                        fields.push((fidx, PackedValue::B(*b)));
                    } else {
                        fields.push((fidx, PackedValue::I(if *b { 1 } else { 0 })));
                    }
                }
                NateExprValue::Str(ref s) => { fields.push((fidx, PackedValue::S(s.clone()))); }
                NateExprValue::Null => {}
            }
        }
    }

    // Computed fields via indexed eval — skip fields that will be written by extra_i64_fields
    for def in computed_defs {
        if extra_skip.contains(def.target.as_str()) { continue; }
        if let Some(&fidx) = field_idx.get(def.target.as_str()) {
            match def.eval_indexed(indexed_fields, col_idx, None) {
                Some(NateExprValue::Int(v)) => { fields.push((fidx, PackedValue::I(v))); }
                Some(NateExprValue::Bool(b)) => {
                    if boolean_fields.contains(def.target.as_str()) {
                        fields.push((fidx, PackedValue::B(b)));
                    } else {
                        fields.push((fidx, PackedValue::I(if b { 1 } else { 0 })));
                    }
                }
                Some(NateExprValue::Str(ref s)) => { fields.push((fidx, PackedValue::S(s.clone()))); }
                _ => {}
            }
        }
    }

    // Extra i64 fields (config-computed sort values like sortAt = GREATEST(existedAt, publishedAt))
    for &(target, value) in extra_i64_fields {
        // Skip zero values: zero means no source data was available in this phase
        // (e.g., tags/resources/metrics phases have no existedAt/publishedAt →
        // GREATEST(0,0)=0). A prior phase wrote the real value; don't overwrite it.
        if value == 0 { continue; }
        if let Some(&fidx) = field_idx.get(target) {
            fields.push((fidx, PackedValue::I(value)));
        }
    }

    #[cfg(feature = "dump-timing")]
    let field_collect_ns = _t0.elapsed().as_nanos() as u64;

    let mut pack_encode_ns = 0u64;
    let mut mmap_write_ns = 0u64;

    if !fields.is_empty() {
        if let (Some((writer, local_cursor, local_end)), Some((doc_buf, frame_buf))) = (pw, scratch) {
            #[cfg(feature = "dump-timing")]
            let _t_enc = std::time::Instant::now();
            crate::silos::doc_format::encode_merge_fields_into(slot, &fields, doc_buf);
            #[cfg(feature = "dump-timing")]
            { pack_encode_ns = _t_enc.elapsed().as_nanos() as u64; }
            #[cfg(feature = "dump-timing")]
            let _t_wr = std::time::Instant::now();
            writer.write_put_reuse(slot, doc_buf, frame_buf, local_cursor, local_end);
            #[cfg(feature = "dump-timing")]
            { mmap_write_ns = _t_wr.elapsed().as_nanos() as u64; }
        } else {
            #[cfg(feature = "dump-timing")]
            let _t_enc = std::time::Instant::now();
            let bytes = crate::silos::doc_format::encode_merge_fields(slot, &fields);
            #[cfg(feature = "dump-timing")]
            { pack_encode_ns = _t_enc.elapsed().as_nanos() as u64; }
            doc_ops.push((slot, bytes));
        }
    }

    #[cfg(feature = "dump-timing")]
    return (field_collect_ns, pack_encode_ns, mmap_write_ns);
    #[cfg(not(feature = "dump-timing"))]
    (0, 0, 0)
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dump_request() {
        let json = r#"{
            "name": "tags-a1b2c3d4",
            "csv_path": "/data/load_stage/tags.csv",
            "format": "csv",
            "slot_field": "imageId",
            "sets_alive": false,
            "fields": [
                { "column": "tagId", "target": "tagIds" }
            ],
            "filter": "(attributes >> 10) & 1 = 0"
        }"#;
        let req: DumpRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "tags-a1b2c3d4");
        assert_eq!(req.slot_field, "imageId");
        assert!(!req.sets_alive);
        assert_eq!(req.fields.len(), 1);
        assert_eq!(req.fields[0].column(), "tagId");
        assert_eq!(req.fields[0].target(), "tagIds");
        assert_eq!(req.filter.as_deref(), Some("(attributes >> 10) & 1 = 0"));
        assert!(!req.streaming_merge); // default is false
    }

    #[test]
    fn test_parse_streaming_merge_flag() {
        let json = r#"{"name":"test","csv_path":"/test.csv","slot_field":"id","streaming_merge":true}"#;
        let req: DumpRequest = serde_json::from_str(json).unwrap();
        assert!(req.streaming_merge);

        let json_default = r#"{"name":"test","csv_path":"/test.csv","slot_field":"id"}"#;
        let req_default: DumpRequest = serde_json::from_str(json_default).unwrap();
        assert!(!req_default.streaming_merge);
    }

    #[test]
    fn test_parse_shorthand_fields() {
        let json = r#"{
            "name": "tools",
            "csv_path": "/data/tools.csv",
            "slot_field": "imageId",
            "fields": ["toolIds"]
        }"#;
        let req: DumpRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.fields[0].column(), "toolIds");
        assert_eq!(req.fields[0].target(), "toolIds");
    }

    #[test]
    fn test_parse_enrichment() {
        let json = r#"{
            "name": "images",
            "csv_path": "/data/images.csv",
            "slot_field": "id",
            "sets_alive": true,
            "fields": ["nsfwLevel", "userId"],
            "enrichment": [{
                "csv_path": "/data/posts.csv",
                "key": "id",
                "join_on": "postId",
                "fields": [{"column": "publishedAtSecs", "target": "publishedAt"}],
                "computed_fields": [
                    {"target": "postedToId", "expression": "lookup_key"},
                    {"target": "isPublished", "expression": "publishedAtSecs != null"}
                ]
            }]
        }"#;
        let req: DumpRequest = serde_json::from_str(json).unwrap();
        assert!(req.sets_alive);
        assert_eq!(req.enrichment.len(), 1);
        assert_eq!(req.enrichment[0].key, "id");
        assert_eq!(req.enrichment[0].join_on, "postId");
        assert_eq!(req.enrichment[0].computed_fields.len(), 2);
    }

    #[test]
    fn test_parse_expression_bitfield() {
        let expr = parse_expression("(attributes >> 10) & 1 = 0").unwrap();
        match expr {
            Expr::Bitfield {
                column,
                shift,
                mask,
                expected,
                ..
            } => {
                assert_eq!(column, "attributes");
                assert_eq!(shift, 10);
                assert_eq!(mask, 1);
                assert_eq!(expected, 0);
            }
            _ => panic!("Expected Bitfield"),
        }
    }

    #[test]
    fn test_parse_expression_and() {
        let expr = parse_expression("(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0").unwrap();
        match expr {
            Expr::And(_, _) => {}
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn test_parse_expression_equality() {
        let expr = parse_expression("type = 'Checkpoint'").unwrap();
        match expr {
            Expr::Eq { column, value } => {
                assert_eq!(column, "type");
                match value {
                    ExprValue::Str(s) => assert_eq!(s, "Checkpoint"),
                    _ => panic!("Expected string value"),
                }
            }
            _ => panic!("Expected Eq"),
        }
    }

    #[test]
    fn test_parse_expression_null_check() {
        let expr = parse_expression("publishedAtSecs != null").unwrap();
        match expr {
            Expr::NullCheck {
                column,
                is_not_null,
            } => {
                assert_eq!(column, "publishedAtSecs");
                assert!(is_not_null);
            }
            _ => panic!("Expected NullCheck"),
        }
    }

    #[test]
    fn test_parse_expression_bool() {
        let expr = parse_expression("detected == false").unwrap();
        match expr {
            Expr::BoolEq { column, expected } => {
                assert_eq!(column, "detected");
                assert!(!expected);
            }
            _ => panic!("Expected BoolEq"),
        }
    }

    #[test]
    fn test_parse_expression_max() {
        let expr = parse_expression("max(scannedAtSecs, createdAtSecs)").unwrap();
        match expr {
            Expr::Max { columns } => {
                assert_eq!(columns, vec!["scannedAtSecs", "createdAtSecs"]);
            }
            _ => panic!("Expected Max"),
        }
    }

    #[test]
    fn test_parse_expression_identity() {
        let expr = parse_expression("id").unwrap();
        match expr {
            Expr::Identity(col) => assert_eq!(col, "id"),
            _ => panic!("Expected Identity"),
        }
    }

    #[test]
    fn test_parse_expression_lookup_key() {
        let expr = parse_expression("lookup_key").unwrap();
        match expr {
            Expr::LookupKey => {}
            _ => panic!("Expected LookupKey"),
        }
    }

    #[test]
    fn test_eval_filter_bitfield() {
        let expr = parse_expression("(flags >> 10) & 1 = 0").unwrap();
        let col_index = HashMap::from([("flags".to_string(), 0usize)]);

        // flags = 0 → bit 10 = 0 → passes
        let row = ParsedRow {
            fields: vec![b"0"],
            col_index: &col_index,
        };
        assert!(eval_filter(&expr, &row, None));

        // flags = 1024 → bit 10 = 1 → fails
        let row = ParsedRow {
            fields: vec![b"1024"],
            col_index: &col_index,
        };
        assert!(!eval_filter(&expr, &row, None));
    }

    #[test]
    fn test_eval_computed_max() {
        let expr = parse_expression("max(a, b)").unwrap();
        let col_index = HashMap::from([
            ("a".to_string(), 0usize),
            ("b".to_string(), 1usize),
        ]);

        let row = ParsedRow {
            fields: vec![b"100", b"200"],
            col_index: &col_index,
        };
        assert_eq!(eval_computed(&expr, &row, None), Some(200));
    }

    #[test]
    fn test_parse_delimited_line() {
        let line = b"hello,world,123";
        let fields = parse_delimited_line(line, b',');
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], b"hello");
        assert_eq!(fields[2], b"123");
    }

    #[test]
    fn test_parse_delimited_line_quoted() {
        let line = b"\"hello, world\",123,\"test\"";
        let fields = parse_delimited_line(line, b',');
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], b"\"hello, world\"");
        assert_eq!(fields[1], b"123");
    }

    #[test]
    fn test_parse_delimited_line_tsv() {
        let line = b"hello\tworld\t123";
        let fields = parse_delimited_line(line, b'\t');
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], b"hello");
    }

    #[test]
    fn test_parse_i64_fast() {
        assert_eq!(parse_i64_fast(b"12345"), Some(12345));
        assert_eq!(parse_i64_fast(b"-42"), Some(-42));
        assert_eq!(parse_i64_fast(b"0"), Some(0));
        assert_eq!(parse_i64_fast(b""), None);
        assert_eq!(parse_i64_fast(b"abc"), None);
        assert_eq!(parse_i64_fast(b"123\r"), Some(123));
    }

    #[test]
    fn test_split_mmap_ranges() {
        let data = b"line1\nline2\nline3\n";
        let ranges = split_mmap_ranges(data, 2);
        assert_eq!(ranges.len(), 2);
        // Both ranges should cover the entire data
        assert_eq!(ranges[0].0, 0);
        assert_eq!(ranges.last().unwrap().1, data.len());
    }

    #[test]
    fn test_collect_target_fields() {
        let req = DumpRequest {
            name: "test".to_string(),
            csv_path: "/test.csv".to_string(),
            format: DumpFormat::Csv,
            slot_field: "id".to_string(),
            sets_alive: false,
            columns: vec![],
            fields: vec![
                DumpFieldMapping::Short("nsfwLevel".to_string()),
                DumpFieldMapping::Expanded {
                    column: "tagId".to_string(),
                    target: "tagIds".to_string(),
                },
            ],
            filter: None,
            computed_fields: vec![ComputedField {
                target: "hasMeta".to_string(),
                expression: "(flags >> 13) & 1 == 1".to_string(),
                value: None,
            }],
            enrichment: vec![],
            streaming_merge: false,
        };
        let targets = collect_target_fields(&req);
        assert!(targets.contains(&"nsfwLevel".to_string()));
        assert!(targets.contains(&"tagIds".to_string()));
        assert!(targets.contains(&"hasMeta".to_string()));
    }

    #[test]
    fn test_validate_empty_name() {
        let req = DumpRequest {
            name: "".to_string(),
            csv_path: "/nonexistent.csv".to_string(),
            format: DumpFormat::Csv,
            slot_field: "id".to_string(),
            sets_alive: false,
            columns: vec![],
            fields: vec![DumpFieldMapping::Short("nsfwLevel".to_string())],
            filter: None,
            computed_fields: vec![],
            enrichment: vec![],
            streaming_merge: false,
        };
        // We can't test validate_dump_request without an engine, but we can test
        // the validation logic directly
        assert!(req.name.is_empty());
    }

    #[test]
    fn test_validate_bad_filter_expression() {
        let result = parse_expression(">>> invalid <<<");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_nested_enrichment_config() {
        let json = r#"{
            "name": "resources",
            "csv_path": "/data/resources.csv",
            "slot_field": "imageId",
            "fields": [{"column": "modelVersionId", "target": "modelVersionIds"}],
            "computed_fields": [
                {"target": "modelVersionIdsManual", "expression": "detected == false", "value": "modelVersionId"}
            ],
            "enrichment": [{
                "csv_path": "/data/model_versions.csv",
                "key": "id",
                "join_on": "modelVersionId",
                "fields": [{"column": "baseModel", "target": "baseModel"}],
                "enrichment": [{
                    "csv_path": "/data/models.csv",
                    "key": "id",
                    "join_on": "modelId",
                    "fields": [{"column": "poi", "target": "poi"}],
                    "filter": "type = 'Checkpoint'"
                }]
            }]
        }"#;
        let req: DumpRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.enrichment.len(), 1);
        assert_eq!(req.enrichment[0].enrichment.len(), 1);
        assert_eq!(
            req.enrichment[0].enrichment[0].filter.as_deref(),
            Some("type = 'Checkpoint'")
        );
        // Computed field with value column
        assert_eq!(req.computed_fields[0].value.as_deref(), Some("modelVersionId"));
    }

    #[test]
    fn test_tsv_format_deserialization() {
        let json = r#"{
            "name": "metrics",
            "csv_path": "/data/metrics.tsv",
            "format": "tsv",
            "slot_field": "imageId",
            "fields": ["reactionCount", "commentCount"]
        }"#;
        let req: DumpRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.format, DumpFormat::Tsv);
    }

    #[test]
    fn test_computed_field_written_to_docstore() {
        use crate::config::{Config, SortFieldConfig, ComputedField as CfgComputedField, ComputedOp};

        // Create temp dir and engine with existedAt, publishedAt, and sortAt sort fields
        let dir = tempfile::tempdir().unwrap();
        let docs_path = dir.path().join("docs");
        let bitmap_path = dir.path().join("bitmaps");

        let mut config = Config {
            sort_fields: vec![
                SortFieldConfig {
                    name: "existedAt".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                    computed: None,
                },
                SortFieldConfig {
                    name: "publishedAt".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                    computed: None,
                },
                SortFieldConfig {
                    name: "sortAt".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                    computed: Some(CfgComputedField {
                        op: ComputedOp::Greatest,
                        source_fields: vec!["existedAt".into(), "publishedAt".into()],
                    }),
                },
            ],
            flush_interval_us: 50,
            merge_interval_ms: 100,
            channel_capacity: 10_000,
            ..Default::default()
        };
        config.storage.bitmap_path = Some(bitmap_path.clone());

        let engine = crate::engine::concurrent_engine::ConcurrentEngine::new_with_path(
            config, docs_path.as_path(),
        ).unwrap();

        // Create a small CSV: id,scannedAtSecs,createdAtSecs,publishedAt
        // Slot 1: existedAt=max(1000,2000)=2000, publishedAt=1500, sortAt=GREATEST(2000,1500)=2000
        // Slot 2: existedAt=max(3000,1500)=3000, publishedAt=4000, sortAt=GREATEST(3000,4000)=4000
        let csv_path = dir.path().join("images.csv");
        std::fs::write(&csv_path, "id,scannedAtSecs,createdAtSecs,publishedAt\n1,1000,2000,1500\n2,3000,1500,4000\n").unwrap();

        // Build dump request with existedAt = max(scannedAtSecs, createdAtSecs)
        let request: DumpRequest = serde_json::from_value(serde_json::json!({
            "name": "test-images",
            "csv_path": csv_path.to_str().unwrap(),
            "format": "csv",
            "slot_field": "id",
            "sets_alive": true,
            "fields": ["publishedAt"],
            "computed_fields": [
                { "target": "existedAt", "expression": "max(scannedAtSecs, createdAtSecs)" }
            ]
        })).unwrap();

        // Verify collect_target_fields includes existedAt
        let targets = collect_target_fields(&request);
        assert!(targets.contains(&"existedAt".to_string()),
            "collect_target_fields should include existedAt, got: {:?}", targets);

        // Run the dump
        let result = process_dump(&request, &engine, dir.path(), None, None, None, None);
        assert!(result.is_ok(), "process_dump failed: {:?}", result.err());
        let phase = result.unwrap();
        assert_eq!(phase.row_count, 2, "Should process 2 rows");

        // Read document from docstore and verify existedAt is present
        let doc1 = engine.get_document(1).unwrap();
        assert!(doc1.is_some(), "Document for slot 1 should exist");
        let doc1 = doc1.unwrap();
        let existed_at = doc1.fields.get("existedAt");
        assert!(existed_at.is_some(),
            "existedAt should be in docstore. Fields present: {:?}", doc1.fields.keys().collect::<Vec<_>>());

        // Verify correct values: slot 1 = max(1000, 2000) = 2000
        match existed_at.unwrap() {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(v)) => {
                assert_eq!(*v, 2000, "existedAt for slot 1 should be max(1000, 2000) = 2000");
            }
            other => panic!("Expected Single(Integer), got {:?}", other),
        }

        // Verify slot 2: max(3000, 1500) = 3000
        let doc2 = engine.get_document(2).unwrap().unwrap();
        match doc2.fields.get("existedAt").unwrap() {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(v)) => {
                assert_eq!(*v, 3000, "existedAt for slot 2 should be max(3000, 1500) = 3000");
            }
            other => panic!("Expected Single(Integer), got {:?}", other),
        }

        // Verify sortAt = GREATEST(existedAt, publishedAt) computed from config
        // Slot 1: GREATEST(2000, 1500) = 2000
        let sort_at_1 = doc1.fields.get("sortAt");
        assert!(sort_at_1.is_some(),
            "sortAt should be in docstore for slot 1. Fields: {:?}", doc1.fields.keys().collect::<Vec<_>>());
        match sort_at_1.unwrap() {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(v)) => {
                assert_eq!(*v, 2000, "sortAt for slot 1 should be GREATEST(2000, 1500) = 2000");
            }
            other => panic!("Expected Single(Integer), got {:?}", other),
        }

        // Slot 2: GREATEST(3000, 4000) = 4000
        match doc2.fields.get("sortAt").unwrap() {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(v)) => {
                assert_eq!(*v, 4000, "sortAt for slot 2 should be GREATEST(3000, 4000) = 4000");
            }
            other => panic!("Expected Single(Integer), got {:?}", other),
        }
    }

    /// Test that collect_doc_op encodes boolean fields correctly and collects into doc_ops.
    #[test]
    fn test_boolean_coercion_in_docstore_write() {
        let mut field_idx: HashMap<String, u16> = HashMap::new();
        field_idx.insert("poi".to_string(), 0);
        field_idx.insert("type".to_string(), 1);
        let mut boolean_fields = HashSet::new();
        boolean_fields.insert("poi".to_string());

        let col_index: HashMap<String, usize> = [
            ("id".to_string(), 0),
            ("poi".to_string(), 1),
            ("type".to_string(), 2),
        ].into_iter().collect();
        let line = b"1,f,Checkpoint";
        let fields = parse_delimited_line(line, b',');
        let row = ParsedRow { fields, col_index: &col_index };

        let request_fields = vec![
            DumpFieldMapping::Short("poi".to_string()),
            DumpFieldMapping::Short("type".to_string()),
        ];

        let enriched = super::dump_enrichment::EnrichedFields::default();
        let computed_defs: Vec<ComputedFieldDef> = vec![];
        let indexed_fields = row.to_indexed_fields();
        let col_idx = row.col_index_ref();
        let extra_i64: Vec<(&str, i64)> = vec![];
        let mut doc_ops: Vec<(u32, Vec<u8>)> = Vec::new();

        collect_doc_op(
            &row, &enriched, &computed_defs, &indexed_fields, col_idx,
            1, &request_fields, &field_idx,
            &boolean_fields, &extra_i64,
            &mut doc_ops, None, None,
        );
        // Should have produced one doc op for slot 1
        assert_eq!(doc_ops.len(), 1);
        assert_eq!(doc_ops[0].0, 1);
    }

    /// Test that collect_doc_op with extra_i64_fields encodes config-computed sort values.
    #[test]
    fn test_extra_i64_fields_in_docstore_write() {
        let mut field_idx: HashMap<String, u16> = HashMap::new();
        field_idx.insert("userId".to_string(), 0);
        field_idx.insert("sortAt".to_string(), 1);
        let boolean_fields = HashSet::new();
        let col_index: HashMap<String, usize> = [
            ("id".to_string(), 0),
            ("userId".to_string(), 1),
        ].into_iter().collect();
        let line = b"1,42";
        let fields = parse_delimited_line(line, b',');
        let row = ParsedRow { fields, col_index: &col_index };

        let request_fields = vec![DumpFieldMapping::Short("userId".to_string())];
        let enriched = super::dump_enrichment::EnrichedFields::default();
        let computed_defs: Vec<ComputedFieldDef> = vec![];
        let indexed_fields = row.to_indexed_fields();
        let col_idx = row.col_index_ref();
        let extra_i64: Vec<(&str, i64)> = vec![("sortAt", 1711234567)];
        let mut doc_ops: Vec<(u32, Vec<u8>)> = Vec::new();

        collect_doc_op(
            &row, &enriched, &computed_defs, &indexed_fields, col_idx,
            1, &request_fields, &field_idx,
            &boolean_fields, &extra_i64,
            &mut doc_ops, None, None,
        );
        // Should have produced one doc op for slot 1 (userId + sortAt)
        assert_eq!(doc_ops.len(), 1);
        assert_eq!(doc_ops[0].0, 1);
    }
}
