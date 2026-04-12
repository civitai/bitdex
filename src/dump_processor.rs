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
//!   7. Save bitmaps to ShardStore, drop from memory
//!
//! Processing is sequential per phase (no cross-phase parallelism in V2).

use std::collections::BTreeMap;
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::concurrent_engine::ConcurrentEngine;
use crate::dictionary::FieldDictionary;
use crate::doc_silo::slot_to_key;
use crate::dump_enrichment;
use crate::dump_expression::{FilterExpression, ComputedFieldDef, CsvRow};
use crate::dump_expression::ExprValue as NateExprValue;

const LOG_INTERVAL: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// DocWriteTarget — dispatches doc writes to either BulkWriter or MergeWriter
// ---------------------------------------------------------------------------

/// Wraps either a DocSiloBulkWriter (phase 1, creates new entries) or a
/// DumpMergeWriter (phases 2+, in-place read-modify-write).
enum DocWriteTarget {
    /// Phase 1: create new snapshot entries in data.bin
    Bulk(Arc<crate::doc_silo::DocSiloBulkWriter>),
    /// Phases 2+: merge into existing entries via DumpMergeWriter
    Merge(Arc<datasilo::DumpMergeWriter>),
}

impl DocWriteTarget {
    /// Write a doc to the silo. `payload` is in encode_dump_merge format:
    /// `[OP_TAG_MERGE:u8][slot:u32][num_fields:u16][field_pairs...]`
    ///
    /// For Bulk: delegates to `append_merge_payload` (creates new entry)
    /// For Merge: converts to snapshot bytes and calls `merge_put` (in-place update)
    fn write_doc(&self, slot: u32, payload: &[u8]) {
        match self {
            DocWriteTarget::Bulk(w) => {
                w.append_merge_payload(slot, payload);
            }
            DocWriteTarget::Merge(w) => {
                if payload.len() < 7 { return; }
                // Convert merge payload to snapshot bytes:
                // Strip [OP_TAG(1) + slot(4)] prefix, prepend alive=1
                let fields_bytes = &payload[5..];
                let mut snap_bytes = Vec::with_capacity(1 + fields_bytes.len());
                snap_bytes.push(1u8); // alive
                snap_bytes.extend_from_slice(fields_bytes);

                let key = slot_to_key(slot);
                w.merge_put(key, &snap_bytes, |existing, new| {
                    crate::doc_silo::merge_encoded_snapshots(existing, new)
                });
            }
        }
    }

    fn finalize(&self) -> Result<(), String> {
        match self {
            DocWriteTarget::Bulk(w) => w.finalize().map_err(|e| format!("finalize: {e}")),
            DocWriteTarget::Merge(w) => {
                // DumpMergeWriter is behind Arc — we can't call flush(&mut self).
                // The mmap will flush on drop. This is fine for dump phases.
                let _ = w;
                Ok(())
            }
        }
    }
}

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
    config_computed_sort_early: u64,
    config_computed_sort_late: u64,
    filter_bitmap_insert: u64,
    sort_bitmap_insert: u64,
    enrichment_bitmap: u64,
    computed_field: u64,
    doc_encode: u64,
    doc_field_collect: u64,  // sub-timing: execute_doc_plan
    doc_pack_encode: u64,    // sub-timing: encode_dump_merge
    doc_mmap_write: u64,     // sub-timing: append_merge_payload
    deferred_alive: u64,
    total: u64,
    enriched_get_calls: u64,
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
        eprintln!("    TOP 3: {}, {}, {}", sorted[0].0, sorted[1].0, sorted[2].0);
    }
}

/// Helper macro to time a block and accumulate into RowTimings field.
/// No-op when dump-timing feature is off.
#[cfg(feature = "dump-timing")]
macro_rules! time_block {
    ($timings:expr, $field:ident, $block:expr) => {{
        let _t_start = std::time::Instant::now();
        let _result = $block;
        $timings.$field += _t_start.elapsed().as_nanos() as u64;
        _result
    }};
}

#[cfg(not(feature = "dump-timing"))]
macro_rules! time_block {
    ($timings:expr, $field:ident, $block:expr) => { $block };
}

/// Emit a structured JSON stage marker to stderr for phase monitoring.
/// Zero overhead — only called at stage transitions, not per row.
fn emit_stage(dump_name: &str, stage: &str, detail: &str, t0: &Instant, rows: u64) {
    let rss = crate::concurrent_engine::get_rss_bytes();
    let elapsed_ms = t0.elapsed().as_millis();
    eprintln!(
        r#"{{"dump":"{}","stage":"{}","detail":"{}","elapsed_ms":{},"rss_bytes":{},"rss_gb":{:.3},"rows":{}}}"#,
        dump_name, stage, detail, elapsed_ms, rss, rss as f64 / 1e9, rows
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

    /// Use streaming N-way merge (`MultiOps::union`) instead of per-field
    /// parallel reduce. Better for large datasets (107M+) where per-thread
    /// bitmaps are large and memory-bandwidth dominates. Slower for small
    /// datasets (<20M) due to collection overhead. Default: false.
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
    /// Uses lifetime 'a (mmap chunk) so the Vec can live outside the row borrow.
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
/// Zero-allocation fast path for two-column multi-value CSVs.
/// Extracts two integer columns by index without allocating a Vec of fields.
/// Returns (slot_value, value_value) as (u32, i64).
#[inline]
fn parse_two_cols_fast(line: &[u8], delimiter: u8, slot_idx: usize, value_idx: usize) -> Option<(u32, i64)> {
    let max_idx = slot_idx.max(value_idx);
    let mut col = 0;
    let mut start = 0;
    let mut slot_val: Option<i64> = None;
    let mut value_val: Option<i64> = None;

    for i in 0..line.len() {
        if line[i] == delimiter {
            if col == slot_idx {
                slot_val = parse_i64_fast(&line[start..i]);
            }
            if col == value_idx {
                value_val = parse_i64_fast(&line[start..i]);
            }
            col += 1;
            start = i + 1;
            if col > max_idx { break; }
        }
    }
    // Last field (no trailing delimiter)
    if col == slot_idx && slot_val.is_none() {
        slot_val = parse_i64_fast(&line[start..]);
    }
    if col == value_idx && value_val.is_none() {
        value_val = parse_i64_fast(&line[start..]);
    }

    Some((slot_val? as u32, value_val?))
}

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
        _docstore_root: std::path::PathBuf,
        bitmap_path: Option<std::path::PathBuf>,
        filter_field_names: Vec<String>,
    ) -> Self {
        let handle = std::thread::Builder::new()
            .name("shard-precreator".into())
            .spawn(move || {
                let _created_up_to: u32 = 0;
                let files_created: u32 = 0;
                let mut bitmap_dirs_done = false;

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
                        eprintln!("  ShardPreCreator: done — bitmap dirs created");
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

/// Drain `filter_tuples` into per-field running bitmap map, merging with OR.
/// Matches the end-of-chunk sort + grouped `from_sorted_iter` batch logic.
fn flush_filter_tuples(
    filter_tuples: &mut Vec<(u16, u64, u32)>,
    filter_idx_to_name: &[String],
    running: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
) {
    if filter_tuples.is_empty() {
        return;
    }
    filter_tuples.sort_unstable();
    filter_tuples.dedup();
    let mut prev_field = filter_tuples[0].0;
    let mut prev_value = filter_tuples[0].1;
    let mut slots: Vec<u32> = Vec::new();
    for &(field_idx, value, slot) in filter_tuples.iter() {
        if field_idx != prev_field || value != prev_value {
            if !slots.is_empty() {
                let field_name = &filter_idx_to_name[prev_field as usize];
                let value_map = running.entry(field_name.clone()).or_default();
                let bm = RoaringBitmap::from_sorted_iter(slots.drain(..)).unwrap_or_default();
                value_map
                    .entry(prev_value)
                    .and_modify(|existing| *existing |= &bm)
                    .or_insert(bm);
            }
            prev_field = field_idx;
            prev_value = value;
        }
        slots.push(slot);
    }
    if !slots.is_empty() {
        let field_name = &filter_idx_to_name[prev_field as usize];
        let value_map = running.entry(field_name.clone()).or_default();
        let bm = RoaringBitmap::from_sorted_iter(slots.drain(..)).unwrap_or_default();
        value_map
            .entry(prev_value)
            .and_modify(|existing| *existing |= &bm)
            .or_insert(bm);
    }
    filter_tuples.clear();
}

/// Drain each bit-layer Vec<u32> in `sort_vecs` into running bitmap layers, merging with OR.
fn flush_sort_vecs(
    sort_vecs: &mut HashMap<String, Vec<Vec<u32>>>,
    running: &mut HashMap<String, Vec<RoaringBitmap>>,
) {
    for (field, layers) in sort_vecs.iter_mut() {
        let running_layers = running.entry(field.clone()).or_insert_with(|| {
            (0..layers.len()).map(|_| RoaringBitmap::new()).collect()
        });
        if running_layers.len() < layers.len() {
            running_layers.resize_with(layers.len(), RoaringBitmap::new);
        }
        for (bit, slots) in layers.iter_mut().enumerate() {
            if slots.is_empty() {
                continue;
            }
            slots.sort_unstable();
            slots.dedup();
            let bm = RoaringBitmap::from_sorted_iter(slots.drain(..)).unwrap_or_default();
            running_layers[bit] |= bm;
        }
    }
}

/// Flush threshold for filter_tuples — ~140 MB per thread (10M * 14 bytes).
const FILTER_TUPLE_FLUSH_THRESHOLD: usize = 10_000_000;

pub fn process_dump(
    request: &DumpRequest,
    engine: &ConcurrentEngine,
    stage_dir: &Path,
    progress_counter: Option<Arc<AtomicU64>>,
    data_schema: Option<&crate::config::DataSchema>,
    slot_watermark: Option<Arc<AtomicU64>>,
    shutdown: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
) -> Result<PhaseResult, String> {
    // Configure rayon thread pool if specified in config (0 = rayon default)
    let rayon_threads = engine.config().rayon_threads;
    if rayon_threads > 0 {
        // build_global is idempotent — only the first call takes effect
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(rayon_threads)
            .build_global();
    }

    let t_total = Instant::now();
    let mut result = process_dump_with_progress(request, engine, stage_dir, progress_counter, data_schema, slot_watermark.as_ref(), shutdown.as_ref())?;
    eprintln!("  Dump {} process_dump_with_progress returned in {:.1}s", request.name, t_total.elapsed().as_secs_f64());
    let (alive_s, filter_s, sort_s, meta_s) = engine
        .shard_stores()
        .ok_or_else(|| "no bitmap_path configured; cannot process dump".to_string())?;
    let bitmap_path = engine.config().storage.bitmap_path.as_ref()
        .ok_or_else(|| "no bitmap_path configured".to_string())?.clone();
    let dictionaries = engine.dictionaries_arc();
    let t_save = Instant::now();
    save_phase_to_disk(&mut result, &alive_s, &filter_s, &sort_s, &meta_s, &bitmap_path, &dictionaries, &request.name, request.sets_alive)?;
    eprintln!("  Dump {} save_phase_to_disk in {:.1}s", request.name, t_save.elapsed().as_secs_f64());
    // Clear the doc cache so subsequent reads see merged fields written by this
    // phase. Without this, any slot cached during a prior phase keeps its old
    // field set (e.g., images-phase cache entries hide tagIds added by tags phase).
    engine.clear_doc_cache();
    // Mark every filter and sort field as pending lazy reload from disk so the
    // next query path picks up the bitmaps we just wrote. Without this, the
    // engine's in-memory bitmap state remains empty (or stale) and queries
    // return zero results until the field is reloaded explicitly.
    let filter_field_names: Vec<String> = engine.config()
        .filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_field_names: Vec<String> = engine.config()
        .sort_fields.iter().map(|f| f.name.clone()).collect();
    engine.mark_fields_pending_reload(&filter_field_names, &sort_field_names);
    eprintln!("  Dump {} total process_dump in {:.1}s", request.name, t_total.elapsed().as_secs_f64());
    Ok(result)
}

/// Reload fields after dump phases complete. Call ONCE after the last dump.
pub fn reload_after_dumps(engine: &ConcurrentEngine, had_alive_phase: bool) {
    let t = Instant::now();
    let filter_names: Vec<String> = engine.config()
        .filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_names: Vec<String> = engine.config()
        .sort_fields.iter().map(|f| f.name.clone()).collect();
    let t_mark = Instant::now();
    engine.mark_fields_pending_reload(&filter_names, &sort_names);
    let mark_s = t_mark.elapsed().as_secs_f64();
    // Clear the doc cache so subsequent reads see merged fields from this phase.
    // Without this, any slot that was cached during a prior phase keeps its old
    // field set (e.g., images-phase cache entries hide tagIds added by tags phase).
    engine.clear_doc_cache();
    let mut alive_s = 0.0;
    if had_alive_phase {
        let t_alive = Instant::now();
        engine.reload_alive_from_disk();
        alive_s = t_alive.elapsed().as_secs_f64();
    }
    eprintln!("  Dump reload: mark_pending={:.2}s alive_reload={:.2}s total={:.2}s", mark_s, alive_s, t.elapsed().as_secs_f64());
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
    let multi_value_fields: HashSet<String> = config.filter_fields.iter()
        .filter(|f| f.field_type == crate::filter::FilterFieldType::MultiValue)
        .map(|f| f.name.clone())
        .collect();
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
    let dictionaries: Arc<HashMap<String, FieldDictionary>> = engine.dictionaries_arc();

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

    // Prepare BulkWriter for docstore — exclude filter_only fields so that
    // field_to_idx().get(target) returns None and docstore writes are skipped.
    let mut all_target_names: Vec<String> = target_fields
        .iter()
        .filter(|t| !filter_only_fields.contains(*t))
        .cloned()
        .collect();
    // Also include config-computed sort field targets (e.g., sortAt) so the
    // BulkWriter can write their values to docstore.
    // ONLY for the sets_alive phase (images) — later phases (resources, tools,
    // techniques, metrics) lack the source fields (existedAt, publishedAt) and
    // would write sortAt=GREATEST(0,0)=0, which overwrites the correct value
    // from the images phase via DocStore V2 LIFO scan.
    if request.sets_alive {
        for sc in &config.sort_fields {
            if sc.computed.is_some() && !all_target_names.contains(&sc.name) {
                all_target_names.push(sc.name.clone());
            }
        }
    }
    // Phase 1 (sets_alive): create fresh DocSiloBulkWriter (truncates data.bin)
    // Phases 2+ (!sets_alive): use DumpMergeWriter for in-place updates
    let doc_target: DocWriteTarget = if request.sets_alive {
        DocWriteTarget::Bulk(Arc::new(
            engine
                .prepare_silo_bulk_writer(&all_target_names)
                .map_err(|e| format!("prepare_silo_bulk_writer: {e}"))?,
        ))
    } else {
        // Try to open DumpMergeWriter on the existing data.bin + index.
        // The writer borrows from the HashIndex, so we need the silo to
        // outlive the writer. prepare_dump_merge opens its own writable
        // mmap and holds a *const pointer to the silo's index — the silo
        // Arc keeps it alive for the duration of the phase.
        let silo_arc = engine.doc_silo_arc();
        let merge_result = silo_arc.read().prepare_dump_merge();
        match merge_result {
            Ok(Some(writer)) => {
                eprintln!("  Phase 2+: using DumpMergeWriter for in-place doc updates");
                DocWriteTarget::Merge(Arc::new(writer))
            }
            Ok(None) => {
                eprintln!("  WARNING: no existing data.bin/index for DumpMergeWriter — falling back to BulkWriter");
                DocWriteTarget::Bulk(Arc::new(
                    engine
                        .prepare_silo_bulk_writer(&all_target_names)
                        .map_err(|e| format!("prepare_silo_bulk_writer fallback: {e}"))?,
                ))
            }
            Err(e) => {
                eprintln!("  WARNING: DumpMergeWriter prepare failed: {e} — falling back to BulkWriter");
                DocWriteTarget::Bulk(Arc::new(
                    engine
                        .prepare_silo_bulk_writer(&all_target_names)
                        .map_err(|e| format!("prepare_silo_bulk_writer fallback: {e}"))?,
                ))
            }
        }
    };
    // Get field dictionary for doc encoding
    let field_idx_map: ahash::AHashMap<String, u16> = match &doc_target {
        DocWriteTarget::Bulk(w) => w.field_to_idx().clone(),
        DocWriteTarget::Merge(_) => {
            let silo_arc = engine.doc_silo_arc();
            let guard = silo_arc.read();
            guard.field_to_idx().iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        }
    };
    let bulk_writer = &doc_target;

    // Log docstore field dictionary for debugging computed field persistence
    {
        let field_idx = &field_idx_map;
        let computed_targets: Vec<&str> = computed_defs.iter().map(|d| d.target.as_str()).collect();
        for ct in &computed_targets {
            if !field_idx.contains_key(*ct) {
                eprintln!("  WARNING: computed field '{}' NOT in BulkWriter field_idx — will NOT be written to docstore", ct);
            }
        }
        if !computed_targets.is_empty() {
            eprintln!("  Docstore field_idx has {} fields, computed targets: {:?}", field_idx.len(), computed_targets);
        }
        // Log config-computed sort fields presence in field_idx
        for sc in &config.sort_fields {
            if sc.computed.is_some() {
                let in_idx = field_idx.contains_key(&sc.name);
                eprintln!("  [diag] config-computed sort '{}': in field_idx={}, sources={:?}",
                    sc.name, in_idx, sc.computed.as_ref().map(|c| &c.source_fields));
            }
        }
    }

    // Mmap the CSV/TSV file
    let csv_path = std::path::Path::new(&request.csv_path);
    let file = std::fs::File::open(csv_path)
        .map_err(|e| format!("open {}: {e}", csv_path.display()))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| format!("mmap {}: {e}", csv_path.display()))?;
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
    let config_computed_sources: HashSet<String> = config_computed_sorts
        .iter()
        .flat_map(|ccs| ccs.source_fields.iter().cloned())
        .collect();
    let config_computed_sources_ref = &config_computed_sources;

    // Extend filter_targets with computed filter field targets so the field→idx
    // mapping covers them too. Pre-build a compact u16 index for flat tuple Vecs.
    for def in &computed_defs {
        if filter_field_names.contains(&def.target) && !filter_targets.contains(&def.target) {
            filter_targets.push(def.target.clone());
        }
    }
    let filter_field_to_idx: HashMap<String, u16> = filter_targets
        .iter()
        .enumerate()
        .map(|(i, n)| (n.clone(), i as u16))
        .collect();
    let filter_idx_to_name: Vec<String> = filter_targets.clone();
    let filter_field_to_idx_ref = &filter_field_to_idx;
    let filter_idx_to_name_ref = &filter_idx_to_name;

    // Build compiled doc field plan — pre-resolves all HashMap lookups and
    // HashSet checks for the per-row doc encode path. Replaces the runtime
    // `write_docstore_row_indexed` String-allocating path with a flat Vec walk.
    let extra_i64_targets: Vec<String> = config_computed_sorts
        .iter()
        .map(|ccs| ccs.target.clone())
        .collect();
    let mut enrichment_computed_targets: Vec<String> = Vec::new();
    for ec in &request.enrichment {
        collect_enrichment_computed_targets(ec, &mut enrichment_computed_targets);
    }
    let doc_field_plan = build_doc_field_plan(
        request_fields,
        &enrichment_targets,
        &enrichment_computed_targets,
        &computed_defs,
        &extra_i64_targets,
        &field_idx_map,
        &boolean_fields,
        &filter_field_names,
        &multi_value_fields,
    );
    let doc_field_plan_ref = &doc_field_plan;

    // Ollie #5: Vec<RoaringBitmap> for sort bit layers instead of HashMap<usize, _>.
    // Preallocate Vec of size num_bits — eliminates per-bit hash overhead.
    type ThreadResult = (
        HashMap<String, HashMap<u64, RoaringBitmap>>,
        HashMap<String, Vec<RoaringBitmap>>,
        RoaringBitmap,
        Vec<(u32, u64)>,
        u64,
        u32,
    );

    let thread_results: Vec<ThreadResult> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &body[range_start..range_end];

            let col_idx_ref: &HashMap<String, usize> = col_index.as_ref();
            let mut enriched_buf = dump_enrichment::EnrichedFields::default();
            let mut enrichment_lookup_buf: Vec<Option<&str>> = Vec::with_capacity(16);


            // Flat Vec for filter bitmap tuples — push (field_idx, value, slot) per row.
            // Bitmaps built in post-pass via sort + from_sorted_iter (5.3x faster than per-row HashMap insert).
            // Periodically flushed into `filter_maps_running` to bound memory at production scale.
            let mut filter_tuples: Vec<(u16, u64, u32)> = Vec::with_capacity(
                FILTER_TUPLE_FLUSH_THRESHOLD + 1024
            );
            // Per-thread running filter/sort bitmap maps. Incrementally merged from filter_tuples/sort_vecs
            // whenever the flat accumulators exceed their flush threshold, keeping peak memory bounded.
            let mut filter_maps_running: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
            let mut sort_maps_running: HashMap<String, Vec<RoaringBitmap>> = HashMap::new();
            // Collect sort slots into Vec<u32> per bit-layer (not RoaringBitmap).
            // After the row loop, sort + from_sorted_iter builds bitmaps faster.
            let mut sort_vecs: HashMap<String, Vec<Vec<u32>>> = sort_targets
                .iter()
                .chain(computed_sort_targets.iter())
                .map(|(n, b)| {
                    let layers: Vec<Vec<u32>> = (0..*b as usize).map(|_| Vec::with_capacity(4096)).collect();
                    (n.clone(), layers)
                })
                .collect();
            for ccs in config_computed_sorts_ref {
                sort_vecs.entry(ccs.target.clone()).or_insert_with(|| {
                    (0..ccs.bits as usize).map(|_| Vec::with_capacity(4096)).collect()
                });
            }
            let mut alive = RoaringBitmap::new();
            let mut deferred: Vec<(u32, u64)> = Vec::with_capacity(1024);
            // Doc encode scratch buffers — reused across rows.
            let mut doc_encode_buf: Vec<u8> = Vec::with_capacity(512);
            // Per-slot MultiInt accumulator (tags/tools/techniques).
            // Consecutive rows sharing a slot accumulate values; flushed as a single
            // Merge(Mi([...])) when slot changes. 4.5B ops -> ~109M ops for tags.
            let has_multi_int = doc_field_plan_ref
                .iter()
                .any(|e| matches!(e.value_type, DocValueType::MultiInt));
            let mi_field_idx: u16 = doc_field_plan_ref
                .iter()
                .find(|e| matches!(e.value_type, DocValueType::MultiInt))
                .map(|e| e.doc_field_idx)
                .unwrap_or(0);
            let mut mi_prev_slot: Option<u32> = None;
            let mut mi_accum: Vec<i64> = if has_multi_int { Vec::with_capacity(64) } else { Vec::new() };
            let mut count = 0u64;
            let mut max_slot: u32 = 0;
            let mut line_start = 0;
            let mut indexed_fields_buf: Vec<Option<&str>> = Vec::new();
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
                let slot = match row.slot(slot_field) {
                    Some(s) => s,
                    None => continue,
                };
                if slot > max_slot {
                    max_slot = slot;
                    // Update watermark for progressive shard pre-creation
                    if let Some(ref wm) = slot_watermark {
                        wm.fetch_max(slot as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                }

                // Build indexed fields — reuse buffer across rows (no allocation per row)
                row.fill_indexed_fields(&mut indexed_fields_buf);
                let col_idx = row.col_index_ref();

                // Apply filter via indexed path (zero-allocation)
                if let Some(ref fexpr) = filter_expr_ref {
                    if !fexpr.eval_indexed(&indexed_fields_buf, col_idx, None) {
                        continue;
                    }
                }

                #[cfg(feature = "dump-timing")]
                let _t_enrich = std::time::Instant::now();
                // Resolve enrichment via indexed path with buffer reuse
                if enrichment_mgr_ref.table_count() > 0 {
                    enrichment_mgr_ref.enrich_row_indexed_into(&indexed_fields_buf, col_idx, &mut enriched_buf, &mut enrichment_lookup_buf);
                } else {
                    enriched_buf.fields.clear();
                    enriched_buf.computed.clear();
                }
                let enriched = &enriched_buf;
                // Build a flat HashMap for O(1) enriched field lookups (replaces O(n) linear scan)
                let mut enriched_map: HashMap<&str, &str> = HashMap::with_capacity(
                    enriched.fields.len() + enriched.computed.len()
                );
                for (t, v) in &enriched.fields {
                    enriched_map.insert(t.as_str(), v.as_str());
                }
                for (t, v) in &enriched.computed {
                    if let NateExprValue::Str(s) = v {
                        enriched_map.insert(t.as_str(), s.as_str());
                    }
                    // Int values handled separately in sort/filter paths
                }
                let enriched_get = |target: &str| -> Option<&str> {
                    enriched_map.get(target).copied()
                };
                #[cfg(feature = "dump-timing")]
                { timings.enrichment += _t_enrich.elapsed().as_nanos() as u64; }

                // Evaluate config-computed sort values (e.g., sortAt = GREATEST(existedAt, publishedAt)).
                // Computed early so both the deferred alive path and normal path can include them
                // in the docstore write. Without this, deferred rows get sortAt:0 in docstore.
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

                // Check deferred alive: if publishedAt from enrichment is in the future
                if has_deferred_alive {
                    if let Some(pub_str) = enriched_get("publishedAt") {
                        if let Ok(pub_secs) = pub_str.parse::<u64>() {
                            if pub_secs > now_unix {
                                // Write docstore only, skip all bitmaps.
                                // Deferred rows never contain MultiInt fields (the deferred
                                // path is images-phase only), so we encode directly with no
                                // per-slot accumulation.
                                let mut doc_fields: Vec<(u16, DumpFieldValue)> = Vec::with_capacity(20);
                                execute_doc_plan(
                                    doc_field_plan_ref,
                                    &row,
                                    &enriched_map,
                                    &enriched,
                                    computed_defs_ref,
                                    &indexed_fields_buf,
                                    col_idx,
                                    &config_computed_sort_vals,
                                    &mut doc_fields,
                                );
                                if !doc_fields.is_empty() {
                                    encode_dump_merge(slot, &doc_fields, &mut doc_encode_buf);
                                    doc_target.write_doc(slot, &doc_encode_buf);
                                }
                                deferred.push((slot, pub_secs));
                                #[cfg(feature = "dump-timing")]
                                {
                                    timings.deferred_alive += _row_start.elapsed().as_nanos() as u64;
                                    timings.total += _row_start.elapsed().as_nanos() as u64;
                                    timings.rows += 1;
                                }
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

                // Set alive bit
                if sets_alive {
                    alive.insert(slot);
                }

                // Build filter + sort bitmaps from direct fields
                for field_mapping in request_fields {
                    let target = field_mapping.target();
                    let column = field_mapping.column();

                    // Filter bitmap: push tuple if field is in filter index
                    if let Some(&fidx) = filter_field_to_idx_ref.get(target) {
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

                // Build filter + sort bitmaps from enrichment-only fields
                // (fields that appear in enrichment targets but not in request.fields)
                for target in enrichment_targets_ref {
                    if let Some(val_str) = enriched_get(target) {
                        // Filter bitmap
                        if let Some(&fidx) = filter_field_to_idx_ref.get(target.as_str()) {
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
                            if let Some(&fidx) = filter_field_to_idx_ref.get(target.as_str()) {
                                filter_tuples.push((fidx, key, slot));
                            }
                        }
                        NateExprValue::Int(n) => {
                            if let Some(&fidx) = filter_field_to_idx_ref.get(target.as_str()) {
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

                // Build bitmaps from computed fields (Nate's ComputedFieldDef API)
                for def in computed_defs_ref {
                    let computed_val = def.eval_indexed(&indexed_fields_buf, col_idx, None);

                    match computed_val {
                        Some(NateExprValue::Int(v)) if def.value_column.is_none() => {
                            // Regular computed field — use value directly as bitmap key
                            if let Some(&fidx) = filter_field_to_idx_ref.get(def.target.as_str()) {
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
                                if filter_field_names_ref.contains(&def.target) {
                                    if let Some(&fidx) = filter_field_to_idx_ref.get(def.target.as_str()) {
                                        filter_tuples.push((fidx, v as u64, slot));
                                    }
                                }
                            }
                        }
                        Some(NateExprValue::Bool(b)) if def.value_column.is_none() => {
                            // Boolean computed field (e.g. hasMeta, isPublished)
                            let key = if b { 1u64 } else { 0u64 };
                            if let Some(&fidx) = filter_field_to_idx_ref.get(def.target.as_str()) {
                                filter_tuples.push((fidx, key, slot));
                            }
                        }
                        _ => {} // Null or non-matching pattern
                    }
                }


                // Evaluate config-driven computed sort fields (e.g., sortAt = GREATEST(existedAt, publishedAt)).
                // These use the per-row sort values already set above.
                if !config_computed_sorts_ref.is_empty() {
                    // Collect per-row sort values from direct fields, enrichment, and dump computed fields.
                    // We need the u32 values that were just set in sort_maps.
                    let mut row_sort_vals: HashMap<&str, u32> = HashMap::with_capacity(8);

                    // Direct fields (sort fields + computed sort sources)
                    for field_mapping in request_fields {
                        let target = field_mapping.target();
                        let column = field_mapping.column();
                        if sort_bits_ref.contains_key(target) || config_computed_sources_ref.contains(target) {
                            if let Some(v) = row.get_i64(column).or_else(|| {
                                enriched_get(target).and_then(|s| s.parse::<i64>().ok())
                            }) {
                                row_sort_vals.insert(target, v.max(0) as u32);
                            }
                        }
                    }
                    // Enrichment-only sort fields + computed sort sources
                    for target in enrichment_targets_ref {
                        if sort_bits_ref.contains_key(target.as_str()) || config_computed_sources_ref.contains(target.as_str()) {
                            if let Some(val_str) = enriched_get(target) {
                                if let Ok(v) = val_str.parse::<i64>() {
                                    row_sort_vals.insert(target.as_str(), v.max(0) as u32);
                                }
                            }
                        }
                    }
                    // Enrichment computed Int fields + computed sort sources
                    for (target, value) in &enriched.computed {
                        if sort_bits_ref.contains_key(target.as_str()) || config_computed_sources_ref.contains(target.as_str()) {
                            if let NateExprValue::Int(n) = value {
                                row_sort_vals.insert(target.as_str(), (*n).max(0) as u32);
                            }
                        }
                    }
                    // Dump computed fields + computed sort sources
                    for def in computed_defs_ref {
                        if sort_bits_ref.contains_key(&def.target) || config_computed_sources_ref.contains(&def.target) {
                            if let Some(NateExprValue::Int(v)) = def.eval_indexed(&indexed_fields_buf, col_idx, None) {
                                row_sort_vals.insert(&def.target, v.max(0) as u32);
                            }
                        }
                    }

                    // Now evaluate each config-computed sort field
                    for ccs in config_computed_sorts_ref {
                        let values: Vec<u32> = ccs.source_fields.iter()
                            .map(|sf| row_sort_vals.get(sf.as_str()).copied().unwrap_or(0))
                            .collect();
                        let computed_val = match ccs.op {
                            crate::config::ComputedOp::Greatest => *values.iter().max().unwrap_or(&0),
                            crate::config::ComputedOp::Least => *values.iter().min().unwrap_or(&0),
                        };
                        if let Some(sv) = sort_vecs.get_mut(&ccs.target) {
                            for bit in 0..(ccs.bits as usize) {
                                if (computed_val >> bit) & 1 == 1 {
                                    sv[bit].push(slot);
                                }
                            }
                        }
                    }
                }

                // Write docstore via compiled plan (zero-copy, borrowed strings).
                //
                // MultiInt path: accumulate values per-slot and flush one Merge
                // op per slot when the slot changes. Collapses ~4.5B tagIds rows
                // into ~109M per-slot Merge ops.
                //
                // Standard path: one Merge op per row.
                #[cfg(feature = "dump-timing")]
                let _t_doc = std::time::Instant::now();
                if has_multi_int {
                    // Flush previous slot if it changed.
                    if let Some(prev) = mi_prev_slot {
                        if prev != slot && !mi_accum.is_empty() {
                            let fields: Vec<(u16, DumpFieldValue)> = vec![(
                                mi_field_idx,
                                DumpFieldValue::MultiInt(std::mem::take(&mut mi_accum)),
                            )];
                            encode_dump_merge(prev, &fields, &mut doc_encode_buf);
                            doc_target.write_doc(prev, &doc_encode_buf);
                        }
                    }
                    mi_prev_slot = Some(slot);
                    // Collect this row's doc fields — extract MultiInt values into accum.
                    let mut doc_fields: Vec<(u16, DumpFieldValue)> = Vec::with_capacity(4);
                    execute_doc_plan(
                        doc_field_plan_ref,
                        &row,
                        &enriched_map,
                        &enriched,
                        computed_defs_ref,
                        &indexed_fields_buf,
                        col_idx,
                        &config_computed_sort_vals,
                        &mut doc_fields,
                    );
                    for (_, val) in doc_fields.drain(..) {
                        if let DumpFieldValue::MultiInt(vals) = val {
                            mi_accum.extend(vals);
                        }
                        // Non-MultiInt fields in a MultiInt phase are dropped
                        // (in practice MV phases only carry the MV field).
                    }
                } else {
                    let mut doc_fields: Vec<(u16, DumpFieldValue)> = Vec::with_capacity(20);
                    #[cfg(feature = "dump-timing")]
                    let _t_collect = std::time::Instant::now();
                    execute_doc_plan(
                        doc_field_plan_ref,
                        &row,
                        &enriched_map,
                        &enriched,
                        computed_defs_ref,
                        &indexed_fields_buf,
                        col_idx,
                        &config_computed_sort_vals,
                        &mut doc_fields,
                    );
                    #[cfg(feature = "dump-timing")]
                    { timings.doc_field_collect += _t_collect.elapsed().as_nanos() as u64; }
                    if !doc_fields.is_empty() {
                        #[cfg(feature = "dump-timing")]
                        let _t_pack = std::time::Instant::now();
                        encode_dump_merge(slot, &doc_fields, &mut doc_encode_buf);
                        #[cfg(feature = "dump-timing")]
                        { timings.doc_pack_encode += _t_pack.elapsed().as_nanos() as u64; }
                        #[cfg(feature = "dump-timing")]
                        let _t_write = std::time::Instant::now();
                        doc_target.write_doc(slot, &doc_encode_buf);
                        #[cfg(feature = "dump-timing")]
                        { timings.doc_mmap_write += _t_write.elapsed().as_nanos() as u64; }
                    }
                }
                #[cfg(feature = "dump-timing")]
                { timings.doc_encode += _t_doc.elapsed().as_nanos() as u64; }

                #[cfg(feature = "dump-timing")]
                {
                    timings.total += _row_start.elapsed().as_nanos() as u64;
                    timings.rows += 1;
                }

                count += 1;
                if count % LOG_INTERVAL == 0 {
                    let t = total_ref.fetch_add(LOG_INTERVAL, Ordering::Relaxed) + LOG_INTERVAL;
                    if let Some(ref p) = ext_progress { p.fetch_add(LOG_INTERVAL, Ordering::Relaxed); }
                    eprintln!("  dump {}: {}M rows...", request.name, t / 1_000_000);
                    // Check shutdown flag — abort early on Ctrl+C
                    if let Some(ref sf) = shutdown { if sf() { break; } }

                }
                // Bound per-thread ephemeral memory: flush flat accumulators into
                // per-thread running bitmap maps once filter_tuples grows large.
                if filter_tuples.len() >= FILTER_TUPLE_FLUSH_THRESHOLD {
                    flush_filter_tuples(
                        &mut filter_tuples,
                        filter_idx_to_name_ref,
                        &mut filter_maps_running,
                    );
                    flush_sort_vecs(&mut sort_vecs, &mut sort_maps_running);
                }
            }
            let remainder = count % LOG_INTERVAL;
            total_ref.fetch_add(remainder, Ordering::Relaxed);
            if let Some(ref p) = ext_progress { p.fetch_add(remainder, Ordering::Relaxed); }

            // Flush final accumulated MultiInt batch for the last slot in this chunk.
            if has_multi_int && !mi_accum.is_empty() {
                if let Some(prev) = mi_prev_slot {
                    let fields: Vec<(u16, DumpFieldValue)> = vec![(
                        mi_field_idx,
                        DumpFieldValue::MultiInt(std::mem::take(&mut mi_accum)),
                    )];
                    encode_dump_merge(prev, &fields, &mut doc_encode_buf);
                    doc_target.write_doc(prev, &doc_encode_buf);
                }
            }

            #[cfg(feature = "dump-timing")]
            timings.print_summary(rayon::current_thread_index().unwrap_or(0));

            // Final drain of flat accumulators into per-thread running maps.
            // Incremental flushes during the row loop keep peak memory bounded —
            // this last flush just merges whatever's left over.
            flush_filter_tuples(
                &mut filter_tuples,
                filter_idx_to_name_ref,
                &mut filter_maps_running,
            );
            flush_sort_vecs(&mut sort_vecs, &mut sort_maps_running);
            drop(filter_tuples);
            drop(sort_vecs);

            let filter_maps = filter_maps_running;
            let sort_maps = sort_maps_running;

            (filter_maps, sort_maps, alive, deferred, count, max_slot)
        })
        .collect();

    emit_stage(&request.name, "parallel_parse", "done", &t, total.load(Ordering::Relaxed));

    // Drop the mmap immediately after parsing — prevents zombie processes from
    // holding 80+ GB of virtual memory if the process is force-killed during
    // the merge/save phase. NLL ensures the borrow of `body`/`data` has ended.
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
    // - streaming_merge=false (default): per-field parallel reduce — 3.78x faster than
    //   fold+reduce tree reduction. Best for typical workloads.
    // - streaming_merge=true: collect + MultiOps::union() — faster for very large datasets
    //   (107M+) where per-thread bitmaps are large and memory-bandwidth dominates.
    let (merged_filters, merged_sorts, merged_alive, merged_deferred, total_count, max_slot) = if request.streaming_merge {
        use roaring::MultiOps;

        let mut filter_collectors: HashMap<String, HashMap<u64, Vec<RoaringBitmap>>> = HashMap::new();
        let mut sort_collectors: HashMap<String, Vec<Vec<RoaringBitmap>>> = HashMap::new();
        let mut all_alive: Vec<RoaringBitmap> = Vec::with_capacity(thread_results.len());
        let mut merged_deferred: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        let mut total_count: u64 = 0;
        let mut max_slot: u32 = 0;

        for (filter_maps, sort_maps, alive, deferred, count, thread_max) in thread_results {
            all_alive.push(alive);
            total_count += count;
            if thread_max > max_slot { max_slot = thread_max; }
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

        let mut merged_filters: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
        for (field, values) in filter_collectors {
            let dest = merged_filters.entry(field).or_default();
            for (val, bitmaps) in values {
                dest.insert(val, bitmaps.iter().union());
            }
        }

        let mut merged_sorts: HashMap<String, Vec<RoaringBitmap>> = HashMap::new();
        for (field, layers) in sort_collectors {
            let bitmaps: Vec<RoaringBitmap> = layers.into_iter()
                .map(|bms| bms.iter().union())
                .collect();
            merged_sorts.insert(field, bitmaps);
        }

        (merged_filters, merged_sorts, merged_alive, merged_deferred, total_count, max_slot)
    } else {
        // Per-field parallel merge — 3.78x faster than fold+reduce tree reduction.
        // Step 1: Sequential collect — group per-thread results by field name (~1ms).
        // Step 2: Parallel reduce — dispatch each field's merge as an independent rayon task.
        // Large fields (userId with 2M values) run in parallel with cheap fields (nsfwLevel with 5 values).
        let mut per_field_filters: HashMap<String, Vec<HashMap<u64, RoaringBitmap>>> = HashMap::new();
        let mut per_field_sorts: HashMap<String, Vec<Vec<RoaringBitmap>>> = HashMap::new();
        let mut merged_alive = RoaringBitmap::new();
        let mut merged_deferred: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        let mut total_count: u64 = 0;
        let mut max_slot: u32 = 0;

        for (filter_maps, sort_maps, alive, deferred, count, thread_max) in thread_results {
            merged_alive |= alive;
            total_count += count;
            if thread_max > max_slot { max_slot = thread_max; }
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

        // Step 2a: Parallel merge filter fields.
        // NOTE: AHashMap doesn't impl FromParallelIterator, so we collect into
        // Vec<(String, ...)> first and then sequentially into HashMap.
        let filter_pairs: Vec<(String, HashMap<u64, RoaringBitmap>)> = per_field_filters
            .into_iter()
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|(field, thread_maps)| {
                let mut merged: HashMap<u64, RoaringBitmap> = HashMap::new();
                for tm in thread_maps {
                    for (val, bm) in tm {
                        merged.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
                    }
                }
                (field, merged)
            })
            .collect();
        let merged_filters: HashMap<String, HashMap<u64, RoaringBitmap>> = filter_pairs.into_iter().collect();

        // Step 2b: Parallel merge sort fields.
        let sort_pairs: Vec<(String, Vec<RoaringBitmap>)> = per_field_sorts
            .into_iter()
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|(field, thread_layers)| {
                let num_layers = thread_layers.first().map(|l| l.len()).unwrap_or(0);
                let mut merged: Vec<RoaringBitmap> = (0..num_layers).map(|_| RoaringBitmap::new()).collect();
                for layers in thread_layers {
                    for (bit, bm) in layers.into_iter().enumerate() {
                        if bit < merged.len() {
                            merged[bit] |= bm;
                        }
                    }
                }
                (field, merged)
            })
            .collect();
        let merged_sorts: HashMap<String, Vec<RoaringBitmap>> = sort_pairs.into_iter().collect();

        (merged_filters, merged_sorts, merged_alive, merged_deferred, total_count, max_slot)
    };

    emit_stage(&request.name, "merge", "done", &t, total_count);

    // Finalize doc writer: for BulkWriter, writes layouts.bin; for MergeWriter, flushes mmap.
    if let Err(e) = doc_target.finalize() {
        eprintln!("  dump {}: doc writer finalize error: {e}", request.name);
    }

    let elapsed = t.elapsed();
    eprintln!(
        "  Dump {} parse+merge complete: {} rows in {:.1}s ({:.0}/s)",
        request.name,
        total_count,
        elapsed.as_secs_f64(),
        total_count as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok(PhaseResult {
        row_count: total_count,
        filter_maps: merged_filters,
        sort_maps: merged_sorts,
        alive: merged_alive,
        deferred_slots: merged_deferred,
        max_slot,
    })
}

// ---------------------------------------------------------------------------
// save_phase_to_disk — extracted save logic for pipeline save
// ---------------------------------------------------------------------------

/// Save a PhaseResult's bitmaps to ShardStore. Drains filter/sort HashMaps
/// incrementally as each field is written to free memory while saving.
///
/// Call this after `process_dump_with_progress` to persist bitmaps.
/// Can be run on a background thread via `SaveHandle::spawn`.
pub fn save_phase_to_disk(
    result: &mut PhaseResult,
    alive_store: &crate::shard_store_bitmap::AliveBitmapStore,
    filter_store: &crate::shard_store_bitmap::FilterBitmapStore,
    sort_store: &crate::shard_store_bitmap::SortBitmapStore,
    meta_store: &crate::shard_store_meta::MetaStore,
    bitmap_path: &Path,
    dictionaries: &HashMap<String, FieldDictionary>,
    dump_name: &str,
    sets_alive: bool,
) -> Result<(), String> {
    let t = Instant::now();
    emit_stage(dump_name, "bitmap_save", "start", &t, result.row_count);

    let save_start = Instant::now();
    let t_filter_save = Instant::now();

    // Parallel filter saves — drain into per-bucket Vecs, write buckets in parallel.
    // Same pattern as the old BitmapFs path: parallel per-bucket writes with
    // incremental drop. Each bucket drops after its shard file is written.
    let filter_items: Vec<_> = result.filter_maps.drain()
        .filter(|(_, values)| !values.is_empty())
        .collect();

    // Pre-create shard directories for all fields (avoids per-write create_dir_all)
    for (field_name, _) in &filter_items {
        let buckets: Vec<u8> = (0..=255u8).collect();
        filter_store.ensure_filter_dirs(field_name, &buckets)
            .map_err(|e| format!("ensure_filter_dirs({field_name}): {e}"))?;
    }

    // Bucket and parallel-write each field
    let filter_results: Vec<Result<(String, usize), String>> = filter_items
        .into_par_iter()
        .map(|(field_name, values)| {
            let count = values.len();
            // Drain into per-bucket owned Vecs
            let mut by_bucket: HashMap<u8, Vec<(u64, RoaringBitmap)>> = HashMap::new();
            for (value, bm) in values {
                let bucket = ((value >> 8) & 0xFF) as u8;
                by_bucket.entry(bucket).or_default().push((value, bm));
            }
            // Parallel bucket writes within each field
            let buckets: Vec<_> = by_bucket.into_iter().collect();
            buckets.into_par_iter().try_for_each(|(bucket, entries)| -> Result<(), String> {
                let refs: Vec<(u64, &RoaringBitmap)> = entries.iter()
                    .map(|(v, bm)| (*v, bm))
                    .collect();
                filter_store.write_filter_bucket_raw(&field_name, bucket, &refs)
                    .map_err(|e| format!("write_bucket({field_name}, {bucket:02x}): {e}"))?;
                drop(entries); // free this bucket's bitmaps
                Ok(())
            })?;
            Ok((field_name, count))
        })
        .collect();
    for r in filter_results {
        let (field_name, count) = r?;
        eprintln!("  Saved filter {}: {} values", field_name, count);
    }

    let filter_save_s = t_filter_save.elapsed().as_secs_f64();
    let t_sort_save = Instant::now();
    // Parallel sort field saves via ShardStore — drain for memory release
    let sort_items: Vec<_> = result.sort_maps.drain()
        .filter(|(_, layers)| !layers.is_empty() && layers.iter().any(|bm| !bm.is_empty()))
        .collect();
    // Pre-create sort field dirs
    for (field_name, _) in &sort_items {
        sort_store.ensure_sort_dir(field_name)
            .map_err(|e| format!("ensure_sort_dir({field_name}): {e}"))?;
    }
    let sort_results: Vec<Result<(String, usize), String>> = sort_items
        .par_iter()
        .map(|(field_name, layers)| {
            let layer_refs: Vec<&RoaringBitmap> = layers.iter().collect();
            sort_store.write_sort_layers(field_name, &layer_refs)
                .map_err(|e| format!("write_sort_layers({field_name}): {e}"))?;
            Ok((field_name.to_string(), layers.len()))
        })
        .collect();
    for r in sort_results {
        let (field_name, num_layers) = r?;
        eprintln!("  Saved sort {}: {} layers", field_name, num_layers);
    }

    let sort_save_s = t_sort_save.elapsed().as_secs_f64();
    let t_meta_save = Instant::now();

    if sets_alive {
        alive_store
            .write_alive(&result.alive)
            .map_err(|e| format!("write_alive: {e}"))?;
        eprintln!("  Saved alive bitmap: {} bits", result.alive.len());

        // Slot counter: max of alive + deferred slots
        let max_deferred = result.deferred_slots
            .values()
            .flat_map(|v| v.iter())
            .copied()
            .max()
            .unwrap_or(0);
        let slot_counter = result.max_slot.max(max_deferred).saturating_add(1);
        meta_store
            .write_slot_counter(slot_counter)
            .map_err(|e| format!("write_slot_counter: {e}"))?;

        if !result.deferred_slots.is_empty() {
            meta_store
                .write_deferred_alive(&result.deferred_slots)
                .map_err(|e| format!("write_deferred_alive: {e}"))?;
            let deferred_total: usize = result.deferred_slots.values().map(|v| v.len()).sum();
            eprintln!("  Saved deferred alive: {} slots", deferred_total);
        }
    }

    let meta_save_s = t_meta_save.elapsed().as_secs_f64();

    // Persist LCS dictionaries
    let dict_dir = bitmap_path.join("dictionaries");
    std::fs::create_dir_all(&dict_dir).ok();
    for (name, dict) in dictionaries {
        let snap = dict.snapshot();
        if snap.forward.is_empty() {
            continue;
        }
        let path = dict_dir.join(format!("{name}.dict"));
        if let Err(e) = crate::dictionary::save_dictionary(&snap, &path) {
            eprintln!("WARNING: failed to save dictionary for '{name}': {e}");
        } else {
            eprintln!("  Saved dictionary '{name}': {} entries", snap.forward.len());
        }
    }

    let total_save_s = save_start.elapsed().as_secs_f64();
    eprintln!("  Save breakdown: filter={:.2}s sort={:.2}s alive_meta={:.2}s total={:.2}s",
        filter_save_s, sort_save_s, meta_save_s, total_save_s);
    eprintln!(
        r#"{{"dump":"{}","stage":"save_timing","filter_s":{:.3},"sort_s":{:.3},"alive_meta_s":{:.3},"total_s":{:.3}}}"#,
        dump_name, filter_save_s, sort_save_s, meta_save_s, total_save_s,
    );
    emit_stage(dump_name, "bitmap_save", "done", &t, result.row_count);

    Ok(())
}

// ---------------------------------------------------------------------------
// SaveHandle — background thread for bitmap persistence
// ---------------------------------------------------------------------------

/// Handle to a background save thread. The caller should `join()` this
/// before any operation that depends on the save being complete (e.g.,
/// `mark_fields_pending_reload`, `reload_alive_from_disk`).
pub struct SaveHandle {
    handle: Option<std::thread::JoinHandle<Result<(), String>>>,
    unit_handle: Option<std::thread::JoinHandle<()>>,
}

impl SaveHandle {
    /// Spawn a background thread that saves a PhaseResult to ShardStore.
    /// Takes ownership of the PhaseResult so bitmaps can be dropped
    /// incrementally as each field is written.
    pub fn spawn(
        mut result: PhaseResult,
        alive_store: Arc<crate::shard_store_bitmap::AliveBitmapStore>,
        filter_store: Arc<crate::shard_store_bitmap::FilterBitmapStore>,
        sort_store: Arc<crate::shard_store_bitmap::SortBitmapStore>,
        meta_store: Arc<crate::shard_store_meta::MetaStore>,
        bitmap_path: std::path::PathBuf,
        dictionaries: Arc<HashMap<String, FieldDictionary>>,
        dump_name: String,
        sets_alive: bool,
    ) -> Self {
        let handle = std::thread::Builder::new()
            .name(format!("save-{}", dump_name))
            .spawn(move || {
                save_phase_to_disk(
                    &mut result,
                    &alive_store,
                    &filter_store,
                    &sort_store,
                    &meta_store,
                    &bitmap_path,
                    &dictionaries,
                    &dump_name,
                    sets_alive,
                )
            })
            .expect("failed to spawn save thread");
        SaveHandle {
            handle: Some(handle),
            unit_handle: None,
        }
    }

    /// Block until the save completes. Returns the save result.
    pub fn join(mut self) -> Result<(), String> {
        if let Some(h) = self.handle.take() {
            h.join().map_err(|e| format!("save thread panicked: {:?}", e))?
        } else if let Some(h) = self.unit_handle.take() {
            h.join().map_err(|e| format!("save thread panicked: {:?}", e))
        } else {
            Ok(())
        }
    }

    /// Create a no-op handle (for phases that have no save work).
    pub fn noop() -> Self {
        SaveHandle { handle: None, unit_handle: None }
    }

    /// Wrap an existing JoinHandle (e.g., a monitor thread that does save + reload).
    pub fn from_join_handle(handle: std::thread::JoinHandle<()>) -> Self {
        SaveHandle {
            handle: None,
            unit_handle: Some(handle),
        }
    }
}

// ---------------------------------------------------------------------------
// Multi-value phase (tags, tools, techniques) — now unified into the main
// row loop via DocFieldPlan + DocValueType::MultiInt + per-slot accumulation.
// The legacy process_multi_value_phase has been removed.
// ---------------------------------------------------------------------------

#[cfg(any())]
#[allow(clippy::all)]
fn process_multi_value_phase_removed_placeholder(
    request: &DumpRequest,
    body: &[u8],
    delimiter: u8,
    col_index: &Arc<HashMap<String, usize>>,
    filter_expr: &Option<FilterExpression>,
    bulk_writer: &Arc<DocSiloBulkWriter>,
    progress_counter: &Option<Arc<AtomicU64>>,
    slot_watermark: Option<&Arc<AtomicU64>>,
    shutdown: Option<&Arc<dyn Fn() -> bool + Send + Sync>>,
) -> Result<PhaseResult, String> {
    let target = request.fields[0].target().to_string();
    let value_column = request.fields[0].column().to_string();
    let slot_field = &request.slot_field;

    const MAX_TAG_ID: usize = 300_000;
    let use_vec = target == "tagIds"; // Only tagIds uses vec optimization

    let field_idx = &field_idx_map.get(&target).copied();

    let ranges = split_mmap_ranges(body, rayon::current_num_threads());
    let total = AtomicU64::new(0);
    let total_ref = &total;

    // For the vec path (tagIds): docstore writes are deferred to a post-pass after
    // bitmap merge. We invert the merged bitmaps shard-by-shard and write one Merge
    // op per slot with the complete multi-value array. This reduces 4.5B individual
    // writes to ~109M (one per slot) and fixes the correctness bug where Set overwrote
    // previous values instead of accumulating.
    //
    // For the HashMap path (tools/techniques): use the old channel-based writer since
    // these are small datasets where per-row Set ops are fine.
    let (doc_tx, doc_rx) = if !use_vec && field_idx.is_some() {
        let (tx, rx) = crossbeam_channel::bounded::<Vec<(u32, i64)>>(64);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let doc_writer_handle = doc_rx.map(|rx| {
        let bw = Arc::clone(bulk_writer);
        let fidx = field_idx.unwrap();
        std::thread::spawn(move || {
            let mut buf = Vec::with_capacity(32);
            for batch in rx {
                for (slot, value) in batch {
                    buf.clear();
                    if rmp_serde::encode::write(&mut buf, &PackedValue::Mi(vec![value])).is_ok() {
                        bw.append_tuple_raw(slot, fidx, &buf);
                    }
                }
            }
            if let Err(e) = bw.finalize() {
                eprintln!("StreamingDocWriter: multi-value finalize error: {e}");
            }
        })
    });

    // Resolve column indices upfront for zero-alloc fast path
    let value_col_idx = col_index.get(value_column.as_str()).copied();
    let slot_col_idx = col_index.get(slot_field.as_str()).copied();
    let can_fast_path = filter_expr.is_none() && value_col_idx.is_some() && slot_col_idx.is_some();
    let value_idx = value_col_idx.unwrap_or(0);
    let slot_idx = slot_col_idx.unwrap_or(1);

    let t_mv = Instant::now();
    emit_stage(&request.name, "parallel_parse", "start", &t_mv, 0);

    if use_vec {

        let bw_ref = &*bulk_writer;
        let thread_results: Vec<Vec<RoaringBitmap>> = ranges
            .par_iter()
            .map(|&(range_start, range_end)| {
                let chunk = &body[range_start..range_end];
                let mut bitmaps: Vec<RoaringBitmap> =
                    (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect();
                let mut local_max_slot: u32 = 0;
                let mut count = 0u64;
                let mut line_start = 0;

                // Accumulate tags per slot — flush when slot changes.
                // The tag CSV is grouped by imageId, so most slots are contiguous.
                // At rayon chunk boundaries a slot's tags may split across threads;
                // each thread writes its partial set as a Merge (last-write-wins on
                // the field, acceptable for bulk load — ops pipeline corrects later).
                let mut accum_slot: u32 = u32::MAX;
                let mut accum_tags: Vec<i64> = Vec::with_capacity(64);

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

                    // Fast path: zero-alloc binary parse for simple two-column CSV
                    // (no filter expression, column indices known). Avoids Vec allocation
                    // from parse_delimited_line — saves ~80s on 5.4B tag rows.
                    let (slot, value) = if can_fast_path {
                        match parse_two_cols_fast(line, delimiter, slot_idx, value_idx) {
                            Some((s, v)) => (s, v as usize),
                            None => continue,
                        }
                    } else {
                        let fields = parse_delimited_line(line, delimiter);
                        let row = ParsedRow {
                            fields,
                            col_index: col_index.as_ref(),
                        };
                        if let Some(ref fexpr) = filter_expr {
                            let csv_row = row.to_csv_row();
                            if !fexpr.eval(&csv_row, None) {
                                continue;
                            }
                        }
                        let s = match row.slot(slot_field) { Some(s) => s, None => continue };
                        let v = match row.get_i64(&value_column) { Some(v) => v as usize, None => continue };
                        (s, v)
                    };

                    if slot > local_max_slot { local_max_slot = slot; }

                    if value < MAX_TAG_ID {
                        bitmaps[value].insert(slot);
                    }

                    // Accumulate tags per slot, flush on slot change
                    if let Some(fidx) = field_idx {
                        if slot != accum_slot {
                            if accum_slot != u32::MAX && !accum_tags.is_empty() {
                                bw_ref.write_merge_doc(accum_slot, &[
                                    (fidx, PackedValue::Mi(std::mem::take(&mut accum_tags))),
                                ]);
                            }
                            accum_slot = slot;
                            accum_tags.clear();
                        }
                        accum_tags.push(value as i64);
                    }

                    count += 1;
                    if count % LOG_INTERVAL == 0 {
                        total_ref.fetch_add(LOG_INTERVAL, Ordering::Relaxed);
                        if let Some(ref p) = progress_counter { p.fetch_add(LOG_INTERVAL, Ordering::Relaxed); }
                        if let Some(ref sf) = shutdown { if sf() { break; } }
                    }
                }
                // Flush final accumulated slot
                if let Some(fidx) = field_idx {
                    if accum_slot != u32::MAX && !accum_tags.is_empty() {
                        bw_ref.write_merge_doc(accum_slot, &[
                            (fidx, PackedValue::Mi(accum_tags)),
                        ]);
                    }
                }
                let remainder = count % LOG_INTERVAL;
                total_ref.fetch_add(remainder, Ordering::Relaxed);
                if let Some(ref p) = progress_counter { p.fetch_add(remainder, Ordering::Relaxed); }
                // Flush final watermark for this thread
                if let Some(ref wm) = slot_watermark {
                    wm.fetch_max(local_max_slot as u64, std::sync::atomic::Ordering::Relaxed);
                }
                bitmaps
            })
            .collect();

        // Merge Vec<RoaringBitmap> — parallel tree reduction
        let merged_vec = thread_results
            .into_par_iter()
            .reduce(
                || (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect::<Vec<_>>(),
                |mut dst, src| {
                    for (i, bm) in src.into_iter().enumerate() {
                        if !bm.is_empty() {
                            dst[i] |= bm;
                        }
                    }
                    dst
                },
            );

        let total_rows = total.load(Ordering::Relaxed);
        let distinct_count = merged_vec.iter().filter(|bm| !bm.is_empty()).count();
        eprintln!(
            "  Dump {} ({target}): {} rows, {} distinct values",
            request.name, total_rows, distinct_count,
        );

        emit_stage(&request.name, "parallel_parse", "done", &t_mv, total_rows);

        // Convert to HashMap for return
        let mut filter_map: HashMap<u64, RoaringBitmap> = HashMap::new();
        for (i, bm) in merged_vec.into_iter().enumerate() {
            if !bm.is_empty() {
                filter_map.insert(i as u64, bm);
            }
        }
        let mut filter_maps = HashMap::new();
        filter_maps.insert(target, filter_map);

        Ok(PhaseResult {
            row_count: total_rows,
            filter_maps,
            sort_maps: HashMap::new(),
            alive: RoaringBitmap::new(),
            deferred_slots: BTreeMap::new(),
            max_slot: 0,
        })
    } else {
        // HashMap path for tools, techniques (smaller datasets)
        // Also collect per-slot value lists for docstore writes
        let thread_results: Vec<HashMap<u64, RoaringBitmap>> = ranges
            .par_iter()
            .map(|&(range_start, range_end)| {
                let chunk = &body[range_start..range_end];
                let mut bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();
                let mut doc_batch: Vec<(u32, i64)> = Vec::with_capacity(10_000);
                let mut count = 0u64;
                let mut line_start = 0;
                let mut local_max_slot: u32 = 0;

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

                    let (slot, value) = if can_fast_path {
                        match parse_two_cols_fast(line, delimiter, slot_idx, value_idx) {
                            Some((s, v)) => (s, v as u64),
                            None => continue,
                        }
                    } else {
                        let fields = parse_delimited_line(line, delimiter);
                        let row = ParsedRow {
                            fields,
                            col_index: col_index.as_ref(),
                        };
                        if let Some(ref fexpr) = filter_expr {
                            let csv_row = row.to_csv_row();
                            if !fexpr.eval(&csv_row, None) {
                                continue;
                            }
                        }
                        let s = match row.slot(slot_field) { Some(s) => s, None => continue };
                        let v = match row.get_u64(&value_column) { Some(v) => v, None => continue };
                        (s, v)
                    };

                    if slot > local_max_slot { local_max_slot = slot; }

                    bitmaps
                        .entry(value)
                        .or_insert_with(RoaringBitmap::new)
                        .insert(slot);
                    // Batch for writer thread
                    if doc_tx.is_some() {
                        doc_batch.push((slot, value as i64));
                        if doc_batch.len() >= 10_000 {
                            if let Some(ref tx) = doc_tx {
                                let _ = tx.send(std::mem::take(&mut doc_batch));
                                doc_batch = Vec::with_capacity(10_000);
                            }
                        }
                    }
                    count += 1;
                    if count % LOG_INTERVAL == 0 {
                        total_ref.fetch_add(LOG_INTERVAL, Ordering::Relaxed);
                        if let Some(ref p) = progress_counter { p.fetch_add(LOG_INTERVAL, Ordering::Relaxed); }
                        if let Some(ref sf) = shutdown { if sf() { break; } }
                    }
                }
                if !doc_batch.is_empty() {
                    if let Some(ref tx) = doc_tx {
                        let _ = tx.send(doc_batch);
                    }
                }
                let remainder = count % LOG_INTERVAL;
                total_ref.fetch_add(remainder, Ordering::Relaxed);
                if let Some(ref p) = progress_counter { p.fetch_add(remainder, Ordering::Relaxed); }
                // Flush final watermark for this thread
                if let Some(ref wm) = slot_watermark {
                    wm.fetch_max(local_max_slot as u64, std::sync::atomic::Ordering::Relaxed);
                }
                bitmaps
            })
            .collect();

        // Docstore writes already done inline per-row

        // Merge
        let mut merged: HashMap<u64, RoaringBitmap> = HashMap::new();
        for bitmaps in thread_results {
            for (val, bm) in bitmaps {
                merged.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
            }
        }

        let total_rows = total.load(Ordering::Relaxed);
        eprintln!(
            "  Dump {} ({target}): {} rows, {} distinct values",
            request.name,
            total_rows,
            merged.len(),
        );

        let mut filter_maps = HashMap::new();
        filter_maps.insert(target, merged);

        Ok(PhaseResult {
            row_count: total_rows,
            filter_maps,
            sort_maps: HashMap::new(),
            alive: RoaringBitmap::new(),
            deferred_slots: BTreeMap::new(),
            max_slot: 0,
        })
    }
}

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

/// Collect only enrichment *computed* field target names (recursive).
/// Used by `build_doc_field_plan` to emit `DocFieldSource::EnrichedComputed`
/// entries — these have Int/Bool/Str values stored on `enriched.computed`.
fn collect_enrichment_computed_targets(config: &EnrichmentConfig, targets: &mut Vec<String>) {
    for cf in &config.computed_fields {
        targets.push(cf.target.clone());
    }
    for child in &config.enrichment {
        collect_enrichment_computed_targets(child, targets);
    }
}

/// Write a single row's data to the docstore via BulkWriter (indexed path).
///
/// DEPRECATED — kept under #[cfg(test)] for the legacy regression tests at the
/// bottom of this file. The production dump path now goes through
/// `build_doc_field_plan` + `execute_doc_plan` + `encode_dump_merge` +
/// `StreamingDocWriter::append_merge_payload`, which is zero-copy for borrowed
/// strings and saves ~321M String allocations at 107M rows.
// DISABLED: tests reference deleted DocStoreV3
#[cfg(all(test, feature = "DISABLED_pending_v3_port"))]
fn write_docstore_row_indexed(
    row: &ParsedRow,
    enriched: &dump_enrichment::EnrichedFields,
    computed_defs: &[ComputedFieldDef],
    indexed_fields: &[Option<&str>],
    col_idx: &HashMap<String, usize>,
    slot: u32,
    request_fields: &[DumpFieldMapping],
    bulk_writer: &Arc<DocSiloBulkWriter>,
    field_idx: &HashMap<String, u16>,
    boolean_fields: &HashSet<String>,
    extra_i64_fields: &[(&str, i64)],
    serialize_buf: &mut Vec<u8>,
    tuple_buf: &mut Vec<(u16, u32, u32)>,
    write_buf: &mut Vec<u8>,
) {
    serialize_buf.clear();
    tuple_buf.clear();

    // Build skip set: fields provided by extra_i64_fields (config-computed sort values
    // like sortAt = GREATEST) take priority over direct/enriched/computed writes.
    // Without this, a data_schema mapping (e.g., sortAtUnix → sortAt) that fails to
    // find its source column could overwrite the correct computed value with 0.
    let extra_skip: HashSet<&str> = extra_i64_fields.iter().map(|&(t, _)| t).collect();

    // Collect all fields into serialize_buf, track (field_idx, offset, len) in tuple_buf
    macro_rules! collect_packed {
        ($fidx:expr, $value:expr) => {
            let start = serialize_buf.len() as u32;
            if rmp_serde::encode::write(serialize_buf, $value).is_ok() {
                let len = serialize_buf.len() as u32 - start;
                tuple_buf.push(($fidx, start, len));
            }
        };
    }

    // Direct fields — skip fields that will be written by extra_i64_fields
    for mapping in request_fields {
        let target = mapping.target();
        if extra_skip.contains(target) { continue; }
        let column = mapping.column();
        if let Some(&fidx) = field_idx.get(target) {
            if let Some(v) = row.get_i64(column) {
                collect_packed!(fidx, &PackedValue::I(v));
            } else if let Some(s) = row.get_str(column) {
                if boolean_fields.contains(target) {
                    match s {
                        "t" | "true" => { collect_packed!(fidx, &PackedValue::B(true)); }
                        "f" | "false" => { collect_packed!(fidx, &PackedValue::B(false)); }
                        _ => { collect_packed!(fidx, &PackedValue::S(s.to_string())); }
                    }
                } else {
                    collect_packed!(fidx, &PackedValue::S(s.to_string()));
                }
            }
        }
    }

    // Enriched fields — skip fields that will be written by extra_i64_fields
    for (target, value) in &enriched.fields {
        if extra_skip.contains(target.as_str()) { continue; }
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            if let Ok(v) = value.parse::<i64>() {
                collect_packed!(fidx, &PackedValue::I(v));
            } else if boolean_fields.contains(target.as_str()) {
                match value.as_str() {
                    "t" | "true" => { collect_packed!(fidx, &PackedValue::B(true)); }
                    "f" | "false" => { collect_packed!(fidx, &PackedValue::B(false)); }
                    _ => { collect_packed!(fidx, &PackedValue::S(value.clone())); }
                }
            } else {
                collect_packed!(fidx, &PackedValue::S(value.clone()));
            }
        }
    }

    // Enriched computed fields — skip fields that will be written by extra_i64_fields
    for (target, value) in &enriched.computed {
        if extra_skip.contains(target.as_str()) { continue; }
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            match value {
                NateExprValue::Int(v) => { collect_packed!(fidx, &PackedValue::I(*v)); }
                NateExprValue::Bool(b) => {
                    if boolean_fields.contains(target.as_str()) {
                        collect_packed!(fidx, &PackedValue::B(*b));
                    } else {
                        collect_packed!(fidx, &PackedValue::I(if *b { 1 } else { 0 }));
                    }
                }
                NateExprValue::Str(ref s) => { collect_packed!(fidx, &PackedValue::S(s.clone())); }
                NateExprValue::Null => {}
            }
        }
    }

    // Computed fields via indexed eval — skip fields that will be written by extra_i64_fields
    for def in computed_defs {
        if extra_skip.contains(def.target.as_str()) { continue; }
        if let Some(&fidx) = field_idx.get(def.target.as_str()) {
            match def.eval_indexed(indexed_fields, col_idx, None) {
                Some(NateExprValue::Int(v)) => { collect_packed!(fidx, &PackedValue::I(v)); }
                Some(NateExprValue::Bool(b)) => {
                    if boolean_fields.contains(def.target.as_str()) {
                        collect_packed!(fidx, &PackedValue::B(b));
                    } else {
                        collect_packed!(fidx, &PackedValue::I(if b { 1 } else { 0 }));
                    }
                }
                Some(NateExprValue::Str(ref s)) => { collect_packed!(fidx, &PackedValue::S(s.clone())); }
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
            collect_packed!(fidx, &PackedValue::I(value));
        }
    }

    // One lock acquisition for all fields
    if !tuple_buf.is_empty() {
        let refs: Vec<(u16, &[u8])> = tuple_buf.iter()
            .map(|&(idx, off, len)| (idx, &serialize_buf[off as usize..(off + len) as usize]))
            .collect();
        bulk_writer.append_tuples_merge(slot, &refs, write_buf);
    }
}

// ---------------------------------------------------------------------------
// DumpFieldValue — zero-copy field value for dump pipeline encoding
// ---------------------------------------------------------------------------

/// Dump-specific field value that borrows strings from mmap/enrichment buffers.
/// Only used in the dump parse loop — never stored, never crosses thread boundaries.
/// Eliminates per-row String allocation for string fields (~321M alloc savings at 107M rows).
#[allow(dead_code)]
pub(crate) enum DumpFieldValue<'a> {
    Int(i64),
    Bool(bool),
    Str(&'a str),
    MultiInt(Vec<i64>),
}

/// Encode a DocOp::Merge from DumpFieldValues into a buffer.
/// Same wire format as `DocOpCodec::encode_op` for `DocOp::Merge` but writes
/// directly from borrowed refs — no PackedValue / String allocation.
///
/// See `test_encode_dump_merge_matches_codec` for byte-for-byte equivalence
/// with `DocOpCodec::encode_op` against the canonical `PackedValue` path.
#[allow(dead_code)]
pub(crate) fn encode_dump_merge(slot: u32, fields: &[(u16, DumpFieldValue)], buf: &mut Vec<u8>) {
    debug_assert!(fields.len() <= u16::MAX as usize, "encode_dump_merge: too many fields");
    buf.clear();
    crate::doc_wire_format::write_merge_header(slot, fields.len() as u16, buf);
    for (field_idx, value) in fields {
        match value {
            DumpFieldValue::Int(v) => crate::doc_wire_format::write_field_int(*field_idx, *v, buf),
            DumpFieldValue::Bool(v) => crate::doc_wire_format::write_field_bool(*field_idx, *v, buf),
            DumpFieldValue::Str(s) => crate::doc_wire_format::write_field_str(*field_idx, s, buf),
            DumpFieldValue::MultiInt(v) => crate::doc_wire_format::write_field_multi_int(*field_idx, v, buf),
        }
    }
}

// ---------------------------------------------------------------------------
// Compiled DocFieldPlan — eliminates per-row HashMap/HashSet lookups
// ---------------------------------------------------------------------------

/// How to read a field value during doc encoding.
#[allow(dead_code)]
pub(crate) enum DocFieldSource {
    /// Direct CSV field — use row.get_i64(column) / row.get_str(column).
    Direct { column: String },
    /// Enrichment result — look up in enriched_map.
    Enriched { target: String },
    /// Enrichment computed field — look up in enriched.computed Vec.
    EnrichedComputed { target: String },
    /// Computed field — eval_indexed on computed_defs[def_index].
    Computed { def_index: usize },
    /// Config-computed sort value (extra_i64) — pre-computed before doc encoding.
    ExtraI64 { index: usize },
}

/// How to interpret the raw value.
#[allow(dead_code)]
#[derive(Clone, Copy)]
pub(crate) enum DocValueType {
    Int,
    Boolean,
    String,
    IntOrString,
    /// Multi-value integer field — each row contributes one element to an array.
    /// Compaction merges Mi arrays via concatenation.
    MultiInt,
}

/// One entry in the compiled doc field plan.
#[allow(dead_code)]
pub(crate) struct DocFieldPlanEntry {
    pub doc_field_idx: u16,
    pub source: DocFieldSource,
    pub value_type: DocValueType,
}

/// Build the compiled doc field plan at phase setup. Called once per dump phase
/// instead of per row — pre-resolves all HashMap/HashSet lookups upfront so the
/// row hot path is a flat loop over plan entries.
///
/// NOTE (faithful V3 port): this function does not currently emit
/// `DocFieldSource::EnrichedComputed` entries — V3 also leaves this gap. The
/// variant is defined and handled by `execute_doc_plan`, but the wiring stage
/// (Ivy's items 8/9/13) will need to thread the enrichment computed-field list
/// through here when activating the plan in the row loop.
#[allow(dead_code)]
pub(crate) fn build_doc_field_plan(
    request_fields: &[DumpFieldMapping],
    enrichment_targets: &[String],
    enrichment_computed_targets: &[String],
    computed_defs: &[ComputedFieldDef],
    extra_i64_targets: &[String],
    field_idx: &HashMap<String, u16>,
    boolean_fields: &HashSet<String>,
    _filter_field_names: &HashSet<String>,
    multi_value_fields: &HashSet<String>,
) -> Vec<DocFieldPlanEntry> {
    let extra_skip: HashSet<&str> = extra_i64_targets.iter().map(|s| s.as_str()).collect();
    let enriched_computed_skip: HashSet<&str> =
        enrichment_computed_targets.iter().map(|s| s.as_str()).collect();
    let mut plan = Vec::new();

    // Direct fields
    for mapping in request_fields {
        let target = mapping.target();
        if extra_skip.contains(target) { continue; }
        if let Some(&fidx) = field_idx.get(target) {
            let vtype = if multi_value_fields.contains(target) {
                DocValueType::MultiInt
            } else if boolean_fields.contains(target) {
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

    // Enrichment (direct) fields — skip targets that are enrichment-computed
    // (those get an EnrichedComputed entry below which handles Int/Bool/Str).
    for target in enrichment_targets {
        if extra_skip.contains(target.as_str()) { continue; }
        if enriched_computed_skip.contains(target.as_str()) { continue; }
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

    // Enrichment computed fields — handle Int/Bool/Str values from enriched.computed.
    for target in enrichment_computed_targets {
        if extra_skip.contains(target.as_str()) { continue; }
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            let vtype = if boolean_fields.contains(target.as_str()) {
                DocValueType::Boolean
            } else {
                DocValueType::IntOrString
            };
            plan.push(DocFieldPlanEntry {
                doc_field_idx: fidx,
                source: DocFieldSource::EnrichedComputed { target: target.clone() },
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
#[allow(dead_code)]
pub(crate) fn execute_doc_plan<'a>(
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
                    match entry.value_type {
                        DocValueType::MultiInt => fields.push((entry.doc_field_idx, DumpFieldValue::MultiInt(vec![v]))),
                        _ => fields.push((entry.doc_field_idx, DumpFieldValue::Int(v))),
                    }
                } else if let Some(s) = row.get_str(column).or_else(|| enriched_map.get(column.as_str()).copied()) {
                    match entry.value_type {
                        DocValueType::MultiInt => {
                            if let Ok(v) = s.parse::<i64>() {
                                fields.push((entry.doc_field_idx, DumpFieldValue::MultiInt(vec![v])));
                            }
                        }
                        DocValueType::Boolean => {
                            match s {
                                "t" | "true" => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(true))),
                                "f" | "false" => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(false))),
                                _ => fields.push((entry.doc_field_idx, DumpFieldValue::Str(s))),
                            }
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
                                match val {
                                    "t" | "true" => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(true))),
                                    "f" | "false" => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(false))),
                                    _ => fields.push((entry.doc_field_idx, DumpFieldValue::Str(val))),
                                }
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
                // Str requires allocation since eval owns the result. Skip Str (rare in practice;
                // current computed fields are almost always Int or Bool).
                //
                // NOTE (faithful V3 port): V3 has the same skip. If a future computed field
                // emits Str, the wiring stage should add an `OwnedStr(String)` variant to
                // `DumpFieldValue` rather than silently dropping the value.
                match computed_defs[*def_index].eval_indexed(indexed_fields, col_idx, None) {
                    Some(NateExprValue::Int(v)) => fields.push((entry.doc_field_idx, DumpFieldValue::Int(v))),
                    Some(NateExprValue::Bool(b)) => fields.push((entry.doc_field_idx, DumpFieldValue::Bool(b))),
                    Some(NateExprValue::Str(_)) => {} // skip — would require allocation
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// DISABLED: tests reference deleted DocStoreV3
#[cfg(all(test, feature = "DISABLED_pending_v3_port"))]
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

        let engine = crate::concurrent_engine::ConcurrentEngine::new_with_path(
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

    /// Test that write_docstore_row_indexed correctly coerces PG boolean strings
    /// ("t"/"f") to PackedValue::B for fields declared as boolean in the data schema.
    #[test]
    fn test_boolean_coercion_in_docstore_write() {
        use crate::doc_wire_format::DocStoreV3;
        use crate::doc_wire_format::PackedValue;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut ds = DocStoreV3::open(&docs_dir).unwrap();

        let field_names = vec!["poi".to_string(), "type".to_string()];
        let bulk_writer = Arc::new(ds.prepare_streaming_writer(&field_names).unwrap());
        let field_idx = &field_idx_map.clone();

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

        let enriched = crate::dump_enrichment::EnrichedFields::default();
        let computed_defs: Vec<ComputedFieldDef> = vec![];
        let indexed_fields = row.to_indexed_fields();
        let col_idx = row.col_index_ref();
        let extra_i64: Vec<(&str, i64)> = vec![];

        let mut serialize_buf = Vec::new();
        let mut tuple_buf = Vec::new();
        let mut write_buf = Vec::new();

        write_docstore_row_indexed(
            &row, &enriched, &computed_defs, &indexed_fields, col_idx,
            1, &request_fields, &bulk_writer, &field_idx,
            &boolean_fields, &extra_i64,
            &mut serialize_buf, &mut tuple_buf, &mut write_buf,
        );
        bulk_writer.finalize().unwrap();

        // Read back via DocStoreV3 — fields are FieldValue, not JSON
        let doc = ds.get(1).unwrap().unwrap();
        match doc.fields.get("poi") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Bool(false))) => {}
            other => panic!("poi should be boolean false, got: {:?}", other),
        }
        match doc.fields.get("type") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::String(s))) => {
                assert_eq!(s, "Checkpoint");
            }
            other => panic!("type should be string 'Checkpoint', got: {:?}", other),
        }
    }

    /// Test that extra_i64_fields (config-computed sorts) are written to docstore.
    #[test]
    fn test_extra_i64_fields_in_docstore_write() {
        use crate::doc_wire_format::DocStoreV3;
        use crate::doc_wire_format::PackedValue;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut ds = DocStoreV3::open(&docs_dir).unwrap();

        let field_names = vec!["userId".to_string(), "sortAt".to_string()];
        let bulk_writer = Arc::new(ds.prepare_streaming_writer(&field_names).unwrap());
        let field_idx = &field_idx_map.clone();

        let boolean_fields = HashSet::new();
        let col_index: HashMap<String, usize> = [
            ("id".to_string(), 0),
            ("userId".to_string(), 1),
        ].into_iter().collect();
        let line = b"1,42";
        let fields = parse_delimited_line(line, b',');
        let row = ParsedRow { fields, col_index: &col_index };

        let request_fields = vec![DumpFieldMapping::Short("userId".to_string())];
        let enriched = crate::dump_enrichment::EnrichedFields::default();
        let computed_defs: Vec<ComputedFieldDef> = vec![];
        let indexed_fields = row.to_indexed_fields();
        let col_idx = row.col_index_ref();

        let extra_i64: Vec<(&str, i64)> = vec![("sortAt", 1711234567)];

        let mut serialize_buf = Vec::new();
        let mut tuple_buf = Vec::new();
        let mut write_buf = Vec::new();

        write_docstore_row_indexed(
            &row, &enriched, &computed_defs, &indexed_fields, col_idx,
            1, &request_fields, &bulk_writer, &field_idx,
            &boolean_fields, &extra_i64,
            &mut serialize_buf, &mut tuple_buf, &mut write_buf,
        );
        bulk_writer.finalize().unwrap();

        // Read back via DocStoreV3
        let doc = ds.get(1).unwrap().unwrap();
        match doc.fields.get("userId") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(42))) => {}
            other => panic!("userId should be 42, got: {:?}", other),
        }
        match doc.fields.get("sortAt") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(v))) => {
                assert_eq!(*v, 1711234567, "sortAt should be written via extra_i64_fields");
            }
            other => panic!("sortAt should be 1711234567, got: {:?}", other),
        }
    }

    /// Byte-for-byte regression test: `encode_dump_merge` must produce identical
    /// bytes to `DocOpCodec::encode_op` for an equivalent `DocOp::Merge`.
    /// Catches any wire format drift between the zero-copy dump path and the
    /// canonical PackedValue path.
    #[test]
    fn test_encode_dump_merge_matches_codec() {
        use crate::shard_store::OpCodec;
        use crate::doc_wire_format::{DocOp, DocOpCodec, PackedValue};

        // Build the same merge two ways and compare.
        let cases: Vec<(u32, Vec<(u16, DumpFieldValue, PackedValue)>)> = vec![
            // Empty merge
            (0, vec![]),
            // Single int
            (1, vec![(0, DumpFieldValue::Int(42), PackedValue::I(42))]),
            // Single bool true
            (2, vec![(1, DumpFieldValue::Bool(true), PackedValue::B(true))]),
            // Single bool false
            (3, vec![(2, DumpFieldValue::Bool(false), PackedValue::B(false))]),
            // Empty string
            (4, vec![(3, DumpFieldValue::Str(""), PackedValue::S(String::new()))]),
            // UTF-8 string
            (5, vec![(4, DumpFieldValue::Str("héllo wörld 🦀"), PackedValue::S("héllo wörld 🦀".to_string()))]),
            // Empty multi-int
            (6, vec![(5, DumpFieldValue::MultiInt(vec![]), PackedValue::Mi(vec![]))]),
            // Multi-int with values
            (7, vec![(6, DumpFieldValue::MultiInt(vec![10, 20, 30, -1, i64::MAX]), PackedValue::Mi(vec![10, 20, 30, -1, i64::MAX]))]),
            // Mixed fields, ordered to test order preservation
            (
                u32::MAX,
                vec![
                    (100, DumpFieldValue::Int(-1), PackedValue::I(-1)),
                    (50, DumpFieldValue::Bool(true), PackedValue::B(true)),
                    (200, DumpFieldValue::Str("test"), PackedValue::S("test".to_string())),
                    (75, DumpFieldValue::MultiInt(vec![1, 2, 3]), PackedValue::Mi(vec![1, 2, 3])),
                ],
            ),
        ];

        for (slot, case_fields) in cases {
            let dump_fields: Vec<(u16, DumpFieldValue)> = case_fields.iter()
                .map(|(idx, dv, _)| (*idx, match dv {
                    DumpFieldValue::Int(v) => DumpFieldValue::Int(*v),
                    DumpFieldValue::Bool(v) => DumpFieldValue::Bool(*v),
                    DumpFieldValue::Str(s) => DumpFieldValue::Str(s),
                    DumpFieldValue::MultiInt(v) => DumpFieldValue::MultiInt(v.clone()),
                }))
                .collect();
            let pv_fields: Vec<(u16, PackedValue)> = case_fields.iter()
                .map(|(idx, _, pv)| (*idx, pv.clone()))
                .collect();

            let mut zero_copy_buf = Vec::new();
            encode_dump_merge(slot, &dump_fields, &mut zero_copy_buf);

            let mut codec_buf = Vec::new();
            DocOpCodec::encode_op(&DocOp::Merge { slot, fields: pv_fields }, &mut codec_buf);

            assert_eq!(
                zero_copy_buf, codec_buf,
                "wire format mismatch for slot {} — encode_dump_merge produced different bytes than DocOpCodec::encode_op",
                slot
            );
        }
    }

    /// Reproduces a production bug where multi-value field (tagIds) loaded in a
    /// second dump phase ends up with values belonging to a different slot in
    /// the docstore. Exercises the per-slot MultiInt accumulator in
    /// `process_dump_with_progress` (mi_accum / mi_prev_slot path) plus the
    /// merge into docs already written by a prior phase.
    #[test]
    fn test_multi_int_phase_merges_with_images() {
        use crate::config::{Config, FilterFieldConfig, FilterFieldType, SortFieldConfig};
        use crate::query::{BitdexQuery, FilterClause, Value};

        let dir = tempfile::tempdir().unwrap();
        let docs_path = dir.path().join("docs");
        let bitmap_path = dir.path().join("bitmaps");

        let mut config = Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: true,
                    per_value_lazy: false,
                },
                FilterFieldConfig {
                    name: "userId".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                },
            ],
            sort_fields: vec![
                SortFieldConfig {
                    name: "id".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                    computed: None,
                },
                SortFieldConfig {
                    name: "existedAt".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                    computed: None,
                },
            ],
            flush_interval_us: 50,
            merge_interval_ms: 100,
            channel_capacity: 10_000,
            ..Default::default()
        };
        config.storage.bitmap_path = Some(bitmap_path.clone());

        let engine = crate::concurrent_engine::ConcurrentEngine::new_with_path(
            config, docs_path.as_path(),
        ).unwrap();

        // --- Phase 1: images ---
        let images_csv = dir.path().join("images.csv");
        std::fs::write(
            &images_csv,
            "id,nsfwLevel,userId,scannedAtSecs,createdAtSecs\n\
             1,1,100,1000,2000\n\
             2,1,100,1100,2100\n\
             3,4,200,1200,2200\n\
             4,1,300,1300,2300\n",
        ).unwrap();

        let images_request: DumpRequest = serde_json::from_value(serde_json::json!({
            "name": "images",
            "csv_path": images_csv.to_str().unwrap(),
            "format": "csv",
            "slot_field": "id",
            "sets_alive": true,
            "fields": ["nsfwLevel", "userId"],
            "computed_fields": [
                { "target": "existedAt", "expression": "max(scannedAtSecs, createdAtSecs)" }
            ]
        })).unwrap();

        let r1 = process_dump(&images_request, &engine, dir.path(), None, None, None, None);
        assert!(r1.is_ok(), "images phase failed: {:?}", r1.err());
        assert_eq!(r1.unwrap().row_count, 4);

        // Sanity: slot 1 after phase 1
        let doc1_p1 = engine.get_document(1).unwrap().expect("slot 1 should exist after phase 1");
        match doc1_p1.fields.get("nsfwLevel") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(1))) => {}
            other => panic!("slot 1 nsfwLevel after phase 1: {:?}", other),
        }
        match doc1_p1.fields.get("userId") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(100))) => {}
            other => panic!("slot 1 userId after phase 1: {:?}", other),
        }
        match doc1_p1.fields.get("existedAt") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(2000))) => {}
            other => panic!("slot 1 existedAt after phase 1: {:?}", other),
        }

        // --- Phase 2: tags (multi-value, rows ordered by imageId) ---
        let tags_csv = dir.path().join("tags.csv");
        std::fs::write(
            &tags_csv,
            "tagId,imageId\n\
             10,1\n\
             20,1\n\
             30,1\n\
             10,2\n\
             40,2\n\
             10,3\n\
             50,4\n\
             60,4\n",
        ).unwrap();

        let tags_request: DumpRequest = serde_json::from_value(serde_json::json!({
            "name": "tags",
            "csv_path": tags_csv.to_str().unwrap(),
            "format": "csv",
            "slot_field": "imageId",
            "fields": [
                { "column": "tagId", "target": "tagIds" }
            ]
        })).unwrap();

        let r2 = process_dump(&tags_request, &engine, dir.path(), None, None, None, None);
        assert!(r2.is_ok(), "tags phase failed: {:?}", r2.err());

        // --- Docstore assertions ---
        let get_tags = |slot: u32| -> Vec<i64> {
            let doc = engine.get_document(slot).unwrap()
                .unwrap_or_else(|| panic!("slot {} missing after phase 2", slot));
            let f = doc.fields.get("tagIds").cloned()
                .unwrap_or_else(|| panic!("slot {} has no tagIds, fields: {:?}", slot, doc.fields.keys().collect::<Vec<_>>()));
            match f {
                crate::mutation::FieldValue::Multi(vals) => vals.into_iter().map(|v| match v {
                    crate::query::Value::Integer(i) => i,
                    other => panic!("slot {} tag value not integer: {:?}", slot, other),
                }).collect(),
                other => panic!("slot {} tagIds not Multi: {:?}", slot, other),
            }
        };

        let sort_sorted = |mut v: Vec<i64>| -> Vec<i64> { v.sort(); v };

        assert_eq!(sort_sorted(get_tags(1)), vec![10, 20, 30], "slot 1 tagIds mismatch");
        assert_eq!(sort_sorted(get_tags(2)), vec![10, 40], "slot 2 tagIds mismatch");
        assert_eq!(sort_sorted(get_tags(3)), vec![10], "slot 3 tagIds mismatch");
        assert_eq!(sort_sorted(get_tags(4)), vec![50, 60], "slot 4 tagIds mismatch");

        // Verify phase 1 fields are still present on slot 1 (merge not clobbered)
        let doc1 = engine.get_document(1).unwrap().unwrap();
        match doc1.fields.get("nsfwLevel") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(1))) => {}
            other => panic!("slot 1 nsfwLevel after phase 2 (merge clobbered?): {:?}", other),
        }
        match doc1.fields.get("userId") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(100))) => {}
            other => panic!("slot 1 userId after phase 2: {:?}", other),
        }
        match doc1.fields.get("existedAt") {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(2000))) => {}
            other => panic!("slot 1 existedAt after phase 2: {:?}", other),
        }

        // --- Bitmap assertions via execute_query ---
        let query_tag = |tag: i64| -> Vec<i64> {
            let q = BitdexQuery {
                filters: vec![FilterClause::Eq("tagIds".to_string(), Value::Integer(tag))],
                sort: None,
                limit: 100,
                cursor: None,
                offset: None,
                skip_cache: true,
            };
            let mut ids = engine.execute_query(&q).unwrap().ids;
            ids.sort();
            ids
        };

        assert_eq!(query_tag(10), vec![1, 2, 3], "bitmap tagIds=10");
        assert_eq!(query_tag(20), vec![1], "bitmap tagIds=20");
        assert_eq!(query_tag(30), vec![1], "bitmap tagIds=30");
        assert_eq!(query_tag(40), vec![2], "bitmap tagIds=40");
        assert_eq!(query_tag(50), vec![4], "bitmap tagIds=50");
        assert_eq!(query_tag(60), vec![4], "bitmap tagIds=60");
    }
}
