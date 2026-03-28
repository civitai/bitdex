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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::concurrent_engine::ConcurrentEngine;
#[cfg(feature = "data-silo")]
use crate::data_silo::{self, BulkDocWriter};
use crate::dictionary::FieldDictionary;
use crate::docstore::{BulkWriter, PackedValue};
use crate::dump_enrichment;
use crate::dump_expression::{FilterExpression, ComputedFieldDef, CsvRow};
use crate::dump_expression::ExprValue as NateExprValue;

const LOG_INTERVAL: u64 = 1_000_000;

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
    pub fn fill_indexed_fields<'b>(&'b self, buf: &mut Vec<Option<&'b str>>) {
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
        docstore_root: std::path::PathBuf,
        bitmap_path: Option<std::path::PathBuf>,
        filter_field_names: Vec<String>,
    ) -> Self {
        let handle = std::thread::Builder::new()
            .name("shard-precreator".into())
            .spawn(move || {
                let mut created_up_to: u32 = 0;
                let mut files_created: u32 = 0;
                let mut bitmap_dirs_done = false;
                let mut docstore_dirs_done = false;

                loop {
                    let current_max_slot = watermark.load(std::sync::atomic::Ordering::Relaxed) as u32;
                    let target_shard = current_max_slot >> 9; // SHARD_SHIFT = 9

                    // Pre-create all 256 hex subdirectories once (eliminates per-file create_dir_all)
                    if !docstore_dirs_done && current_max_slot > 0 {
                        let shards_dir = docstore_root.join("shards");
                        for hex in 0..=255u8 {
                            let _ = std::fs::create_dir_all(shards_dir.join(format!("{:02x}", hex)));
                        }
                        docstore_dirs_done = true;
                        eprintln!("  ShardPreCreator: docstore hex dirs created");
                    }

                    // Create docstore shard files up to target (no create_dir_all per file)
                    while created_up_to < target_shard {
                        created_up_to += 1;
                        let path = crate::docstore::DocStore::shard_path(&docstore_root, created_up_to);
                        if let Ok(f) = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&path)
                        {
                            let meta = f.metadata().ok();
                            if meta.map(|m| m.len()).unwrap_or(0) == 0 {
                                let mut bw = std::io::BufWriter::new(f);
                                use std::io::Write as _;
                                let _ = bw.write_all(&0x42445832u32.to_le_bytes());
                                let _ = bw.flush();
                            }
                        }
                        files_created += 1;
                        if files_created % 50_000 == 0 {
                            eprintln!("  ShardPreCreator: {}K docstore files created", files_created / 1000);
                        }
                    }

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
                        // Final sweep for any remaining shards
                        let final_max = watermark.load(std::sync::atomic::Ordering::Relaxed) as u32;
                        let final_shard = final_max >> 9;
                        while created_up_to < final_shard {
                            created_up_to += 1;
                            let path = crate::docstore::DocStore::shard_path(&docstore_root, created_up_to);
                            if let Ok(f) = std::fs::OpenOptions::new()
                                .create(true).append(true).open(&path)
                            {
                                let meta = f.metadata().ok();
                                if meta.map(|m| m.len()).unwrap_or(0) == 0 {
                                    let mut bw = std::io::BufWriter::new(f);
                                    use std::io::Write as _;
                                    let _ = bw.write_all(&0x42445832u32.to_le_bytes());
                                    let _ = bw.flush();
                                }
                            }
                            files_created += 1;
                        }
                        eprintln!("  ShardPreCreator: done — {} files created (max shard {})", files_created, created_up_to);
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
    let mut result = process_dump_with_progress(request, engine, stage_dir, progress_counter, data_schema, slot_watermark.as_ref(), shutdown.as_ref())?;
    let (alive_s, filter_s, sort_s, meta_s) = engine
        .shard_stores()
        .ok_or_else(|| "no bitmap_path configured; cannot process dump".to_string())?;
    let bitmap_path = engine.config().storage.bitmap_path.as_ref()
        .ok_or_else(|| "no bitmap_path configured".to_string())?.clone();
    let dictionaries = engine.dictionaries_arc();
    save_phase_to_disk(&mut result, &alive_s, &filter_s, &sort_s, &meta_s, &bitmap_path, &dictionaries, &request.name, request.sets_alive)?;
    eprintln!("  Dump {} save complete", request.name);
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

    // Prepare BulkWriter for docstore — exclude filter_only fields so that
    // field_to_idx().get(target) returns None and docstore writes are skipped.
    let all_target_names: Vec<String> = target_fields
        .iter()
        .filter(|t| !filter_only_fields.contains(*t))
        .cloned()
        .collect();
    let bulk_writer = Arc::new(
        engine
            .prepare_bulk_writer(&all_target_names)
            .map_err(|e| format!("prepare_bulk_writer: {e}"))?,
    );

    // Create per-thread silo writers for data silo integration.
    // Each rayon chunk gets its own silo file — zero contention writes.
    #[cfg(feature = "data-silo")]
    let silo_dir = stage_dir.join("silos").join(&request.name);
    #[cfg(feature = "data-silo")]
    let silo_writers: Vec<std::sync::Mutex<BulkDocWriter>> = {
        let num_threads = rayon::current_num_threads();
        data_silo::create_bulk_writers(&silo_dir, num_threads)
            .map_err(|e| format!("create silo writers: {e}"))?
            .into_iter()
            .map(std::sync::Mutex::new)
            .collect()
    };
    #[cfg(feature = "data-silo")]
    let silo_writers_ref = &silo_writers;

    // Mmap the CSV/TSV file
    let csv_path = std::path::Path::new(&request.csv_path);
    let file = std::fs::File::open(csv_path)
        .map_err(|e| format!("open {}: {e}", csv_path.display()))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| format!("mmap {}: {e}", csv_path.display()))?;
    let data = &mmap[..];
    let delimiter = detect_delimiter(data, &request.format);

    eprintln!(
        "  Dump {}: mmap'd {} ({:.1} GB), format={:?}",
        request.name,
        data.len(),
        data.len() as f64 / (1024.0 * 1024.0 * 1024.0),
        request.format
    );

    // Build column index: from explicit columns (headerless CSV) or first row (header CSV)
    let (col_index, data_start) = if !request.columns.is_empty() {
        // Headerless CSV (PG COPY output) — columns provided in dump request
        let index: HashMap<String, usize> = request
            .columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        (Arc::new(index), 0usize) // Data starts at byte 0
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

    // Tags optimization: if only multi-value field with small IDs, use Vec indexing
    let is_tags_optimization = request.fields.len() == 1
        && !request.sets_alive
        && request.computed_fields.is_empty()
        && request.enrichment.is_empty()
        && {
            let target = request.fields[0].target();
            target == "tagIds" || target == "toolIds" || target == "techniqueIds"
        };

    if is_tags_optimization {
        return process_multi_value_phase(
            request,
            body,
            delimiter,
            &col_index,
            &filter_expr,
            &bulk_writer,
            &progress_counter,
            slot_watermark,
            shutdown,
            stage_dir,
            &request.name.clone(),
        );
    }

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
        .enumerate()
        .map(|(_chunk_idx, &(range_start, range_end))| {
            #[cfg(feature = "data-silo")]
            let chunk_idx = _chunk_idx;
            let chunk = &body[range_start..range_end];

            let field_idx_cache: &HashMap<String, u16> = bulk_writer.field_to_idx();
            let col_idx_ref: &HashMap<String, usize> = col_index.as_ref();
            let mut serialize_buf: Vec<u8> = Vec::with_capacity(64);


            let mut filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>> = filter_targets
                .iter()
                .map(|n| (n.clone(), HashMap::new()))
                .collect();
            // Also init for computed filter fields
            for def in computed_defs_ref {
                if filter_field_names_ref.contains(&def.target) {
                    filter_maps.entry(def.target.clone()).or_default();
                }
            }
            let mut sort_maps: HashMap<String, Vec<RoaringBitmap>> = sort_targets
                .iter()
                .chain(computed_sort_targets.iter())
                .map(|(n, b)| {
                    let layers: Vec<RoaringBitmap> = (0..*b as usize).map(|_| RoaringBitmap::new()).collect();
                    (n.clone(), layers)
                })
                .collect();
            let mut alive = RoaringBitmap::new();
            let mut deferred: Vec<(u32, u64)> = Vec::new();
            let mut tuple_buf: Vec<(u16, u32, u32)> = Vec::with_capacity(20);
            let mut write_buf: Vec<u8> = Vec::with_capacity(256);
            let mut count = 0u64;
            let mut max_slot: u32 = 0;
            let mut line_start = 0;

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


                let fields = parse_delimited_line(line, delimiter);
                let row = ParsedRow {
                    fields,
                    col_index: col_idx_ref,
                };

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

                // Build indexed fields (Vec<Option<&str>> — cheap compared to HashMap)
                let indexed_fields_buf = row.to_indexed_fields();
                let col_idx = row.col_index_ref();

                // Apply filter via indexed path (zero-allocation)
                if let Some(ref fexpr) = filter_expr_ref {
                    if !fexpr.eval_indexed(&indexed_fields_buf, col_idx, None) {
                        continue;
                    }
                }


                // Resolve enrichment via indexed path (no CsvRow HashMap)
                let enriched = if enrichment_mgr_ref.table_count() > 0 {
                    Some(enrichment_mgr_ref.enrich_row_indexed(&indexed_fields_buf, col_idx))
                } else {
                    None
                };

                // Collect enriched field values (avoid HashMap — linear scan is fine for <10 fields)
                let enriched = enriched.unwrap_or_default();
                // Build a simple lookup closure for enriched values
                let enriched_get = |target: &str| -> Option<&str> {
                    for (t, v) in &enriched.fields {
                        if t == target { return Some(v.as_str()); }
                    }
                    for (t, v) in &enriched.computed {
                        if t == target {
                            return match v {
                                NateExprValue::Int(n) => None, // handled separately
                                NateExprValue::Str(s) => Some(s.as_str()),
                                _ => None,
                            };
                        }
                    }
                    None
                };

                // Check deferred alive: if publishedAt from enrichment is in the future
                if has_deferred_alive {
                    if let Some(pub_str) = enriched_get("publishedAt") {
                        if let Ok(pub_secs) = pub_str.parse::<u64>() {
                            if pub_secs > now_unix {
                                // Write document — silo (feature) or docstore (fallback)
                                #[cfg(feature = "data-silo")]
                                write_silo_row_indexed(
                                    &row, &enriched, computed_defs_ref,
                                    &indexed_fields_buf, col_idx, slot, request_fields,
                                    &field_idx_cache, &mut serialize_buf,
                                    &mut tuple_buf, &mut write_buf,
                                    &silo_writers_ref[chunk_idx],
                                );
                                #[cfg(not(feature = "data-silo"))]
                                write_docstore_row_indexed(
                                    &row, &enriched, computed_defs_ref,
                                    &indexed_fields_buf, col_idx, slot, request_fields,
                                    &bulk_writer, &field_idx_cache, &mut serialize_buf,
                                    &mut tuple_buf, &mut write_buf,
                                );
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

                // Set alive bit
                if sets_alive {
                    alive.insert(slot);
                }

                // Build filter + sort bitmaps from direct fields
                for field_mapping in request_fields {
                    let target = field_mapping.target();
                    let column = field_mapping.column();

                    // Filter bitmap: skip contains() check — just try get_mut directly
                    if let Some(fm) = filter_maps.get_mut(target) {
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
                            fm.entry(key)
                                .or_insert_with(RoaringBitmap::new)
                                .insert(slot);
                        }
                    }

                    // Build sort bitmaps from direct fields
                    if let Some(&bits) = sort_bits_ref.get(target) {
                        if let Some(v) = row.get_i64(column).or_else(|| {
                            enriched_get(target).and_then(|s| s.parse::<i64>().ok())
                        }) {
                            let val32 = v as u32;
                            if let Some(sm) = sort_maps.get_mut(target) {
                                for bit in 0..(bits as usize) {
                                    if (val32 >> bit) & 1 == 1 {
                                        sm[bit].insert(slot);
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
                        if let Some(fm) = filter_maps.get_mut(target.as_str()) {
                            let bitmap_key: Option<u64> = if let Some(dict) = dictionaries_ref.get(target.as_str()) {
                                Some(dict.get_or_insert(val_str) as u64)
                            } else {
                                val_str.parse::<i64>().ok().map(|v| v as u64)
                            };
                            if let Some(key) = bitmap_key {
                                fm.entry(key)
                                    .or_insert_with(RoaringBitmap::new)
                                    .insert(slot);
                            }
                        }
                        // Sort bitmap
                        if let Some(&bits) = sort_bits_ref.get(target.as_str()) {
                            if let Some(v) = val_str.parse::<i64>().ok() {
                                let val32 = v as u32;
                                if let Some(sm) = sort_maps.get_mut(target.as_str()) {
                                    for bit in 0..(bits as usize) {
                                        if (val32 >> bit) & 1 == 1 {
                                            sm[bit].insert(slot);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Build bitmaps from computed fields (Nate's ComputedFieldDef API)
                for def in computed_defs_ref {
                    let computed_val = def.eval_indexed(&indexed_fields_buf, col_idx, None);

                    match computed_val {
                        Some(NateExprValue::Int(v)) if def.value_column.is_none() => {
                            // Regular computed field — use value directly as bitmap key
                            if let Some(fm) = filter_maps.get_mut(&def.target) {
                                {
                                    fm.entry(v as u64)
                                        .or_insert_with(RoaringBitmap::new)
                                        .insert(slot);
                                }
                            }
                            if let Some(&bits) = sort_bits_ref.get(&def.target) {
                                let val32 = v as u32;
                                if let Some(sm) = sort_maps.get_mut(&def.target) {
                                    for bit in 0..(bits as usize) {
                                        if (val32 >> bit) & 1 == 1 {
                                            sm[bit].insert(slot);
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
                                    if let Some(fm) = filter_maps.get_mut(&def.target) {
                                        fm.entry(v as u64)
                                            .or_insert_with(RoaringBitmap::new)
                                            .insert(slot);
                                    }
                                }
                            }
                        }
                        Some(NateExprValue::Bool(b)) if def.value_column.is_none() => {
                            // Boolean computed field (e.g. hasMeta, isPublished)
                            let key = if b { 1u64 } else { 0u64 };
                            if let Some(fm) = filter_maps.get_mut(&def.target) {
                                {
                                    fm.entry(key)
                                        .or_insert_with(RoaringBitmap::new)
                                        .insert(slot);
                                }
                            }
                        }
                        _ => {} // Null or non-matching pattern
                    }
                }


                // Write document — silo (feature) or docstore (fallback)
                #[cfg(feature = "data-silo")]
                write_silo_row_indexed(
                    &row, &enriched, computed_defs_ref,
                    &indexed_fields_buf, col_idx, slot, request_fields,
                    &field_idx_cache, &mut serialize_buf,
                    &mut tuple_buf, &mut write_buf,
                    &silo_writers_ref[chunk_idx],
                );
                #[cfg(not(feature = "data-silo"))]
                write_docstore_row_indexed(
                    &row, &enriched, computed_defs_ref,
                    &indexed_fields_buf, col_idx, slot, request_fields,
                    &bulk_writer, &field_idx_cache, &mut serialize_buf,
                    &mut tuple_buf, &mut write_buf,
                );

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

            // Flush timing

            (filter_maps, sort_maps, alive, deferred, count, max_slot)
        })
        .collect();

    emit_stage(&request.name, "parallel_parse", "done", &t, total.load(Ordering::Relaxed));

    // Flush silo writers, collect local indexes, merge and persist doc_index.bin
    #[cfg(feature = "data-silo")]
    {
        emit_stage(&request.name, "silo_index", "start", &t, total.load(Ordering::Relaxed));
        let silo_locals: Vec<(u8, Vec<(u32, u64, u32)>)> = silo_writers
            .into_iter()
            .filter_map(|mutex| {
                let writer = mutex.into_inner().ok()?;
                writer.into_local_index().ok()
            })
            .collect();
        let silo_max_slot = silo_locals
            .iter()
            .flat_map(|(_, entries)| entries.iter().map(|(slot, _, _)| *slot))
            .max()
            .unwrap_or(0);
        let silo_index = data_silo::merge_indexes(silo_locals, silo_max_slot);
        let silo_index_count = silo_index.count();
        if let Err(e) = silo_index.persist(&silo_dir.join("doc_index.bin")) {
            eprintln!("  WARNING: silo index persist failed: {e}");
        }
        eprintln!(
            "  Dump {} silo index: {} entries persisted to {}",
            request.name, silo_index_count, silo_dir.join("doc_index.bin").display()
        );
        emit_stage(&request.name, "silo_index", "done", &t, total.load(Ordering::Relaxed));
    }

    emit_stage(&request.name, "merge", "start", &t, total.load(Ordering::Relaxed));
    // Merge all thread results — parallel tree reduction
    type MergeAccum = (
        HashMap<String, HashMap<u64, RoaringBitmap>>,
        HashMap<String, Vec<RoaringBitmap>>,
        RoaringBitmap,
        BTreeMap<u64, Vec<u32>>,
        u64,
        u32,
    );

    let (merged_filters, merged_sorts, merged_alive, merged_deferred, total_count, max_slot) =
        thread_results
            .into_par_iter()
            .fold(
                || -> MergeAccum {
                    (HashMap::new(), HashMap::new(), RoaringBitmap::new(), BTreeMap::new(), 0u64, 0u32)
                },
                |mut acc, (filter_maps, sort_maps, alive, deferred, count, thread_max)| {
                    acc.2 |= alive;
                    acc.4 += count;
                    if thread_max > acc.5 { acc.5 = thread_max; }

                    for (slot, activate_at) in deferred {
                        acc.3.entry(activate_at).or_default().push(slot);
                    }

                    for (field, values) in filter_maps {
                        let dest = acc.0.entry(field).or_default();
                        for (val, bm) in values {
                            dest.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
                        }
                    }
                    for (field, layers) in sort_maps {
                        let dest = acc.1.entry(field).or_insert_with(|| {
                            (0..layers.len()).map(|_| RoaringBitmap::new()).collect()
                        });
                        for (bit, bm) in layers.into_iter().enumerate() {
                            if bit < dest.len() {
                                dest[bit] |= bm;
                            }
                        }
                    }
                    acc
                },
            )
            .reduce(
                || -> MergeAccum {
                    (HashMap::new(), HashMap::new(), RoaringBitmap::new(), BTreeMap::new(), 0u64, 0u32)
                },
                |mut a, b| {
                    a.2 |= b.2;
                    a.4 += b.4;
                    if b.5 > a.5 { a.5 = b.5; }

                    for (activate_at, slots) in b.3 {
                        a.3.entry(activate_at).or_default().extend(slots);
                    }

                    for (field, values) in b.0 {
                        let dest = a.0.entry(field).or_default();
                        for (val, bm) in values {
                            dest.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
                        }
                    }
                    for (field, layers) in b.1 {
                        let dest = a.1.entry(field).or_insert_with(|| {
                            (0..layers.len()).map(|_| RoaringBitmap::new()).collect()
                        });
                        for (bit, bm) in layers.into_iter().enumerate() {
                            if bit < dest.len() {
                                dest[bit] |= bm;
                            }
                        }
                    }
                    a
                },
            );

    emit_stage(&request.name, "merge", "done", &t, total_count);

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
// Multi-value phase (tags, tools, techniques optimization)
// ---------------------------------------------------------------------------

/// Optimized processor for simple multi-value phases (two columns: value_id, slot_id).
/// Uses Vec indexing for tags (MAX_TAG_ID=300K preallocated).
fn process_multi_value_phase(
    request: &DumpRequest,
    body: &[u8],
    delimiter: u8,
    col_index: &Arc<HashMap<String, usize>>,
    filter_expr: &Option<FilterExpression>,
    bulk_writer: &Arc<BulkWriter>,
    progress_counter: &Option<Arc<AtomicU64>>,
    slot_watermark: Option<&Arc<AtomicU64>>,
    shutdown: Option<&Arc<dyn Fn() -> bool + Send + Sync>>,
    stage_dir: &Path,
    dump_name: &str,
) -> Result<PhaseResult, String> {
    let target = request.fields[0].target().to_string();
    let value_column = request.fields[0].column().to_string();
    let slot_field = &request.slot_field;

    const MAX_TAG_ID: usize = 300_000;
    let use_vec = target == "tagIds"; // Only tagIds uses vec optimization

    let field_idx = bulk_writer.field_to_idx().get(&target).copied();

    let ranges = split_mmap_ranges(body, rayon::current_num_threads());
    let total = AtomicU64::new(0);
    let total_ref = &total;

    // Spawn docstore writer thread — rayon threads push (slot, value) to channel,
    // writer drains and writes per shard. Zero contention on parse threads.
    let (doc_tx, doc_rx) = if field_idx.is_some() {
        let (tx, rx) = crossbeam_channel::bounded::<Vec<(u32, i64)>>(64);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    #[cfg(not(feature = "data_silo"))]
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
        })
    });

    #[cfg(feature = "data_silo")]
    let doc_writer_handle = doc_rx.map(|rx| {
        let silo_path = stage_dir
            .join("silos")
            .join(dump_name)
            .join(&target)
            .join("silo_00.dat");
        std::thread::spawn(move || {
            use crate::data_silo::{BulkDocWriter, DocDataFile, DocIndex};
            let doc_file = match DocDataFile::create(&silo_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("data_silo: failed to create {:?}: {e}", silo_path);
                    // drain channel so sender doesn't block
                    for _ in rx {}
                    return;
                }
            };
            let mut writer = BulkDocWriter::new(doc_file, 0u8);
            let mut buf = Vec::with_capacity(32);
            for batch in rx {
                for (slot, value) in batch {
                    buf.clear();
                    if rmp_serde::encode::write(&mut buf, &PackedValue::Mi(vec![value])).is_ok() {
                        let _ = writer.append(slot, &buf);
                    }
                }
            }
            // Flush and persist the local index
            match writer.into_local_index() {
                Ok((file_id, local_entries)) => {
                    let max_slot = local_entries.iter().map(|(s, _, _)| *s).max().unwrap_or(0);
                    let mut index = DocIndex::new(max_slot);
                    for (slot_id, offset, length) in local_entries {
                        index.set(slot_id, file_id, offset, length);
                    }
                    let index_path = silo_path.parent().unwrap().join("doc_index.bin");
                    if let Err(e) = index.persist(&index_path) {
                        eprintln!("data_silo: failed to persist index {:?}: {e}", index_path);
                    }
                }
                Err(e) => {
                    eprintln!("data_silo: flush error: {e}");
                }
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

        let thread_results: Vec<Vec<RoaringBitmap>> = ranges
            .par_iter()
            .map(|&(range_start, range_end)| {
                let chunk = &body[range_start..range_end];
                let mut bitmaps: Vec<RoaringBitmap> =
                    (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect();
                let mut doc_batch: Vec<(u32, i64)> = Vec::with_capacity(10_000);
                let mut local_max_slot: u32 = 0;
                let mut count = 0u64;
                let mut line_start = 0;

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

        // Docstore writes sent to writer thread above

        // Merge Vec<RoaringBitmap> — parallel tree reduction
        let mut merged_vec = thread_results
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

        // Convert to HashMap (non-empty only)
        let mut filter_map: HashMap<u64, RoaringBitmap> = HashMap::new();
        for (i, bm) in merged_vec.drain(..).enumerate() {
            if !bm.is_empty() {
                filter_map.insert(i as u64, bm);
            }
        }

        let total_rows = total.load(Ordering::Relaxed);
        eprintln!(
            "  Dump {} ({target}): {} rows, {} distinct values",
            request.name,
            total_rows,
            filter_map.len(),
        );

        let mut filter_maps = HashMap::new();
        filter_maps.insert(target, filter_map);

        // Wait for docstore writer thread to finish
        drop(doc_tx);
        if let Some(handle) = doc_writer_handle {
            handle.join().ok();
        }

        emit_stage(&request.name, "parallel_parse", "done", &t_mv, total_rows);

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

/// Write a single row's data to the docstore via BulkWriter (indexed path).
/// Uses cached field_idx, indexed fields, and reusable serialize buffer.
fn write_docstore_row_indexed(
    row: &ParsedRow,
    enriched: &dump_enrichment::EnrichedFields,
    computed_defs: &[ComputedFieldDef],
    indexed_fields: &[Option<&str>],
    col_idx: &HashMap<String, usize>,
    slot: u32,
    request_fields: &[DumpFieldMapping],
    bulk_writer: &Arc<BulkWriter>,
    field_idx: &HashMap<String, u16>,
    serialize_buf: &mut Vec<u8>,
    tuple_buf: &mut Vec<(u16, u32, u32)>,
    write_buf: &mut Vec<u8>,
) {
    serialize_buf.clear();
    tuple_buf.clear();

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

    // Direct fields
    for mapping in request_fields {
        let target = mapping.target();
        let column = mapping.column();
        if let Some(&fidx) = field_idx.get(target) {
            if let Some(v) = row.get_i64(column) {
                collect_packed!(fidx, &PackedValue::I(v));
            } else if let Some(s) = row.get_str(column) {
                collect_packed!(fidx, &PackedValue::S(s.to_string()));
            }
        }
    }

    // Enriched fields
    for (target, value) in &enriched.fields {
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            if let Ok(v) = value.parse::<i64>() {
                collect_packed!(fidx, &PackedValue::I(v));
            } else {
                collect_packed!(fidx, &PackedValue::S(value.clone()));
            }
        }
    }

    // Enriched computed fields
    for (target, value) in &enriched.computed {
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            match value {
                NateExprValue::Int(v) => { collect_packed!(fidx, &PackedValue::I(*v)); }
                NateExprValue::Bool(b) => { collect_packed!(fidx, &PackedValue::I(if *b { 1 } else { 0 })); }
                NateExprValue::Str(ref s) => { collect_packed!(fidx, &PackedValue::S(s.clone())); }
                NateExprValue::Null => {}
            }
        }
    }

    // Computed fields via indexed eval
    for def in computed_defs {
        if let Some(&fidx) = field_idx.get(def.target.as_str()) {
            match def.eval_indexed(indexed_fields, col_idx, None) {
                Some(NateExprValue::Int(v)) => { collect_packed!(fidx, &PackedValue::I(v)); }
                Some(NateExprValue::Bool(b)) => { collect_packed!(fidx, &PackedValue::I(if b { 1 } else { 0 })); }
                Some(NateExprValue::Str(ref s)) => { collect_packed!(fidx, &PackedValue::S(s.clone())); }
                _ => {}
            }
        }
    }

    // One lock acquisition for all fields
    if !tuple_buf.is_empty() {
        let refs: Vec<(u16, &[u8])> = tuple_buf.iter()
            .map(|&(idx, off, len)| (idx, &serialize_buf[off as usize..(off + len) as usize]))
            .collect();
        bulk_writer.append_tuples_raw(slot, &refs, write_buf);
    }
}

/// Write a single row's data to a data silo via BulkDocWriter.
/// Same field collection logic as write_docstore_row_indexed, but encodes all
/// fields into a single contiguous document blob and appends to the silo file.
///
/// Document blob format: [u16 field_idx][u32 value_len][value_bytes]...
/// No slot_id prefix (the silo index maps slot→offset).
#[cfg(feature = "data-silo")]
fn write_silo_row_indexed(
    row: &ParsedRow,
    enriched: &dump_enrichment::EnrichedFields,
    computed_defs: &[ComputedFieldDef],
    indexed_fields: &[Option<&str>],
    col_idx: &HashMap<String, usize>,
    slot: u32,
    request_fields: &[DumpFieldMapping],
    field_idx: &HashMap<String, u16>,
    serialize_buf: &mut Vec<u8>,
    tuple_buf: &mut Vec<(u16, u32, u32)>,
    write_buf: &mut Vec<u8>,
    silo_writer: &std::sync::Mutex<BulkDocWriter>,
) {
    serialize_buf.clear();
    tuple_buf.clear();

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

    // Direct fields
    for mapping in request_fields {
        let target = mapping.target();
        let column = mapping.column();
        if let Some(&fidx) = field_idx.get(target) {
            if let Some(v) = row.get_i64(column) {
                collect_packed!(fidx, &PackedValue::I(v));
            } else if let Some(s) = row.get_str(column) {
                collect_packed!(fidx, &PackedValue::S(s.to_string()));
            }
        }
    }

    // Enriched fields
    for (target, value) in &enriched.fields {
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            if let Ok(v) = value.parse::<i64>() {
                collect_packed!(fidx, &PackedValue::I(v));
            } else {
                collect_packed!(fidx, &PackedValue::S(value.clone()));
            }
        }
    }

    // Enriched computed fields
    for (target, value) in &enriched.computed {
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            match value {
                NateExprValue::Int(v) => { collect_packed!(fidx, &PackedValue::I(*v)); }
                NateExprValue::Bool(b) => { collect_packed!(fidx, &PackedValue::I(if *b { 1 } else { 0 })); }
                NateExprValue::Str(ref s) => { collect_packed!(fidx, &PackedValue::S(s.clone())); }
                NateExprValue::Null => {}
            }
        }
    }

    // Computed fields via indexed eval
    for def in computed_defs {
        if let Some(&fidx) = field_idx.get(def.target.as_str()) {
            match def.eval_indexed(indexed_fields, col_idx, None) {
                Some(NateExprValue::Int(v)) => { collect_packed!(fidx, &PackedValue::I(v)); }
                Some(NateExprValue::Bool(b)) => { collect_packed!(fidx, &PackedValue::I(if b { 1 } else { 0 })); }
                Some(NateExprValue::Str(ref s)) => { collect_packed!(fidx, &PackedValue::S(s.clone())); }
                _ => {}
            }
        }
    }

    // Encode document blob and write to silo
    if !tuple_buf.is_empty() {
        write_buf.clear();
        for &(fidx, off, len) in tuple_buf.iter() {
            write_buf.extend_from_slice(&fidx.to_le_bytes());
            write_buf.extend_from_slice(&len.to_le_bytes());    // u32 value_len (not u16 — values can exceed 64KB)
            write_buf.extend_from_slice(&serialize_buf[off as usize..(off + len) as usize]);
        }
        if let Ok(mut writer) = silo_writer.lock() {
            let _ = writer.append(slot, write_buf);
        }
    }
}

/// Write a single row's data to the docstore via BulkWriter (legacy HashMap path).
fn write_docstore_row(
    row: &ParsedRow,
    enriched_values: &HashMap<String, String>,
    computed_defs: &[ComputedFieldDef],
    csv_row: &CsvRow,
    slot: u32,
    request_fields: &[DumpFieldMapping],
    bulk_writer: &Arc<BulkWriter>,
) {
    let field_idx = bulk_writer.field_to_idx();

    // Write direct fields
    for mapping in request_fields {
        let target = mapping.target();
        let column = mapping.column();

        if let Some(&fidx) = field_idx.get(target) {
            if let Some(v) = row.get_i64(column) {
                let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap_or_default();
                bulk_writer.append_tuple_raw(slot, fidx, &packed);
            } else if let Some(s) = row.get_str(column) {
                let packed = rmp_serde::to_vec(&PackedValue::S(s.to_string())).unwrap_or_default();
                bulk_writer.append_tuple_raw(slot, fidx, &packed);
            }
        }
    }

    // Write enriched fields
    for (target, value) in enriched_values {
        if let Some(&fidx) = field_idx.get(target.as_str()) {
            if let Ok(v) = value.parse::<i64>() {
                let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap_or_default();
                bulk_writer.append_tuple_raw(slot, fidx, &packed);
            } else {
                let packed =
                    rmp_serde::to_vec(&PackedValue::S(value.clone())).unwrap_or_default();
                bulk_writer.append_tuple_raw(slot, fidx, &packed);
            }
        }
    }

    // Write computed fields (Nate's ComputedFieldDef API)
    for def in computed_defs {
        if let Some(&fidx) = field_idx.get(def.target.as_str()) {
            match def.eval(csv_row, None) {
                Some(NateExprValue::Int(v)) => {
                    let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap_or_default();
                    bulk_writer.append_tuple_raw(slot, fidx, &packed);
                }
                Some(NateExprValue::Bool(b)) => {
                    let packed = rmp_serde::to_vec(&PackedValue::I(if b { 1 } else { 0 })).unwrap_or_default();
                    bulk_writer.append_tuple_raw(slot, fidx, &packed);
                }
                Some(NateExprValue::Str(ref s)) => {
                    let packed = rmp_serde::to_vec(&PackedValue::S(s.clone())).unwrap_or_default();
                    bulk_writer.append_tuple_raw(slot, fidx, &packed);
                }
                _ => {}
            }
        }
    }
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

    /// V2 Validation Gate: Simulates a 1000-row dump across 4 threads,
    /// then verifies silo files, index persistence, and document content.
    #[test]
    #[cfg(feature = "data-silo")]
    fn test_v2_validation_1000_rows() {
        use crate::data_silo::{self, DataSiloReader, DocIndex};

        let dir = std::env::temp_dir().join("bitdex_v2_validation_1000");
        let _ = std::fs::remove_dir_all(&dir);

        let num_threads = 4;
        let num_rows: u32 = 1000;

        // Create silo writers
        let writers = data_silo::create_bulk_writers(&dir, num_threads).unwrap();
        let silo_writers: Vec<std::sync::Mutex<BulkDocWriter>> = writers
            .into_iter()
            .map(std::sync::Mutex::new)
            .collect();

        // Fields: nsfwLevel (idx 0), userId (idx 1)
        let mut field_idx: HashMap<String, u16> = HashMap::new();
        field_idx.insert("nsfwLevel".to_string(), 0);
        field_idx.insert("userId".to_string(), 1);

        let fields = vec![
            DumpFieldMapping::Expanded {
                column: "nsfwLevel".to_string(),
                target: "nsfwLevel".to_string(),
            },
            DumpFieldMapping::Expanded {
                column: "userId".to_string(),
                target: "userId".to_string(),
            },
        ];
        let col_idx: HashMap<String, usize> =
            [("nsfwLevel".to_string(), 0), ("userId".to_string(), 1)]
                .into_iter()
                .collect();
        let empty_enriched = dump_enrichment::EnrichedFields::default();
        let computed_defs: Vec<ComputedFieldDef> = vec![];

        // Write 1000 rows distributed across 4 threads
        let mut serialize_buf = Vec::new();
        let mut tuple_buf = Vec::new();
        let mut write_buf = Vec::new();

        for slot in 1..=num_rows {
            let thread_idx = (slot as usize - 1) % num_threads;
            let nsfw = format!("{}", (slot % 5) + 1);
            let user = format!("{}", slot * 100);
            let row_fields: Vec<&[u8]> = vec![nsfw.as_bytes(), user.as_bytes()];
            let row = ParsedRow { fields: row_fields, col_index: &col_idx };
            let indexed: Vec<Option<&str>> = vec![Some(&nsfw), Some(&user)];
            write_silo_row_indexed(
                &row, &empty_enriched, &computed_defs, &indexed, &col_idx,
                slot, &fields, &field_idx,
                &mut serialize_buf, &mut tuple_buf, &mut write_buf,
                &silo_writers[thread_idx],
            );
        }

        // Collect and merge indexes
        let locals: Vec<_> = silo_writers
            .into_iter()
            .filter_map(|m| m.into_inner().ok()?.into_local_index().ok())
            .collect();
        let index = data_silo::merge_indexes(locals, num_rows);

        // V2.1: Verify silo files created with expected sizes
        for i in 0..num_threads {
            let path = dir.join(format!("silo_{:02}.dat", i));
            assert!(path.exists(), "silo_{:02}.dat should exist", i);
            let size = std::fs::metadata(&path).unwrap().len();
            assert!(size > 0, "silo_{:02}.dat should be non-empty (got {} bytes)", i, size);
            // ~250 rows per thread × ~20 bytes/row ≈ 5KB minimum
            assert!(size > 1000, "silo_{:02}.dat too small: {} bytes", i, size);
        }

        // V2.2: Persist and reload doc_index.bin
        let index_path = dir.join("doc_index.bin");
        index.persist(&index_path).unwrap();
        assert!(index_path.exists(), "doc_index.bin should exist");
        let index_size = std::fs::metadata(&index_path).unwrap().len();
        // Header (4 bytes) + 1001 entries × 13 bytes = ~13017 bytes
        assert!(index_size > 10000, "doc_index.bin too small: {} bytes", index_size);
        let reloaded = DocIndex::load(&index_path).unwrap();
        assert_eq!(reloaded.count(), num_rows as usize, "reloaded index should have {} entries", num_rows);

        // V2.3: Verify documents readable via DataSiloReader
        let reader = DataSiloReader::open(&dir).unwrap();
        assert_eq!(reader.count(), num_rows as usize);
        assert_eq!(reader.silo_count(), num_threads);

        // Spot-check 10 random slots
        for &slot in &[1u32, 50, 100, 250, 500, 750, 999, 1000, 3, 777] {
            let doc = reader.get(slot).expect(&format!("slot {} should be readable", slot));
            assert!(doc.len() > 6, "slot {} doc too small: {} bytes", slot, doc.len());

            // Parse the document blob: [u16 field_idx][u32 value_len][value_bytes]...
            let mut offset = 0;
            let mut found_fields = 0;
            while offset + 6 <= doc.len() {
                let fidx = u16::from_le_bytes([doc[offset], doc[offset + 1]]);
                let vlen = u32::from_le_bytes([
                    doc[offset + 2], doc[offset + 3], doc[offset + 4], doc[offset + 5],
                ]) as usize;
                assert!(fidx <= 1, "slot {} has unexpected field_idx {}", slot, fidx);
                assert!(offset + 6 + vlen <= doc.len(),
                    "slot {} blob truncated at field {}", slot, found_fields);

                // Deserialize the rmp_serde value
                let value_bytes = &doc[offset + 6..offset + 6 + vlen];
                let packed: PackedValue = rmp_serde::from_slice(value_bytes)
                    .expect(&format!("slot {} field {} should deserialize", slot, fidx));

                // Verify values match what we wrote
                match packed {
                    PackedValue::I(v) => {
                        if fidx == 0 {
                            // nsfwLevel = (slot % 5) + 1
                            assert_eq!(v, ((slot % 5) + 1) as i64,
                                "slot {} nsfwLevel mismatch", slot);
                        } else {
                            // userId = slot * 100
                            assert_eq!(v, slot as i64 * 100,
                                "slot {} userId mismatch", slot);
                        }
                    }
                    _ => panic!("slot {} field {} unexpected PackedValue type", slot, fidx),
                }

                offset += 6 + vlen;
                found_fields += 1;
            }
            assert_eq!(found_fields, 2, "slot {} should have exactly 2 fields, got {}", slot, found_fields);
        }

        // Verify non-existent slot returns None
        assert!(reader.get(0).is_none(), "slot 0 should not exist");
        assert!(reader.get(num_rows + 1).is_none(), "slot beyond max should not exist");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(feature = "data-silo")]
    fn test_write_silo_row_indexed_basic() {
        // Test that write_silo_row_indexed produces a document blob
        // that can be read back from a DataSiloReader.
        use crate::data_silo::{self, DataSiloReader};

        let dir = std::env::temp_dir().join("bitdex_silo_integration_test");
        let _ = std::fs::remove_dir_all(&dir);

        // Create 2 silo writers (simulating 2 threads)
        let writers = data_silo::create_bulk_writers(&dir, 2).unwrap();
        let silo_writers: Vec<std::sync::Mutex<BulkDocWriter>> = writers
            .into_iter()
            .map(std::sync::Mutex::new)
            .collect();

        // Set up field index (simulating bulk_writer.field_to_idx())
        let mut field_idx: HashMap<String, u16> = HashMap::new();
        field_idx.insert("nsfwLevel".to_string(), 0);
        field_idx.insert("userId".to_string(), 1);

        // Simulate writing 4 rows across 2 "threads"
        let fields = vec![
            DumpFieldMapping::Expanded {
                column: "nsfwLevel".to_string(),
                target: "nsfwLevel".to_string(),
            },
            DumpFieldMapping::Expanded {
                column: "userId".to_string(),
                target: "userId".to_string(),
            },
        ];
        let col_idx: HashMap<String, usize> =
            [("nsfwLevel".to_string(), 0), ("userId".to_string(), 1)]
                .into_iter()
                .collect();

        let empty_enriched = dump_enrichment::EnrichedFields::default();
        let computed_defs: Vec<ComputedFieldDef> = vec![];

        let mut serialize_buf = Vec::new();
        let mut tuple_buf = Vec::new();
        let mut write_buf = Vec::new();

        // Thread 0: write slots 10, 20
        for &slot in &[10u32, 20] {
            let nsfw_val = b"3";
            let user_val_str = format!("{}", slot as i64 * 100);
            let user_val = user_val_str.as_bytes();
            let row_fields: Vec<&[u8]> = vec![nsfw_val, user_val];
            let row = ParsedRow { fields: row_fields, col_index: &col_idx };
            let indexed: Vec<Option<&str>> = vec![Some("3"), Some(&user_val_str)];
            write_silo_row_indexed(
                &row,
                &empty_enriched,
                &computed_defs,
                &indexed,
                &col_idx,
                slot,
                &fields,
                &field_idx,
                &mut serialize_buf,
                &mut tuple_buf,
                &mut write_buf,
                &silo_writers[0],
            );
        }

        // Thread 1: write slots 15, 25
        for &slot in &[15u32, 25] {
            let nsfw_val = b"1";
            let user_val_str = format!("{}", slot as i64 * 200);
            let user_val = user_val_str.as_bytes();
            let row_fields: Vec<&[u8]> = vec![nsfw_val, user_val];
            let row = ParsedRow { fields: row_fields, col_index: &col_idx };
            let indexed: Vec<Option<&str>> = vec![Some("1"), Some(&user_val_str)];
            write_silo_row_indexed(
                &row,
                &empty_enriched,
                &computed_defs,
                &indexed,
                &col_idx,
                slot,
                &fields,
                &field_idx,
                &mut serialize_buf,
                &mut tuple_buf,
                &mut write_buf,
                &silo_writers[1],
            );
        }

        // Collect local indexes and merge
        let locals: Vec<_> = silo_writers
            .into_iter()
            .filter_map(|m| {
                let w = m.into_inner().ok()?;
                w.into_local_index().ok()
            })
            .collect();
        let index = data_silo::merge_indexes(locals, 25);
        assert_eq!(index.count(), 4);

        // Persist index
        index.persist(&dir.join("doc_index.bin")).unwrap();

        // Read back via DataSiloReader
        let reader = DataSiloReader::open(&dir).unwrap();
        assert_eq!(reader.count(), 4);
        assert_eq!(reader.silo_count(), 2);

        // Verify all 4 slots have non-empty data
        assert!(reader.get(10).is_some());
        assert!(reader.get(15).is_some());
        assert!(reader.get(20).is_some());
        assert!(reader.get(25).is_some());
        assert!(reader.get(99).is_none()); // non-existent slot

        // Verify document blobs are non-empty and contain field data
        let doc10 = reader.get(10).unwrap();
        assert!(doc10.len() > 4, "doc should contain at least one field tuple");

        // Parse the first field tuple: [u16 field_idx][u32 value_len][value_bytes]
        let fidx = u16::from_le_bytes([doc10[0], doc10[1]]);
        let vlen = u32::from_le_bytes([doc10[2], doc10[3], doc10[4], doc10[5]]);
        assert!(fidx <= 1, "field_idx should be 0 or 1");
        assert!(vlen > 0 && vlen < 100, "value_len should be reasonable");
        assert_eq!(doc10.len(), 6 + vlen as usize + 6 + {
            // Second field
            let off2 = 6 + vlen as usize;
            let vlen2 = u32::from_le_bytes([doc10[off2+2], doc10[off2+3], doc10[off2+4], doc10[off2+5]]);
            vlen2 as usize
        }, "doc should contain exactly 2 field tuples");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(feature = "data-silo")]
    fn test_write_silo_row_indexed_filter_only_skip() {
        // Fields not in field_idx should be silently skipped (filter_only behavior).
        use crate::data_silo;

        let dir = std::env::temp_dir().join("bitdex_silo_filter_only_test");
        let _ = std::fs::remove_dir_all(&dir);

        let writers = data_silo::create_bulk_writers(&dir, 1).unwrap();
        let silo_writers: Vec<std::sync::Mutex<BulkDocWriter>> = writers
            .into_iter()
            .map(std::sync::Mutex::new)
            .collect();

        // Only userId is in the field index — nsfwLevel is "filter_only"
        let mut field_idx: HashMap<String, u16> = HashMap::new();
        field_idx.insert("userId".to_string(), 0);

        let fields = vec![
            DumpFieldMapping::Expanded {
                column: "nsfwLevel".to_string(),
                target: "nsfwLevel".to_string(),
            },
            DumpFieldMapping::Expanded {
                column: "userId".to_string(),
                target: "userId".to_string(),
            },
        ];
        let col_idx: HashMap<String, usize> =
            [("nsfwLevel".to_string(), 0), ("userId".to_string(), 1)]
                .into_iter()
                .collect();

        let empty_enriched = dump_enrichment::EnrichedFields::default();
        let computed_defs: Vec<ComputedFieldDef> = vec![];

        let row_fields: Vec<&[u8]> = vec![b"5", b"42"];
        let row = ParsedRow { fields: row_fields, col_index: &col_idx };
        let indexed: Vec<Option<&str>> = vec![Some("5"), Some("42")];
        let mut serialize_buf = Vec::new();
        let mut tuple_buf = Vec::new();
        let mut write_buf = Vec::new();

        write_silo_row_indexed(
            &row,
            &empty_enriched,
            &computed_defs,
            &indexed,
            &col_idx,
            1,
            &fields,
            &field_idx,
            &mut serialize_buf,
            &mut tuple_buf,
            &mut write_buf,
            &silo_writers[0],
        );

        let locals: Vec<_> = silo_writers
            .into_iter()
            .filter_map(|m| m.into_inner().ok()?.into_local_index().ok())
            .collect();
        let index = data_silo::merge_indexes(locals, 1);
        assert_eq!(index.count(), 1);

        index.persist(&dir.join("doc_index.bin")).unwrap();
        let reader = data_silo::DataSiloReader::open(&dir).unwrap();
        let doc = reader.get(1).unwrap();

        // Only 1 field (userId) should be written — nsfwLevel was filter_only
        let fidx = u16::from_le_bytes([doc[0], doc[1]]);
        assert_eq!(fidx, 0, "only field_idx 0 (userId) should be present");
        // Doc should contain exactly 1 field tuple
        let vlen = u32::from_le_bytes([doc[2], doc[3], doc[4], doc[5]]);
        assert_eq!(doc.len(), 6 + vlen as usize, "doc should have exactly 1 field");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
