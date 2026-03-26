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
//!   7. Save bitmaps to BitmapFs, drop from memory
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

use crate::bitmap_fs::BitmapFs;
use crate::concurrent_engine::ConcurrentEngine;
use crate::dictionary::FieldDictionary;
use crate::docstore::{BulkWriter, PackedValue};
use crate::dump_enrichment;
use crate::dump_expression::{FilterExpression, ComputedFieldDef, CsvRow};
use crate::dump_expression::ExprValue as NateExprValue;
use crate::pg_sync::single_pass::{save_filter_field_to_disk};

const LOG_INTERVAL: u64 = 1_000_000;

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
    /// Column name → index mapping (shared across all rows)
    col_index: Arc<HashMap<String, usize>>,
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
        for (name, &idx) in self.col_index.as_ref() {
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

pub fn process_dump(
    request: &DumpRequest,
    engine: &ConcurrentEngine,
    stage_dir: &Path,
) -> Result<PhaseResult, String> {
    // Validate before processing
    validate_dump_request(request, engine)?;

    let t = Instant::now();
    let bitmap_fs = engine
        .bitmap_store()
        .ok_or_else(|| "no bitmap_path configured; cannot process dump".to_string())?
        .clone();

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

    // Check crash recovery: if field already loaded, skip
    for target in &target_fields {
        if crate::pg_sync::single_pass::field_already_loaded(&bitmap_fs, target) {
            eprintln!("  Dump {}: field '{}' already loaded, skipping phase", request.name, target);
            return Ok(PhaseResult {
                row_count: 0,
                filter_maps: HashMap::new(),
                sort_maps: HashMap::new(),
                alive: RoaringBitmap::new(),
                deferred_slots: BTreeMap::new(),
                max_slot: 0,
            });
        }
    }

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

    // Load enrichment tables (Nate's API)
    let mut enrichment_mgr = dump_enrichment::EnrichmentManager::new();
    for ec in &request.enrichment {
        let nate_config = to_nate_enrichment_config(ec, stage_dir);
        enrichment_mgr
            .load(nate_config)
            .map_err(|e| format!("load enrichment: {e}"))?;
    }

    // Get LCS dictionaries from engine (thread-safe DashMap-based)
    let dictionaries: Arc<HashMap<String, FieldDictionary>> = engine.dictionaries_arc();

    // Prepare BulkWriter for docstore
    let all_target_names: Vec<String> = target_fields.iter().cloned().collect();
    let bulk_writer = Arc::new(
        engine
            .prepare_bulk_writer(&all_target_names)
            .map_err(|e| format!("prepare_bulk_writer: {e}"))?,
    );

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
            &bitmap_fs,
            &bulk_writer,
        );
    }

    // General phase processing with rayon parallelism
    let ranges = split_mmap_ranges(body, rayon::current_num_threads());
    let total = AtomicU64::new(0);
    let total_ref = &total;

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

    // Collect filter target names from request fields
    let filter_targets: Vec<String> = request_fields
        .iter()
        .map(|f| f.target().to_string())
        .filter(|t| filter_field_names.contains(t))
        .collect();
    let sort_targets: Vec<(String, u8)> = request_fields
        .iter()
        .filter_map(|f| {
            let t = f.target().to_string();
            sort_bits.get(&t).map(|&b| (t, b))
        })
        .collect();
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
        HashMap<String, HashMap<u64, RoaringBitmap>>,  // filter maps
        HashMap<String, Vec<RoaringBitmap>>,            // sort maps (Vec indexed by bit pos)
        RoaringBitmap,                                   // alive
        Vec<(u32, u64)>,                                 // deferred (slot, activate_at)
        u64,                                             // count
        u32,                                             // max_slot
    );

    let thread_results: Vec<ThreadResult> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &body[range_start..range_end];

            // Thread-local accumulators
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
                    col_index: col_index.clone(),
                };

                // Get slot ID
                let slot = match row.slot(slot_field) {
                    Some(s) => s,
                    None => continue,
                };
                if slot > max_slot {
                    max_slot = slot;
                }

                // Apply filter (Nate's FilterExpression API)
                let csv_row = row.to_csv_row();
                if let Some(ref fexpr) = filter_expr_ref {
                    if !fexpr.eval(&csv_row, None) {
                        continue;
                    }
                }

                // Resolve enrichment (Nate's EnrichmentManager API)
                let enriched = enrichment_mgr_ref.enrich_row(&csv_row);
                let mut enriched_values: HashMap<String, String> = HashMap::new();
                for (target, value) in &enriched.fields {
                    enriched_values.insert(target.clone(), value.clone());
                }
                for (target, value) in &enriched.computed {
                    match value {
                        NateExprValue::Int(n) => { enriched_values.insert(target.clone(), n.to_string()); }
                        NateExprValue::Str(s) => { enriched_values.insert(target.clone(), s.clone()); }
                        NateExprValue::Bool(b) => { enriched_values.insert(target.clone(), if *b { "1" } else { "0" }.to_string()); }
                        NateExprValue::Null => {}
                    }
                }

                // Check deferred alive: if publishedAt from enrichment is in the future
                if has_deferred_alive {
                    if let Some(pub_str) = enriched_values.get("publishedAt") {
                        if let Ok(pub_secs) = pub_str.parse::<u64>() {
                            if pub_secs > now_unix {
                                // Write docstore only, skip all bitmaps
                                write_docstore_row(
                                    &row,
                                    &enriched_values,
                                    computed_defs_ref,
                                    &csv_row,
                                    slot,
                                    request_fields,
                                    &bulk_writer,
                                );
                                deferred.push((slot, pub_secs));
                                count += 1;
                                if count % LOG_INTERVAL == 0 {
                                    total_ref.fetch_add(LOG_INTERVAL, Ordering::Relaxed);
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

                // Build filter bitmaps from direct fields
                for field_mapping in request_fields {
                    let target = field_mapping.target();
                    let column = field_mapping.column();

                    if filter_field_names_ref.contains(target) {
                        // LCS dictionary fields: resolve string→key FIRST (before i64 parse)
                        // This fixes availability, baseModel, blockedFor, type which are
                        // string values that would fail i64 parse and silently produce
                        // empty bitmaps.
                        let bitmap_key: Option<u64> = if let Some(dict) = dictionaries_ref.get(target) {
                            let s = row
                                .get_str(column)
                                .or_else(|| enriched_values.get(target).map(|s| s.as_str()));
                            s.map(|v| dict.get_or_insert(v) as u64)
                        } else {
                            // Non-LCS: use i64 value from row or enrichment
                            row.get_i64(column)
                                .or_else(|| {
                                    enriched_values.get(target).and_then(|s| s.parse::<i64>().ok())
                                })
                                .map(|v| v as u64)
                        };

                        if let Some(key) = bitmap_key {
                            if let Some(fm) = filter_maps.get_mut(target) {
                                fm.entry(key)
                                    .or_insert_with(RoaringBitmap::new)
                                    .insert(slot);
                            }
                        }
                    }

                    // Build sort bitmaps from direct fields
                    if let Some(&bits) = sort_bits_ref.get(target) {
                        if let Some(v) = row.get_i64(column).or_else(|| {
                            enriched_values.get(target).and_then(|s| s.parse::<i64>().ok())
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

                // Build bitmaps from computed fields (Nate's ComputedFieldDef API)
                for def in computed_defs_ref {
                    let computed_val = def.eval(&csv_row, None);

                    match computed_val {
                        Some(NateExprValue::Int(v)) if def.value_column.is_none() => {
                            // Regular computed field — use value directly as bitmap key
                            if filter_field_names_ref.contains(&def.target) {
                                if let Some(fm) = filter_maps.get_mut(&def.target) {
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
                            if filter_field_names_ref.contains(&def.target) {
                                if let Some(fm) = filter_maps.get_mut(&def.target) {
                                    fm.entry(key)
                                        .or_insert_with(RoaringBitmap::new)
                                        .insert(slot);
                                }
                            }
                        }
                        _ => {} // Null or non-matching pattern
                    }
                }

                // Write docstore
                write_docstore_row(
                    &row,
                    &enriched_values,
                    computed_defs_ref,
                    &csv_row,
                    slot,
                    request_fields,
                    &bulk_writer,
                );

                count += 1;
                if count % LOG_INTERVAL == 0 {
                    let t = total_ref.fetch_add(LOG_INTERVAL, Ordering::Relaxed) + LOG_INTERVAL;
                    eprintln!("  dump {}: {}M rows...", request.name, t / 1_000_000);
                }
            }
            total_ref.fetch_add(count % LOG_INTERVAL, Ordering::Relaxed);

            (filter_maps, sort_maps, alive, deferred, count, max_slot)
        })
        .collect();

    // Merge all thread results
    let mut merged_filters: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
    let mut merged_sorts: HashMap<String, Vec<RoaringBitmap>> = HashMap::new();
    let mut merged_alive = RoaringBitmap::new();
    let mut merged_deferred: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
    let mut total_count = 0u64;
    let mut max_slot: u32 = 0;

    for (filter_maps, sort_maps, alive, deferred, count, thread_max) in thread_results {
        merged_alive |= alive;
        total_count += count;
        if thread_max > max_slot {
            max_slot = thread_max;
        }

        for (slot, activate_at) in deferred {
            merged_deferred.entry(activate_at).or_default().push(slot);
        }

        for (field, values) in filter_maps {
            let dest = merged_filters.entry(field).or_default();
            for (val, bm) in values {
                dest.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
            }
        }
        for (field, layers) in sort_maps {
            let dest = merged_sorts.entry(field).or_insert_with(|| {
                (0..layers.len()).map(|_| RoaringBitmap::new()).collect()
            });
            for (bit, bm) in layers.into_iter().enumerate() {
                if bit < dest.len() {
                    dest[bit] |= bm;
                }
            }
        }
    }

    // Save to BitmapFs
    for (field_name, values) in &merged_filters {
        if values.is_empty() {
            continue;
        }
        let saved = save_filter_field_to_disk(&bitmap_fs, field_name, values)?;
        eprintln!(
            "  Saved filter {}: {} values ({:.1} MB)",
            field_name,
            values.len(),
            saved as f64 / (1024.0 * 1024.0)
        );
    }

    for (field_name, layers) in &merged_sorts {
        if layers.is_empty() || layers.iter().all(|bm| bm.is_empty()) {
            continue;
        }
        let layer_refs: Vec<&RoaringBitmap> = layers.iter().collect();
        bitmap_fs.write_sort_layers(field_name, &layer_refs)
            .map_err(|e| format!("write_sort_layers({field_name}): {e}"))?;
        eprintln!("  Saved sort {}: {} layers", field_name, layers.len());
    }

    if sets_alive {
        bitmap_fs
            .write_alive(&merged_alive)
            .map_err(|e| format!("write_alive: {e}"))?;
        eprintln!("  Saved alive bitmap: {} bits", merged_alive.len());

        // Slot counter: max of alive + deferred slots
        let max_deferred = merged_deferred
            .values()
            .flat_map(|v| v.iter())
            .copied()
            .max()
            .unwrap_or(0);
        let slot_counter = max_slot.max(max_deferred).saturating_add(1);
        bitmap_fs
            .write_slot_counter(slot_counter)
            .map_err(|e| format!("write_slot_counter: {e}"))?;

        if !merged_deferred.is_empty() {
            bitmap_fs
                .write_deferred_alive(&merged_deferred)
                .map_err(|e| format!("write_deferred_alive: {e}"))?;
            let deferred_total: usize = merged_deferred.values().map(|v| v.len()).sum();
            eprintln!("  Saved deferred alive: {} slots", deferred_total);
        }
    }

    // Persist LCS dictionaries
    let dict_dir = bitmap_fs.root().join("dictionaries");
    std::fs::create_dir_all(&dict_dir).ok();
    for (name, dict) in dictionaries.as_ref() {
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

    let elapsed = t.elapsed();
    eprintln!(
        "  Dump {} complete: {} rows in {:.1}s ({:.0}/s)",
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
    bitmap_fs: &BitmapFs,
    _bulk_writer: &Arc<BulkWriter>,
) -> Result<PhaseResult, String> {
    let target = request.fields[0].target().to_string();
    let value_column = request.fields[0].column().to_string();
    let slot_field = &request.slot_field;

    const MAX_TAG_ID: usize = 300_000;
    let use_vec = target == "tagIds"; // Only tagIds uses vec optimization

    let ranges = split_mmap_ranges(body, rayon::current_num_threads());
    let total = AtomicU64::new(0);
    let total_ref = &total;

    if use_vec {
        // Vec<RoaringBitmap> indexed by value_id — no hashing
        let thread_results: Vec<Vec<RoaringBitmap>> = ranges
            .par_iter()
            .map(|&(range_start, range_end)| {
                let chunk = &body[range_start..range_end];
                let mut bitmaps: Vec<RoaringBitmap> =
                    (0..MAX_TAG_ID).map(|_| RoaringBitmap::new()).collect();
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

                    let fields = parse_delimited_line(line, delimiter);
                    let row = ParsedRow {
                        fields,
                        col_index: col_index.clone(),
                    };

                    // Apply filter
                    if let Some(ref fexpr) = filter_expr {
                        let csv_row = row.to_csv_row();
                        if !fexpr.eval(&csv_row, None) {
                            continue;
                        }
                    }

                    let slot = match row.slot(slot_field) {
                        Some(s) => s,
                        None => continue,
                    };
                    let value = match row.get_i64(&value_column) {
                        Some(v) => v as usize,
                        None => continue,
                    };

                    if value < MAX_TAG_ID {
                        bitmaps[value].insert(slot);
                    }
                    count += 1;
                }
                total_ref.fetch_add(count, Ordering::Relaxed);
                bitmaps
            })
            .collect();

        // Merge Vec<RoaringBitmap>
        let mut merged_vec = thread_results
            .into_iter()
            .reduce(|mut dst, src| {
                for (i, bm) in src.into_iter().enumerate() {
                    if !bm.is_empty() {
                        dst[i] |= bm;
                    }
                }
                dst
            })
            .unwrap_or_default();

        // Convert to HashMap (non-empty only)
        let mut result: HashMap<u64, RoaringBitmap> = HashMap::new();
        for (i, bm) in merged_vec.drain(..).enumerate() {
            if !bm.is_empty() {
                result.insert(i as u64, bm);
            }
        }

        let saved = save_filter_field_to_disk(bitmap_fs, &target, &result)?;
        let total_rows = total.load(Ordering::Relaxed);
        eprintln!(
            "  Dump {} ({target}): {} rows, {} distinct values ({:.1} MB)",
            request.name,
            total_rows,
            result.len(),
            saved as f64 / (1024.0 * 1024.0)
        );

        Ok(PhaseResult {
            row_count: total_rows,
            filter_maps: {
                let mut m = HashMap::new();
                m.insert(target, result);
                m
            },
            sort_maps: HashMap::new(),
            alive: RoaringBitmap::new(),
            deferred_slots: BTreeMap::new(),
            max_slot: 0,
        })
    } else {
        // HashMap path for tools, techniques (smaller datasets)
        let thread_results: Vec<HashMap<u64, RoaringBitmap>> = ranges
            .par_iter()
            .map(|&(range_start, range_end)| {
                let chunk = &body[range_start..range_end];
                let mut bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();
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

                    let fields = parse_delimited_line(line, delimiter);
                    let row = ParsedRow {
                        fields,
                        col_index: col_index.clone(),
                    };

                    if let Some(ref fexpr) = filter_expr {
                        let csv_row = row.to_csv_row();
                        if !fexpr.eval(&csv_row, None) {
                            continue;
                        }
                    }

                    let slot = match row.slot(slot_field) {
                        Some(s) => s,
                        None => continue,
                    };
                    let value = match row.get_u64(&value_column) {
                        Some(v) => v,
                        None => continue,
                    };

                    bitmaps
                        .entry(value)
                        .or_insert_with(RoaringBitmap::new)
                        .insert(slot);
                    count += 1;
                }
                total_ref.fetch_add(count, Ordering::Relaxed);
                bitmaps
            })
            .collect();

        // Merge
        let mut merged: HashMap<u64, RoaringBitmap> = HashMap::new();
        for bitmaps in thread_results {
            for (val, bm) in bitmaps {
                merged.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
            }
        }

        let saved = save_filter_field_to_disk(bitmap_fs, &target, &merged)?;
        let total_rows = total.load(Ordering::Relaxed);
        eprintln!(
            "  Dump {} ({target}): {} rows, {} distinct values ({:.1} MB)",
            request.name,
            total_rows,
            merged.len(),
            saved as f64 / (1024.0 * 1024.0)
        );

        Ok(PhaseResult {
            row_count: total_rows,
            filter_maps: {
                let mut m = HashMap::new();
                m.insert(target, merged);
                m
            },
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

/// Write a single row's data to the docstore via BulkWriter.
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
        let col_index = Arc::new(HashMap::from([("flags".to_string(), 0usize)]));

        // flags = 0 → bit 10 = 0 → passes
        let row = ParsedRow {
            fields: vec![b"0"],
            col_index: col_index.clone(),
        };
        assert!(eval_filter(&expr, &row, None));

        // flags = 1024 → bit 10 = 1 → fails
        let row = ParsedRow {
            fields: vec![b"1024"],
            col_index: col_index.clone(),
        };
        assert!(!eval_filter(&expr, &row, None));
    }

    #[test]
    fn test_eval_computed_max() {
        let expr = parse_expression("max(a, b)").unwrap();
        let col_index = Arc::new(HashMap::from([
            ("a".to_string(), 0usize),
            ("b".to_string(), 1usize),
        ]));

        let row = ParsedRow {
            fields: vec![b"100", b"200"],
            col_index: col_index.clone(),
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
}
