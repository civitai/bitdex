//! Expression evaluator for dump pipeline filter and computed field expressions.
//!
//! Supports the expression types from the Sync V2 design (D7):
//! - Bitfield extraction: `(flags >> 13) & 1 == 1`
//! - Equality: `type = 'Checkpoint'`
//! - Null check: `publishedAtSecs != null`
//! - Max: `max(scannedAtSecs, createdAtSecs)`
//! - Identity: `id` (pass-through column value)
//! - Lookup key: `lookup_key` (enrichment join key)
//! - Boolean inversion: `detected == false`
//! - Compound: `expr1 && expr2`
//!
//! All expressions evaluate against a `CsvRow` (column name → optional string value).

use ahash::AHashMap as HashMap;
use std::fmt;

/// A row of CSV data: column name → optional string value.
/// None means the column was missing or empty.
pub type CsvRow<'a> = HashMap<&'a str, Option<&'a str>>;

/// Result of evaluating an expression.
#[derive(Debug, Clone, PartialEq)]
pub enum ExprValue {
    Bool(bool),
    Int(i64),
    Str(String),
    Null,
}

impl ExprValue {
    /// Coerce to bool. Int 0 / empty string / Null → false, everything else → true.
    pub fn as_bool(&self) -> bool {
        match self {
            ExprValue::Bool(b) => *b,
            ExprValue::Int(n) => *n != 0,
            ExprValue::Str(s) => !s.is_empty(),
            ExprValue::Null => false,
        }
    }

    /// Coerce to i64. Bool true→1/false→0, Str parsed, Null→None.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ExprValue::Int(n) => Some(*n),
            ExprValue::Bool(b) => Some(if *b { 1 } else { 0 }),
            ExprValue::Str(s) => s.parse().ok(),
            ExprValue::Null => None,
        }
    }

    /// Coerce to string.
    pub fn as_str_value(&self) -> Option<&str> {
        match self {
            ExprValue::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ExprValue::Null)
    }
}

/// Parsed expression AST node.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Reference to a CSV column value.
    Column(String),
    /// Integer literal.
    IntLit(i64),
    /// String literal (from 'single quotes').
    StrLit(String),
    /// Boolean literal.
    BoolLit(bool),
    /// Null literal.
    NullLit,
    /// Special: the enrichment join key value (resolved at eval time).
    LookupKey,

    /// `(expr >> shift) & mask`
    BitfieldExtract {
        expr: Box<Expr>,
        shift: u32,
        mask: u64,
    },

    /// `left == right` or `left = right`
    Eq(Box<Expr>, Box<Expr>),
    /// `left != right`
    NotEq(Box<Expr>, Box<Expr>),

    /// `left && right`
    And(Box<Expr>, Box<Expr>),
    /// `left || right`
    Or(Box<Expr>, Box<Expr>),

    /// `max(col1, col2, ...)`
    Max(Vec<String>),
}

/// Context for expression evaluation.
pub struct EvalContext<'a> {
    /// The current CSV row being processed.
    pub row: &'a CsvRow<'a>,
    /// The enrichment join key value (for `lookup_key` expressions).
    pub lookup_key: Option<i64>,
}

/// Column name → index mapping for zero-allocation row access.
/// Build once from CSV headers, reuse for every row in the phase.
pub type ColumnIndex = HashMap<String, usize>;

/// Build a ColumnIndex from CSV header names.
pub fn build_column_index(headers: &[&str]) -> ColumnIndex {
    headers.iter().enumerate().map(|(i, &name)| (name.to_string(), i)).collect()
}

/// Zero-allocation evaluation context using column indices.
/// The row is a slice of parsed fields — no HashMap per row.
pub struct IndexedEvalContext<'a> {
    /// The current CSV row fields (indexed by column position).
    pub fields: &'a [Option<&'a str>],
    /// Column name → index mapping (shared across all rows).
    pub col_idx: &'a ColumnIndex,
    /// The enrichment join key value (for `lookup_key` expressions).
    pub lookup_key: Option<i64>,
}

impl<'a> IndexedEvalContext<'a> {
    /// Look up a column value by name.
    #[inline]
    pub fn get(&self, name: &str) -> Option<&'a str> {
        self.col_idx.get(name)
            .and_then(|&idx| self.fields.get(idx))
            .and_then(|opt| opt.as_deref())
    }
}

impl Expr {
    /// Evaluate the expression against a row context.
    pub fn eval(&self, ctx: &EvalContext) -> ExprValue {
        match self {
            Expr::Column(name) => {
                match ctx.row.get(name.as_str()) {
                    Some(Some(val)) if !val.is_empty() => {
                        // Try to parse as integer first, then keep as string
                        if let Ok(n) = val.parse::<i64>() {
                            ExprValue::Int(n)
                        } else if *val == "true" || *val == "t" {
                            ExprValue::Bool(true)
                        } else if *val == "false" || *val == "f" {
                            ExprValue::Bool(false)
                        } else {
                            ExprValue::Str(val.to_string())
                        }
                    }
                    _ => ExprValue::Null,
                }
            }
            Expr::IntLit(n) => ExprValue::Int(*n),
            Expr::StrLit(s) => ExprValue::Str(s.clone()),
            Expr::BoolLit(b) => ExprValue::Bool(*b),
            Expr::NullLit => ExprValue::Null,
            Expr::LookupKey => match ctx.lookup_key {
                Some(k) => ExprValue::Int(k),
                None => ExprValue::Null,
            },

            Expr::BitfieldExtract { expr, shift, mask } => {
                let val = expr.eval(ctx);
                match val.as_i64() {
                    Some(n) => ExprValue::Int((n >> shift) & (*mask as i64)),
                    None => ExprValue::Null,
                }
            }

            Expr::Eq(left, right) => {
                let l = left.eval(ctx);
                let r = right.eval(ctx);
                // null != null (SQL semantics for filter context)
                if l.is_null() && r.is_null() {
                    // Special case: `col != null` is handled by NotEq
                    // For `col = null`, we check if left is null
                    return ExprValue::Bool(true);
                }
                if l.is_null() || r.is_null() {
                    return ExprValue::Bool(false);
                }
                let result = match (&l, &r) {
                    (ExprValue::Int(a), ExprValue::Int(b)) => a == b,
                    (ExprValue::Str(a), ExprValue::Str(b)) => a == b,
                    (ExprValue::Bool(a), ExprValue::Bool(b)) => a == b,
                    // Cross-type: try i64 comparison
                    _ => l.as_i64() == r.as_i64(),
                };
                ExprValue::Bool(result)
            }

            Expr::NotEq(left, right) => {
                let l = left.eval(ctx);
                let r = right.eval(ctx);
                // `col != null` means "col is not null"
                if r.is_null() {
                    return ExprValue::Bool(!l.is_null());
                }
                if l.is_null() {
                    return ExprValue::Bool(true);
                }
                let result = match (&l, &r) {
                    (ExprValue::Int(a), ExprValue::Int(b)) => a != b,
                    (ExprValue::Str(a), ExprValue::Str(b)) => a != b,
                    (ExprValue::Bool(a), ExprValue::Bool(b)) => a != b,
                    _ => l.as_i64() != r.as_i64(),
                };
                ExprValue::Bool(result)
            }

            Expr::And(left, right) => {
                let l = left.eval(ctx);
                if !l.as_bool() {
                    return ExprValue::Bool(false);
                }
                let r = right.eval(ctx);
                ExprValue::Bool(r.as_bool())
            }

            Expr::Or(left, right) => {
                let l = left.eval(ctx);
                if l.as_bool() {
                    return ExprValue::Bool(true);
                }
                let r = right.eval(ctx);
                ExprValue::Bool(r.as_bool())
            }

            Expr::Max(columns) => {
                let mut max_val: Option<i64> = None;
                for col in columns {
                    if let Some(Some(val)) = ctx.row.get(col.as_str()) {
                        if let Ok(n) = val.parse::<i64>() {
                            max_val = Some(match max_val {
                                Some(cur) => cur.max(n),
                                None => n,
                            });
                        }
                    }
                }
                match max_val {
                    Some(n) => ExprValue::Int(n),
                    None => ExprValue::Null,
                }
            }
        }
    }

    /// Evaluate against an indexed row context (zero-allocation per row).
    /// This is the hot-path method for 107M+ row processing.
    pub fn eval_indexed(&self, ctx: &IndexedEvalContext) -> ExprValue {
        match self {
            Expr::Column(name) => {
                match ctx.get(name) {
                    Some(val) if !val.is_empty() => {
                        if let Ok(n) = val.parse::<i64>() {
                            ExprValue::Int(n)
                        } else if val == "true" || val == "t" {
                            ExprValue::Bool(true)
                        } else if val == "false" || val == "f" {
                            ExprValue::Bool(false)
                        } else {
                            ExprValue::Str(val.to_string())
                        }
                    }
                    _ => ExprValue::Null,
                }
            }
            Expr::IntLit(n) => ExprValue::Int(*n),
            Expr::StrLit(s) => ExprValue::Str(s.clone()),
            Expr::BoolLit(b) => ExprValue::Bool(*b),
            Expr::NullLit => ExprValue::Null,
            Expr::LookupKey => match ctx.lookup_key {
                Some(k) => ExprValue::Int(k),
                None => ExprValue::Null,
            },
            Expr::BitfieldExtract { expr, shift, mask } => {
                match expr.eval_indexed(ctx).as_i64() {
                    Some(n) => ExprValue::Int((n >> shift) & (*mask as i64)),
                    None => ExprValue::Null,
                }
            }
            Expr::Eq(left, right) => {
                let l = left.eval_indexed(ctx);
                let r = right.eval_indexed(ctx);
                if l.is_null() && r.is_null() {
                    return ExprValue::Bool(true);
                }
                if l.is_null() || r.is_null() {
                    return ExprValue::Bool(false);
                }
                let result = match (&l, &r) {
                    (ExprValue::Int(a), ExprValue::Int(b)) => a == b,
                    (ExprValue::Str(a), ExprValue::Str(b)) => a == b,
                    (ExprValue::Bool(a), ExprValue::Bool(b)) => a == b,
                    _ => l.as_i64() == r.as_i64(),
                };
                ExprValue::Bool(result)
            }
            Expr::NotEq(left, right) => {
                let l = left.eval_indexed(ctx);
                let r = right.eval_indexed(ctx);
                if r.is_null() {
                    return ExprValue::Bool(!l.is_null());
                }
                if l.is_null() {
                    return ExprValue::Bool(true);
                }
                let result = match (&l, &r) {
                    (ExprValue::Int(a), ExprValue::Int(b)) => a != b,
                    (ExprValue::Str(a), ExprValue::Str(b)) => a != b,
                    (ExprValue::Bool(a), ExprValue::Bool(b)) => a != b,
                    _ => l.as_i64() != r.as_i64(),
                };
                ExprValue::Bool(result)
            }
            Expr::And(left, right) => {
                if !left.eval_indexed(ctx).as_bool() {
                    return ExprValue::Bool(false);
                }
                ExprValue::Bool(right.eval_indexed(ctx).as_bool())
            }
            Expr::Or(left, right) => {
                if left.eval_indexed(ctx).as_bool() {
                    return ExprValue::Bool(true);
                }
                ExprValue::Bool(right.eval_indexed(ctx).as_bool())
            }
            Expr::Max(columns) => {
                let mut max_val: Option<i64> = None;
                for col in columns {
                    if let Some(val) = ctx.get(col) {
                        if let Ok(n) = val.parse::<i64>() {
                            max_val = Some(match max_val {
                                Some(cur) => cur.max(n),
                                None => n,
                            });
                        }
                    }
                }
                match max_val {
                    Some(n) => ExprValue::Int(n),
                    None => ExprValue::Null,
                }
            }
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(name) => write!(f, "{}", name),
            Expr::IntLit(n) => write!(f, "{}", n),
            Expr::StrLit(s) => write!(f, "'{}'", s),
            Expr::BoolLit(b) => write!(f, "{}", b),
            Expr::NullLit => write!(f, "null"),
            Expr::LookupKey => write!(f, "lookup_key"),
            Expr::BitfieldExtract { expr, shift, mask } => {
                write!(f, "({} >> {}) & {}", expr, shift, mask)
            }
            Expr::Eq(l, r) => write!(f, "{} == {}", l, r),
            Expr::NotEq(l, r) => write!(f, "{} != {}", l, r),
            Expr::And(l, r) => write!(f, "{} && {}", l, r),
            Expr::Or(l, r) => write!(f, "{} || {}", l, r),
            Expr::Max(cols) => write!(f, "max({})", cols.join(", ")),
        }
    }
}

// ---- Parser ----

/// Parse an expression string into an AST.
///
/// Supports the dump expression syntax from D7:
/// - `(flags >> 13) & 1 == 1`
/// - `type = 'Checkpoint'`
/// - `publishedAtSecs != null`
/// - `max(scannedAtSecs, createdAtSecs)`
/// - `id` (identity)
/// - `lookup_key`
/// - `detected == false`
/// - `expr1 && expr2`
pub fn parse_expression(input: &str) -> Result<Expr, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("empty expression".into());
    }
    let tokens = tokenize(input)?;
    let (expr, rest) = parse_or(&tokens)?;
    if !rest.is_empty() {
        return Err(format!("unexpected tokens after expression: {:?}", rest));
    }
    Ok(expr)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Int(i64),
    Str(String),
    LParen,
    RParen,
    ShiftRight,  // >>
    Ampersand,   // &
    EqEq,        // ==
    Eq,          // =
    NotEq,       // !=
    AndAnd,      // &&
    OrOr,        // ||
    Comma,
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            b'(' => { tokens.push(Token::LParen); i += 1; }
            b')' => { tokens.push(Token::RParen); i += 1; }
            b',' => { tokens.push(Token::Comma); i += 1; }
            b'>' if i + 1 < bytes.len() && bytes[i + 1] == b'>' => {
                tokens.push(Token::ShiftRight);
                i += 2;
            }
            b'&' if i + 1 < bytes.len() && bytes[i + 1] == b'&' => {
                tokens.push(Token::AndAnd);
                i += 2;
            }
            b'&' => { tokens.push(Token::Ampersand); i += 1; }
            b'=' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                tokens.push(Token::EqEq);
                i += 2;
            }
            b'=' => { tokens.push(Token::Eq); i += 1; }
            b'!' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                tokens.push(Token::NotEq);
                i += 2;
            }
            b'|' if i + 1 < bytes.len() && bytes[i + 1] == b'|' => {
                tokens.push(Token::OrOr);
                i += 2;
            }
            b'\'' => {
                // String literal
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b'\'' {
                    i += 1;
                }
                if i >= bytes.len() {
                    return Err("unterminated string literal".into());
                }
                let s = std::str::from_utf8(&bytes[start..i])
                    .map_err(|e| format!("invalid utf8 in string literal: {}", e))?;
                tokens.push(Token::Str(s.to_string()));
                i += 1; // skip closing quote
            }
            b'0'..=b'9' => {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                let s = std::str::from_utf8(&bytes[start..i]).unwrap();
                let n: i64 = s.parse().map_err(|e| format!("invalid integer: {}", e))?;
                tokens.push(Token::Int(n));
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let s = std::str::from_utf8(&bytes[start..i]).unwrap();
                match s {
                    "null" => tokens.push(Token::Ident("null".into())),
                    "true" => tokens.push(Token::Ident("true".into())),
                    "false" => tokens.push(Token::Ident("false".into())),
                    _ => tokens.push(Token::Ident(s.to_string())),
                }
            }
            _ => return Err(format!("unexpected character: '{}'", bytes[i] as char)),
        }
    }
    Ok(tokens)
}

// Recursive descent parser: or > and > comparison > bitwise > shift > atom

fn parse_or<'a>(tokens: &'a [Token]) -> Result<(Expr, &'a [Token]), String> {
    let (mut left, mut rest) = parse_and(tokens)?;
    while let Some(Token::OrOr) = rest.first() {
        let (right, r) = parse_and(&rest[1..])?;
        left = Expr::Or(Box::new(left), Box::new(right));
        rest = r;
    }
    Ok((left, rest))
}

fn parse_and<'a>(tokens: &'a [Token]) -> Result<(Expr, &'a [Token]), String> {
    let (mut left, mut rest) = parse_comparison(tokens)?;
    while let Some(Token::AndAnd) = rest.first() {
        let (right, r) = parse_comparison(&rest[1..])?;
        left = Expr::And(Box::new(left), Box::new(right));
        rest = r;
    }
    Ok((left, rest))
}

fn parse_comparison<'a>(tokens: &'a [Token]) -> Result<(Expr, &'a [Token]), String> {
    let (left, rest) = parse_bitwise(tokens)?;
    match rest.first() {
        Some(Token::EqEq) | Some(Token::Eq) => {
            let (right, rest) = parse_bitwise(&rest[1..])?;
            Ok((Expr::Eq(Box::new(left), Box::new(right)), rest))
        }
        Some(Token::NotEq) => {
            let (right, rest) = parse_bitwise(&rest[1..])?;
            Ok((Expr::NotEq(Box::new(left), Box::new(right)), rest))
        }
        _ => Ok((left, rest)),
    }
}

fn parse_bitwise<'a>(tokens: &'a [Token]) -> Result<(Expr, &'a [Token]), String> {
    let (left, rest) = parse_shift(tokens)?;
    if let Some(Token::Ampersand) = rest.first() {
        let (right, rest) = parse_shift(&rest[1..])?;
        // `expr & mask` → BitfieldExtract with shift=0 if left isn't already a shift,
        // or wrap an existing shift
        let mask = match &right {
            Expr::IntLit(n) => *n as u64,
            _ => return Err("bitwise AND mask must be an integer literal".into()),
        };
        match left {
            Expr::BitfieldExtract { expr, shift, mask: _ } => {
                Ok((Expr::BitfieldExtract { expr, shift, mask }, rest))
            }
            other => {
                Ok((Expr::BitfieldExtract { expr: Box::new(other), shift: 0, mask }, rest))
            }
        }
    } else {
        Ok((left, rest))
    }
}

fn parse_shift<'a>(tokens: &'a [Token]) -> Result<(Expr, &'a [Token]), String> {
    let (left, rest) = parse_atom(tokens)?;
    if let Some(Token::ShiftRight) = rest.first() {
        let (right, rest) = parse_atom(&rest[1..])?;
        let shift = match &right {
            Expr::IntLit(n) => *n as u32,
            _ => return Err("shift amount must be an integer literal".into()),
        };
        // Create BitfieldExtract with mask=u64::MAX (will be replaced by & operator)
        Ok((Expr::BitfieldExtract { expr: Box::new(left), shift, mask: u64::MAX }, rest))
    } else {
        Ok((left, rest))
    }
}

fn parse_atom<'a>(tokens: &'a [Token]) -> Result<(Expr, &'a [Token]), String> {
    match tokens.first() {
        Some(Token::LParen) => {
            let (expr, rest) = parse_or(&tokens[1..])?;
            match rest.first() {
                Some(Token::RParen) => Ok((expr, &rest[1..])),
                _ => Err("expected closing parenthesis".into()),
            }
        }
        Some(Token::Int(n)) => Ok((Expr::IntLit(*n), &tokens[1..])),
        Some(Token::Str(s)) => Ok((Expr::StrLit(s.clone()), &tokens[1..])),
        Some(Token::Ident(name)) => {
            match name.as_str() {
                "null" => Ok((Expr::NullLit, &tokens[1..])),
                "true" => Ok((Expr::BoolLit(true), &tokens[1..])),
                "false" => Ok((Expr::BoolLit(false), &tokens[1..])),
                "lookup_key" => Ok((Expr::LookupKey, &tokens[1..])),
                "max" => {
                    // max(col1, col2, ...)
                    let rest = &tokens[1..];
                    match rest.first() {
                        Some(Token::LParen) => {
                            let mut args = Vec::new();
                            let mut r = &rest[1..];
                            loop {
                                match r.first() {
                                    Some(Token::Ident(col)) => {
                                        args.push(col.clone());
                                        r = &r[1..];
                                    }
                                    _ => return Err("expected column name in max()".into()),
                                }
                                match r.first() {
                                    Some(Token::Comma) => r = &r[1..],
                                    Some(Token::RParen) => {
                                        r = &r[1..];
                                        break;
                                    }
                                    _ => return Err("expected ',' or ')' in max()".into()),
                                }
                            }
                            if args.is_empty() {
                                return Err("max() requires at least one argument".into());
                            }
                            Ok((Expr::Max(args), r))
                        }
                        _ => Err("expected '(' after max".into()),
                    }
                }
                _ => Ok((Expr::Column(name.clone()), &tokens[1..])),
            }
        }
        None => Err("unexpected end of expression".into()),
        other => Err(format!("unexpected token: {:?}", other)),
    }
}

// ---- Convenience types for dump config ----

/// A parsed filter expression. Evaluates to bool.
#[derive(Debug, Clone)]
pub struct FilterExpression {
    pub expr: Expr,
    pub source: String,
}

impl FilterExpression {
    /// Parse a filter expression string.
    pub fn parse(source: &str) -> Result<Self, String> {
        let expr = parse_expression(source)?;
        Ok(Self { expr, source: source.to_string() })
    }

    /// Evaluate the filter against a row. Returns true if the row passes.
    pub fn eval(&self, row: &CsvRow, lookup_key: Option<i64>) -> bool {
        let ctx = EvalContext { row, lookup_key };
        self.expr.eval(&ctx).as_bool()
    }

    /// Evaluate against an indexed row (zero-allocation hot path).
    #[inline]
    pub fn eval_indexed(&self, fields: &[Option<&str>], col_idx: &ColumnIndex, lookup_key: Option<i64>) -> bool {
        let ctx = IndexedEvalContext { fields, col_idx, lookup_key };
        self.expr.eval_indexed(&ctx).as_bool()
    }
}

/// A parsed computed field definition.
#[derive(Debug, Clone)]
pub struct ComputedFieldDef {
    /// Target field name in the index.
    pub target: String,
    /// The expression to evaluate.
    pub expr: Expr,
    /// Original expression source string.
    pub source: String,
    /// For conditional multi-value: the column to take the value from.
    /// When set, `expr` is a filter (bool), and `value_column` provides the actual value.
    /// Example: `detected == false` with value=modelVersionId → add to modelVersionIdsManual.
    pub value_column: Option<String>,
}

impl ComputedFieldDef {
    /// Parse a computed field definition.
    pub fn parse(target: &str, expression: &str, value_column: Option<&str>) -> Result<Self, String> {
        let expr = parse_expression(expression)?;
        Ok(Self {
            target: target.to_string(),
            expr,
            source: expression.to_string(),
            value_column: value_column.map(|s| s.to_string()),
        })
    }

    /// Evaluate the computed field for a row.
    ///
    /// Returns `Some(value)` if the field should be set, `None` if it should be skipped.
    /// For conditional fields (value_column set), returns the value from that column
    /// only when the expression evaluates to true.
    pub fn eval(&self, row: &CsvRow, lookup_key: Option<i64>) -> Option<ExprValue> {
        let ctx = EvalContext { row, lookup_key };

        if let Some(ref value_col) = self.value_column {
            // Conditional: expression is a filter, value comes from column
            if self.expr.eval(&ctx).as_bool() {
                match row.get(value_col.as_str()) {
                    Some(Some(val)) if !val.is_empty() => {
                        if let Ok(n) = val.parse::<i64>() {
                            Some(ExprValue::Int(n))
                        } else {
                            Some(ExprValue::Str(val.to_string()))
                        }
                    }
                    _ => None,
                }
            } else {
                None
            }
        } else {
            // Standard: expression IS the value
            let val = self.expr.eval(&ctx);
            if val.is_null() {
                None
            } else {
                Some(val)
            }
        }
    }

    /// Evaluate against an indexed row (zero-allocation hot path).
    pub fn eval_indexed(&self, fields: &[Option<&str>], col_idx: &ColumnIndex, lookup_key: Option<i64>) -> Option<ExprValue> {
        let ctx = IndexedEvalContext { fields, col_idx, lookup_key };

        if let Some(ref value_col) = self.value_column {
            if self.expr.eval_indexed(&ctx).as_bool() {
                match ctx.get(value_col) {
                    Some(val) if !val.is_empty() => {
                        if let Ok(n) = val.parse::<i64>() {
                            Some(ExprValue::Int(n))
                        } else {
                            Some(ExprValue::Str(val.to_string()))
                        }
                    }
                    _ => None,
                }
            } else {
                None
            }
        } else {
            let val = self.expr.eval_indexed(&ctx);
            if val.is_null() {
                None
            } else {
                Some(val)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row<'a>(pairs: &[(&'a str, &'a str)]) -> CsvRow<'a> {
        pairs.iter().map(|&(k, v)| (k, Some(v))).collect()
    }

    fn make_row_with_nulls<'a>(pairs: &[(&'a str, Option<&'a str>)]) -> CsvRow<'a> {
        pairs.iter().cloned().collect()
    }

    // --- Tokenizer tests ---

    #[test]
    fn test_tokenize_bitfield() {
        let tokens = tokenize("(flags >> 13) & 1 == 1").unwrap();
        assert_eq!(tokens.len(), 9);
        assert_eq!(tokens[0], Token::LParen);
        assert_eq!(tokens[1], Token::Ident("flags".into()));
        assert_eq!(tokens[2], Token::ShiftRight);
        assert_eq!(tokens[3], Token::Int(13));
    }

    #[test]
    fn test_tokenize_string_literal() {
        let tokens = tokenize("type = 'Checkpoint'").unwrap();
        assert_eq!(tokens[2], Token::Str("Checkpoint".into()));
    }

    // --- Parser tests ---

    #[test]
    fn test_parse_identity() {
        let expr = parse_expression("id").unwrap();
        assert!(matches!(expr, Expr::Column(ref s) if s == "id"));
    }

    #[test]
    fn test_parse_lookup_key() {
        let expr = parse_expression("lookup_key").unwrap();
        assert!(matches!(expr, Expr::LookupKey));
    }

    #[test]
    fn test_parse_null_check() {
        let expr = parse_expression("publishedAtSecs != null").unwrap();
        assert!(matches!(expr, Expr::NotEq(_, _)));
    }

    #[test]
    fn test_parse_equality() {
        let expr = parse_expression("type = 'Checkpoint'").unwrap();
        assert!(matches!(expr, Expr::Eq(_, _)));
    }

    #[test]
    fn test_parse_boolean_check() {
        let expr = parse_expression("detected == false").unwrap();
        assert!(matches!(expr, Expr::Eq(_, _)));
    }

    #[test]
    fn test_parse_bitfield() {
        let expr = parse_expression("(flags >> 13) & 1 == 1").unwrap();
        // Should be: Eq(BitfieldExtract{flags, 13, 1}, IntLit(1))
        match expr {
            Expr::Eq(left, right) => {
                assert!(matches!(*left, Expr::BitfieldExtract { shift: 13, mask: 1, .. }));
                assert!(matches!(*right, Expr::IntLit(1)));
            }
            _ => panic!("expected Eq, got {:?}", expr),
        }
    }

    #[test]
    fn test_parse_compound_bitfield() {
        let expr = parse_expression("(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0").unwrap();
        assert!(matches!(expr, Expr::And(_, _)));
    }

    #[test]
    fn test_parse_max() {
        let expr = parse_expression("max(scannedAtSecs, createdAtSecs)").unwrap();
        match expr {
            Expr::Max(cols) => {
                assert_eq!(cols, vec!["scannedAtSecs", "createdAtSecs"]);
            }
            _ => panic!("expected Max"),
        }
    }

    // --- Evaluator tests ---

    #[test]
    fn test_eval_identity() {
        let expr = parse_expression("id").unwrap();
        let row = make_row(&[("id", "12345")]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Int(12345));
    }

    #[test]
    fn test_eval_lookup_key() {
        let expr = parse_expression("lookup_key").unwrap();
        let row = CsvRow::new();
        let ctx = EvalContext { row: &row, lookup_key: Some(42) };
        assert_eq!(expr.eval(&ctx), ExprValue::Int(42));
    }

    #[test]
    fn test_eval_null_check_present() {
        let expr = parse_expression("publishedAtSecs != null").unwrap();
        let row = make_row(&[("publishedAtSecs", "1700000000")]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Bool(true));
    }

    #[test]
    fn test_eval_null_check_absent() {
        let expr = parse_expression("publishedAtSecs != null").unwrap();
        let row = make_row_with_nulls(&[("publishedAtSecs", None)]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Bool(false));
    }

    #[test]
    fn test_eval_equality_string() {
        let expr = parse_expression("type = 'Checkpoint'").unwrap();
        let row = make_row(&[("type", "Checkpoint")]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Bool(true));
    }

    #[test]
    fn test_eval_equality_string_mismatch() {
        let expr = parse_expression("type = 'Checkpoint'").unwrap();
        let row = make_row(&[("type", "LORA")]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Bool(false));
    }

    #[test]
    fn test_eval_boolean_false() {
        let expr = parse_expression("detected == false").unwrap();
        let row = make_row(&[("detected", "false")]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Bool(true));
    }

    #[test]
    fn test_eval_boolean_true() {
        let expr = parse_expression("detected == false").unwrap();
        let row = make_row(&[("detected", "true")]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Bool(false));
    }

    #[test]
    fn test_eval_bitfield_set() {
        // (flags >> 13) & 1 == 1
        let expr = parse_expression("(flags >> 13) & 1 == 1").unwrap();
        let flags = (1i64 << 13).to_string();
        let row = make_row(&[("flags", &flags)]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Bool(true));
    }

    #[test]
    fn test_eval_bitfield_unset() {
        let expr = parse_expression("(flags >> 13) & 1 == 1").unwrap();
        let row = make_row(&[("flags", "0")]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Bool(false));
    }

    #[test]
    fn test_eval_compound_bitfield() {
        // hasMeta: (flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0
        let expr = parse_expression("(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0").unwrap();
        // bit 13 set, bit 2 NOT set → true
        let flags = (1i64 << 13).to_string();
        let row = make_row(&[("flags", &flags)]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Bool(true));

        // bit 13 set, bit 2 ALSO set → false
        let flags2 = ((1i64 << 13) | (1i64 << 2)).to_string();
        let row2 = make_row(&[("flags", &flags2)]);
        let ctx2 = EvalContext { row: &row2, lookup_key: None };
        assert_eq!(expr.eval(&ctx2), ExprValue::Bool(false));
    }

    #[test]
    fn test_eval_max() {
        let expr = parse_expression("max(scannedAtSecs, createdAtSecs)").unwrap();
        let row = make_row(&[("scannedAtSecs", "1000"), ("createdAtSecs", "2000")]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Int(2000));
    }

    #[test]
    fn test_eval_max_with_null() {
        let expr = parse_expression("max(scannedAtSecs, createdAtSecs)").unwrap();
        let row = make_row_with_nulls(&[
            ("scannedAtSecs", None),
            ("createdAtSecs", Some("2000")),
        ]);
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Int(2000));
    }

    // --- Filter expression tests ---

    #[test]
    fn test_filter_disabled_tags() {
        // (attributes >> 10) & 1 = 0 — skip disabled tags (filter returns true to include)
        let filter = FilterExpression::parse("(attributes >> 10) & 1 = 0").unwrap();

        // Not disabled (bit 10 not set) → include
        let row = make_row(&[("attributes", "0")]);
        assert!(filter.eval(&row, None));

        // Disabled (bit 10 set) → exclude
        let disabled = (1i64 << 10).to_string();
        let row2 = make_row(&[("attributes", &disabled)]);
        assert!(!filter.eval(&row2, None));
    }

    // --- Computed field tests ---

    #[test]
    fn test_computed_has_meta() {
        let cf = ComputedFieldDef::parse("hasMeta", "(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0", None).unwrap();
        let flags = (1i64 << 13).to_string();
        let row = make_row(&[("flags", &flags)]);
        assert_eq!(cf.eval(&row, None), Some(ExprValue::Bool(true)));
    }

    #[test]
    fn test_computed_is_published() {
        let cf = ComputedFieldDef::parse("isPublished", "publishedAtSecs != null", None).unwrap();
        let row = make_row(&[("publishedAtSecs", "1700000000")]);
        assert_eq!(cf.eval(&row, None), Some(ExprValue::Bool(true)));

        let row2 = make_row_with_nulls(&[("publishedAtSecs", None)]);
        // false is not null, so it should return Some(Bool(false))
        assert_eq!(cf.eval(&row2, None), Some(ExprValue::Bool(false)));
    }

    #[test]
    fn test_computed_posted_to_id() {
        let cf = ComputedFieldDef::parse("postedToId", "lookup_key", None).unwrap();
        let row = CsvRow::new();
        assert_eq!(cf.eval(&row, Some(999)), Some(ExprValue::Int(999)));
    }

    #[test]
    fn test_computed_conditional_multi_value() {
        // modelVersionIdsManual: detected == false, value = modelVersionId
        let cf = ComputedFieldDef::parse(
            "modelVersionIdsManual",
            "detected == false",
            Some("modelVersionId"),
        ).unwrap();

        // detected=false → include with modelVersionId value
        let row = make_row(&[("detected", "false"), ("modelVersionId", "42")]);
        assert_eq!(cf.eval(&row, None), Some(ExprValue::Int(42)));

        // detected=true → skip
        let row2 = make_row(&[("detected", "true"), ("modelVersionId", "42")]);
        assert_eq!(cf.eval(&row2, None), None);
    }

    #[test]
    fn test_computed_max_sort() {
        let cf = ComputedFieldDef::parse("existedAt", "max(scannedAtSecs, createdAtSecs)", None).unwrap();
        let row = make_row(&[("scannedAtSecs", "100"), ("createdAtSecs", "200")]);
        assert_eq!(cf.eval(&row, None), Some(ExprValue::Int(200)));
    }

    #[test]
    fn test_computed_identity() {
        let cf = ComputedFieldDef::parse("id", "id", None).unwrap();
        let row = make_row(&[("id", "12345")]);
        assert_eq!(cf.eval(&row, None), Some(ExprValue::Int(12345)));
    }

    // --- Error handling tests ---

    #[test]
    fn test_parse_empty() {
        assert!(parse_expression("").is_err());
    }

    #[test]
    fn test_parse_unterminated_string() {
        assert!(parse_expression("type = 'Checkpoint").is_err());
    }

    #[test]
    fn test_parse_unmatched_paren() {
        assert!(parse_expression("(flags >> 13").is_err());
    }

    #[test]
    fn test_eval_missing_column() {
        let expr = parse_expression("missing_col").unwrap();
        let row = CsvRow::new();
        let ctx = EvalContext { row: &row, lookup_key: None };
        assert_eq!(expr.eval(&ctx), ExprValue::Null);
    }
}
