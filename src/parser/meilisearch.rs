//! Meilisearch-compatible query parser.
//!
//! Parses Meilisearch filter string expressions into BitDex FilterClauses.
//!
//! ```json
//! {
//!   "filter": "nsfwLevel = 1 AND tagIds IN [12, 45] AND type = 'image'",
//!   "sort": ["reactionCount:desc"],
//!   "limit": 50,
//!   "offset": 0
//! }
//! ```
//!
//! Supported filter syntax:
//! - Equality: `field = value`, `field != value`
//! - Comparison: `field > value`, `field >= value`, `field < value`, `field <= value`
//! - Range: `field min TO max` (inclusive)
//! - Multi-value: `field IN [v1, v2, ...]`, `field NOT IN [v1, v2, ...]`
//! - Boolean: `NOT expr`, `expr AND expr`, `expr OR expr`
//! - Grouping: `(expr)`
//!
//! Precedence: NOT > AND > OR. Parentheses override.

use serde_json::Value as JsonValue;

use crate::query::{
    BitdexQuery, FilterClause, ParseError, QueryParser, SortClause, SortDirection,
    Value,
};

const DEFAULT_LIMIT: usize = 20; // Meilisearch default
const MAX_LIMIT: usize = 10_000;

pub struct MeilisearchQueryParser;

impl QueryParser for MeilisearchQueryParser {
    fn parse(&self, input: &[u8]) -> Result<BitdexQuery, ParseError> {
        let raw: JsonValue = serde_json::from_slice(input)
            .map_err(|e| ParseError::new(format!("invalid JSON: {e}")))?;

        let obj = raw
            .as_object()
            .ok_or_else(|| ParseError::new("query must be a JSON object"))?;

        // Parse filter (string or array format)
        let filters = match obj.get("filter") {
            Some(JsonValue::String(s)) => {
                if s.trim().is_empty() {
                    vec![]
                } else {
                    let clause = parse_filter_string(s)?;
                    vec![clause]
                }
            }
            Some(JsonValue::Array(arr)) => parse_array_filter(arr)?,
            Some(JsonValue::Null) | None => vec![],
            Some(_) => {
                return Err(ParseError::new(
                    "'filter' must be a string or array of strings",
                ))
            }
        };

        // Parse sort: array of "field:asc|desc" strings
        let sort = match obj.get("sort") {
            Some(JsonValue::Array(arr)) => {
                if let Some(first) = arr.first() {
                    let s = first
                        .as_str()
                        .ok_or_else(|| ParseError::new("sort entries must be strings"))?;
                    Some(parse_sort_entry(s)?)
                } else {
                    None
                }
            }
            Some(JsonValue::Null) | None => None,
            Some(_) => return Err(ParseError::new("'sort' must be an array of strings")),
        };

        // Parse limit
        let limit = match obj.get("limit") {
            Some(v) => v
                .as_u64()
                .ok_or_else(|| ParseError::new("'limit' must be a positive integer"))?
                as usize,
            None => DEFAULT_LIMIT,
        }
        .min(MAX_LIMIT);
        if limit == 0 {
            return Err(ParseError::new("limit must be greater than 0"));
        }

        // Parse offset
        let offset = match obj.get("offset") {
            Some(JsonValue::Null) => None,
            Some(v) => Some(
                v.as_u64()
                    .ok_or_else(|| ParseError::new("'offset' must be a non-negative integer"))?
                    as usize,
            ),
            None => None,
        };

        // Parse page/hitsPerPage (alternative pagination)
        let (offset, limit) = if let Some(page_val) = obj.get("page") {
            let page = page_val
                .as_u64()
                .ok_or_else(|| ParseError::new("'page' must be a positive integer"))?
                as usize;
            let hits_per_page = match obj.get("hitsPerPage") {
                Some(v) => v
                    .as_u64()
                    .ok_or_else(|| ParseError::new("'hitsPerPage' must be a positive integer"))?
                    as usize,
                None => 20,
            };
            if page == 0 {
                return Err(ParseError::new("'page' must be >= 1"));
            }
            (Some((page - 1) * hits_per_page), hits_per_page.min(MAX_LIMIT))
        } else {
            (offset, limit)
        };

        // Parse skip_cache (debugging flag)
        let skip_cache = obj
            .get("skip_cache")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(BitdexQuery {
            filters,
            sort,
            limit,
            cursor: None, // Meilisearch uses offset pagination, not cursor
            offset,
            skip_cache,
        })
    }

    fn content_type(&self) -> &str {
        "application/json"
    }
}

// ---------------------------------------------------------------------------
// Sort parsing
// ---------------------------------------------------------------------------

fn parse_sort_entry(s: &str) -> Result<SortClause, ParseError> {
    let s = s.trim();
    if let Some(idx) = s.rfind(':') {
        let field = &s[..idx];
        let dir = &s[idx + 1..];
        if field.is_empty() {
            return Err(ParseError::new("sort field must not be empty"));
        }
        let direction = match dir {
            "asc" => SortDirection::Asc,
            "desc" => SortDirection::Desc,
            other => {
                return Err(ParseError::new(format!(
                    "invalid sort direction '{other}'; expected 'asc' or 'desc'"
                )))
            }
        };
        Ok(SortClause {
            field: field.to_string(),
            direction,
        })
    } else {
        // No colon → default to Asc
        if s.is_empty() {
            return Err(ParseError::new("sort field must not be empty"));
        }
        Ok(SortClause {
            field: s.to_string(),
            direction: SortDirection::Asc,
        })
    }
}

// ---------------------------------------------------------------------------
// Array-format filter: outer = AND, inner arrays = OR
// ---------------------------------------------------------------------------

fn parse_array_filter(arr: &[JsonValue]) -> Result<Vec<FilterClause>, ParseError> {
    if arr.is_empty() {
        return Ok(vec![]);
    }

    let mut and_clauses = Vec::new();
    for item in arr {
        match item {
            JsonValue::String(s) => {
                if !s.trim().is_empty() {
                    and_clauses.push(parse_filter_string(s)?);
                }
            }
            JsonValue::Array(inner) => {
                // Inner array = OR
                let mut or_clauses = Vec::new();
                for inner_item in inner {
                    let s = inner_item
                        .as_str()
                        .ok_or_else(|| ParseError::new("filter array entries must be strings"))?;
                    if !s.trim().is_empty() {
                        or_clauses.push(parse_filter_string(s)?);
                    }
                }
                if !or_clauses.is_empty() {
                    if or_clauses.len() == 1 {
                        and_clauses.push(or_clauses.into_iter().next().unwrap());
                    } else {
                        and_clauses.push(FilterClause::Or(or_clauses));
                    }
                }
            }
            _ => return Err(ParseError::new("filter array entries must be strings or arrays")),
        }
    }

    Ok(and_clauses)
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),    // field name or unquoted string value
    Str(String),      // single-quoted string
    Number(f64),      // numeric literal
    True,
    False,
    Null,             // NULL keyword
    Is,               // IS keyword
    Eq,               // =
    NotEq,            // !=
    Gt,               // >
    Gte,              // >=
    Lt,               // <
    Lte,              // <=
    And,
    Or,
    Not,
    In,
    To,               // TO (range)
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Eof,
}

struct Tokenizer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    #[allow(dead_code)]
    fn peek_byte(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn next_token(&mut self) -> Result<Token, ParseError> {
        self.skip_whitespace();

        if self.pos >= self.input.len() {
            return Ok(Token::Eof);
        }

        let ch = self.input[self.pos];

        match ch {
            b'(' => { self.pos += 1; Ok(Token::LParen) }
            b')' => { self.pos += 1; Ok(Token::RParen) }
            b'[' => { self.pos += 1; Ok(Token::LBracket) }
            b']' => { self.pos += 1; Ok(Token::RBracket) }
            b',' => { self.pos += 1; Ok(Token::Comma) }

            b'=' => { self.pos += 1; Ok(Token::Eq) }

            b'!' if self.pos + 1 < self.input.len() && self.input[self.pos + 1] == b'=' => {
                self.pos += 2;
                Ok(Token::NotEq)
            }

            b'>' => {
                if self.pos + 1 < self.input.len() && self.input[self.pos + 1] == b'=' {
                    self.pos += 2;
                    Ok(Token::Gte)
                } else {
                    self.pos += 1;
                    Ok(Token::Gt)
                }
            }

            b'<' => {
                if self.pos + 1 < self.input.len() && self.input[self.pos + 1] == b'=' {
                    self.pos += 2;
                    Ok(Token::Lte)
                } else {
                    self.pos += 1;
                    Ok(Token::Lt)
                }
            }

            // Single-quoted string
            b'\'' => {
                self.pos += 1;
                let start = self.pos;
                while self.pos < self.input.len() && self.input[self.pos] != b'\'' {
                    if self.input[self.pos] == b'\\' && self.pos + 1 < self.input.len() {
                        self.pos += 2; // skip escaped char
                    } else {
                        self.pos += 1;
                    }
                }
                if self.pos >= self.input.len() {
                    return Err(ParseError::new("unterminated string literal"));
                }
                let s = std::str::from_utf8(&self.input[start..self.pos])
                    .map_err(|_| ParseError::new("invalid UTF-8 in string"))?
                    .to_string();
                self.pos += 1; // skip closing quote
                Ok(Token::Str(s))
            }

            // Double-quoted string (also supported)
            b'"' => {
                self.pos += 1;
                let start = self.pos;
                while self.pos < self.input.len() && self.input[self.pos] != b'"' {
                    if self.input[self.pos] == b'\\' && self.pos + 1 < self.input.len() {
                        self.pos += 2;
                    } else {
                        self.pos += 1;
                    }
                }
                if self.pos >= self.input.len() {
                    return Err(ParseError::new("unterminated string literal"));
                }
                let s = std::str::from_utf8(&self.input[start..self.pos])
                    .map_err(|_| ParseError::new("invalid UTF-8 in string"))?
                    .to_string();
                self.pos += 1;
                Ok(Token::Str(s))
            }

            // Number (possibly negative)
            b'0'..=b'9' => self.read_number(),

            b'-' => {
                // Could be negative number
                if self.pos + 1 < self.input.len() && self.input[self.pos + 1].is_ascii_digit() {
                    self.read_number()
                } else {
                    Err(ParseError::new(format!(
                        "unexpected character '-' at position {}",
                        self.pos
                    )))
                }
            }

            // Identifier or keyword
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                let start = self.pos;
                while self.pos < self.input.len()
                    && (self.input[self.pos].is_ascii_alphanumeric()
                        || self.input[self.pos] == b'_')
                {
                    self.pos += 1;
                }
                let word = std::str::from_utf8(&self.input[start..self.pos])
                    .map_err(|_| ParseError::new("invalid UTF-8"))?;

                match word {
                    "AND" | "and" => Ok(Token::And),
                    "OR" | "or" => Ok(Token::Or),
                    "NOT" | "not" => Ok(Token::Not),
                    "IN" | "in" => Ok(Token::In),
                    "TO" | "to" => Ok(Token::To),
                    "IS" | "is" => Ok(Token::Is),
                    "NULL" | "null" => Ok(Token::Null),
                    "true" | "TRUE" => Ok(Token::True),
                    "false" | "FALSE" => Ok(Token::False),
                    _ => Ok(Token::Ident(word.to_string())),
                }
            }

            other => Err(ParseError::new(format!(
                "unexpected character '{}' at position {}",
                other as char, self.pos
            ))),
        }
    }

    fn read_number(&mut self) -> Result<Token, ParseError> {
        let start = self.pos;
        if self.pos < self.input.len() && self.input[self.pos] == b'-' {
            self.pos += 1;
        }
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        // Check for decimal
        if self.pos < self.input.len() && self.input[self.pos] == b'.' {
            self.pos += 1;
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        let num_str = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|_| ParseError::new("invalid UTF-8 in number"))?;
        let num: f64 = num_str
            .parse()
            .map_err(|_| ParseError::new(format!("invalid number: {num_str}")))?;
        Ok(Token::Number(num))
    }
}

// ---------------------------------------------------------------------------
// Parser (recursive descent, precedence: NOT > AND > OR)
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<(), ParseError> {
        let got = self.advance();
        if &got != expected {
            Err(ParseError::new(format!(
                "expected {expected:?}, got {got:?}"
            )))
        } else {
            Ok(())
        }
    }

    /// Parse top-level expression (OR precedence).
    fn parse_expr(&mut self) -> Result<FilterClause, ParseError> {
        let mut left = self.parse_and()?;

        while matches!(self.peek(), Token::Or) {
            self.advance(); // consume OR
            let right = self.parse_and()?;
            left = match left {
                FilterClause::Or(mut clauses) => {
                    clauses.push(right);
                    FilterClause::Or(clauses)
                }
                _ => FilterClause::Or(vec![left, right]),
            };
        }

        Ok(left)
    }

    /// Parse AND expressions.
    fn parse_and(&mut self) -> Result<FilterClause, ParseError> {
        let mut left = self.parse_not()?;

        while matches!(self.peek(), Token::And) {
            self.advance(); // consume AND
            let right = self.parse_not()?;
            left = match left {
                FilterClause::And(mut clauses) => {
                    clauses.push(right);
                    FilterClause::And(clauses)
                }
                _ => FilterClause::And(vec![left, right]),
            };
        }

        Ok(left)
    }

    /// Parse NOT expressions.
    fn parse_not(&mut self) -> Result<FilterClause, ParseError> {
        if matches!(self.peek(), Token::Not) {
            self.advance(); // consume NOT

            // NOT IN special case: peek ahead for field + IN
            // "field NOT IN [...]" is handled by parse_primary seeing NOT token consumed
            // Actually, NOT before a primary expression
            let inner = self.parse_not()?; // NOT is right-associative
            Ok(FilterClause::Not(Box::new(inner)))
        } else {
            self.parse_primary()
        }
    }

    /// Parse primary expression: field comparison, IN, TO, or parenthesized group.
    fn parse_primary(&mut self) -> Result<FilterClause, ParseError> {
        match self.peek().clone() {
            Token::LParen => {
                self.advance(); // consume (
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Token::Ident(field) => {
                let field = field.clone();
                self.advance(); // consume field name

                // Check what follows: operator, IN, NOT IN, IS NULL, IS NOT NULL, or number (TO range)
                match self.peek().clone() {
                    Token::Is => {
                        self.advance(); // consume IS
                        if matches!(self.peek(), Token::Not) {
                            self.advance(); // consume NOT
                            if !matches!(self.peek(), Token::Null) {
                                return Err(ParseError::new(
                                    "expected 'NULL' after 'IS NOT'",
                                ));
                            }
                            self.advance(); // consume NULL
                            Ok(FilterClause::IsNotNull(field))
                        } else if matches!(self.peek(), Token::Null) {
                            self.advance(); // consume NULL
                            Ok(FilterClause::IsNull(field))
                        } else {
                            Err(ParseError::new(
                                "expected 'NULL' or 'NOT NULL' after 'IS'",
                            ))
                        }
                    }
                    Token::Eq => {
                        self.advance();
                        // Rewrite = null → IsNull
                        if matches!(self.peek(), Token::Null) {
                            self.advance(); // consume NULL
                            return Ok(FilterClause::IsNull(field));
                        }
                        let val = self.parse_value()?;
                        Ok(FilterClause::Eq(field, val))
                    }
                    Token::NotEq => {
                        self.advance();
                        let val = self.parse_value()?;
                        Ok(FilterClause::NotEq(field, val))
                    }
                    Token::Gt => {
                        self.advance();
                        let val = self.parse_value()?;
                        Ok(FilterClause::Gt(field, val))
                    }
                    Token::Gte => {
                        self.advance();
                        let val = self.parse_value()?;
                        Ok(FilterClause::Gte(field, val))
                    }
                    Token::Lt => {
                        self.advance();
                        let val = self.parse_value()?;
                        Ok(FilterClause::Lt(field, val))
                    }
                    Token::Lte => {
                        self.advance();
                        let val = self.parse_value()?;
                        Ok(FilterClause::Lte(field, val))
                    }
                    Token::In => {
                        self.advance(); // consume IN
                        let values = self.parse_value_list()?;
                        Ok(FilterClause::In(field, values))
                    }
                    Token::Not => {
                        // field NOT IN [...]
                        self.advance(); // consume NOT
                        if !matches!(self.peek(), Token::In) {
                            return Err(ParseError::new(
                                "expected 'IN' after 'NOT' in field expression",
                            ));
                        }
                        self.advance(); // consume IN
                        let values = self.parse_value_list()?;
                        Ok(FilterClause::NotIn(field, values))
                    }
                    // TO range: field min TO max → Gte(field, min) AND Lte(field, max)
                    Token::Number(_) => {
                        let min_val = self.parse_value()?;
                        if matches!(self.peek(), Token::To) {
                            self.advance(); // consume TO
                            let max_val = self.parse_value()?;
                            Ok(FilterClause::And(vec![
                                FilterClause::Gte(field.clone(), min_val),
                                FilterClause::Lte(field, max_val),
                            ]))
                        } else {
                            Err(ParseError::new(format!(
                                "expected operator after field '{field}', got a number (did you mean TO range?)"
                            )))
                        }
                    }
                    other => Err(ParseError::new(format!(
                        "expected operator after field '{field}', got {other:?}"
                    ))),
                }
            }
            other => Err(ParseError::new(format!(
                "unexpected token {other:?} at start of filter expression"
            ))),
        }
    }

    /// Parse a single value: number, string, boolean, or unquoted identifier.
    fn parse_value(&mut self) -> Result<Value, ParseError> {
        match self.advance() {
            Token::Number(n) => {
                // If it's a whole number, store as Integer
                if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                    Ok(Value::Integer(n as i64))
                } else {
                    Ok(Value::Float(n))
                }
            }
            Token::Str(s) => Ok(Value::String(s)),
            Token::Ident(s) => Ok(Value::String(s)),
            Token::True => Ok(Value::Bool(true)),
            Token::False => Ok(Value::Bool(false)),
            other => Err(ParseError::new(format!(
                "expected a value, got {other:?}"
            ))),
        }
    }

    /// Parse a bracketed value list: `[v1, v2, ...]`
    fn parse_value_list(&mut self) -> Result<Vec<Value>, ParseError> {
        self.expect(&Token::LBracket)?;

        let mut values = Vec::new();
        if !matches!(self.peek(), Token::RBracket) {
            values.push(self.parse_value()?);
            while matches!(self.peek(), Token::Comma) {
                self.advance(); // consume comma
                if matches!(self.peek(), Token::RBracket) {
                    break; // trailing comma
                }
                values.push(self.parse_value()?);
            }
        }

        self.expect(&Token::RBracket)?;
        Ok(values)
    }
}

// ---------------------------------------------------------------------------
// Public parse function
// ---------------------------------------------------------------------------

fn parse_filter_string(input: &str) -> Result<FilterClause, ParseError> {
    let mut tokenizer = Tokenizer::new(input);
    let mut tokens = Vec::new();
    loop {
        let tok = tokenizer.next_token()?;
        if tok == Token::Eof {
            tokens.push(tok);
            break;
        }
        tokens.push(tok);
    }

    let mut parser = Parser::new(tokens);
    let result = parser.parse_expr()?;

    // Ensure all input consumed
    if !matches!(parser.peek(), Token::Eof) {
        return Err(ParseError::new(format!(
            "unexpected trailing token: {:?}",
            parser.peek()
        )));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Result<BitdexQuery, ParseError> {
        MeilisearchQueryParser.parse(json.as_bytes())
    }

    fn parse_filter(filter: &str) -> Result<FilterClause, ParseError> {
        parse_filter_string(filter)
    }

    // === Basic filter expressions ===

    #[test]
    fn test_eq_integer() {
        let c = parse_filter("nsfwLevel = 1").unwrap();
        assert_eq!(c, FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)));
    }

    #[test]
    fn test_eq_string_unquoted() {
        let c = parse_filter("type = image").unwrap();
        assert_eq!(
            c,
            FilterClause::Eq("type".into(), Value::String("image".into()))
        );
    }

    #[test]
    fn test_eq_string_single_quoted() {
        let c = parse_filter("director = 'Jordan Peele'").unwrap();
        assert_eq!(
            c,
            FilterClause::Eq("director".into(), Value::String("Jordan Peele".into()))
        );
    }

    #[test]
    fn test_eq_string_double_quoted() {
        let c = parse_filter(r#"type = "image""#).unwrap();
        assert_eq!(
            c,
            FilterClause::Eq("type".into(), Value::String("image".into()))
        );
    }

    #[test]
    fn test_neq() {
        let c = parse_filter("type != video").unwrap();
        assert_eq!(
            c,
            FilterClause::NotEq("type".into(), Value::String("video".into()))
        );
    }

    #[test]
    fn test_gt() {
        let c = parse_filter("score > 100").unwrap();
        assert_eq!(c, FilterClause::Gt("score".into(), Value::Integer(100)));
    }

    #[test]
    fn test_gte() {
        let c = parse_filter("score >= 100").unwrap();
        assert_eq!(c, FilterClause::Gte("score".into(), Value::Integer(100)));
    }

    #[test]
    fn test_lt() {
        let c = parse_filter("score < 50").unwrap();
        assert_eq!(c, FilterClause::Lt("score".into(), Value::Integer(50)));
    }

    #[test]
    fn test_lte() {
        let c = parse_filter("score <= 50").unwrap();
        assert_eq!(c, FilterClause::Lte("score".into(), Value::Integer(50)));
    }

    #[test]
    fn test_eq_bool_true() {
        let c = parse_filter("onSite = true").unwrap();
        assert_eq!(c, FilterClause::Eq("onSite".into(), Value::Bool(true)));
    }

    #[test]
    fn test_eq_bool_false() {
        let c = parse_filter("onSite = false").unwrap();
        assert_eq!(c, FilterClause::Eq("onSite".into(), Value::Bool(false)));
    }

    #[test]
    fn test_negative_number() {
        let c = parse_filter("score > -5").unwrap();
        assert_eq!(c, FilterClause::Gt("score".into(), Value::Integer(-5)));
    }

    #[test]
    fn test_float_value() {
        let c = parse_filter("score > 3.14").unwrap();
        assert_eq!(c, FilterClause::Gt("score".into(), Value::Float(3.14)));
    }

    // === IN operator ===

    #[test]
    fn test_in() {
        let c = parse_filter("tagIds IN [12, 45, 678]").unwrap();
        assert_eq!(
            c,
            FilterClause::In(
                "tagIds".into(),
                vec![
                    Value::Integer(12),
                    Value::Integer(45),
                    Value::Integer(678)
                ]
            )
        );
    }

    #[test]
    fn test_in_strings() {
        let c = parse_filter("type IN [image, video]").unwrap();
        assert_eq!(
            c,
            FilterClause::In(
                "type".into(),
                vec![
                    Value::String("image".into()),
                    Value::String("video".into()),
                ]
            )
        );
    }

    #[test]
    fn test_in_empty() {
        let c = parse_filter("tagIds IN []").unwrap();
        assert_eq!(c, FilterClause::In("tagIds".into(), vec![]));
    }

    #[test]
    fn test_not_in() {
        let c = parse_filter("tagIds NOT IN [1, 2]").unwrap();
        assert_eq!(
            c,
            FilterClause::NotIn(
                "tagIds".into(),
                vec![Value::Integer(1), Value::Integer(2)]
            )
        );
    }

    // === TO range ===

    #[test]
    fn test_to_range() {
        let c = parse_filter("score 80 TO 100").unwrap();
        assert_eq!(
            c,
            FilterClause::And(vec![
                FilterClause::Gte("score".into(), Value::Integer(80)),
                FilterClause::Lte("score".into(), Value::Integer(100)),
            ])
        );
    }

    // === Boolean operators ===

    #[test]
    fn test_and() {
        let c = parse_filter("nsfwLevel = 1 AND type = image").unwrap();
        assert_eq!(
            c,
            FilterClause::And(vec![
                FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                FilterClause::Eq("type".into(), Value::String("image".into())),
            ])
        );
    }

    #[test]
    fn test_or() {
        let c = parse_filter("nsfwLevel = 1 OR nsfwLevel = 2").unwrap();
        assert_eq!(
            c,
            FilterClause::Or(vec![
                FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                FilterClause::Eq("nsfwLevel".into(), Value::Integer(2)),
            ])
        );
    }

    #[test]
    fn test_not() {
        let c = parse_filter("NOT type = video").unwrap();
        assert_eq!(
            c,
            FilterClause::Not(Box::new(FilterClause::Eq(
                "type".into(),
                Value::String("video".into())
            )))
        );
    }

    #[test]
    fn test_precedence_and_before_or() {
        // "A OR B AND C" should parse as "A OR (B AND C)"
        let c = parse_filter("nsfwLevel = 1 OR type = image AND onSite = true").unwrap();
        match c {
            FilterClause::Or(clauses) => {
                assert_eq!(clauses.len(), 2);
                assert!(matches!(&clauses[0], FilterClause::Eq(..)));
                assert!(matches!(&clauses[1], FilterClause::And(..)));
            }
            _ => panic!("expected Or, got {c:?}"),
        }
    }

    #[test]
    fn test_parentheses_override_precedence() {
        let c = parse_filter("(nsfwLevel = 1 OR type = image) AND onSite = true").unwrap();
        match c {
            FilterClause::And(clauses) => {
                assert_eq!(clauses.len(), 2);
                assert!(matches!(&clauses[0], FilterClause::Or(..)));
                assert!(matches!(&clauses[1], FilterClause::Eq(..)));
            }
            _ => panic!("expected And, got {c:?}"),
        }
    }

    #[test]
    fn test_multiple_and() {
        let c = parse_filter("a = 1 AND b = 2 AND c = 3").unwrap();
        match c {
            FilterClause::And(clauses) => assert_eq!(clauses.len(), 3),
            _ => panic!("expected And with 3 clauses"),
        }
    }

    #[test]
    fn test_multiple_or() {
        let c = parse_filter("a = 1 OR b = 2 OR c = 3").unwrap();
        match c {
            FilterClause::Or(clauses) => assert_eq!(clauses.len(), 3),
            _ => panic!("expected Or with 3 clauses"),
        }
    }

    // === Full query parsing ===

    #[test]
    fn test_empty_query() {
        let q = parse("{}").unwrap();
        assert!(q.filters.is_empty());
        assert!(q.sort.is_none());
        assert_eq!(q.limit, DEFAULT_LIMIT);
    }

    #[test]
    fn test_full_meilisearch_query() {
        let q = parse(
            r#"{
                "filter": "nsfwLevel = 1 AND tagIds IN [12, 45, 678] AND type = 'image' AND publishedAtUnix >= 1700000000",
                "sort": ["reactionCount:desc"],
                "limit": 50,
                "offset": 0
            }"#,
        )
        .unwrap();

        assert_eq!(q.limit, 50);
        assert_eq!(q.offset, Some(0));
        let sort = q.sort.unwrap();
        assert_eq!(sort.field, "reactionCount");
        assert_eq!(sort.direction, SortDirection::Desc);

        match &q.filters[0] {
            FilterClause::And(clauses) => assert_eq!(clauses.len(), 4),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_sort_asc() {
        let q = parse(r#"{"sort": ["score:asc"]}"#).unwrap();
        let sort = q.sort.unwrap();
        assert_eq!(sort.field, "score");
        assert_eq!(sort.direction, SortDirection::Asc);
    }

    #[test]
    fn test_sort_no_direction() {
        let q = parse(r#"{"sort": ["score"]}"#).unwrap();
        let sort = q.sort.unwrap();
        assert_eq!(sort.field, "score");
        assert_eq!(sort.direction, SortDirection::Asc);
    }

    #[test]
    fn test_page_pagination() {
        let q = parse(r#"{"page": 3, "hitsPerPage": 10}"#).unwrap();
        assert_eq!(q.offset, Some(20)); // (3-1) * 10
        assert_eq!(q.limit, 10);
    }

    // === Array filter format ===

    #[test]
    fn test_array_filter_simple() {
        let q = parse(
            r#"{"filter": ["nsfwLevel = 1", "type = image"]}"#,
        )
        .unwrap();
        // Two top-level clauses (implicit AND handled by caller)
        assert_eq!(q.filters.len(), 2);
    }

    #[test]
    fn test_array_filter_nested_or() {
        let q = parse(
            r#"{"filter": [["type = image", "type = video"], "nsfwLevel = 1"]}"#,
        )
        .unwrap();
        // First entry is OR, second is plain
        assert_eq!(q.filters.len(), 2);
        assert!(matches!(&q.filters[0], FilterClause::Or(..)));
    }

    #[test]
    fn test_empty_filter_string() {
        let q = parse(r#"{"filter": ""}"#).unwrap();
        assert!(q.filters.is_empty());
    }

    // === IS NULL / IS NOT NULL ===

    #[test]
    fn test_is_null() {
        let c = parse_filter("publishedAt IS NULL").unwrap();
        assert_eq!(c, FilterClause::IsNull("publishedAt".into()));
    }

    #[test]
    fn test_is_not_null() {
        let c = parse_filter("publishedAt IS NOT NULL").unwrap();
        assert_eq!(c, FilterClause::IsNotNull("publishedAt".into()));
    }

    #[test]
    fn test_is_null_lowercase() {
        let c = parse_filter("publishedAt is null").unwrap();
        assert_eq!(c, FilterClause::IsNull("publishedAt".into()));
    }

    #[test]
    fn test_is_not_null_lowercase() {
        let c = parse_filter("publishedAt is not null").unwrap();
        assert_eq!(c, FilterClause::IsNotNull("publishedAt".into()));
    }

    #[test]
    fn test_eq_null_rewrites_to_is_null() {
        let c = parse_filter("publishedAt = null").unwrap();
        assert_eq!(c, FilterClause::IsNull("publishedAt".into()));
    }

    #[test]
    fn test_is_null_in_and_expression() {
        let c = parse_filter("nsfwLevel = 1 AND publishedAt IS NULL").unwrap();
        assert_eq!(
            c,
            FilterClause::And(vec![
                FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                FilterClause::IsNull("publishedAt".into()),
            ])
        );
    }

    #[test]
    fn test_is_not_null_in_or_expression() {
        let c = parse_filter("publishedAt IS NOT NULL OR deletedAt IS NULL").unwrap();
        assert_eq!(
            c,
            FilterClause::Or(vec![
                FilterClause::IsNotNull("publishedAt".into()),
                FilterClause::IsNull("deletedAt".into()),
            ])
        );
    }

    // === Error cases ===

    #[test]
    fn test_unterminated_string() {
        let err = parse(r#"{"filter": "type = 'unterminated"}"#).unwrap_err();
        assert!(err.message.contains("unterminated"));
    }

    #[test]
    fn test_invalid_filter_syntax() {
        let err = parse(r#"{"filter": "= = ="}"#).unwrap_err();
        assert!(!err.message.is_empty());
    }

    #[test]
    fn test_limit_zero() {
        assert!(parse(r#"{"limit": 0}"#).is_err());
    }

    #[test]
    fn test_page_zero() {
        assert!(parse(r#"{"page": 0}"#).is_err());
    }

    #[test]
    fn test_trailing_comma_in_list() {
        let c = parse_filter("tagIds IN [1, 2,]").unwrap();
        assert_eq!(
            c,
            FilterClause::In(
                "tagIds".into(),
                vec![Value::Integer(1), Value::Integer(2)]
            )
        );
    }

    #[test]
    fn test_trait_object_safe() {
        let parser: Box<dyn QueryParser> = Box::new(MeilisearchQueryParser);
        assert_eq!(parser.content_type(), "application/json");
    }

    // === Fuzz tests ===

    #[cfg(test)]
    mod fuzz_tests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn fuzz_arbitrary_bytes(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
                let _ = MeilisearchQueryParser.parse(&data);
            }

            #[test]
            fn fuzz_filter_strings(s in "[a-zA-Z0-9 =!<>()\\[\\],.'\"AND OR NOT IN TO]{0,200}") {
                let json = format!(r#"{{"filter": "{}"}}"#, s.replace('"', "\\\""));
                let _ = MeilisearchQueryParser.parse(json.as_bytes());
            }

            #[test]
            fn fuzz_valid_comparisons(
                field in "[a-zA-Z][a-zA-Z0-9]{0,10}",
                op in prop_oneof![Just("="), Just("!="), Just(">"), Just(">="), Just("<"), Just("<=")],
                val in any::<i32>(),
            ) {
                // Skip reserved words that can't be used as field names
                let reserved = ["in", "IN", "In", "or", "OR", "Or", "and", "AND", "And",
                                "not", "NOT", "Not", "to", "TO", "To", "IS", "is", "Is",
                                "NULL", "null", "Null", "EMPTY", "empty", "Empty",
                                "EXISTS", "exists", "Exists"];
                if reserved.contains(&field.as_str()) {
                    return Ok(());
                }
                let filter = format!("{field} {op} {val}");
                let json = format!(r#"{{"filter": "{filter}"}}"#);
                let result = MeilisearchQueryParser.parse(json.as_bytes());
                assert!(result.is_ok(), "should parse: {filter}");
            }
        }
    }
}
