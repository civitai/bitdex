//! Compact MongoDB-style query parser.
//!
//! Designed for AI agents and concise human authoring. Filters use field names as keys
//! with value shape determining the operator:
//!
//! ```json
//! {
//!   "filter": {
//!     "nsfwLevel": 1,            // Eq
//!     "tagIds": [12, 45],        // In
//!     "score": {"$gte": 100},    // Range
//!     "type": {"$ne": "video"}   // NotEq
//!   },
//!   "sort": "-reactionCount",    // "-" prefix = Desc
//!   "limit": 50,
//!   "offset": 0,
//!   "cursor": "342:7042"         // sort_value:slot_id
//! }
//! ```

use serde_json::Value as JsonValue;

use crate::query::{
    BitdexQuery, CursorPosition, FilterClause, ParseError, QueryParser, SortClause, SortDirection,
    Value,
};

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 10_000;
const MAX_NESTING_DEPTH: usize = 16;

pub struct CompactQueryParser;

impl QueryParser for CompactQueryParser {
    fn parse(&self, input: &[u8]) -> Result<BitdexQuery, ParseError> {
        let raw: JsonValue = serde_json::from_slice(input)
            .map_err(|e| ParseError::new(format!("invalid JSON: {e}")))?;

        let obj = raw
            .as_object()
            .ok_or_else(|| ParseError::new("query must be a JSON object"))?;

        // Parse filters
        let filters = match obj.get("filter") {
            Some(filter_val) => parse_filter_object(filter_val, 0)?,
            None => vec![],
        };

        // Parse sort: "-field" = Desc, "field" = Asc
        let sort = match obj.get("sort") {
            Some(JsonValue::String(s)) => Some(parse_sort_string(s)?),
            Some(_) => return Err(ParseError::new("'sort' must be a string like '-reactionCount' or 'reactionCount'")),
            None => None,
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

        // Parse cursor: "sort_value:slot_id" string
        let cursor = match obj.get("cursor") {
            Some(JsonValue::String(s)) => Some(parse_cursor_string(s)?),
            Some(JsonValue::Null) => None,
            Some(_) => return Err(ParseError::new("'cursor' must be a string like '342:7042'")),
            None => None,
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
            cursor,
            offset,
            skip_cache,
        })
    }

    fn content_type(&self) -> &str {
        "application/json"
    }
}

/// Parse a filter object into FilterClause vec.
///
/// Top-level keys are field names (implicit AND). Special keys:
/// - `$or`: array of filter objects (union)
/// - `$and`: array of filter objects (intersection)
/// - `$not`: single filter object (complement)
fn parse_filter_object(value: &JsonValue, depth: usize) -> Result<Vec<FilterClause>, ParseError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(ParseError::new(format!(
            "filter nesting exceeds maximum depth of {MAX_NESTING_DEPTH}"
        )));
    }

    let obj = value
        .as_object()
        .ok_or_else(|| ParseError::new("filter must be a JSON object"))?;

    let mut clauses = Vec::new();

    for (key, val) in obj {
        match key.as_str() {
            "$or" => {
                let arr = val
                    .as_array()
                    .ok_or_else(|| ParseError::new("'$or' requires an array"))?;
                if arr.is_empty() {
                    return Err(ParseError::new("'$or' requires at least one clause"));
                }
                let mut or_clauses = Vec::new();
                for item in arr {
                    let inner = parse_filter_object(item, depth + 1)?;
                    if inner.len() == 1 {
                        or_clauses.push(inner.into_iter().next().unwrap());
                    } else {
                        or_clauses.push(FilterClause::And(inner));
                    }
                }
                clauses.push(FilterClause::Or(or_clauses));
            }
            "$and" => {
                let arr = val
                    .as_array()
                    .ok_or_else(|| ParseError::new("'$and' requires an array"))?;
                if arr.is_empty() {
                    return Err(ParseError::new("'$and' requires at least one clause"));
                }
                let mut and_clauses = Vec::new();
                for item in arr {
                    let inner = parse_filter_object(item, depth + 1)?;
                    and_clauses.extend(inner);
                }
                clauses.push(FilterClause::And(and_clauses));
            }
            "$not" => {
                let inner = parse_filter_object(val, depth + 1)?;
                let inner_clause = if inner.len() == 1 {
                    inner.into_iter().next().unwrap()
                } else {
                    FilterClause::And(inner)
                };
                clauses.push(FilterClause::Not(Box::new(inner_clause)));
            }
            field => {
                let field_clauses = parse_field_predicate(field, val)?;
                clauses.extend(field_clauses);
            }
        }
    }

    Ok(clauses)
}

/// Parse a field predicate from its value shape.
///
/// - Scalar → Eq
/// - Array → In
/// - Object with $-prefixed keys → operator(s)
fn parse_field_predicate(field: &str, value: &JsonValue) -> Result<Vec<FilterClause>, ParseError> {
    if field.is_empty() {
        return Err(ParseError::new("field name must not be empty"));
    }

    match value {
        // Scalar → Eq
        JsonValue::Number(_) | JsonValue::Bool(_) | JsonValue::String(_) => {
            let val = json_to_value(value)?;
            Ok(vec![FilterClause::Eq(field.to_string(), val)])
        }

        // Array → In
        JsonValue::Array(arr) => {
            let values: Result<Vec<Value>, _> = arr.iter().map(json_to_value).collect();
            Ok(vec![FilterClause::In(field.to_string(), values?)])
        }

        // Object with operator keys
        JsonValue::Object(ops) => {
            let mut clauses = Vec::new();
            for (op, op_val) in ops {
                let clause = match op.as_str() {
                    "$exists" => {
                        let exists = op_val.as_bool().ok_or_else(|| {
                            ParseError::new(format!(
                                "'$exists' on '{field}' requires a boolean value"
                            ))
                        })?;
                        if exists {
                            FilterClause::IsNotNull(field.to_string())
                        } else {
                            FilterClause::IsNull(field.to_string())
                        }
                    }
                    "$eq" => {
                        // $eq: null → IsNull
                        if op_val.is_null() {
                            FilterClause::IsNull(field.to_string())
                        } else {
                            let val = json_to_value(op_val)?;
                            FilterClause::Eq(field.to_string(), val)
                        }
                    }
                    "$ne" => {
                        if op_val.is_null() {
                            FilterClause::IsNotNull(field.to_string())
                        } else {
                            let val = json_to_value(op_val)?;
                            FilterClause::NotEq(field.to_string(), val)
                        }
                    }
                    "$gt" => {
                        let val = json_to_value(op_val)?;
                        FilterClause::Gt(field.to_string(), val)
                    }
                    "$gte" => {
                        let val = json_to_value(op_val)?;
                        FilterClause::Gte(field.to_string(), val)
                    }
                    "$lt" => {
                        let val = json_to_value(op_val)?;
                        FilterClause::Lt(field.to_string(), val)
                    }
                    "$lte" => {
                        let val = json_to_value(op_val)?;
                        FilterClause::Lte(field.to_string(), val)
                    }
                    "$in" => {
                        let arr = op_val.as_array().ok_or_else(|| {
                            ParseError::new(format!("'$in' on '{field}' requires an array"))
                        })?;
                        let values: Result<Vec<Value>, _> = arr.iter().map(json_to_value).collect();
                        FilterClause::In(field.to_string(), values?)
                    }
                    "$nin" => {
                        let arr = op_val.as_array().ok_or_else(|| {
                            ParseError::new(format!("'$nin' on '{field}' requires an array"))
                        })?;
                        let values: Result<Vec<Value>, _> = arr.iter().map(json_to_value).collect();
                        FilterClause::NotIn(field.to_string(), values?)
                    }
                    other => {
                        return Err(ParseError::new(format!(
                            "unsupported operator '{other}' on '{field}'; expected $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists"
                        )));
                    }
                };
                clauses.push(clause);
            }
            Ok(clauses)
        }

        // null as a direct value → IsNull (same as { "$exists": false })
        JsonValue::Null => Ok(vec![FilterClause::IsNull(field.to_string())]),
    }
}

fn json_to_value(json: &JsonValue) -> Result<Value, ParseError> {
    match json {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(ParseError::new(format!("unsupported number value: {n}")))
            }
        }
        JsonValue::Bool(b) => Ok(Value::Bool(*b)),
        JsonValue::String(s) => Ok(Value::String(s.clone())),
        JsonValue::Null => Err(ParseError::new("null is not a valid filter value")),
        JsonValue::Array(_) => Err(ParseError::new("nested arrays are not valid filter values")),
        JsonValue::Object(_) => Err(ParseError::new("nested objects are not valid filter values")),
    }
}

fn parse_sort_string(s: &str) -> Result<SortClause, ParseError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ParseError::new("sort field must not be empty"));
    }
    let (field, direction) = if let Some(field) = s.strip_prefix('-') {
        (field, SortDirection::Desc)
    } else {
        (s, SortDirection::Asc)
    };

    if field.is_empty() {
        return Err(ParseError::new("sort field must not be empty"));
    }

    Ok(SortClause {
        field: field.to_string(),
        direction,
    })
}

fn parse_cursor_string(s: &str) -> Result<CursorPosition, ParseError> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(ParseError::new(
            "cursor must be 'sort_value:slot_id' (e.g. '342:7042')",
        ));
    }
    let sort_value: u64 = parts[0]
        .parse()
        .map_err(|_| ParseError::new("cursor sort_value must be a non-negative integer"))?;
    let slot_id: u64 = parts[1]
        .parse()
        .map_err(|_| ParseError::new("cursor slot_id must be a non-negative integer"))?;
    if slot_id > u32::MAX as u64 {
        return Err(ParseError::new(format!(
            "cursor slot_id {slot_id} exceeds u32 maximum"
        )));
    }
    Ok(CursorPosition {
        sort_value,
        slot_id: slot_id as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Result<BitdexQuery, ParseError> {
        CompactQueryParser.parse(json.as_bytes())
    }

    // === Basic parsing ===

    #[test]
    fn test_empty_query() {
        let q = parse("{}").unwrap();
        assert!(q.filters.is_empty());
        assert!(q.sort.is_none());
        assert_eq!(q.limit, DEFAULT_LIMIT);
    }

    #[test]
    fn test_limit_only() {
        let q = parse(r#"{"limit": 10}"#).unwrap();
        assert_eq!(q.limit, 10);
    }

    #[test]
    fn test_limit_capped() {
        let q = parse(r#"{"limit": 999999}"#).unwrap();
        assert_eq!(q.limit, MAX_LIMIT);
    }

    #[test]
    fn test_limit_zero_rejected() {
        assert!(parse(r#"{"limit": 0}"#).is_err());
    }

    // === Equality (scalar values) ===

    #[test]
    fn test_eq_integer() {
        let q = parse(r#"{"filter": {"nsfwLevel": 1}}"#).unwrap();
        assert_eq!(q.filters.len(), 1);
        assert_eq!(
            q.filters[0],
            FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))
        );
    }

    #[test]
    fn test_eq_bool() {
        let q = parse(r#"{"filter": {"onSite": true}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Eq("onSite".into(), Value::Bool(true))
        );
    }

    #[test]
    fn test_eq_string() {
        let q = parse(r#"{"filter": {"type": "image"}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Eq("type".into(), Value::String("image".into()))
        );
    }

    // === In (array values) ===

    #[test]
    fn test_in_array() {
        let q = parse(r#"{"filter": {"tagIds": [12, 45, 678]}}"#).unwrap();
        assert_eq!(
            q.filters[0],
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
    fn test_in_empty_array() {
        let q = parse(r#"{"filter": {"tagIds": []}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::In("tagIds".into(), vec![]));
    }

    // === Operator objects ===

    #[test]
    fn test_gte_operator() {
        let q = parse(r#"{"filter": {"score": {"$gte": 100}}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Gte("score".into(), Value::Integer(100))
        );
    }

    #[test]
    fn test_ne_operator() {
        let q = parse(r#"{"filter": {"type": {"$ne": "video"}}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::NotEq("type".into(), Value::String("video".into()))
        );
    }

    #[test]
    fn test_combined_range() {
        let q =
            parse(r#"{"filter": {"score": {"$gte": 10, "$lt": 100}}}"#).unwrap();
        assert_eq!(q.filters.len(), 2);
        // Both clauses should be present (order may vary due to HashMap)
        let has_gte = q.filters.iter().any(|c| {
            matches!(c, FilterClause::Gte(f, Value::Integer(10)) if f == "score")
        });
        let has_lt = q.filters.iter().any(|c| {
            matches!(c, FilterClause::Lt(f, Value::Integer(100)) if f == "score")
        });
        assert!(has_gte, "should have $gte clause");
        assert!(has_lt, "should have $lt clause");
    }

    #[test]
    fn test_nin_operator() {
        let q = parse(r#"{"filter": {"tagIds": {"$nin": [1, 2]}}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::NotIn(
                "tagIds".into(),
                vec![Value::Integer(1), Value::Integer(2)]
            )
        );
    }

    #[test]
    fn test_in_operator_explicit() {
        let q = parse(r#"{"filter": {"tagIds": {"$in": [1, 2]}}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::In(
                "tagIds".into(),
                vec![Value::Integer(1), Value::Integer(2)]
            )
        );
    }

    // === Multiple filters (implicit AND) ===

    #[test]
    fn test_multiple_filters_implicit_and() {
        let q = parse(
            r#"{"filter": {"nsfwLevel": 1, "type": "image"}}"#,
        )
        .unwrap();
        assert_eq!(q.filters.len(), 2);
    }

    // === $or / $and / $not ===

    #[test]
    fn test_or() {
        let q = parse(
            r#"{"filter": {"$or": [{"nsfwLevel": 1}, {"nsfwLevel": 2}]}}"#,
        )
        .unwrap();
        assert_eq!(q.filters.len(), 1);
        match &q.filters[0] {
            FilterClause::Or(clauses) => assert_eq!(clauses.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn test_and_explicit() {
        let q = parse(
            r#"{"filter": {"$and": [{"nsfwLevel": 1}, {"type": "image"}]}}"#,
        )
        .unwrap();
        match &q.filters[0] {
            FilterClause::And(clauses) => assert_eq!(clauses.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_not() {
        let q = parse(r#"{"filter": {"$not": {"type": "video"}}}"#).unwrap();
        match &q.filters[0] {
            FilterClause::Not(inner) => {
                assert_eq!(
                    **inner,
                    FilterClause::Eq("type".into(), Value::String("video".into()))
                );
            }
            _ => panic!("expected Not"),
        }
    }

    // === Sort ===

    #[test]
    fn test_sort_desc() {
        let q = parse(r#"{"sort": "-reactionCount"}"#).unwrap();
        let sort = q.sort.unwrap();
        assert_eq!(sort.field, "reactionCount");
        assert_eq!(sort.direction, SortDirection::Desc);
    }

    #[test]
    fn test_sort_asc() {
        let q = parse(r#"{"sort": "reactionCount"}"#).unwrap();
        let sort = q.sort.unwrap();
        assert_eq!(sort.field, "reactionCount");
        assert_eq!(sort.direction, SortDirection::Asc);
    }

    // === Cursor ===

    #[test]
    fn test_cursor_string() {
        let q = parse(r#"{"cursor": "342:7042"}"#).unwrap();
        let cursor = q.cursor.unwrap();
        assert_eq!(cursor.sort_value, 342);
        assert_eq!(cursor.slot_id, 7042);
    }

    #[test]
    fn test_cursor_invalid_format() {
        assert!(parse(r#"{"cursor": "invalid"}"#).is_err());
    }

    // === Offset ===

    #[test]
    fn test_offset() {
        let q = parse(r#"{"offset": 20}"#).unwrap();
        assert_eq!(q.offset, Some(20));
    }

    // === Full example from design doc ===

    #[test]
    fn test_full_example() {
        let q = parse(
            r#"{
                "filter": {
                    "nsfwLevel": 1,
                    "tagIds": [12, 45, 678],
                    "type": "image",
                    "publishedAtUnix": {"$gte": 1700000000}
                },
                "sort": "-reactionCount",
                "limit": 50,
                "offset": 0
            }"#,
        )
        .unwrap();

        assert_eq!(q.limit, 50);
        assert_eq!(q.offset, Some(0));
        assert!(q.sort.is_some());
        assert_eq!(q.sort.as_ref().unwrap().direction, SortDirection::Desc);
        // 4 field filters: nsfwLevel, tagIds, type, publishedAtUnix
        assert_eq!(q.filters.len(), 4);
    }

    // === Complex nested example ===

    #[test]
    fn test_complex_nested() {
        let q = parse(
            r#"{
                "filter": {
                    "$or": [
                        {"nsfwLevel": 1},
                        {"tagIds": [99]}
                    ],
                    "type": {"$ne": "video"},
                    "publishedAtUnix": {"$gte": 1704067200, "$lt": 1735689600}
                },
                "sort": "-publishedAtUnix",
                "limit": 20
            }"#,
        )
        .unwrap();

        assert_eq!(q.limit, 20);
        // Should have: $or clause, $ne clause, $gte clause, $lt clause
        let has_or = q
            .filters
            .iter()
            .any(|c| matches!(c, FilterClause::Or(_)));
        assert!(has_or, "should have Or clause");
    }

    // === Error cases ===

    #[test]
    fn test_null_value_rewrites_to_is_null() {
        // null as a direct field value is shorthand for IsNull
        let q = parse(r#"{"filter": {"x": null}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::IsNull("x".into()));
    }

    #[test]
    fn test_exists_false() {
        let q = parse(r#"{"filter": {"publishedAt": {"$exists": false}}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::IsNull("publishedAt".into()));
    }

    #[test]
    fn test_exists_true() {
        let q = parse(r#"{"filter": {"publishedAt": {"$exists": true}}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::IsNotNull("publishedAt".into()));
    }

    #[test]
    fn test_eq_null_rewrites_to_is_null() {
        let q = parse(r#"{"filter": {"publishedAt": {"$eq": null}}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::IsNull("publishedAt".into()));
    }

    #[test]
    fn test_unknown_operator() {
        assert!(parse(r#"{"filter": {"x": {"$regex": ".*"}}}"#).is_err());
    }

    #[test]
    fn test_trait_object_safe() {
        let parser: Box<dyn QueryParser> = Box::new(CompactQueryParser);
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
                let _ = CompactQueryParser.parse(&data);
            }

            #[test]
            fn fuzz_json_objects(
                field in "[a-zA-Z][a-zA-Z0-9_]{0,20}",
                int_val in any::<i64>(),
                limit in 0..100000usize,
            ) {
                let json = format!(
                    r#"{{"filter": {{"{field}": {int_val}}}, "limit": {limit}}}"#
                );
                let _ = CompactQueryParser.parse(json.as_bytes());
            }
        }
    }
}
