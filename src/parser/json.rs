use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::query::{
    BitdexQuery, CursorPosition, FilterClause, ParseError, QueryParser, SortClause, SortDirection,
    Value,
};

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 10_000;
const MAX_NESTING_DEPTH: usize = 16;

/// JSON query parser for Bitdex V2.
///
/// Accepts JSON bodies conforming to the Bitdex query format:
/// ```json
/// {
///   "filters": { "AND": [...] },
///   "sort": { "field": "reactionCount", "direction": "desc" },
///   "limit": 50,
///   "cursor": { "sort_value": 342, "slot_id": 7042 }
/// }
/// ```
///
/// Supported filter operators: eq, not_eq, in, gt, lt, gte, lte
/// Supported compound operators: AND, OR, NOT
pub struct JsonQueryParser;

impl QueryParser for JsonQueryParser {
    fn parse(&self, input: &[u8]) -> Result<BitdexQuery, ParseError> {
        let raw: RawQuery = serde_json::from_slice(input)
            .map_err(|e| ParseError::new(format!("invalid JSON: {e}")))?;

        let filters = match raw.filters {
            Some(filter_value) => {
                let clause = parse_filter_node(&filter_value, 0)?;
                vec![clause]
            }
            None => vec![],
        };

        let sort = raw.sort.map(parse_sort).transpose()?;

        let limit = raw.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
        if limit == 0 {
            return Err(ParseError::new("limit must be greater than 0"));
        }

        let cursor = raw.cursor.map(parse_cursor).transpose()?;

        Ok(BitdexQuery {
            filters,
            sort,
            limit,
            cursor,
            offset: raw.offset,
            skip_cache: raw.skip_cache,
        })
    }

    fn content_type(&self) -> &str {
        "application/json"
    }
}

#[derive(Deserialize)]
struct RawQuery {
    filters: Option<JsonValue>,
    sort: Option<JsonValue>,
    limit: Option<usize>,
    cursor: Option<JsonValue>,
    offset: Option<usize>,
    #[serde(default)]
    skip_cache: bool,
}

fn parse_filter_node(value: &JsonValue, depth: usize) -> Result<FilterClause, ParseError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(ParseError::new(format!(
            "filter nesting exceeds maximum depth of {MAX_NESTING_DEPTH}"
        )));
    }

    let obj = value
        .as_object()
        .ok_or_else(|| ParseError::new("filter clause must be a JSON object"))?;

    // Check for IsNull / IsNotNull shorthand: { "IsNull": "fieldName" }
    if let Some(field_val) = get_case_insensitive(obj, "IsNull") {
        let field = field_val
            .as_str()
            .ok_or_else(|| ParseError::new("'IsNull' requires a field name string"))?;
        if field.is_empty() {
            return Err(ParseError::new("field name must not be empty"));
        }
        return Ok(FilterClause::IsNull(field.to_string()));
    }

    if let Some(field_val) = get_case_insensitive(obj, "IsNotNull") {
        let field = field_val
            .as_str()
            .ok_or_else(|| ParseError::new("'IsNotNull' requires a field name string"))?;
        if field.is_empty() {
            return Err(ParseError::new("field name must not be empty"));
        }
        return Ok(FilterClause::IsNotNull(field.to_string()));
    }

    // Check for compound operators (case-insensitive keys)
    if let Some(and_val) = get_case_insensitive(obj, "AND") {
        let clauses = parse_filter_array(and_val, depth + 1)?;
        if clauses.is_empty() {
            return Err(ParseError::new("AND requires at least one clause"));
        }
        return Ok(FilterClause::And(clauses));
    }

    if let Some(or_val) = get_case_insensitive(obj, "OR") {
        let clauses = parse_filter_array(or_val, depth + 1)?;
        if clauses.is_empty() {
            return Err(ParseError::new("OR requires at least one clause"));
        }
        return Ok(FilterClause::Or(clauses));
    }

    if let Some(not_val) = get_case_insensitive(obj, "NOT") {
        let inner = parse_filter_node(not_val, depth + 1)?;
        return Ok(FilterClause::Not(Box::new(inner)));
    }

    // Simple filter: { "field": "...", "op": "...", "value": ... }
    let field_name = obj
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ParseError::new("filter clause requires a 'field' string"))?;

    if field_name.is_empty() {
        return Err(ParseError::new("field name must not be empty"));
    }

    let op = obj
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ParseError::new(format!("filter on '{field_name}' requires an 'op' string")))?;

    match op {
        "is_null" => {
            return Ok(FilterClause::IsNull(field_name.to_string()));
        }
        "is_not_null" => {
            return Ok(FilterClause::IsNotNull(field_name.to_string()));
        }
        "eq" => {
            let raw_val = obj.get("value").ok_or_else(|| {
                ParseError::new(format!("'{op}' on '{field_name}' requires a 'value'"))
            })?;
            // Rewrite eq + null → IsNull
            if raw_val.is_null() {
                return Ok(FilterClause::IsNull(field_name.to_string()));
            }
            let val = json_to_value(raw_val)?;
            Ok(FilterClause::Eq(field_name.to_string(), val))
        }
        "not_eq" => {
            let raw_val = obj.get("value").ok_or_else(|| {
                ParseError::new(format!("'{op}' on '{field_name}' requires a 'value'"))
            })?;
            if raw_val.is_null() {
                Ok(FilterClause::IsNotNull(field_name.to_string()))
            } else {
                let val = json_to_value(raw_val)?;
                Ok(FilterClause::NotEq(field_name.to_string(), val))
            }
        }
        "in" => {
            let raw_arr = obj
                .get("value")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    ParseError::new(format!("'in' on '{field_name}' requires a 'value' array"))
                })?;
            let values: Result<Vec<Value>, _> = raw_arr.iter().map(json_to_value).collect();
            Ok(FilterClause::In(field_name.to_string(), values?))
        }
        "gt" => {
            let raw_val = obj.get("value").ok_or_else(|| {
                ParseError::new(format!("'{op}' on '{field_name}' requires a 'value'"))
            })?;
            let val = json_to_value(raw_val)?;
            Ok(FilterClause::Gt(field_name.to_string(), val))
        }
        "lt" => {
            let raw_val = obj.get("value").ok_or_else(|| {
                ParseError::new(format!("'{op}' on '{field_name}' requires a 'value'"))
            })?;
            let val = json_to_value(raw_val)?;
            Ok(FilterClause::Lt(field_name.to_string(), val))
        }
        "gte" => {
            let raw_val = obj.get("value").ok_or_else(|| {
                ParseError::new(format!("'{op}' on '{field_name}' requires a 'value'"))
            })?;
            let val = json_to_value(raw_val)?;
            Ok(FilterClause::Gte(field_name.to_string(), val))
        }
        "lte" => {
            let raw_val = obj.get("value").ok_or_else(|| {
                ParseError::new(format!("'{op}' on '{field_name}' requires a 'value'"))
            })?;
            let val = json_to_value(raw_val)?;
            Ok(FilterClause::Lte(field_name.to_string(), val))
        }
        other => Err(ParseError::new(format!(
            "unsupported operator '{other}'; expected one of: eq, not_eq, in, gt, lt, gte, lte, is_null, is_not_null"
        ))),
    }
}

fn parse_filter_array(value: &JsonValue, depth: usize) -> Result<Vec<FilterClause>, ParseError> {
    let arr = value.as_array().ok_or_else(|| {
        ParseError::new("compound filter (AND/OR) requires an array of filter clauses")
    })?;
    arr.iter().map(|v| parse_filter_node(v, depth)).collect()
}

fn parse_sort(value: JsonValue) -> Result<SortClause, ParseError> {
    let obj = value
        .as_object()
        .ok_or_else(|| ParseError::new("'sort' must be a JSON object with 'field' and 'direction'"))?;

    let field = obj
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ParseError::new("'sort' requires a 'field' string"))?;

    if field.is_empty() {
        return Err(ParseError::new("sort field name must not be empty"));
    }

    let direction_str = obj
        .get("direction")
        .and_then(|v| v.as_str())
        .unwrap_or("asc");

    let direction = match direction_str {
        "asc" => SortDirection::Asc,
        "desc" => SortDirection::Desc,
        other => {
            return Err(ParseError::new(format!(
                "invalid sort direction '{other}'; expected 'asc' or 'desc'"
            )));
        }
    };

    Ok(SortClause {
        field: field.to_string(),
        direction,
    })
}

fn parse_cursor(value: JsonValue) -> Result<CursorPosition, ParseError> {
    let obj = value
        .as_object()
        .ok_or_else(|| ParseError::new("'cursor' must be a JSON object with 'sort_value' and 'slot_id'"))?;

    let sort_value = obj
        .get("sort_value")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| ParseError::new("'cursor' requires a 'sort_value' unsigned integer"))?;

    let slot_id = obj
        .get("slot_id")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| ParseError::new("'cursor' requires a 'slot_id' unsigned integer"))?;

    if slot_id > u32::MAX as u64 {
        return Err(ParseError::new(format!(
            "slot_id {slot_id} exceeds u32 maximum"
        )));
    }

    Ok(CursorPosition {
        sort_value,
        slot_id: slot_id as u32,
    })
}

/// Convert a JSON value to a Bitdex `Value`.
///
/// Integers map to `Value::Integer`, floats to `Value::Float`,
/// booleans to `Value::Bool`, strings to `Value::String`.
/// Null, arrays, and objects are rejected.
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
        JsonValue::Array(_) => Err(ParseError::new(
            "array is not a valid filter value (use 'in' operator for multiple values)",
        )),
        JsonValue::Object(_) => Err(ParseError::new("object is not a valid filter value")),
    }
}

/// Case-insensitive key lookup. Checks exact match first, then uppercase.
fn get_case_insensitive<'a>(
    obj: &'a serde_json::Map<String, JsonValue>,
    key: &str,
) -> Option<&'a JsonValue> {
    // Try exact match first (uppercase, as per spec: "AND", "OR", "NOT")
    if let Some(v) = obj.get(key) {
        return Some(v);
    }
    // Try lowercase
    let lower = key.to_lowercase();
    if let Some(v) = obj.get(&lower) {
        return Some(v);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Result<BitdexQuery, ParseError> {
        JsonQueryParser.parse(json.as_bytes())
    }

    // === Basic parsing ===

    #[test]
    fn test_empty_query() {
        let q = parse("{}").unwrap();
        assert!(q.filters.is_empty());
        assert!(q.sort.is_none());
        assert_eq!(q.limit, DEFAULT_LIMIT);
        assert!(q.cursor.is_none());
    }

    #[test]
    fn test_limit_only() {
        let q = parse(r#"{"limit": 10}"#).unwrap();
        assert_eq!(q.limit, 10);
    }

    #[test]
    fn test_limit_capped_at_max() {
        let q = parse(r#"{"limit": 999999}"#).unwrap();
        assert_eq!(q.limit, MAX_LIMIT);
    }

    #[test]
    fn test_limit_zero_rejected() {
        assert!(parse(r#"{"limit": 0}"#).is_err());
    }

    // === Filter operators ===

    #[test]
    fn test_eq_integer() {
        let q = parse(r#"{"filters": {"field": "nsfwLevel", "op": "eq", "value": 1}}"#).unwrap();
        assert_eq!(q.filters.len(), 1);
        assert_eq!(
            q.filters[0],
            FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))
        );
    }

    #[test]
    fn test_eq_bool() {
        let q = parse(r#"{"filters": {"field": "onSite", "op": "eq", "value": true}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Eq("onSite".into(), Value::Bool(true))
        );
    }

    #[test]
    fn test_eq_string() {
        let q =
            parse(r#"{"filters": {"field": "type", "op": "eq", "value": "image"}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Eq("type".into(), Value::String("image".into()))
        );
    }

    #[test]
    fn test_not_eq() {
        let q =
            parse(r#"{"filters": {"field": "userId", "op": "not_eq", "value": 999}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::NotEq("userId".into(), Value::Integer(999))
        );
    }

    #[test]
    fn test_in_operator() {
        let q = parse(r#"{"filters": {"field": "tagIds", "op": "in", "value": [456, 789]}}"#)
            .unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::In(
                "tagIds".into(),
                vec![Value::Integer(456), Value::Integer(789)]
            )
        );
    }

    #[test]
    fn test_gt_operator() {
        let q = parse(r#"{"filters": {"field": "score", "op": "gt", "value": 100}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Gt("score".into(), Value::Integer(100))
        );
    }

    #[test]
    fn test_lt_operator() {
        let q = parse(r#"{"filters": {"field": "score", "op": "lt", "value": 50}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Lt("score".into(), Value::Integer(50))
        );
    }

    #[test]
    fn test_gte_operator() {
        let q = parse(r#"{"filters": {"field": "score", "op": "gte", "value": 10}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Gte("score".into(), Value::Integer(10))
        );
    }

    #[test]
    fn test_lte_operator() {
        let q = parse(r#"{"filters": {"field": "score", "op": "lte", "value": 90}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Lte("score".into(), Value::Integer(90))
        );
    }

    // === Compound operators ===

    #[test]
    fn test_and_compound() {
        let q = parse(
            r#"{
                "filters": {
                    "AND": [
                        {"field": "nsfwLevel", "op": "eq", "value": 1},
                        {"field": "tagIds", "op": "in", "value": [456, 789]},
                        {"field": "onSite", "op": "eq", "value": true},
                        {"field": "userId", "op": "not_eq", "value": 999}
                    ]
                }
            }"#,
        )
        .unwrap();

        match &q.filters[0] {
            FilterClause::And(clauses) => assert_eq!(clauses.len(), 4),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_or_compound() {
        let q = parse(
            r#"{
                "filters": {
                    "OR": [
                        {"field": "nsfwLevel", "op": "eq", "value": 1},
                        {"field": "nsfwLevel", "op": "eq", "value": 2}
                    ]
                }
            }"#,
        )
        .unwrap();

        match &q.filters[0] {
            FilterClause::Or(clauses) => assert_eq!(clauses.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn test_not_compound() {
        let q = parse(
            r#"{
                "filters": {
                    "NOT": {"field": "onSite", "op": "eq", "value": true}
                }
            }"#,
        )
        .unwrap();

        match &q.filters[0] {
            FilterClause::Not(inner) => {
                assert_eq!(
                    **inner,
                    FilterClause::Eq("onSite".into(), Value::Bool(true))
                );
            }
            _ => panic!("expected Not"),
        }
    }

    #[test]
    fn test_nested_compound() {
        let q = parse(
            r#"{
                "filters": {
                    "AND": [
                        {"field": "nsfwLevel", "op": "eq", "value": 1},
                        {
                            "OR": [
                                {"field": "userId", "op": "eq", "value": 100},
                                {"field": "userId", "op": "eq", "value": 200}
                            ]
                        }
                    ]
                }
            }"#,
        )
        .unwrap();

        match &q.filters[0] {
            FilterClause::And(clauses) => {
                assert_eq!(clauses.len(), 2);
                match &clauses[1] {
                    FilterClause::Or(or_clauses) => assert_eq!(or_clauses.len(), 2),
                    _ => panic!("expected nested Or"),
                }
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_lowercase_compound_operators() {
        let q = parse(
            r#"{"filters": {"and": [{"field": "x", "op": "eq", "value": 1}]}}"#,
        )
        .unwrap();
        match &q.filters[0] {
            FilterClause::And(_) => {}
            _ => panic!("expected And with lowercase key"),
        }
    }

    // === Sort ===

    #[test]
    fn test_sort_desc() {
        let q = parse(r#"{"sort": {"field": "reactionCount", "direction": "desc"}}"#).unwrap();
        let sort = q.sort.unwrap();
        assert_eq!(sort.field, "reactionCount");
        assert_eq!(sort.direction, SortDirection::Desc);
    }

    #[test]
    fn test_sort_asc() {
        let q = parse(r#"{"sort": {"field": "id", "direction": "asc"}}"#).unwrap();
        let sort = q.sort.unwrap();
        assert_eq!(sort.field, "id");
        assert_eq!(sort.direction, SortDirection::Asc);
    }

    #[test]
    fn test_sort_default_direction() {
        let q = parse(r#"{"sort": {"field": "id"}}"#).unwrap();
        let sort = q.sort.unwrap();
        assert_eq!(sort.direction, SortDirection::Asc);
    }

    // === Cursor ===

    #[test]
    fn test_cursor() {
        let q = parse(
            r#"{
                "sort": {"field": "reactionCount", "direction": "desc"},
                "cursor": {"sort_value": 342, "slot_id": 7042},
                "limit": 50
            }"#,
        )
        .unwrap();

        let cursor = q.cursor.unwrap();
        assert_eq!(cursor.sort_value, 342);
        assert_eq!(cursor.slot_id, 7042);
    }

    // === Full spec example ===

    #[test]
    fn test_full_spec_example() {
        let q = parse(
            r#"{
                "filters": {
                    "AND": [
                        {"field": "nsfwLevel", "op": "eq", "value": 1},
                        {"field": "tagIds", "op": "in", "value": [456, 789]},
                        {"field": "onSite", "op": "eq", "value": true},
                        {"field": "userId", "op": "not_eq", "value": 999}
                    ]
                },
                "sort": {"field": "reactionCount", "direction": "desc"},
                "limit": 50,
                "cursor": {"sort_value": 342, "slot_id": 7042}
            }"#,
        )
        .unwrap();

        assert_eq!(q.limit, 50);
        assert!(q.sort.is_some());
        assert!(q.cursor.is_some());
        match &q.filters[0] {
            FilterClause::And(clauses) => assert_eq!(clauses.len(), 4),
            _ => panic!("expected And"),
        }
    }

    // === Error cases ===

    #[test]
    fn test_invalid_json() {
        let err = parse("not json at all").unwrap_err();
        assert!(err.message.contains("invalid JSON"));
    }

    #[test]
    fn test_unsupported_operator() {
        let err =
            parse(r#"{"filters": {"field": "x", "op": "like", "value": "foo"}}"#).unwrap_err();
        assert!(err.message.contains("unsupported operator"));
    }

    #[test]
    fn test_missing_field_key() {
        let err = parse(r#"{"filters": {"op": "eq", "value": 1}}"#).unwrap_err();
        assert!(err.message.contains("field"));
    }

    #[test]
    fn test_missing_op_key() {
        let err = parse(r#"{"filters": {"field": "x", "value": 1}}"#).unwrap_err();
        assert!(err.message.contains("op"));
    }

    #[test]
    fn test_missing_value_for_eq() {
        let err = parse(r#"{"filters": {"field": "x", "op": "eq"}}"#).unwrap_err();
        assert!(err.message.contains("value"));
    }

    #[test]
    fn test_in_requires_array() {
        let err =
            parse(r#"{"filters": {"field": "x", "op": "in", "value": 42}}"#).unwrap_err();
        assert!(err.message.contains("array"));
    }

    #[test]
    fn test_eq_null_rewrites_to_is_null() {
        // eq with null value is a shorthand for IsNull
        let q = parse(r#"{"filters": {"field": "x", "op": "eq", "value": null}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::IsNull("x".into()));
    }

    #[test]
    fn test_is_null_op() {
        let q = parse(r#"{"filters": {"field": "publishedAt", "op": "is_null"}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::IsNull("publishedAt".into()));
    }

    #[test]
    fn test_is_not_null_op() {
        let q = parse(r#"{"filters": {"field": "publishedAt", "op": "is_not_null"}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::IsNotNull("publishedAt".into()));
    }

    #[test]
    fn test_is_null_shorthand() {
        let q = parse(r#"{"filters": {"IsNull": "publishedAt"}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::IsNull("publishedAt".into()));
    }

    #[test]
    fn test_is_not_null_shorthand() {
        let q = parse(r#"{"filters": {"IsNotNull": "publishedAt"}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::IsNotNull("publishedAt".into()));
    }

    #[test]
    fn test_filter_must_be_object() {
        let err = parse(r#"{"filters": "not an object"}"#).unwrap_err();
        assert!(err.message.contains("object"));
    }

    #[test]
    fn test_and_requires_array() {
        let err = parse(r#"{"filters": {"AND": "not an array"}}"#).unwrap_err();
        assert!(err.message.contains("array"));
    }

    #[test]
    fn test_and_empty_rejected() {
        let err = parse(r#"{"filters": {"AND": []}}"#).unwrap_err();
        assert!(err.message.contains("at least one"));
    }

    #[test]
    fn test_invalid_sort_direction() {
        let err =
            parse(r#"{"sort": {"field": "x", "direction": "sideways"}}"#).unwrap_err();
        assert!(err.message.contains("direction"));
    }

    #[test]
    fn test_cursor_missing_sort_value() {
        let err = parse(r#"{"cursor": {"slot_id": 1}}"#).unwrap_err();
        assert!(err.message.contains("sort_value"));
    }

    #[test]
    fn test_cursor_missing_slot_id() {
        let err = parse(r#"{"cursor": {"sort_value": 1}}"#).unwrap_err();
        assert!(err.message.contains("slot_id"));
    }

    #[test]
    fn test_empty_field_name() {
        let err = parse(r#"{"filters": {"field": "", "op": "eq", "value": 1}}"#).unwrap_err();
        assert!(err.message.contains("empty"));
    }

    #[test]
    fn test_empty_sort_field() {
        let err = parse(r#"{"sort": {"field": ""}}"#).unwrap_err();
        assert!(err.message.contains("empty"));
    }

    #[test]
    fn test_max_nesting_depth() {
        // Build a deeply nested NOT chain
        let mut json = r#"{"field": "x", "op": "eq", "value": 1}"#.to_string();
        for _ in 0..MAX_NESTING_DEPTH + 2 {
            json = format!(r#"{{"NOT": {json}}}"#);
        }
        let full = format!(r#"{{"filters": {json}}}"#);
        let err = parse(&full).unwrap_err();
        assert!(err.message.contains("nesting"));
    }

    #[test]
    fn test_trait_object_safe() {
        let parser: Box<dyn QueryParser> = Box::new(JsonQueryParser);
        assert_eq!(parser.content_type(), "application/json");
    }

    // === Float values ===

    #[test]
    fn test_float_value() {
        let q =
            parse(r#"{"filters": {"field": "score", "op": "gt", "value": 3.14}}"#).unwrap();
        assert_eq!(
            q.filters[0],
            FilterClause::Gt("score".into(), Value::Float(3.14))
        );
    }

    // === In with empty array ===

    #[test]
    fn test_in_empty_array() {
        let q = parse(r#"{"filters": {"field": "x", "op": "in", "value": []}}"#).unwrap();
        assert_eq!(q.filters[0], FilterClause::In("x".into(), vec![]));
    }

    // === In with mixed types ===

    #[test]
    fn test_in_mixed_types() {
        let q = parse(
            r#"{"filters": {"field": "x", "op": "in", "value": [1, "two", true]}}"#,
        )
        .unwrap();
        match &q.filters[0] {
            FilterClause::In(_, vals) => {
                assert_eq!(vals.len(), 3);
                assert_eq!(vals[0], Value::Integer(1));
                assert_eq!(vals[1], Value::String("two".into()));
                assert_eq!(vals[2], Value::Bool(true));
            }
            _ => panic!("expected In"),
        }
    }

    // === Large slot_id ===

    #[test]
    fn test_cursor_slot_id_overflow() {
        let err = parse(
            r#"{"cursor": {"sort_value": 1, "slot_id": 5000000000}}"#,
        )
        .unwrap_err();
        assert!(err.message.contains("u32"));
    }
}

#[cfg(test)]
mod fuzz_tests {
    use super::*;
    use proptest::prelude::*;

    // Fuzz test: arbitrary byte input must never panic.
    // The parser should always return Ok or Err, never crash.
    proptest! {
        #[test]
        fn fuzz_arbitrary_bytes(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
            // Must not panic
            let _ = JsonQueryParser.parse(&data);
        }

        #[test]
        fn fuzz_arbitrary_strings(s in "\\PC*") {
            // Must not panic
            let _ = JsonQueryParser.parse(s.as_bytes());
        }

        #[test]
        fn fuzz_json_like_strings(s in r#"\{[a-zA-Z0-9":{}\[\],. _\-]*\}"#) {
            // Must not panic on JSON-like strings
            let _ = JsonQueryParser.parse(s.as_bytes());
        }

        #[test]
        fn fuzz_valid_structure_random_values(
            field in "[a-zA-Z][a-zA-Z0-9_]{0,20}",
            op in prop_oneof![
                Just("eq"), Just("not_eq"), Just("in"),
                Just("gt"), Just("lt"), Just("gte"), Just("lte"),
                Just("invalid_op"), Just(""), Just("EQ")
            ],
            int_val in any::<i64>(),
            limit in 0..100000usize,
        ) {
            let json = format!(
                r#"{{"filters": {{"field": "{field}", "op": "{op}", "value": {int_val}}}, "limit": {limit}}}"#
            );
            let _ = JsonQueryParser.parse(json.as_bytes());
        }

        #[test]
        fn fuzz_nested_compounds(depth in 0..20usize) {
            // Build deeply nested AND/OR/NOT structures
            let mut json = r#"{"field": "x", "op": "eq", "value": 1}"#.to_string();
            for i in 0..depth {
                json = match i % 3 {
                    0 => format!(r#"{{"AND": [{json}]}}"#),
                    1 => format!(r#"{{"OR": [{json}]}}"#),
                    _ => format!(r#"{{"NOT": {json}}}"#),
                };
            }
            let full = format!(r#"{{"filters": {json}}}"#);
            let _ = JsonQueryParser.parse(full.as_bytes());
        }

        #[test]
        fn fuzz_in_operator_random_arrays(
            vals in proptest::collection::vec(
                prop_oneof![
                    any::<i64>().prop_map(|v| v.to_string()),
                    Just("true".to_string()),
                    Just("false".to_string()),
                    Just("null".to_string()),
                    Just(r#""hello""#.to_string()),
                    Just("[]".to_string()),
                    Just("{}".to_string()),
                ],
                0..50
            )
        ) {
            let arr = vals.join(", ");
            let json = format!(
                r#"{{"filters": {{"field": "x", "op": "in", "value": [{arr}]}}}}"#
            );
            let _ = JsonQueryParser.parse(json.as_bytes());
        }

        #[test]
        fn fuzz_cursor_random_values(
            sort_val in any::<i64>(),
            slot_val in any::<i64>(),
        ) {
            let json = format!(
                r#"{{"cursor": {{"sort_value": {sort_val}, "slot_id": {slot_val}}}}}"#
            );
            let _ = JsonQueryParser.parse(json.as_bytes());
        }

        #[test]
        fn fuzz_sort_random_direction(
            field in "[a-zA-Z]{1,20}",
            direction in "\\PC{0,20}",
        ) {
            let json = format!(
                r#"{{"sort": {{"field": "{field}", "direction": "{direction}"}}}}"#
            );
            let _ = JsonQueryParser.parse(json.as_bytes());
        }
    }
}
