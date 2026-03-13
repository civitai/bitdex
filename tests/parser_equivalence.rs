//! Cross-parser equivalence tests.
//!
//! Verifies that the same logical query expressed in all three formats
//! (bitdex, compact, meilisearch) produces identical `BitdexQuery` output.
//! This prevents parser drift as we develop new features.

use bitdex_v2::parser::compact::CompactQueryParser;
use bitdex_v2::parser::json::JsonQueryParser;
use bitdex_v2::parser::meilisearch::MeilisearchQueryParser;
use bitdex_v2::parser::registry;
use bitdex_v2::query::{FilterClause, ParseError, QueryParser, SortDirection, Value};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_bitdex(json: &str) -> Result<bitdex_v2::query::BitdexQuery, ParseError> {
    JsonQueryParser.parse(json.as_bytes())
}

fn parse_compact(json: &str) -> Result<bitdex_v2::query::BitdexQuery, ParseError> {
    CompactQueryParser.parse(json.as_bytes())
}

fn parse_meili(json: &str) -> Result<bitdex_v2::query::BitdexQuery, ParseError> {
    MeilisearchQueryParser.parse(json.as_bytes())
}

/// Normalize filters for comparison: flatten single-element And wrappers and sort
/// top-level clauses by field name for deterministic comparison (HashMap iteration order).
fn sorted_filters(filters: &[FilterClause]) -> Vec<&FilterClause> {
    let mut v: Vec<&FilterClause> = filters.iter().collect();
    v.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    v
}

/// Compare two filter lists ignoring order (top-level clauses come from HashMap keys).
fn filters_match(a: &[FilterClause], b: &[FilterClause]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let sa = sorted_filters(a);
    let sb = sorted_filters(b);
    sa.iter()
        .zip(sb.iter())
        .all(|(x, y)| format!("{x:?}") == format!("{y:?}"))
}

// ---------------------------------------------------------------------------
// Equivalence: simple equality filter + sort + limit
// ---------------------------------------------------------------------------

#[test]
fn equivalence_simple_eq_sort() {
    let bitdex = parse_bitdex(
        r#"{
            "filters": {"field": "nsfwLevel", "op": "eq", "value": 1},
            "sort": {"field": "reactionCount", "direction": "desc"},
            "limit": 50
        }"#,
    )
    .unwrap();

    let compact = parse_compact(
        r#"{
            "filter": {"nsfwLevel": 1},
            "sort": "-reactionCount",
            "limit": 50
        }"#,
    )
    .unwrap();

    let meili = parse_meili(
        r#"{
            "filter": "nsfwLevel = 1",
            "sort": ["reactionCount:desc"],
            "limit": 50
        }"#,
    )
    .unwrap();

    // All should produce Eq("nsfwLevel", Integer(1))
    assert_eq!(bitdex.filters.len(), 1);
    assert_eq!(compact.filters.len(), 1);
    assert_eq!(meili.filters.len(), 1);
    assert_eq!(bitdex.filters[0], compact.filters[0]);
    assert_eq!(bitdex.filters[0], meili.filters[0]);

    // Sort
    assert_eq!(bitdex.sort.as_ref().unwrap().field, "reactionCount");
    assert_eq!(bitdex.sort.as_ref().unwrap().direction, SortDirection::Desc);
    assert_eq!(compact.sort.as_ref().unwrap().field, "reactionCount");
    assert_eq!(compact.sort.as_ref().unwrap().direction, SortDirection::Desc);
    assert_eq!(meili.sort.as_ref().unwrap().field, "reactionCount");
    assert_eq!(meili.sort.as_ref().unwrap().direction, SortDirection::Desc);

    // Limit
    assert_eq!(bitdex.limit, 50);
    assert_eq!(compact.limit, 50);
    assert_eq!(meili.limit, 50);
}

// ---------------------------------------------------------------------------
// Equivalence: In operator
// ---------------------------------------------------------------------------

#[test]
fn equivalence_in_operator() {
    let bitdex = parse_bitdex(
        r#"{"filters": {"field": "tagIds", "op": "in", "value": [12, 45, 678]}}"#,
    )
    .unwrap();

    let compact = parse_compact(r#"{"filter": {"tagIds": [12, 45, 678]}}"#).unwrap();

    let meili = parse_meili(r#"{"filter": "tagIds IN [12, 45, 678]"}"#).unwrap();

    let expected = FilterClause::In(
        "tagIds".into(),
        vec![
            Value::Integer(12),
            Value::Integer(45),
            Value::Integer(678),
        ],
    );

    assert_eq!(bitdex.filters[0], expected);
    assert_eq!(compact.filters[0], expected);
    assert_eq!(meili.filters[0], expected);
}

// ---------------------------------------------------------------------------
// Equivalence: NotEq
// ---------------------------------------------------------------------------

#[test]
fn equivalence_not_eq() {
    let bitdex = parse_bitdex(
        r#"{"filters": {"field": "type", "op": "not_eq", "value": "video"}}"#,
    )
    .unwrap();

    let compact = parse_compact(r#"{"filter": {"type": {"$ne": "video"}}}"#).unwrap();

    let meili = parse_meili(r#"{"filter": "type != video"}"#).unwrap();

    let expected = FilterClause::NotEq("type".into(), Value::String("video".into()));

    assert_eq!(bitdex.filters[0], expected);
    assert_eq!(compact.filters[0], expected);
    assert_eq!(meili.filters[0], expected);
}

// ---------------------------------------------------------------------------
// Equivalence: range (Gte)
// ---------------------------------------------------------------------------

#[test]
fn equivalence_gte() {
    let bitdex = parse_bitdex(
        r#"{"filters": {"field": "publishedAtUnix", "op": "gte", "value": 1700000000}}"#,
    )
    .unwrap();

    let compact = parse_compact(
        r#"{"filter": {"publishedAtUnix": {"$gte": 1700000000}}}"#,
    )
    .unwrap();

    let meili = parse_meili(r#"{"filter": "publishedAtUnix >= 1700000000"}"#).unwrap();

    let expected = FilterClause::Gte("publishedAtUnix".into(), Value::Integer(1700000000));

    assert_eq!(bitdex.filters[0], expected);
    assert_eq!(compact.filters[0], expected);
    assert_eq!(meili.filters[0], expected);
}

// ---------------------------------------------------------------------------
// Equivalence: boolean filter
// ---------------------------------------------------------------------------

#[test]
fn equivalence_bool_eq() {
    let bitdex = parse_bitdex(
        r#"{"filters": {"field": "onSite", "op": "eq", "value": true}}"#,
    )
    .unwrap();

    let compact = parse_compact(r#"{"filter": {"onSite": true}}"#).unwrap();

    let meili = parse_meili(r#"{"filter": "onSite = true"}"#).unwrap();

    let expected = FilterClause::Eq("onSite".into(), Value::Bool(true));

    assert_eq!(bitdex.filters[0], expected);
    assert_eq!(compact.filters[0], expected);
    assert_eq!(meili.filters[0], expected);
}

// ---------------------------------------------------------------------------
// Equivalence: sort direction (asc)
// ---------------------------------------------------------------------------

#[test]
fn equivalence_sort_asc() {
    let bitdex = parse_bitdex(
        r#"{"sort": {"field": "score", "direction": "asc"}}"#,
    )
    .unwrap();

    let compact = parse_compact(r#"{"sort": "score"}"#).unwrap();

    let meili = parse_meili(r#"{"sort": ["score:asc"]}"#).unwrap();

    assert_eq!(bitdex.sort.as_ref().unwrap().field, "score");
    assert_eq!(bitdex.sort.as_ref().unwrap().direction, SortDirection::Asc);
    assert_eq!(compact.sort.as_ref().unwrap().field, "score");
    assert_eq!(compact.sort.as_ref().unwrap().direction, SortDirection::Asc);
    assert_eq!(meili.sort.as_ref().unwrap().field, "score");
    assert_eq!(meili.sort.as_ref().unwrap().direction, SortDirection::Asc);
}

// ---------------------------------------------------------------------------
// Equivalence: AND compound
// ---------------------------------------------------------------------------

#[test]
fn equivalence_and_compound() {
    let bitdex = parse_bitdex(
        r#"{
            "filters": {
                "AND": [
                    {"field": "nsfwLevel", "op": "eq", "value": 1},
                    {"field": "type", "op": "eq", "value": "image"}
                ]
            }
        }"#,
    )
    .unwrap();

    // Compact: implicit AND from multiple top-level keys
    let compact = parse_compact(
        r#"{"filter": {"nsfwLevel": 1, "type": "image"}}"#,
    )
    .unwrap();

    let meili = parse_meili(
        r#"{"filter": "nsfwLevel = 1 AND type = image"}"#,
    )
    .unwrap();

    // BitDex wraps in And(...), compact produces flat list, meili wraps in And(...)
    // Normalize: bitdex has And([Eq, Eq]), compact has [Eq, Eq], meili has And([Eq, Eq])

    // Extract the inner clauses regardless of wrapping
    let bitdex_inner = match &bitdex.filters[0] {
        FilterClause::And(clauses) => clauses.clone(),
        other => vec![other.clone()],
    };

    let meili_inner = match &meili.filters[0] {
        FilterClause::And(clauses) => clauses.clone(),
        other => vec![other.clone()],
    };

    // Compact may produce flat [Eq, Eq] since implicit AND
    let compact_inner = compact.filters.clone();

    assert!(filters_match(&bitdex_inner, &compact_inner), "bitdex vs compact AND mismatch");
    assert!(filters_match(&bitdex_inner, &meili_inner), "bitdex vs meili AND mismatch");
}

// ---------------------------------------------------------------------------
// Equivalence: OR compound
// ---------------------------------------------------------------------------

#[test]
fn equivalence_or_compound() {
    let bitdex = parse_bitdex(
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

    let compact = parse_compact(
        r#"{"filter": {"$or": [{"nsfwLevel": 1}, {"nsfwLevel": 2}]}}"#,
    )
    .unwrap();

    let meili = parse_meili(
        r#"{"filter": "nsfwLevel = 1 OR nsfwLevel = 2"}"#,
    )
    .unwrap();

    // All should produce Or([Eq("nsfwLevel", 1), Eq("nsfwLevel", 2)])
    let expected = FilterClause::Or(vec![
        FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
        FilterClause::Eq("nsfwLevel".into(), Value::Integer(2)),
    ]);

    assert_eq!(bitdex.filters[0], expected);
    assert_eq!(compact.filters[0], expected);
    assert_eq!(meili.filters[0], expected);
}

// ---------------------------------------------------------------------------
// Equivalence: NOT
// ---------------------------------------------------------------------------

#[test]
fn equivalence_not() {
    let bitdex = parse_bitdex(
        r#"{
            "filters": {
                "NOT": {"field": "onSite", "op": "eq", "value": true}
            }
        }"#,
    )
    .unwrap();

    let compact = parse_compact(
        r#"{"filter": {"$not": {"onSite": true}}}"#,
    )
    .unwrap();

    let meili = parse_meili(r#"{"filter": "NOT onSite = true"}"#).unwrap();

    let expected = FilterClause::Not(Box::new(FilterClause::Eq(
        "onSite".into(),
        Value::Bool(true),
    )));

    assert_eq!(bitdex.filters[0], expected);
    assert_eq!(compact.filters[0], expected);
    assert_eq!(meili.filters[0], expected);
}

// ---------------------------------------------------------------------------
// Equivalence: offset pagination
// ---------------------------------------------------------------------------

#[test]
fn equivalence_offset() {
    let bitdex = parse_bitdex(r#"{"limit": 20, "offset": 40}"#).unwrap();

    let compact = parse_compact(r#"{"limit": 20, "offset": 40}"#).unwrap();

    // Meilisearch page/hitsPerPage: page 3, 20 per page = offset 40
    let meili = parse_meili(r#"{"page": 3, "hitsPerPage": 20}"#).unwrap();

    assert_eq!(bitdex.limit, 20);
    assert_eq!(compact.limit, 20);
    assert_eq!(meili.limit, 20);

    assert_eq!(bitdex.offset, Some(40));
    assert_eq!(compact.offset, Some(40));
    assert_eq!(meili.offset, Some(40));
}

// ---------------------------------------------------------------------------
// Equivalence: cursor (compact only — bitdex and compact both support it)
// ---------------------------------------------------------------------------

#[test]
fn equivalence_cursor() {
    let bitdex = parse_bitdex(
        r#"{"cursor": {"sort_value": 342, "slot_id": 7042}}"#,
    )
    .unwrap();

    let compact = parse_compact(r#"{"cursor": "342:7042"}"#).unwrap();

    assert_eq!(bitdex.cursor.as_ref().unwrap().sort_value, 342);
    assert_eq!(bitdex.cursor.as_ref().unwrap().slot_id, 7042);
    assert_eq!(compact.cursor.as_ref().unwrap().sort_value, 342);
    assert_eq!(compact.cursor.as_ref().unwrap().slot_id, 7042);
}

// ---------------------------------------------------------------------------
// Equivalence: Meilisearch TO range = compact combined range
// ---------------------------------------------------------------------------

#[test]
fn equivalence_range_to() {
    let compact = parse_compact(
        r#"{"filter": {"score": {"$gte": 80, "$lte": 100}}}"#,
    )
    .unwrap();

    let meili = parse_meili(r#"{"filter": "score 80 TO 100"}"#).unwrap();

    // Meili produces And([Gte, Lte]), compact produces [Gte, Lte]
    let meili_inner = match &meili.filters[0] {
        FilterClause::And(clauses) => clauses.clone(),
        other => vec![other.clone()],
    };

    assert!(
        filters_match(&compact.filters, &meili_inner),
        "compact range vs meili TO mismatch"
    );
}

// ---------------------------------------------------------------------------
// Equivalence: NotIn
// ---------------------------------------------------------------------------

#[test]
fn equivalence_not_in() {
    let compact = parse_compact(
        r#"{"filter": {"tagIds": {"$nin": [1, 2, 3]}}}"#,
    )
    .unwrap();

    let meili = parse_meili(r#"{"filter": "tagIds NOT IN [1, 2, 3]"}"#).unwrap();

    let expected = FilterClause::NotIn(
        "tagIds".into(),
        vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ],
    );

    assert_eq!(compact.filters[0], expected);
    assert_eq!(meili.filters[0], expected);
}

// ---------------------------------------------------------------------------
// Equivalence: complex multi-filter query (the "real world" query)
// ---------------------------------------------------------------------------

#[test]
fn equivalence_complex_real_world() {
    let bitdex = parse_bitdex(
        r#"{
            "filters": {
                "AND": [
                    {"field": "nsfwLevel", "op": "eq", "value": 1},
                    {"field": "tagIds", "op": "in", "value": [12, 45, 678]},
                    {"field": "type", "op": "eq", "value": "image"},
                    {"field": "publishedAtUnix", "op": "gte", "value": 1700000000}
                ]
            },
            "sort": {"field": "reactionCount", "direction": "desc"},
            "limit": 50,
            "offset": 0
        }"#,
    )
    .unwrap();

    let compact = parse_compact(
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

    let meili = parse_meili(
        r#"{
            "filter": "nsfwLevel = 1 AND tagIds IN [12, 45, 678] AND type = image AND publishedAtUnix >= 1700000000",
            "sort": ["reactionCount:desc"],
            "limit": 50,
            "offset": 0
        }"#,
    )
    .unwrap();

    // All three should have the same 4 clauses
    let bitdex_inner = match &bitdex.filters[0] {
        FilterClause::And(clauses) => clauses.clone(),
        _ => panic!("expected And"),
    };
    let meili_inner = match &meili.filters[0] {
        FilterClause::And(clauses) => clauses.clone(),
        _ => panic!("expected And"),
    };
    let compact_inner = compact.filters.clone();

    assert_eq!(bitdex_inner.len(), 4);
    assert_eq!(compact_inner.len(), 4);
    assert_eq!(meili_inner.len(), 4);

    assert!(filters_match(&bitdex_inner, &compact_inner), "bitdex vs compact");
    assert!(filters_match(&bitdex_inner, &meili_inner), "bitdex vs meili");

    // Sort
    for q in [&bitdex, &compact, &meili] {
        assert_eq!(q.sort.as_ref().unwrap().field, "reactionCount");
        assert_eq!(q.sort.as_ref().unwrap().direction, SortDirection::Desc);
        assert_eq!(q.limit, 50);
        assert_eq!(q.offset, Some(0));
    }
}

// ---------------------------------------------------------------------------
// Registry tests
// ---------------------------------------------------------------------------

#[test]
fn registry_default_is_bitdex() {
    let reg = registry::default_registry();
    assert_eq!(reg.default_format(), "bitdex");
}

#[test]
fn registry_all_formats_parse_empty() {
    let reg = registry::default_registry();
    for fmt in ["bitdex", "compact", "meilisearch"] {
        let q = reg.parse(Some(fmt), b"{}").unwrap();
        assert!(q.filters.is_empty(), "empty query for {fmt}");
        assert!(q.sort.is_none(), "no sort for {fmt}");
    }
}

#[test]
fn registry_unknown_format_errors() {
    let reg = registry::default_registry();
    let err = reg.parse(Some("algolia"), b"{}").unwrap_err();
    assert!(err.message.contains("unknown query format"));
}

#[test]
fn registry_custom_default() {
    let mut reg = registry::default_registry();
    reg.set_default("compact");
    assert_eq!(reg.default_format(), "compact");

    // Parse with None should use compact format
    let q = reg
        .parse(None, br#"{"filter": {"nsfwLevel": 1}, "sort": "-score"}"#)
        .unwrap();
    assert_eq!(q.filters.len(), 1);
    assert_eq!(q.sort.as_ref().unwrap().direction, SortDirection::Desc);
}

// ---------------------------------------------------------------------------
// Meilisearch array filter format
// ---------------------------------------------------------------------------

#[test]
fn meili_array_filter_and_or() {
    // Array format: outer = AND, inner = OR
    let meili = parse_meili(
        r#"{"filter": [["type = image", "type = video"], "nsfwLevel = 1"]}"#,
    )
    .unwrap();

    assert_eq!(meili.filters.len(), 2);

    // First filter should be OR(type=image, type=video)
    match &meili.filters[0] {
        FilterClause::Or(clauses) => {
            assert_eq!(clauses.len(), 2);
        }
        _ => panic!("expected Or from inner array"),
    }

    // Second filter should be Eq(nsfwLevel, 1)
    assert_eq!(
        meili.filters[1],
        FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))
    );
}

// ---------------------------------------------------------------------------
// Meilisearch precedence
// ---------------------------------------------------------------------------

#[test]
fn meili_and_binds_tighter_than_or() {
    // "A OR B AND C" should parse as "A OR (B AND C)"
    let q = parse_meili(
        r#"{"filter": "nsfwLevel = 1 OR type = image AND onSite = true"}"#,
    )
    .unwrap();

    match &q.filters[0] {
        FilterClause::Or(clauses) => {
            assert_eq!(clauses.len(), 2);
            match &clauses[1] {
                FilterClause::And(inner) => assert_eq!(inner.len(), 2),
                _ => panic!("expected inner And"),
            }
        }
        _ => panic!("expected Or"),
    }
}

#[test]
fn meili_parens_override_precedence() {
    let q = parse_meili(
        r#"{"filter": "(nsfwLevel = 1 OR type = image) AND onSite = true"}"#,
    )
    .unwrap();

    match &q.filters[0] {
        FilterClause::And(clauses) => {
            assert_eq!(clauses.len(), 2);
            match &clauses[0] {
                FilterClause::Or(_) => {}
                _ => panic!("expected inner Or"),
            }
        }
        _ => panic!("expected And"),
    }
}

// ---------------------------------------------------------------------------
// Compact: combined range on same field
// ---------------------------------------------------------------------------

#[test]
fn compact_combined_range() {
    let q = parse_compact(
        r#"{"filter": {"score": {"$gte": 10, "$lt": 100}}}"#,
    )
    .unwrap();

    // Should produce two filter clauses: Gte and Lt
    assert_eq!(q.filters.len(), 2);

    let has_gte = q
        .filters
        .iter()
        .any(|c| matches!(c, FilterClause::Gte(f, Value::Integer(10)) if f == "score"));
    let has_lt = q
        .filters
        .iter()
        .any(|c| matches!(c, FilterClause::Lt(f, Value::Integer(100)) if f == "score"));
    assert!(has_gte, "missing $gte clause");
    assert!(has_lt, "missing $lt clause");
}

// ---------------------------------------------------------------------------
// Compact: explicit $in and $nin
// ---------------------------------------------------------------------------

#[test]
fn compact_explicit_in() {
    let q = parse_compact(r#"{"filter": {"tagIds": {"$in": [1, 2, 3]}}}"#).unwrap();
    assert_eq!(
        q.filters[0],
        FilterClause::In(
            "tagIds".into(),
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        )
    );
}

// ---------------------------------------------------------------------------
// Error handling: each parser rejects bad input gracefully
// ---------------------------------------------------------------------------

#[test]
fn all_parsers_reject_garbage() {
    let garbage = b"not json at all";
    assert!(parse_bitdex(std::str::from_utf8(garbage).unwrap()).is_err());
    assert!(CompactQueryParser.parse(garbage).is_err());
    assert!(MeilisearchQueryParser.parse(garbage).is_err());
}

#[test]
fn meili_rejects_unterminated_string() {
    let result = parse_meili(r#"{"filter": "type = 'unterminated"}"#);
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("unterminated"));
}

#[test]
fn compact_rejects_unknown_operator() {
    let result = parse_compact(r#"{"filter": {"x": {"$regex": ".*"}}}"#);
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("unsupported operator"));
}

#[test]
fn all_parsers_reject_limit_zero() {
    assert!(parse_bitdex(r#"{"limit": 0}"#).is_err());
    assert!(parse_compact(r#"{"limit": 0}"#).is_err());
    assert!(parse_meili(r#"{"limit": 0}"#).is_err());
}
