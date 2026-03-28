//! YAML-driven PG trigger SQL generator for V2 ops pipeline.
//!
//! Reads a `sync_sources` YAML config and generates PL/pgSQL trigger functions
//! that emit ops into the BitdexOps table. Two table types:
//!
//! **Direct tables** (slot = PG column):
//! - `track_fields`: scalar fields → emit remove/set pairs via IS DISTINCT FROM
//! - `field` + `value_field`: multi-value join tables → emit add/remove
//! - `on_delete: delete_slot`: emit delete op
//! - `sets_alive: true`: only this table can create new alive slots
//!
//! **Fan-out tables** (slots resolved by BitDex query):
//! - `query`: BitDex query template with {column} placeholders
//! - `query_source`: optional PG subquery for cross-table values
//! - `track_fields`: fields to track on the source table

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::Deserialize;

/// A tracked field entry in a trigger's track_fields list.
/// Can be a simple string ("nsfwLevel") or a structured map
/// ({ column: "type", target: "type", expression: "{type}::text" }).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TrackField {
    /// Simple field name (column = target, no expression).
    Simple(String),
    /// Structured field with optional column/target/expression.
    Mapped {
        column: Option<String>,
        target: Option<String>,
        expression: Option<String>,
    },
}

impl TrackField {
    /// Convert to the string format expected by parse_track_field.
    /// Simple("nsfwLevel") → "nsfwLevel"
    /// Mapped { column: "type", expression: "{type}::text" } → "{type}::text as type"
    pub fn to_track_string(&self) -> String {
        match self {
            TrackField::Simple(s) => s.clone(),
            TrackField::Mapped { column, target, expression } => {
                let field_name = target.as_deref()
                    .or(column.as_deref())
                    .unwrap_or("unknown");
                if let Some(expr) = expression {
                    format!("{} as {}", expr, field_name)
                } else if let Some(col) = column {
                    if target.is_some() && target.as_deref() != Some(col.as_str()) {
                        // column differs from target — use "column" as expression alias
                        format!("\"{}\" as {}", col, field_name)
                    } else {
                        field_name.to_string()
                    }
                } else {
                    field_name.to_string()
                }
            }
        }
    }
}

/// Computed field for triggers (same shape as dump computed_fields).
#[derive(Debug, Clone, Deserialize)]
pub struct TriggerComputedField {
    pub target: String,
    pub expression: String,
    pub value: Option<String>,
}

/// A value that can be bool (true) or string ("delete_slot").
/// The YAML uses `on_delete: true` but trigger_gen expects "delete_slot".
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OnDeleteValue {
    Bool(bool),
    String(String),
}

impl OnDeleteValue {
    /// Returns true if deletion should emit a delete op.
    pub fn is_delete(&self) -> bool {
        match self {
            OnDeleteValue::Bool(b) => *b,
            OnDeleteValue::String(s) => s == "delete_slot" || s == "true",
        }
    }
}

/// A sync source definition from the YAML config.
#[derive(Debug, Clone, Deserialize)]
pub struct SyncSource {
    /// PG table name (e.g., "Image", "TagsOnImageNew")
    pub table: String,

    /// For direct tables: PG column that maps to the BitDex slot ID
    pub slot_field: Option<String>,

    /// For direct tables: list of scalar fields to track.
    /// Can be strings ("nsfwLevel") or maps ({ column, target, expression }).
    pub track_fields: Option<Vec<TrackField>>,

    /// Computed fields for triggers (bitfield extraction, etc.)
    pub computed_fields: Option<Vec<TriggerComputedField>>,

    /// For multi-value join tables: the BitDex field name (e.g., "tagIds")
    pub field: Option<String>,

    /// For multi-value join tables: the PG column containing the value (e.g., "tagId")
    pub value_field: Option<String>,

    /// Optional SQL WHERE filter for the trigger (e.g., CollectionItem status filter)
    pub filter: Option<String>,

    /// If true, this table's INSERT ops set the alive bit on new slots
    #[serde(default)]
    pub sets_alive: bool,

    /// Whether DELETE should emit a delete op. Can be bool or "delete_slot".
    pub on_delete: Option<OnDeleteValue>,

    /// For fan-out tables: BitDex query template with {column} placeholders
    pub query: Option<String>,

    /// For fan-out tables: PG subquery to get values not on the triggering table
    pub query_source: Option<String>,

    /// Table type: "fan_out" for fan-out tables, omit for direct/join tables.
    #[serde(rename = "type")]
    pub table_type: Option<String>,

    /// SQL JOIN clause for fan-out triggers (e.g., Model join for Checkpoint filter).
    pub join: Option<String>,

    /// Custom SQL expression for slot resolution (e.g., Model's json_agg subquery).
    pub expression: Option<String>,

    /// Tables that must be loaded before this one during dumps
    #[serde(rename = "dependsOn")]
    pub depends_on: Option<Vec<String>>,
}

/// Full sync config loaded from YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
    pub sync_sources: Vec<SyncSource>,
}

impl SyncConfig {
    /// Load from a YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        serde_yaml::from_str(yaml).map_err(|e| format!("Failed to parse sync config: {e}"))
    }
}

/// Generate the trigger function name with hash for reconciliation.
/// Format: bitdex_{table}_{hash8}
pub fn trigger_function_name(source: &SyncSource) -> String {
    let body = generate_trigger_body(source);
    let hash = short_hash(&body);
    format!(
        "bitdex_{}_ops_{}",
        source.table.to_lowercase(),
        hash
    )
}

/// Generate the trigger name.
pub fn trigger_name(source: &SyncSource) -> String {
    let body = generate_trigger_body(source);
    let hash = short_hash(&body);
    format!("bitdex_{}_{}", source.table.to_lowercase(), hash)
}

/// Generate the full CREATE OR REPLACE FUNCTION + CREATE TRIGGER SQL
/// for a sync source.
pub fn generate_trigger_sql(source: &SyncSource) -> String {
    let func_name = trigger_function_name(source);
    let trig_name = trigger_name(source);
    let body = generate_trigger_body(source);

    let has_delete = source.on_delete.as_ref().map(|v| v.is_delete()).unwrap_or(false);
    let trigger_events = if source.field.is_some() {
        // Multi-value join table: INSERT and DELETE only
        "AFTER INSERT OR DELETE"
    } else if has_delete {
        "AFTER INSERT OR UPDATE OR DELETE"
    } else {
        "AFTER INSERT OR UPDATE"
    };

    format!(
        r#"CREATE OR REPLACE FUNCTION {func_name}() RETURNS trigger AS $$
{body}
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS {trig_name} ON "{table}";
CREATE TRIGGER {trig_name} {trigger_events} ON "{table}"
  FOR EACH ROW EXECUTE FUNCTION {func_name}();
ALTER TABLE "{table}" ENABLE ALWAYS TRIGGER {trig_name};
"#,
        func_name = func_name,
        trig_name = trig_name,
        body = body,
        trigger_events = trigger_events,
        table = source.table,
    )
}

/// Generate the PL/pgSQL function body for a sync source.
fn generate_trigger_body(source: &SyncSource) -> String {
    if let Some(ref field) = source.field {
        // Multi-value join table (tags, tools, techniques, etc.)
        generate_multi_value_body(source, field)
    } else if source.query.is_some() {
        // Fan-out table (ModelVersion, Post, Model)
        generate_fan_out_body(source)
    } else {
        // Direct table (Image)
        generate_direct_body(source)
    }
}

/// Generate body for direct tables (e.g., Image).
fn generate_direct_body(source: &SyncSource) -> String {
    let slot_field = source.slot_field.as_deref().unwrap_or("id");
    let track_strings: Vec<String> = source.track_fields.as_deref().unwrap_or(&[])
        .iter().map(|tf| tf.to_track_string()).collect();
    let track_fields: Vec<&str> = track_strings.iter().map(|s| s.as_str()).collect();
    let has_delete = source.on_delete.as_ref().map(|v| v.is_delete()).unwrap_or(false);

    let mut body = String::from("DECLARE\n  _ops jsonb;\nBEGIN\n");

    // INSERT: emit set ops for all tracked fields (no remove since no prior state)
    body.push_str("  IF TG_OP = 'INSERT' THEN\n");
    body.push_str("    _ops := jsonb_build_array(\n");
    let insert_ops: Vec<String> = track_fields
        .iter()
        .map(|f| {
            let (field_name, expr) = parse_track_field(f);
            let new_expr = substitute_columns(&expr, "NEW");
            format!(
                "      jsonb_build_object('op', 'set', 'field', '{}', 'value', to_jsonb({}))",
                field_name, new_expr
            )
        })
        .collect();
    body.push_str(&insert_ops.join(",\n"));
    body.push_str("\n    );\n");
    body.push_str(&format!(
        "    INSERT INTO \"BitdexOps\" (entity_id, ops) VALUES (NEW.\"{}\", _ops);\n",
        slot_field
    ));
    body.push_str("    RETURN NEW;\n");

    // DELETE
    if has_delete {
        body.push_str("  ELSIF TG_OP = 'DELETE' THEN\n");
        body.push_str(&format!(
            "    INSERT INTO \"BitdexOps\" (entity_id, ops) VALUES (OLD.\"{}\", '[{{\"op\":\"delete\"}}]'::jsonb);\n",
            slot_field
        ));
        body.push_str("    RETURN OLD;\n");
    }

    // UPDATE: emit remove/set pairs only for changed fields
    body.push_str("  ELSE\n");
    body.push_str("    _ops := '[]'::jsonb;\n");
    for f in track_fields {
        let (field_name, expr) = parse_track_field(f);
        let old_expr = substitute_columns(&expr, "OLD");
        let new_expr = substitute_columns(&expr, "NEW");
        body.push_str(&format!(
            "    IF ({old}) IS DISTINCT FROM ({new}) THEN\n\
             \x20     _ops := _ops || jsonb_build_array(\n\
             \x20       jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb({old})),\n\
             \x20       jsonb_build_object('op', 'set', 'field', '{field}', 'value', to_jsonb({new}))\n\
             \x20     );\n\
             \x20   END IF;\n",
            old = old_expr,
            new = new_expr,
            field = field_name,
        ));
    }
    body.push_str("    IF jsonb_array_length(_ops) > 0 THEN\n");
    body.push_str(&format!(
        "      INSERT INTO \"BitdexOps\" (entity_id, ops) VALUES (NEW.\"{}\", _ops);\n",
        slot_field
    ));
    body.push_str("    END IF;\n");
    body.push_str("    RETURN NEW;\n");
    body.push_str("  END IF;\n");
    body.push_str("END;");

    body
}

/// Generate body for multi-value join tables (e.g., TagsOnImageNew).
fn generate_multi_value_body(source: &SyncSource, field: &str) -> String {
    let slot_field = source.slot_field.as_deref().unwrap_or("imageId");
    let value_field = source.value_field.as_deref().unwrap_or("id");
    let filter_clause = source
        .filter
        .as_ref()
        .map(|f| format!("    IF {} THEN\n", f.replace("imageId", "NEW.\"imageId\"")))
        .unwrap_or_default();
    let filter_end = if source.filter.is_some() {
        "    END IF;\n"
    } else {
        ""
    };

    format!(
        r#"BEGIN
  IF TG_OP = 'INSERT' THEN
{filter_start}    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (NEW."{slot}", jsonb_build_array(
      jsonb_build_object('op', 'add', 'field', '{field}', 'value', to_jsonb(NEW."{value}"))
    ));
{filter_end}    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO "BitdexOps" (entity_id, ops)
    VALUES (OLD."{slot}", jsonb_build_array(
      jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb(OLD."{value}"))
    ));
    RETURN OLD;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;"#,
        slot = slot_field,
        field = field,
        value = value_field,
        filter_start = filter_clause,
        filter_end = filter_end,
    )
}

/// Generate body for fan-out tables (e.g., ModelVersion, Post).
fn generate_fan_out_body(source: &SyncSource) -> String {
    let query_template = source.query.as_deref().unwrap_or("");
    let track_strings: Vec<String> = source.track_fields.as_deref().unwrap_or(&[])
        .iter().map(|tf| tf.to_track_string()).collect();
    let track_fields: Vec<&str> = track_strings.iter().map(|s| s.as_str()).collect();

    let mut body = String::from("DECLARE\n  _ops jsonb;\n  _query text;\n");

    // If there's a query_source, we need a variable for its result
    if source.query_source.is_some() {
        body.push_str("  _source_result jsonb;\n");
    }
    body.push_str("BEGIN\n");
    body.push_str("  IF TG_OP = 'UPDATE' THEN\n");

    // Build the query string with column substitution
    if let Some(ref query_source) = source.query_source {
        let source_sql = substitute_columns(query_source, "NEW");
        body.push_str(&format!(
            "    EXECUTE format('SELECT ({})') INTO _source_result;\n",
            source_sql.replace('\'', "''")
        ));
        // Substitute the query_source result into the query template
        body.push_str(&format!(
            "    _query := '{}';\n",
            query_template
        ));
        // Replace placeholders with source result values
        body.push_str("    -- Substitute source values into query template\n");
    } else {
        // Direct substitution from NEW columns
        let query_sql = substitute_columns(query_template, "NEW");
        body.push_str(&format!("    _query := '{}';\n", query_sql));
    }

    // Build ops array from tracked fields that changed
    body.push_str("    _ops := '[]'::jsonb;\n");
    for f in track_fields {
        let (field_name, expr) = parse_track_field(f);
        let old_expr = substitute_columns(&expr, "OLD");
        let new_expr = substitute_columns(&expr, "NEW");
        body.push_str(&format!(
            "    IF ({old}) IS DISTINCT FROM ({new}) THEN\n\
             \x20     _ops := _ops || jsonb_build_array(\n\
             \x20       jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb({old})),\n\
             \x20       jsonb_build_object('op', 'set', 'field', '{field}', 'value', to_jsonb({new}))\n\
             \x20     );\n\
             \x20   END IF;\n",
            old = old_expr,
            new = new_expr,
            field = field_name,
        ));
    }

    body.push_str("    IF jsonb_array_length(_ops) > 0 THEN\n");
    body.push_str(&format!(
        "      INSERT INTO \"BitdexOps\" (entity_id, ops) VALUES (NEW.id, jsonb_build_array(\n\
         \x20       jsonb_build_object('op', 'queryOpSet', 'query', _query, 'ops', _ops)\n\
         \x20     ));\n"
    ));
    body.push_str("    END IF;\n");
    body.push_str("    RETURN NEW;\n");
    body.push_str("  END IF;\n");
    body.push_str("  RETURN COALESCE(NEW, OLD);\n");
    body.push_str("END;");

    body
}

/// Parse a track_field entry. Returns (bitdex_field_name, sql_expression).
/// Simple field: "nsfwLevel" → ("nsfwLevel", "\"nsfwLevel\"")
/// Expression: "GREATEST({scannedAt}, {createdAt}) as existedAt" → ("existedAt", "GREATEST(\"scannedAt\", \"createdAt\")")
fn parse_track_field(field: &str) -> (String, String) {
    if let Some(as_pos) = field.to_lowercase().rfind(" as ") {
        let expr = &field[..as_pos].trim();
        let alias = &field[as_pos + 4..].trim();
        // Replace {col} with "col" (quoted column reference)
        let sql = expr
            .replace('{', "\"")
            .replace('}', "\"");
        (alias.to_string(), sql)
    } else {
        // Simple field name
        (field.to_string(), format!("\"{}\"", field))
    }
}

/// Substitute {column} placeholders with prefix."column" references.
/// E.g., substitute_columns("GREATEST({scannedAt}, {createdAt})", "NEW")
///   → "GREATEST(NEW.\"scannedAt\", NEW.\"createdAt\")"
fn substitute_columns(expr: &str, prefix: &str) -> String {
    let mut result = String::new();
    let mut chars = expr.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' {
            let mut col = String::new();
            while let Some(&next) = chars.peek() {
                if next == '}' {
                    chars.next();
                    break;
                }
                col.push(chars.next().unwrap());
            }
            result.push_str(&format!("{}.\"{}\"", prefix, col));
        } else {
            result.push(c);
        }
    }
    result
}

/// Compute a short (8-char) hash of a string.
fn short_hash(s: &str) -> String {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:016x}", hasher.finish())[..8].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_track_field_simple() {
        let (name, expr) = parse_track_field("nsfwLevel");
        assert_eq!(name, "nsfwLevel");
        assert_eq!(expr, "\"nsfwLevel\"");
    }

    #[test]
    fn test_parse_track_field_expression() {
        let (name, expr) = parse_track_field("GREATEST({scannedAt}, {createdAt}) as existedAt");
        assert_eq!(name, "existedAt");
        assert_eq!(expr, "GREATEST(\"scannedAt\", \"createdAt\")");
    }

    #[test]
    fn test_substitute_columns() {
        let result = substitute_columns("GREATEST({scannedAt}, {createdAt})", "NEW");
        assert_eq!(result, "GREATEST(NEW.\"scannedAt\", NEW.\"createdAt\")");
    }

    #[test]
    fn test_substitute_columns_simple() {
        let result = substitute_columns("{nsfwLevel}", "OLD");
        assert_eq!(result, "OLD.\"nsfwLevel\"");
    }

    /// Helper to build a test SyncSource with defaults for unused fields.
    fn test_source(table: &str) -> SyncSource {
        SyncSource {
            table: table.into(),
            slot_field: None,
            track_fields: None,
            computed_fields: None,
            field: None,
            value_field: None,
            filter: None,
            sets_alive: false,
            on_delete: None,
            query: None,
            query_source: None,
            table_type: None,
            join: None,
            expression: None,
            depends_on: None,
        }
    }

    #[test]
    fn test_generate_multi_value_trigger() {
        let source = SyncSource {
            slot_field: Some("imageId".into()),
            field: Some("tagIds".into()),
            value_field: Some("tagId".into()),
            ..test_source("TagsOnImageNew")
        };
        let sql = generate_trigger_sql(&source);
        assert!(sql.contains("CREATE OR REPLACE FUNCTION"));
        assert!(sql.contains("'add'"));
        assert!(sql.contains("'remove'"));
        assert!(sql.contains("tagIds"));
        assert!(sql.contains("ENABLE ALWAYS"));
    }

    #[test]
    fn test_generate_direct_trigger() {
        let source = SyncSource {
            slot_field: Some("id".into()),
            track_fields: Some(vec![
                TrackField::Simple("nsfwLevel".into()),
                TrackField::Simple("type".into()),
            ]),
            sets_alive: true,
            on_delete: Some(OnDeleteValue::String("delete_slot".into())),
            ..test_source("Image")
        };
        let sql = generate_trigger_sql(&source);
        assert!(sql.contains("IS DISTINCT FROM"));
        assert!(sql.contains("nsfwLevel"));
        assert!(sql.contains("delete"));
    }

    #[test]
    fn test_generate_fan_out_trigger() {
        let source = SyncSource {
            track_fields: Some(vec![TrackField::Simple("baseModel".into())]),
            query: Some("modelVersionIds eq {id}".into()),
            ..test_source("ModelVersion")
        };
        let sql = generate_trigger_sql(&source);
        assert!(sql.contains("queryOpSet"));
        assert!(sql.contains("modelVersionIds eq"));
    }

    #[test]
    fn test_trigger_name_includes_hash() {
        let source = SyncSource {
            slot_field: Some("id".into()),
            track_fields: Some(vec![TrackField::Simple("nsfwLevel".into())]),
            ..test_source("Image")
        };
        let name = trigger_name(&source);
        assert!(name.starts_with("bitdex_image_"));
        assert_eq!(name.len(), "bitdex_image_".len() + 8);
    }

    #[test]
    fn test_trigger_hash_changes_with_config() {
        let source1 = SyncSource {
            slot_field: Some("id".into()),
            track_fields: Some(vec![TrackField::Simple("nsfwLevel".into())]),
            ..test_source("Image")
        };
        let source2 = SyncSource {
            track_fields: Some(vec![
                TrackField::Simple("nsfwLevel".into()),
                TrackField::Simple("type".into()),
            ]),
            ..source1.clone()
        };
        let name1 = trigger_name(&source1);
        let name2 = trigger_name(&source2);
        assert_ne!(name1, name2, "Different configs should produce different hashes");
    }

    #[test]
    fn test_yaml_parsing() {
        let yaml = r#"
sync_sources:
  - table: Image
    slot_field: id
    sets_alive: true
    track_fields: [nsfwLevel, type]
    on_delete: delete_slot
  - table: TagsOnImageNew
    slot_field: imageId
    field: tagIds
    value_field: tagId
  - table: ModelVersion
    query: "modelVersionIds eq {id}"
    track_fields: [baseModel]
"#;
        let config = SyncConfig::from_yaml(yaml).unwrap();
        assert_eq!(config.sync_sources.len(), 3);
        assert_eq!(config.sync_sources[0].table, "Image");
        assert!(config.sync_sources[0].sets_alive);
        assert_eq!(config.sync_sources[1].field.as_deref(), Some("tagIds"));
        assert!(config.sync_sources[2].query.is_some());
    }

    #[test]
    fn test_yaml_parsing_mapped_track_fields() {
        let yaml = r#"
sync_sources:
  - table: Image
    slot_field: id
    track_fields:
      - nsfwLevel
      - { column: type, expression: "{type}::text" }
      - { column: modelVersionId, target: modelVersionIds }
"#;
        let config = SyncConfig::from_yaml(yaml).unwrap();
        let track = config.sync_sources[0].track_fields.as_ref().unwrap();
        assert_eq!(track.len(), 3);
        // Simple string
        assert_eq!(track[0].to_track_string(), "nsfwLevel");
        // Expression with column
        assert_eq!(track[1].to_track_string(), "{type}::text as type");
        // Column→target mapping
        assert!(track[2].to_track_string().contains("modelVersionIds"));
    }

    #[test]
    fn test_yaml_parsing_on_delete_bool() {
        let yaml = r#"
sync_sources:
  - table: Image
    slot_field: id
    on_delete: true
"#;
        let config = SyncConfig::from_yaml(yaml).unwrap();
        assert!(config.sync_sources[0].on_delete.as_ref().unwrap().is_delete());
    }

    #[test]
    fn test_expression_in_track_fields() {
        let source = SyncSource {
            slot_field: Some("id".into()),
            track_fields: Some(vec![
                TrackField::Simple("nsfwLevel".into()),
                TrackField::Simple("GREATEST({scannedAt}, {createdAt}) as existedAt".into()),
                TrackField::Simple("({flags} & (1 << 13)) != 0 AND ({flags} & (1 << 2)) = 0 as hasMeta".into()),
            ]),
            sets_alive: true,
            on_delete: Some(OnDeleteValue::String("delete_slot".into())),
            ..test_source("Image")
        };
        let sql = generate_trigger_sql(&source);
        assert!(sql.contains("GREATEST"));
        assert!(sql.contains("existedAt"));
        assert!(sql.contains("hasMeta"));
    }

    #[test]
    fn test_track_field_to_string_mapped_with_expression() {
        let tf = TrackField::Mapped {
            column: Some("publishedAt".into()),
            target: Some("publishedAt".into()),
            expression: Some("extract(epoch from {publishedAt})::bigint".into()),
        };
        let s = tf.to_track_string();
        assert_eq!(s, "extract(epoch from {publishedAt})::bigint as publishedAt");
    }

    #[test]
    fn test_track_field_to_string_mapped_column_only() {
        let tf = TrackField::Mapped {
            column: Some("tagId".into()),
            target: Some("tagIds".into()),
            expression: None,
        };
        let s = tf.to_track_string();
        assert!(s.contains("tagIds"), "Expected 'tagIds' in '{s}'");
    }
}
