//! Full sync config YAML parser for V2.
//!
//! Parses `sync-config-civitai.yaml` which defines:
//! - Connection strings (postgres, clickhouse, bitdex)
//! - Dump phases (ordered CSV processing for initial load)
//! - Triggers (PG triggers that write to BitdexOps for steady-state sync)
//!
//! This config is read by `bitdex-sync` only. The BitDex server receives
//! dump instructions via PUT /dumps request body, populated from this config.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::pg_sync::trigger_gen::SyncSource;

/// Top-level sync config parsed from the YAML file.
#[derive(Debug, Clone, Deserialize)]
pub struct FullSyncConfig {
    /// Index name (e.g., "civitai").
    pub index: String,

    /// Postgres connection config.
    pub postgres: Option<PostgresConfig>,

    /// ClickHouse connection config.
    pub clickhouse: Option<ClickHouseConnConfig>,

    /// BitDex server connection config.
    pub bitdex: Option<BitdexConnConfig>,

    /// Ordered dump phases for initial loading.
    #[serde(default)]
    pub dump_phases: Vec<DumpPhase>,

    /// Trigger definitions for steady-state sync.
    #[serde(default)]
    pub triggers: Vec<SyncSource>,
}

/// Postgres connection config from sync YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub connection_string: String,
    pub stage_dir: Option<String>,
}

/// ClickHouse connection config from sync YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseConnConfig {
    pub connection_string: String,
}

/// BitDex server connection config from sync YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct BitdexConnConfig {
    pub url: String,
    pub index: String,
}

/// A single dump phase — defines how to process one CSV table.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DumpPhase {
    /// Phase name (e.g., "tags", "images", "resources").
    pub name: String,

    /// PG table name.
    pub table: Option<String>,

    /// SQL COPY query to download data from PG.
    pub copy_query: Option<String>,

    /// Data source (defaults to "postgres"). "clickhouse" for metrics.
    pub source: Option<String>,

    /// ClickHouse query (when source = "clickhouse").
    pub query: Option<String>,

    /// Data format: "csv" (default) or "tsv".
    #[serde(default = "default_format")]
    pub format: String,

    /// Column that maps to the BitDex slot ID.
    pub slot_field: Option<String>,

    /// If true, this phase sets the alive bit on new slots.
    #[serde(default)]
    pub sets_alive: bool,

    /// Explicit column names for headerless CSVs (PG COPY output).
    /// When present, a header line is prepended during CSV download and
    /// included in the dump request so the dump processor can map columns
    /// by name instead of relying on a header row in the CSV.
    #[serde(default)]
    pub columns: Vec<String>,

    /// Field mappings from CSV columns to BitDex fields.
    /// Can be a simple string (column = target) or { column, target }.
    #[serde(default)]
    pub fields: Vec<FieldMapping>,

    /// Computed fields derived from expressions.
    #[serde(default)]
    pub computed_fields: Vec<ComputedField>,

    /// Row filter expression (e.g., "(attributes >> 10) & 1 = 0").
    pub filter: Option<String>,

    /// Enrichment lookups (joins with other CSVs).
    #[serde(default)]
    pub enrichment: Vec<EnrichmentDef>,
}

/// Field mapping: either a simple string or { column, target }.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FieldMapping {
    /// Simple: column name = target field name.
    Simple(String),
    /// Explicit mapping: { column: "src", target: "dest" }.
    Mapped {
        column: String,
        target: String,
    },
}

impl FieldMapping {
    /// Get the target field name (BitDex field).
    pub fn target(&self) -> &str {
        match self {
            FieldMapping::Simple(s) => s,
            FieldMapping::Mapped { target, .. } => target,
        }
    }

    /// Get the source column name (CSV column).
    pub fn column(&self) -> &str {
        match self {
            FieldMapping::Simple(s) => s,
            FieldMapping::Mapped { column, .. } => column,
        }
    }
}

/// A computed field derived from an expression.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ComputedField {
    /// Target field name in BitDex.
    pub target: String,
    /// Expression to evaluate (e.g., "(flags >> 13) & 1 == 1").
    pub expression: String,
    /// For conditional computed fields: the value column to use when expression is true.
    pub value: Option<String>,
}

/// Enrichment definition — join with another CSV for additional fields.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnrichmentDef {
    /// CSV file path (relative to stage_dir) or lookup name.
    pub lookup: Option<String>,
    /// PG table name for the lookup.
    pub table: Option<String>,
    /// SQL COPY query to download the lookup CSV.
    pub copy_query: Option<String>,
    /// Explicit column names for headerless CSVs (PG COPY output).
    #[serde(default)]
    pub columns: Vec<String>,
    /// Column in the lookup CSV used as the join key.
    pub key: String,
    /// Column in the main CSV used to look up into the enrichment.
    pub join_on: String,
    /// Field mappings from the lookup CSV.
    #[serde(default)]
    pub fields: Vec<FieldMapping>,
    /// Computed fields from the lookup.
    #[serde(default)]
    pub computed_fields: Vec<ComputedField>,
    /// Filter expression on the lookup (e.g., "type = 'Checkpoint'").
    pub filter: Option<String>,
    /// Nested enrichment (e.g., MV → Model chain).
    #[serde(default)]
    pub enrichment: Vec<EnrichmentDef>,
}

fn default_format() -> String {
    "csv".to_string()
}

impl FullSyncConfig {
    /// Load from a YAML file.
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {}", path.display(), e))?;
        Self::from_yaml(&contents)
    }

    /// Parse from a YAML string. Validates each trigger source (see
    /// `SyncSource::validate`) — this is the production entrypoint
    /// (`from_file` -> here; `bin/pg_sync.rs` reads the deploy config through
    /// it), so a structurally invalid trigger (e.g. `rows_from` combined with
    /// `field`/`query`, which `generate_trigger_body`'s dispatch order would
    /// otherwise resolve silently in the wrong direction) must fail loudly
    /// here, not just in trigger_gen's own test-only `SyncConfig::from_yaml`.
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        let config: FullSyncConfig = serde_yaml::from_str(yaml)
            .map_err(|e| format!("failed to parse sync config: {e}"))?;
        for source in &config.triggers {
            source.validate()?;
        }
        Ok(config)
    }

    /// Get the triggers as SyncSource refs (for trigger_gen).
    pub fn trigger_sources(&self) -> &[SyncSource] {
        &self.triggers
    }
}

impl DumpPhase {
    /// Compute a deterministic hash of this dump phase's config.
    /// Used for dump names: "{name}-{hash8}".
    /// Changes when fields, computed_fields, enrichment, filter, or slot_field change.
    pub fn config_hash(&self) -> String {
        // Serialize to a canonical string for hashing.
        // We hash the JSON representation for determinism.
        let hashable = serde_json::to_string(self).unwrap_or_default();
        let mut hasher = DefaultHasher::new();
        hashable.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Build the dump name: "{name}-{hash8}".
    pub fn dump_name(&self) -> String {
        let hash = self.config_hash();
        format!("{}-{}", self.name, &hash[..8])
    }

    /// Build the dump request body JSON to send to BitDex via PUT /dumps.
    pub fn to_dump_request(&self, stage_dir: &Path) -> serde_json::Value {
        let _csv_path = if let Some(_lookup) = self.enrichment.first().and_then(|_| None::<String>) {
            // Not needed at top level
            serde_json::Value::Null
        } else {
            let filename = format!("{}.{}", self.name, if self.format == "tsv" { "tsv" } else { "csv" });
            serde_json::Value::String(stage_dir.join(&filename).to_string_lossy().into_owned())
        };

        let csv_filename = format!("{}.{}", self.name, if self.format == "tsv" { "tsv" } else { "csv" });

        let fields: Vec<serde_json::Value> = self.fields.iter().map(|f| {
            match f {
                FieldMapping::Simple(s) => serde_json::json!(s),
                FieldMapping::Mapped { column, target } => {
                    serde_json::json!({ "column": column, "target": target })
                }
            }
        }).collect();

        let computed: Vec<serde_json::Value> = self.computed_fields.iter().map(|cf| {
            let mut obj = serde_json::json!({
                "target": cf.target,
                "expression": cf.expression,
            });
            if let Some(ref v) = cf.value {
                obj["value"] = serde_json::json!(v);
            }
            obj
        }).collect();

        let enrichment: Vec<serde_json::Value> = self.enrichment.iter().map(|e| {
            enrichment_to_json(e, stage_dir)
        }).collect();

        let mut req = serde_json::json!({
            "name": self.dump_name(),
            "csv_path": stage_dir.join(&csv_filename).to_string_lossy().into_owned(),
            "format": self.format,
            "slot_field": self.slot_field,
            "sets_alive": self.sets_alive,
            "fields": fields,
            "computed_fields": computed,
            "enrichment": enrichment,
        });

        if let Some(ref filter) = self.filter {
            req["filter"] = serde_json::json!(filter);
        }

        if !self.columns.is_empty() {
            req["columns"] = serde_json::json!(self.columns);
        }

        req
    }
}

/// Convert an enrichment def to the JSON format expected by BitDex's dump processor.
fn enrichment_to_json(e: &EnrichmentDef, stage_dir: &Path) -> serde_json::Value {
    let csv_path = if let Some(ref lookup) = e.lookup {
        stage_dir.join(lookup).to_string_lossy().into_owned()
    } else {
        String::new()
    };

    let fields: Vec<serde_json::Value> = e.fields.iter().map(|f| {
        match f {
            FieldMapping::Simple(s) => serde_json::json!(s),
            FieldMapping::Mapped { column, target } => {
                serde_json::json!({ "column": column, "target": target })
            }
        }
    }).collect();

    let computed: Vec<serde_json::Value> = e.computed_fields.iter().map(|cf| {
        let mut obj = serde_json::json!({
            "target": cf.target,
            "expression": cf.expression,
        });
        if let Some(ref v) = cf.value {
            obj["value"] = serde_json::json!(v);
        }
        obj
    }).collect();

    let nested: Vec<serde_json::Value> = e.enrichment.iter().map(|ne| {
        enrichment_to_json(ne, stage_dir)
    }).collect();

    let mut obj = serde_json::json!({
        "csv_path": csv_path,
        "key": e.key,
        "join_on": e.join_on,
        "fields": fields,
        "computed_fields": computed,
        "enrichment": nested,
    });

    if let Some(ref filter) = e.filter {
        obj["filter"] = serde_json::json!(filter);
    }

    if !e.columns.is_empty() {
        obj["columns"] = serde_json::json!(e.columns);
    }

    obj
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_sync_config() {
        let yaml = r#"
index: test
dump_phases: []
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        assert_eq!(config.index, "test");
        assert!(config.dump_phases.is_empty());
        assert!(config.triggers.is_empty());
    }

    /// Delta-review MAJOR: `SyncSource::validate()` was previously only wired
    /// into `trigger_gen::SyncConfig::from_yaml` — a TEST-ONLY type nothing in
    /// production calls. The real production entrypoint is
    /// `FullSyncConfig::from_file` -> `from_yaml` (this file), which
    /// `bin/pg_sync.rs` reads the deploy config through, and the trigger-SQL
    /// review-file generator (`generate_trigger_sql_review_file`, this file)
    /// also loads through it. This test pushes a structurally invalid trigger
    /// (`rows_from` combined with `field` — which `generate_trigger_body`'s
    /// dispatch order, `field > rows_from > query > direct`, would otherwise
    /// resolve silently by ignoring `rows_from`) through the REAL production
    /// type, not the test-only `trigger_gen::SyncConfig`.
    #[test]
    fn test_full_sync_config_rejects_invalid_trigger_source() {
        let yaml = r#"
index: test
dump_phases: []
triggers:
  - table: Post
    type: fan_out_per_row
    field: tagIds
    value_field: tagId
    rows_from: { table: Image, fk: postId }
"#;
        let err = FullSyncConfig::from_yaml(yaml).expect_err(
            "FullSyncConfig::from_yaml (the production entrypoint) must reject \
             rows_from + field, not just trigger_gen's test-only SyncConfig",
        );
        assert!(err.contains("field"), "error should name the conflicting key: {err}");
        assert!(err.contains("Post"), "error should name the offending table: {err}");
    }

    /// Sanity: a valid fan_out_per_row trigger still parses cleanly through
    /// the production entrypoint (no false positive from the new validation).
    #[test]
    fn test_full_sync_config_accepts_valid_fan_out_per_row_trigger() {
        let yaml = r#"
index: test
dump_phases: []
triggers:
  - table: Post
    type: fan_out_per_row
    rows_from: { table: Image, fk: postId }
    track_fields: [publishedAt]
"#;
        let config = FullSyncConfig::from_yaml(yaml)
            .expect("valid fan_out_per_row trigger must parse through FullSyncConfig");
        assert_eq!(config.triggers.len(), 1);
        assert_eq!(config.triggers[0].table_type.as_deref(), Some("fan_out_per_row"));
    }

    #[test]
    fn test_parse_dump_phase_simple_fields() {
        let yaml = r#"
index: test
dump_phases:
  - name: tools
    table: ImageTool
    copy_query: "COPY (SELECT \"toolId\", \"imageId\" FROM \"ImageTool\") TO STDOUT"
    slot_field: imageId
    fields: [toolIds]
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        assert_eq!(config.dump_phases.len(), 1);
        let phase = &config.dump_phases[0];
        assert_eq!(phase.name, "tools");
        assert_eq!(phase.fields.len(), 1);
        assert_eq!(phase.fields[0].target(), "toolIds");
        assert_eq!(phase.fields[0].column(), "toolIds");
    }

    #[test]
    fn test_parse_dump_phase_mapped_fields() {
        let yaml = r#"
index: test
dump_phases:
  - name: tags
    slot_field: imageId
    fields:
      - { column: tagId, target: tagIds }
    filter: "(attributes >> 10) & 1 = 0"
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        let phase = &config.dump_phases[0];
        assert_eq!(phase.fields[0].column(), "tagId");
        assert_eq!(phase.fields[0].target(), "tagIds");
        assert_eq!(phase.filter.as_deref(), Some("(attributes >> 10) & 1 = 0"));
    }

    #[test]
    fn test_parse_computed_fields() {
        let yaml = r#"
index: test
dump_phases:
  - name: images
    slot_field: id
    sets_alive: true
    fields: [nsfwLevel]
    computed_fields:
      - target: hasMeta
        expression: "(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0"
      - target: existedAt
        expression: "max(scannedAtSecs, createdAtSecs)"
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        let phase = &config.dump_phases[0];
        assert!(phase.sets_alive);
        assert_eq!(phase.computed_fields.len(), 2);
        assert_eq!(phase.computed_fields[0].target, "hasMeta");
        assert_eq!(phase.computed_fields[1].target, "existedAt");
    }

    #[test]
    fn test_parse_enrichment() {
        let yaml = r#"
index: test
dump_phases:
  - name: images
    slot_field: id
    fields: [nsfwLevel]
    enrichment:
      - lookup: posts.csv
        key: id
        join_on: postId
        fields:
          - { column: publishedAtSecs, target: publishedAt }
        computed_fields:
          - target: isPublished
            expression: "publishedAtSecs != null"
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        let phase = &config.dump_phases[0];
        assert_eq!(phase.enrichment.len(), 1);
        let enrich = &phase.enrichment[0];
        assert_eq!(enrich.lookup.as_deref(), Some("posts.csv"));
        assert_eq!(enrich.key, "id");
        assert_eq!(enrich.join_on, "postId");
        assert_eq!(enrich.fields[0].target(), "publishedAt");
        assert_eq!(enrich.computed_fields[0].target, "isPublished");
    }

    #[test]
    fn test_parse_nested_enrichment() {
        let yaml = r#"
index: test
dump_phases:
  - name: resources
    slot_field: imageId
    fields:
      - { column: modelVersionId, target: modelVersionIds }
    enrichment:
      - lookup: model_versions.csv
        key: id
        join_on: modelVersionId
        fields:
          - { column: baseModel, target: baseModel }
        enrichment:
          - lookup: models.csv
            key: id
            join_on: modelId
            fields:
              - { column: poi, target: poi }
            filter: "type = 'Checkpoint'"
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        let phase = &config.dump_phases[0];
        let outer = &phase.enrichment[0];
        assert_eq!(outer.enrichment.len(), 1);
        let inner = &outer.enrichment[0];
        assert_eq!(inner.lookup.as_deref(), Some("models.csv"));
        assert_eq!(inner.filter.as_deref(), Some("type = 'Checkpoint'"));
    }

    #[test]
    fn test_parse_triggers() {
        let yaml = r#"
index: test
dump_phases: []
triggers:
  - table: Image
    slot_field: id
    sets_alive: true
    on_delete: delete_slot
    track_fields: [nsfwLevel, type]
  - table: TagsOnImageNew
    slot_field: imageId
    field: tagIds
    value_field: tagId
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        assert_eq!(config.triggers.len(), 2);
        assert_eq!(config.triggers[0].table, "Image");
        assert!(config.triggers[0].sets_alive);
        assert_eq!(config.triggers[1].field.as_deref(), Some("tagIds"));
    }

    #[test]
    fn test_dump_name_includes_hash() {
        let yaml = r#"
index: test
dump_phases:
  - name: tags
    slot_field: imageId
    fields:
      - { column: tagId, target: tagIds }
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        let name = config.dump_phases[0].dump_name();
        assert!(name.starts_with("tags-"), "Expected 'tags-...' got '{name}'");
        assert_eq!(name.len(), "tags-".len() + 8);
    }

    #[test]
    fn test_config_hash_changes_with_fields() {
        let yaml1 = r#"
index: test
dump_phases:
  - name: tags
    slot_field: imageId
    fields:
      - { column: tagId, target: tagIds }
triggers: []
"#;
        let yaml2 = r#"
index: test
dump_phases:
  - name: tags
    slot_field: imageId
    fields:
      - { column: tagId, target: tagIds }
      - { column: disabled, target: tagDisabled }
triggers: []
"#;
        let c1 = FullSyncConfig::from_yaml(yaml1).unwrap();
        let c2 = FullSyncConfig::from_yaml(yaml2).unwrap();
        assert_ne!(
            c1.dump_phases[0].config_hash(),
            c2.dump_phases[0].config_hash(),
            "Different fields should produce different hashes"
        );
    }

    #[test]
    fn test_dump_request_body() {
        let yaml = r#"
index: test
dump_phases:
  - name: tags
    table: TagsOnImageNew
    slot_field: imageId
    fields:
      - { column: tagId, target: tagIds }
    filter: "(attributes >> 10) & 1 = 0"
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        let phase = &config.dump_phases[0];
        let req = phase.to_dump_request(Path::new("/data/load_stage"));

        assert_eq!(req["slot_field"], "imageId");
        assert_eq!(req["sets_alive"], false);
        assert_eq!(req["filter"], "(attributes >> 10) & 1 = 0");
        assert_eq!(req["format"], "csv");
        assert!(req["csv_path"].as_str().unwrap().contains("tags.csv"));
        assert_eq!(req["fields"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_dump_request_with_enrichment() {
        let yaml = r#"
index: test
dump_phases:
  - name: images
    slot_field: id
    sets_alive: true
    fields: [nsfwLevel]
    enrichment:
      - lookup: posts.csv
        key: id
        join_on: postId
        fields:
          - { column: publishedAtSecs, target: publishedAt }
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        let req = config.dump_phases[0].to_dump_request(Path::new("/data"));

        assert_eq!(req["sets_alive"], true);
        let enrichment = req["enrichment"].as_array().unwrap();
        assert_eq!(enrichment.len(), 1);
        assert!(enrichment[0]["csv_path"].as_str().unwrap().contains("posts.csv"));
        assert_eq!(enrichment[0]["key"], "id");
        assert_eq!(enrichment[0]["join_on"], "postId");
    }

    #[test]
    fn test_parse_tsv_metrics_phase() {
        let yaml = r#"
index: test
dump_phases:
  - name: metrics
    source: clickhouse
    format: tsv
    slot_field: imageId
    fields: [reactionCount, commentCount, collectedCount]
triggers: []
"#;
        let config = FullSyncConfig::from_yaml(yaml).unwrap();
        let phase = &config.dump_phases[0];
        assert_eq!(phase.source.as_deref(), Some("clickhouse"));
        assert_eq!(phase.format, "tsv");
        assert_eq!(phase.fields.len(), 3);
    }

    #[test]
    fn generate_trigger_sql_review_file() {
        // Generates docs/design/trigger-sql-review.sql for safety review
        // before deploying triggers to production PG. Reads the DEPLOY config
        // (the one production actually uses) — the old docs/design copy no
        // longer exists, which made this generator silently no-op.
        let yaml_path = concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/configs/sync-config-civitai.yaml");
        let yaml = match std::fs::read_to_string(yaml_path) {
            Ok(y) => y,
            Err(_) => return, // Skip if file not found (CI)
        };

        let config = FullSyncConfig::from_yaml(&yaml).unwrap();
        let mut sql = String::new();

        sql.push_str("-- =======================================================================\n");
        sql.push_str("-- BitDex Sync V2 — Trigger SQL Review\n");
        sql.push_str("-- Generated from: deploy/configs/sync-config-civitai.yaml\n");
        sql.push_str("-- Run `cargo test generate_trigger_sql_review_file` to regenerate\n");
        sql.push_str("-- \n");
        sql.push_str("-- This file is for REVIEW ONLY. Do not execute manually.\n");
        sql.push_str("-- bitdex-sync setup will execute these statements via run_setup_v2().\n");
        sql.push_str("-- =======================================================================\n\n");

        // Part 1: SETUP_V2_SQL (tables)
        sql.push_str("-- -----------------------------------------------------------------------\n");
        sql.push_str("-- Part 1: V2 Tables (BitdexOps + bitdex_cursors + cleanup trigger)\n");
        sql.push_str("-- -----------------------------------------------------------------------\n\n");
        sql.push_str(crate::pg_sync::queries::SETUP_V2_SQL);
        sql.push_str("\n\n");

        // Part 2: Per-trigger SQL
        sql.push_str("-- -----------------------------------------------------------------------\n");
        sql.push_str("-- Part 2: Per-table triggers (generated from sync config YAML)\n");
        sql.push_str("-- -----------------------------------------------------------------------\n\n");

        for (i, trigger) in config.triggers.iter().enumerate() {
            let name = crate::pg_sync::trigger_gen::trigger_name(trigger);
            let trigger_sql = crate::pg_sync::trigger_gen::generate_trigger_sql(trigger);

            sql.push_str(&format!("-- [{}/{}] Table: {} → Trigger: {}\n", i + 1, config.triggers.len(), trigger.table, name));
            if let Some(ref tt) = trigger.table_type {
                sql.push_str(&format!("-- Type: {}\n", tt));
            }
            if trigger.sets_alive {
                sql.push_str("-- Sets alive: yes\n");
            }
            if trigger.on_delete.as_ref().map(|v| v.is_delete()).unwrap_or(false) {
                sql.push_str("-- On delete: emit delete op\n");
            }
            sql.push('\n');
            sql.push_str(&trigger_sql);
            sql.push_str("\n\n");
        }

        // Part 3: Summary
        sql.push_str("-- -----------------------------------------------------------------------\n");
        sql.push_str("-- Summary\n");
        sql.push_str("-- -----------------------------------------------------------------------\n");
        sql.push_str(&format!("-- Tables created: BitdexOps, bitdex_cursors\n"));
        sql.push_str(&format!("-- Triggers: {}\n", config.triggers.len()));
        for trigger in &config.triggers {
            let name = crate::pg_sync::trigger_gen::trigger_name(trigger);
            sql.push_str(&format!("--   {} on \"{}\"\n", name, trigger.table));
        }
        sql.push_str("--\n");
        sql.push_str("-- Safety notes:\n");
        sql.push_str("-- - All triggers use CREATE OR REPLACE (idempotent)\n");
        sql.push_str("-- - All triggers use ENABLE ALWAYS (works with replication)\n");
        sql.push_str("-- - Trigger functions ONLY INSERT into \"BitdexOps\" (no other mutations)\n");
        sql.push_str("-- - No table locks, no schema changes to existing tables\n");
        sql.push_str("-- - BitdexOps cleanup trigger only DELETEs consumed ops rows\n");

        let out_path = concat!(env!("CARGO_MANIFEST_DIR"), "/docs/design/trigger-sql-review.sql");
        std::fs::write(out_path, &sql).unwrap();
        eprintln!("Wrote trigger SQL review to: {out_path}");
        eprintln!("SQL length: {} bytes, {} triggers", sql.len(), config.triggers.len());
    }

    #[test]
    fn test_parse_full_civitai_config() {
        let yaml = std::fs::read_to_string(
            concat!(env!("CARGO_MANIFEST_DIR"), "/docs/design/sync-config-civitai.yaml")
        );
        if let Ok(yaml) = yaml {
            let config = FullSyncConfig::from_yaml(&yaml).unwrap();
            assert_eq!(config.index, "civitai");
            assert!(config.dump_phases.len() >= 6, "Expected >=6 dump phases");
            assert!(config.triggers.len() >= 5, "Expected >=5 triggers");
        }
        // Skip test if file not found (CI)
    }

    #[test]
    fn test_parse_deploy_sync_config() {
        // This test validates the DEPLOY config — the one actually used in production.
        // Must always pass; never silently skip.
        let yaml = std::fs::read_to_string(
            concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/configs/sync-config-civitai.yaml")
        ).expect("deploy/configs/sync-config-civitai.yaml must exist");

        let config = FullSyncConfig::from_yaml(&yaml)
            .expect("deploy sync config must parse without errors");

        assert_eq!(config.index, "civitai");
        assert!(!config.dump_phases.is_empty(), "Must have dump phases");

        // Verify images phase exists with correct fields
        let images = config.dump_phases.iter().find(|p| p.name == "images")
            .expect("Must have images dump phase");
        assert!(images.sets_alive);
        assert!(!images.fields.is_empty());
        assert!(!images.computed_fields.is_empty());
        assert!(!images.columns.is_empty(), "images must have explicit columns");

        // Verify enrichment has columns
        if let Some(enrich) = images.enrichment.first() {
            assert!(!enrich.columns.is_empty(), "posts enrichment must have explicit columns");
        }
    }

    // -----------------------------------------------------------------------
    // W2-1 config-level guards (scheduled-publish plan)
    // -----------------------------------------------------------------------

    /// Load the production deploy sync config.
    fn load_deploy_sync_config() -> FullSyncConfig {
        let yaml = std::fs::read_to_string(
            concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/configs/sync-config-civitai.yaml")
        ).expect("deploy/configs/sync-config-civitai.yaml must exist");
        // from_yaml now runs SyncSource::validate() on every trigger (c4a4aa7),
        // so a green parse here also proves the fan_out_per_row/shared_ops_fields
        // structural rules hold on the real config.
        FullSyncConfig::from_yaml(&yaml).expect("deploy sync config must parse + validate")
    }

    /// Extract the set of BitDex field names a generated trigger emits, by
    /// scanning the generated SQL for `'field', '<name>'` op markers (covers
    /// both inlined ops and the shared `bitdex_*_ops` functions embedded in the
    /// trigger SQL).
    fn emitted_trigger_fields(sql: &str) -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        let marker = "'field', '";
        let mut rest = sql;
        while let Some(pos) = rest.find(marker) {
            let after = &rest[pos + marker.len()..];
            if let Some(end) = after.find('\'') {
                out.insert(after[..end].to_string());
            }
            rest = after;
        }
        out
    }

    /// [W1-1 review minor 3 / PR-m2] Disjointness on the REAL deploy config.
    ///
    /// The load-bearing invariant is that the new `sortAtUnix` writer (the Image
    /// trigger's shared function) and the Post per-image fan-out never emit the
    /// SAME field for one image — an overlap would let op_dedup LIFO-resolve the
    /// two writers nondeterministically.
    ///
    /// The Image trigger DOES also emit {publishedAt, availability, postedToId}
    /// (the 2026-07-07 "Mode B" INSERT-read for images added to an already-
    /// published post). That overlap with the Post fan-out is INTENTIONAL and
    /// safe: the two writers fire for temporally-separated events and resolve to
    /// the SAME value (both read Post.publishedAt), so LIFO order is immaterial.
    /// This test pins that the overlap is EXACTLY that documented set and that
    /// `sortAtUnix` (and `model3dId`) are strictly disjoint.
    #[test]
    fn test_post_fanout_disjoint_from_sortatunix() {
        use crate::pg_sync::trigger_gen::generate_trigger_sql;

        let config = load_deploy_sync_config();
        let post = config.triggers.iter().find(|t| t.table == "Post")
            .expect("Post trigger must exist");
        let image = config.triggers.iter().find(|t| t.table == "Image")
            .expect("Image trigger must exist");

        // The Post trigger is the per-image materialized fan-out (not queryOpSet).
        assert_eq!(post.table_type.as_deref(), Some("fan_out_per_row"),
            "Post must be fan_out_per_row");
        assert!(post.rows_from.is_some(), "Post fan_out_per_row must set rows_from");
        assert!(post.query.is_none(), "Post must not carry a query fan-out anymore");

        let post_fields = emitted_trigger_fields(&generate_trigger_sql(post));
        let image_fields = emitted_trigger_fields(&generate_trigger_sql(image));
        assert!(!post_fields.is_empty() && !image_fields.is_empty());

        // Post fan-out emits exactly the retained payload (PR-M1) — and crucially
        // NOT sortAtUnix.
        let expected_post: std::collections::HashSet<String> =
            ["publishedAt", "availability", "postedToId"].iter().map(|s| s.to_string()).collect();
        assert_eq!(post_fields, expected_post,
            "Post fan-out must emit exactly {{publishedAt, availability, postedToId}}, got {post_fields:?}");

        // sortAtUnix is emitted by the Image trigger and is DISJOINT from Post.
        assert!(image_fields.contains("sortAtUnix"), "Image trigger must emit sortAtUnix");
        assert!(!post_fields.contains("sortAtUnix"),
            "Post fan-out must NOT emit sortAtUnix — it is the Image trigger's field");
        // model3dId likewise Image-only (INSERT-read, not carried on the fan-out).
        assert!(image_fields.contains("model3dId"), "Image trigger must emit model3dId");
        assert!(!post_fields.contains("model3dId"), "Post fan-out must NOT emit model3dId");

        // The ONLY overlap between the two writers is the documented, safe Mode-B set.
        let overlap: std::collections::HashSet<String> =
            post_fields.intersection(&image_fields).cloned().collect();
        assert_eq!(overlap, expected_post,
            "the only Post∩Image overlap must be the intentional Mode-B set \
             {{publishedAt, availability, postedToId}}; any other overlap (esp. \
             sortAtUnix) would be op_dedup-nondeterministic. Got {overlap:?}");
    }

    /// [PR-B4 emission side / AR-4] The Image trigger emits sortAtUnix in
    /// MILLISECONDS via the shared function, on the REAL deploy config. Seconds
    /// here would shrink every value 1000× once the engine's ms_to_seconds
    /// mapping divides by 1000.
    #[test]
    fn test_image_sortatunix_emitted_ms_on_deploy_config() {
        use crate::pg_sync::trigger_gen::generate_trigger_sql;

        let config = load_deploy_sync_config();
        let image = config.triggers.iter().find(|t| t.table == "Image")
            .expect("Image trigger must exist");
        assert_eq!(image.shared_ops_fields.as_deref(), Some(&["sortAtUnix".to_string()][..]),
            "Image must factor sortAtUnix into the shared re-emitter function");

        let sql = generate_trigger_sql(image);
        assert!(sql.contains("* 1000"), "sortAtUnix must be milliseconds:\n{sql}");
        assert!(sql.contains("_ops := _ops || bitdex_image_sortat_ops(NEW);"),
            "INSERT must call the shared sortat ops function (re-emitter parity):\n{sql}");
        assert!(emitted_trigger_fields(&sql).contains("sortAtUnix"));
    }

    /// [PR-B4 mapping side + PR-M5] Index config: sortAt is INGESTED not computed,
    /// the sortAtUnix→sortAt mapping keeps ms_to_seconds + fallback semantics, and
    /// model3dId is indexed. Parsed generically (the `serde_yaml` FEATURE flag
    /// that gates Config::from_yaml is not active under --features pg-sync, but the
    /// crate is available, so we walk the YAML value tree directly).
    #[test]
    fn test_index_config_sortat_ingested_and_model3did() {
        let yaml = std::fs::read_to_string(
            concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/configs/civitai-index.yaml")
        ).expect("deploy/configs/civitai-index.yaml must exist");
        let root: serde_yaml::Value =
            serde_yaml::from_str(&yaml).expect("civitai-index.yaml must be valid YAML");

        let config = &root["config"];

        // sortAt sort field: present, and NO `computed:` block ([PR-M5]).
        let sort_fields = config["sort_fields"].as_sequence().expect("sort_fields seq");
        let sort_at = sort_fields.iter()
            .find(|f| f["name"].as_str() == Some("sortAt"))
            .expect("sortAt sort field must exist");
        assert!(sort_at.get("computed").is_none(),
            "sortAt must NOT be computed — ingested and computed must never coexist ([PR-M5])");

        // model3dId filter field: single_value, per_value_lazy.
        let filter_fields = config["filter_fields"].as_sequence().expect("filter_fields seq");
        let model3did = filter_fields.iter()
            .find(|f| f["name"].as_str() == Some("model3dId"))
            .expect("model3dId filter field must exist");
        assert_eq!(model3did["field_type"].as_str(), Some("single_value"));
        assert_eq!(model3did["per_value_lazy"].as_bool(), Some(true));

        // time_buckets pins the query-side field/units.
        let tb = &config["time_buckets"];
        assert_eq!(tb["filter_field"].as_str(), Some("sortAtUnix"));
        assert_eq!(tb["sort_field"].as_str(), Some("sortAt"));

        // data_schema: sortAtUnix→sortAt mapping intact (ms_to_seconds + fallback),
        // and model3dId ingested.
        let ds_fields = root["data_schema"]["fields"].as_sequence().expect("data_schema.fields seq");
        let sortat_map = ds_fields.iter()
            .find(|f| f["source"].as_str() == Some("sortAtUnix") && f["target"].as_str() == Some("sortAt"))
            .expect("sortAtUnix->sortAt data_schema mapping must exist");
        assert_eq!(sortat_map["ms_to_seconds"].as_bool(), Some(true),
            "sortAtUnix->sortAt must divide ms→s (emission is ms)");
        assert_eq!(sortat_map["fallback"].as_str(), Some("sortAt"),
            "dump path emits `sortAt` seconds via this fallback (no ms division)");
        assert!(ds_fields.iter().any(|f| f["target"].as_str() == Some("model3dId")),
            "model3dId must be in data_schema");
    }
}
