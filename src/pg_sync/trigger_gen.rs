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
                        // column differs from target — use {column} template for substitute_columns
                        format!("{{{}}} as {}", col, field_name)
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

/// Child-row source for the `fan_out_per_row` trigger type.
///
/// Instead of emitting one `queryOpSet` row that BitDex resolves against a
/// moving index, the trigger enumerates the child rows transactionally in PG:
/// `INSERT INTO "BitdexOps" SELECT c."<slot>", <ops> FROM "<table>" c WHERE
/// c."<fk>" = NEW."<parent_key>"`. The match set is fixed inside the owning
/// transaction — it cannot be partial, early, or re-resolved on WAL replay.
#[derive(Debug, Clone, Deserialize)]
pub struct RowsFrom {
    /// Child table the per-row ops are emitted for (e.g. "Image").
    pub table: String,
    /// Foreign-key column on the child table pointing at the parent PK
    /// (e.g. "postId").
    pub fk: String,
    /// Child column that maps to the BitDex slot ID. Defaults to "id".
    #[serde(default = "default_rows_from_id")]
    pub slot: String,
    /// Parent PK column the FK references. Defaults to "id".
    #[serde(default = "default_rows_from_id")]
    pub parent_key: String,
}

fn default_rows_from_id() -> String {
    "id".to_string()
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

    /// Table type: "fan_out" for query-resolved fan-out, "fan_out_per_row" for
    /// PG-materialized per-child-row fan-out, omit for direct/join tables.
    #[serde(rename = "type")]
    pub table_type: Option<String>,

    /// For `fan_out_per_row` tables: the child table + FK that enumerate the
    /// per-row op targets transactionally (e.g. Image rows of a Post).
    pub rows_from: Option<RowsFrom>,

    /// Direct-table (or fan_out_per_row) target field names whose `set` op
    /// should be factored into its own named, callable PG function
    /// (`bitdex_<table>_<suffix>_ops(_i "<table>") RETURNS jsonb`) rather than
    /// inlined into the trigger body. Lets an upstream re-emitter re-assert the
    /// field with mechanical parity to the trigger — same builder, same value —
    /// instead of reimplementing the expression and risking drift. E.g.
    /// `[sortAtUnix]` on the Image source.
    #[serde(default)]
    pub shared_ops_fields: Option<Vec<String>>,

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
    /// Load from a YAML string. Fails loudly (not silently) on structurally
    /// invalid sources — see `SyncSource::validate`.
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        let config: SyncConfig = serde_yaml::from_str(yaml)
            .map_err(|e| format!("Failed to parse sync config: {e}"))?;
        for source in &config.sync_sources {
            source.validate()?;
        }
        Ok(config)
    }
}

impl SyncSource {
    /// Structural validation for table-type combinations that `generate_trigger_body`
    /// would otherwise resolve silently in the wrong direction.
    ///
    /// `generate_trigger_body` dispatches in priority order: `field` (multi-value
    /// join) > `rows_from` (per-row fan-out) > `query` (query-resolved fan-out) >
    /// direct table. That priority means a config that sets `rows_from` alongside
    /// `field` or `query` doesn't error — `field`/`query` silently wins and
    /// `rows_from` is dropped on the floor. Reject that combination at parse time
    /// instead of generating a trigger that quietly ignores half its own config.
    pub fn validate(&self) -> Result<(), String> {
        let is_fan_out_per_row = self.table_type.as_deref() == Some("fan_out_per_row");

        if is_fan_out_per_row && self.rows_from.is_none() {
            return Err(format!(
                "sync source \"{}\": type: fan_out_per_row requires `rows_from`",
                self.table
            ));
        }
        if self.rows_from.is_some() && !is_fan_out_per_row {
            return Err(format!(
                "sync source \"{}\": `rows_from` is set but type is not fan_out_per_row \
                 (got {:?}) — set `type: fan_out_per_row` explicitly",
                self.table, self.table_type
            ));
        }
        if let Some(ref rows) = self.rows_from {
            if rows.table.trim().is_empty() {
                return Err(format!(
                    "sync source \"{}\": rows_from.table must not be empty",
                    self.table
                ));
            }
            if rows.fk.trim().is_empty() {
                return Err(format!(
                    "sync source \"{}\": rows_from.fk must not be empty",
                    self.table
                ));
            }
            // field/query win silently over rows_from in generate_trigger_body's
            // dispatch order — reject the ambiguous combination rather than let
            // half the config vanish.
            if self.field.is_some() {
                return Err(format!(
                    "sync source \"{}\": `rows_from` cannot be combined with `field` \
                     (multi-value join dispatch takes priority and would silently \
                     ignore rows_from)",
                    self.table
                ));
            }
            if self.query.is_some() {
                return Err(format!(
                    "sync source \"{}\": `rows_from` cannot be combined with `query` \
                     — pick one fan-out mechanism (fan_out_per_row via rows_from, or \
                     fan_out via query)",
                    self.table
                ));
            }
        }
        // Fail loudly at parse time, not at codegen time, if a shared_ops_fields
        // entry doesn't resolve to a tracked/computed field.
        for target in self.shared_ops_fields.as_deref().unwrap_or(&[]) {
            generate_shared_field_ops_function_sql(self, target)?;
        }
        Ok(())
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

/// The trigger's firing events, as the `AFTER ...` clause text.
///
/// Paired with [`trigger_type_mask`], which must describe the same events in
/// `pg_trigger.tgtype` form — they are two encodings of one fact, so they are
/// defined next to each other and changed together.
fn trigger_events(source: &SyncSource) -> &'static str {
    let has_delete = source.on_delete.as_ref().map(|v| v.is_delete()).unwrap_or(false);
    if source.field.is_some() {
        // Multi-value join table: INSERT and DELETE only
        "AFTER INSERT OR DELETE"
    } else if has_delete {
        "AFTER INSERT OR UPDATE OR DELETE"
    } else {
        "AFTER INSERT OR UPDATE"
    }
}

/// The same events as [`trigger_events`], encoded as `pg_trigger.tgtype`.
///
/// Bits, from PostgreSQL's `catalog/pg_trigger.h`: ROW = 1, BEFORE = 2 (unset
/// means AFTER), INSERT = 4, DELETE = 8, UPDATE = 16. Every trigger we emit is
/// `AFTER ... FOR EACH ROW`, so BEFORE is never set and ROW always is.
///
/// This exists so a caller can ask "is the trigger already installed exactly as
/// we would install it?" against the catalog, instead of re-issuing the DDL to
/// find out. See `queries::trigger_is_current`.
pub fn trigger_type_mask(source: &SyncSource) -> i16 {
    const ROW: i16 = 1 << 0;
    const INSERT: i16 = 1 << 2;
    const DELETE: i16 = 1 << 3;
    const UPDATE: i16 = 1 << 4;

    match trigger_events(source) {
        "AFTER INSERT OR DELETE" => ROW | INSERT | DELETE,
        "AFTER INSERT OR UPDATE OR DELETE" => ROW | INSERT | UPDATE | DELETE,
        _ => ROW | INSERT | UPDATE,
    }
}

/// The function definitions a trigger depends on: the shared op-payload
/// functions (fan-out and `shared_ops_fields`) plus the trigger function
/// itself.
///
/// Split out from the trigger DDL because the two have very different costs.
/// `CREATE OR REPLACE FUNCTION` locks the `pg_proc` row and nothing else, so it
/// is safe to run on every boot. `CREATE TRIGGER` takes an AccessExclusiveLock
/// on the *table* — see [`generate_trigger_ddl_sql`].
pub fn generate_trigger_functions_sql(source: &SyncSource) -> String {
    let func_name = trigger_function_name(source);
    let body = generate_trigger_body(source);

    // For per-row fan-out, emit the shared op-payload function first so the
    // trigger (and the W1-3 re-emitter) can call it. Single source of the op
    // JSONB shape.
    let mut ops_function = match source.rows_from {
        Some(ref rows) => format!("{}\n", generate_fan_out_ops_function_sql(source, rows)),
        None => String::new(),
    };
    // Each `shared_ops_fields` entry gets its own named function (e.g.
    // bitdex_image_sortat_ops), emitted before the trigger function that
    // references it.
    for target in source.shared_ops_fields.as_deref().unwrap_or(&[]) {
        let fn_sql = generate_shared_field_ops_function_sql(source, target)
            .unwrap_or_else(|e| panic!("trigger_gen: {e}"));
        ops_function.push_str(&fn_sql);
        ops_function.push('\n');
    }

    format!(
        r#"{ops_function}CREATE OR REPLACE FUNCTION {func_name}() RETURNS trigger AS $$
{body}
$$ LANGUAGE plpgsql;
"#,
        ops_function = ops_function,
        func_name = func_name,
        body = body,
    )
}

/// The trigger DDL alone: drop, create, and mark ENABLE ALWAYS.
///
/// Every statement here takes an **AccessExclusiveLock on the source table** —
/// `Image`, `Post`, `ModelVersion` and friends, all of which take continuous
/// write traffic from the app. Under load the lock request can lose its race
/// repeatedly, and the `bitdex` role carries a `lock_timeout`, so re-issuing
/// this DDL when nothing has changed is not free: it is a chance to fail.
///
/// Only run it when the installed trigger does not already match — ask
/// `queries::trigger_is_current` first.
pub fn generate_trigger_ddl_sql(source: &SyncSource) -> String {
    format!(
        r#"DROP TRIGGER IF EXISTS {trig_name} ON "{table}";
CREATE TRIGGER {trig_name} {trigger_events} ON "{table}"
  FOR EACH ROW EXECUTE FUNCTION {func_name}();
ALTER TABLE "{table}" ENABLE ALWAYS TRIGGER {trig_name};
"#,
        trig_name = trigger_name(source),
        func_name = trigger_function_name(source),
        trigger_events = trigger_events(source),
        table = source.table,
    )
}

/// Generate the full CREATE OR REPLACE FUNCTION + CREATE TRIGGER SQL
/// for a sync source.
///
/// This is the whole-thing form, kept for the review-file generator and for
/// callers that genuinely want to install from scratch. The boot path uses
/// [`generate_trigger_functions_sql`] and [`generate_trigger_ddl_sql`]
/// separately so it can skip the table-locking half.
pub fn generate_trigger_sql(source: &SyncSource) -> String {
    format!(
        "{}\n{}",
        generate_trigger_functions_sql(source),
        generate_trigger_ddl_sql(source),
    )
}

/// Generate the PL/pgSQL function body for a sync source.
fn generate_trigger_body(source: &SyncSource) -> String {
    if let Some(ref field) = source.field {
        // Multi-value join table (tags, tools, techniques, etc.)
        generate_multi_value_body(source, field)
    } else if let Some(ref rows) = source.rows_from {
        // Per-row materialized fan-out (Post → its Image rows).
        generate_fan_out_per_row_body(source, rows)
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

    let shared_fields: std::collections::HashSet<&str> = source.shared_ops_fields
        .as_deref().unwrap_or(&[]).iter().map(|s| s.as_str()).collect();

    let mut body = String::from("DECLARE\n  _ops jsonb;\nBEGIN\n");

    // INSERT: emit set ops for all tracked fields (no remove since no prior state)
    body.push_str("  IF TG_OP = 'INSERT' THEN\n");
    let mut insert_ops: Vec<String> = Vec::new();
    // For sets_alive tables, emit an "alive" signal so the ops processor knows
    // this entity should have its alive bit set (new slot creation).
    if source.sets_alive {
        insert_ops.push("      jsonb_build_object('op', 'alive')".to_string());
    }
    // Fields named in `shared_ops_fields` are built by their own shared PG
    // function (see generate_shared_field_ops_function_sql) and merged in below
    // via `||`, rather than inlined here — that function is the single builder
    // the re-emitter also calls, so it must not be duplicated inline.
    insert_ops.extend(track_fields.iter().filter_map(|f| {
        let (field_name, _insert_expr, template_expr) = parse_track_field(f);
        if shared_fields.contains(field_name.as_str()) {
            return None;
        }
        let new_expr = substitute_columns(&template_expr, "NEW");
        Some(format!(
            "      jsonb_build_object('op', 'set', 'field', '{}', 'value', to_jsonb({}))",
            field_name, new_expr
        ))
    }));
    // Computed fields: emit set ops with computed expression values (e.g., existedAt).
    let computed = source.computed_fields.as_deref().unwrap_or(&[]);
    for cf in computed {
        if shared_fields.contains(cf.target.as_str()) {
            continue;
        }
        let new_expr = substitute_columns(&cf.expression, "NEW");
        insert_ops.push(format!(
            "      jsonb_build_object('op', 'set', 'field', '{}', 'value', to_jsonb({}))",
            cf.target, new_expr
        ));
    }
    if insert_ops.is_empty() {
        body.push_str("    _ops := '[]'::jsonb;\n");
    } else {
        body.push_str("    _ops := jsonb_build_array(\n");
        body.push_str(&insert_ops.join(",\n"));
        body.push_str("\n    );\n");
    }
    // Merge in each shared field's op via its own named function.
    let mut shared_names: Vec<&str> = source.shared_ops_fields
        .as_deref().unwrap_or(&[]).iter().map(|s| s.as_str()).collect();
    shared_names.sort_unstable();
    for target in &shared_names {
        let fn_name = shared_field_ops_function_name(&source.table, target);
        body.push_str(&format!("    _ops := _ops || {fn_name}(NEW);\n"));
    }
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
        let (field_name, _insert_expr, template_expr) = parse_track_field(f);
        let old_expr = substitute_columns(&template_expr, "OLD");
        let new_expr = substitute_columns(&template_expr, "NEW");
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
    // Computed fields on UPDATE: emit remove/set when computed value changes.
    //
    // A computed field may target FUTURE_GUARDED_FIELD — on Image, publishedAt
    // is computed from a correlated subquery on the parent Post. That makes the
    // direct trigger a SECOND writer of a field whose whole invariant is that a
    // future value must never reach an alive slot as a bare `set`, and the
    // guard lived only in the fan-out codegen. The value here is already an
    // epoch bigint, so the guard compares against epoch-now rather than the
    // timestamptz column the fan-out guard reads.
    //
    // Only the UPDATE branch is guarded. The INSERT branch must keep emitting a
    // plain future `set`: that Set is what puts a freshly-inserted scheduled
    // image into the deferred map (the engine's deferral check reads Set ops),
    // and guarding it would turn every scheduled insert into an unscheduled one.
    for cf in computed {
        let old_expr = substitute_columns(&cf.expression, "OLD");
        let new_expr = substitute_columns(&cf.expression, "NEW");
        if cf.target == FUTURE_GUARDED_FIELD {
            // Same shape as the fan-out guard: the OLD-side remove always fires
            // (it is what clears the previous value's sort-layer bits), and only
            // the NEW side is guarded. `COALESCE(... , TRUE)` gives the null test
            // for free — `NULL > x` is NULL — so the value expression, which here
            // is a correlated subquery on the parent row, is evaluated once in
            // the predicate rather than twice.
            body.push_str(&format!(
                "    IF ({old}) IS DISTINCT FROM ({new}) THEN\n\
                 \x20     _ops := _ops || jsonb_build_array(\n\
                 \x20       jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb({old})),\n\
                 \x20       CASE WHEN COALESCE(({new}) > extract(epoch from now())::bigint, TRUE)\n\
                 \x20         THEN jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb({new}))\n\
                 \x20         ELSE jsonb_build_object('op', 'set', 'field', '{field}', 'value', to_jsonb({new}))\n\
                 \x20       END\n\
                 \x20     );\n\
                 \x20   END IF;\n",
                old = old_expr,
                new = new_expr,
                field = cf.target,
            ));
        } else {
            body.push_str(&format!(
                "    IF ({old}) IS DISTINCT FROM ({new}) THEN\n\
                 \x20     _ops := _ops || jsonb_build_array(\n\
                 \x20       jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb({old})),\n\
                 \x20       jsonb_build_object('op', 'set', 'field', '{field}', 'value', to_jsonb({new}))\n\
                 \x20     );\n\
                 \x20   END IF;\n",
                old = old_expr,
                new = new_expr,
                field = cf.target,
            ));
        }
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

/// Generate body for multi-value join tables (e.g., TagsOnImageNew, ImageResourceNew).
///
/// JOIN tables only have INSERT and DELETE (no UPDATE — rows are immutable).
/// INSERT → emit `add` op for the field value.
/// DELETE → emit `remove` op for the field value.
///
/// Also handles computed_fields (e.g., modelVersionIdsManual = value when condition).
fn generate_multi_value_body(source: &SyncSource, field: &str) -> String {
    let slot_field = source.slot_field.as_deref().unwrap_or("imageId");
    let value_field = source.value_field.as_deref().unwrap_or("id");

    // Build filter clause with {column} template substitution
    let insert_filter = source.filter.as_ref().map(|f| {
        let resolved = substitute_columns(f, "NEW");
        format!("    IF {} THEN\n", resolved)
    }).unwrap_or_default();
    let delete_filter = source.filter.as_ref().map(|f| {
        let resolved = substitute_columns(f, "OLD");
        format!("    IF {} THEN\n", resolved)
    }).unwrap_or_default();
    let filter_end = if source.filter.is_some() { "    END IF;\n" } else { "" };

    // Build computed field ops (e.g., modelVersionIdsManual when detected=false)
    let computed = source.computed_fields.as_deref().unwrap_or(&[]);
    let mut insert_computed = String::new();
    let mut delete_computed = String::new();
    for cf in computed {
        let condition = substitute_columns(&cf.expression, "NEW");
        let value_col = cf.value.as_ref()
            .map(|v| substitute_columns(v, "NEW"))
            .unwrap_or_else(|| format!("NEW.\"{}\"", value_field));
        let old_value_col = cf.value.as_ref()
            .map(|v| substitute_columns(v, "OLD"))
            .unwrap_or_else(|| format!("OLD.\"{}\"", value_field));

        insert_computed.push_str(&format!(
            "    IF {} THEN\n\
             \x20     INSERT INTO \"BitdexOps\" (entity_id, ops)\n\
             \x20     VALUES (NEW.\"{}\", jsonb_build_array(\n\
             \x20       jsonb_build_object('op', 'add', 'field', '{}', 'value', to_jsonb({}))\n\
             \x20     ));\n\
             \x20   END IF;\n",
            condition, slot_field, cf.target, value_col
        ));
        // For DELETE, invert: always remove if the row had the condition
        let del_condition = substitute_columns(&cf.expression, "OLD");
        delete_computed.push_str(&format!(
            "    IF {} THEN\n\
             \x20     INSERT INTO \"BitdexOps\" (entity_id, ops)\n\
             \x20     VALUES (OLD.\"{}\", jsonb_build_array(\n\
             \x20       jsonb_build_object('op', 'remove', 'field', '{}', 'value', to_jsonb({}))\n\
             \x20     ));\n\
             \x20   END IF;\n",
            del_condition, slot_field, cf.target, old_value_col
        ));
    }

    let mut body = String::from("BEGIN\n");

    // INSERT handler
    body.push_str("  IF TG_OP = 'INSERT' THEN\n");
    body.push_str(&insert_filter);
    body.push_str(&format!(
        "    INSERT INTO \"BitdexOps\" (entity_id, ops)\n\
         \x20   VALUES (NEW.\"{slot}\", jsonb_build_array(\n\
         \x20     jsonb_build_object('op', 'add', 'field', '{field}', 'value', to_jsonb(NEW.\"{value}\"))\n\
         \x20   ));\n",
        slot = slot_field, field = field, value = value_field,
    ));
    body.push_str(filter_end);
    body.push_str(&insert_computed);
    body.push_str("    RETURN NEW;\n");

    // DELETE handler
    body.push_str("  ELSIF TG_OP = 'DELETE' THEN\n");
    body.push_str(&delete_filter);
    body.push_str(&format!(
        "    INSERT INTO \"BitdexOps\" (entity_id, ops)\n\
         \x20   VALUES (OLD.\"{slot}\", jsonb_build_array(\n\
         \x20     jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb(OLD.\"{value}\"))\n\
         \x20   ));\n",
        slot = slot_field, field = field, value = value_field,
    ));
    body.push_str(filter_end);
    body.push_str(&delete_computed);
    body.push_str("    RETURN OLD;\n");

    body.push_str("  END IF;\n");
    body.push_str("  RETURN COALESCE(NEW, OLD);\n");
    body.push_str("END;");

    body
}

/// Generate body for fan-out tables (e.g., ModelVersion, Post).
fn generate_fan_out_body(source: &SyncSource) -> String {
    let query_template = source.query.as_deref().unwrap_or("");
    let track_strings: Vec<String> = source.track_fields.as_deref().unwrap_or(&[])
        .iter().map(|tf| tf.to_track_string()).collect();
    let track_fields: Vec<&str> = track_strings.iter().map(|s| s.as_str()).collect();

    let mut body = String::from("DECLARE\n  _ops jsonb;\n  _query text;\n");

    // If there's a query_source or expression, we need a variable for its result
    if source.query_source.is_some() || source.expression.is_some() {
        body.push_str("  _source_result jsonb;\n");
    }
    body.push_str("BEGIN\n");

    // Helper: build the query string for this fan-out source using NEW row values.
    // Shared between INSERT and UPDATE paths.
    let build_query_block = |body: &mut String, source: &SyncSource, query_template: &str| {
        if let Some(ref expr) = source.expression {
            let (param_sql, param_refs) = build_execute_with_params(expr, "NEW");
            body.push_str(&format!(
                "    EXECUTE '{}' INTO _source_result{};\n",
                param_sql.replace('\'', "''"),
                if param_refs.is_empty() { String::new() } else { format!(" USING {}", param_refs.join(", ")) }
            ));
            // Note: if expression returns NULL (e.g., json_agg with no rows),
            // _query will be NULL due to PG null propagation. The ops processor
            // skips null-query ops gracefully (Option<String> deserialization).
            // A trigger-side guard could prevent emitting the row entirely, but
            // it's harmless — the op is ~100 bytes and skipped in <1μs.
            let query_with_result = query_template
                .replace("{ids}", "' || _source_result::text || '");
            body.push_str(&format!("    _query := '{}';\n", query_with_result));
        } else if let Some(ref query_source) = source.query_source {
            let source_sql = substitute_columns(query_source, "NEW");
            body.push_str(&format!(
                "    EXECUTE format('SELECT ({})') INTO _source_result;\n",
                source_sql.replace('\'', "''")
            ));
            body.push_str(&format!(
                "    _query := {};\n",
                build_query_concatenation(query_template, "NEW")
            ));
        } else {
            body.push_str(&format!(
                "    _query := {};\n",
                build_query_concatenation(query_template, "NEW")
            ));
        }
    };

    // INSERT: emit set ops for all tracked fields (new entity, no prior state).
    // Fan-out on INSERT ensures e.g. Post publishedAt reaches images immediately.
    body.push_str("  IF TG_OP = 'INSERT' THEN\n");
    build_query_block(&mut body, source, query_template);
    body.push_str("    _ops := jsonb_build_array(\n");
    let insert_ops: Vec<String> = track_fields.iter().map(|f| {
        let (field_name, _insert_expr, template_expr) = parse_track_field(f);
        let new_expr = substitute_columns(&template_expr, "NEW");
        format!(
            "      jsonb_build_object('op', 'set', 'field', '{}', 'value', to_jsonb({}))",
            field_name, new_expr
        )
    }).collect();
    body.push_str(&insert_ops.join(",\n"));
    body.push_str("\n    );\n");
    body.push_str(&format!(
        "    INSERT INTO \"BitdexOps\" (entity_id, ops) VALUES (NEW.id, jsonb_build_array(\n\
         \x20       jsonb_build_object('op', 'queryOpSet', 'query', _query, 'ops', _ops)\n\
         \x20     ));\n"
    ));
    body.push_str("    RETURN NEW;\n");

    // UPDATE: emit remove/set pairs only for changed fields
    body.push_str("  ELSIF TG_OP = 'UPDATE' THEN\n");
    build_query_block(&mut body, source, query_template);

    // Build ops array from tracked fields that changed
    body.push_str("    _ops := '[]'::jsonb;\n");
    for f in track_fields {
        let (field_name, _insert_expr, template_expr) = parse_track_field(f);
        let old_expr = substitute_columns(&template_expr, "OLD");
        let new_expr = substitute_columns(&template_expr, "NEW");
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

/// Stable name of the shared per-row op-payload function for a `fan_out_per_row`
/// source, e.g. `bitdex_post_fanout_ops`. Deliberately UNHASHED and stable so
/// the upstream re-emitter (W1-3) can call it by a fixed name; when the config
/// changes, the function body is replaced (CREATE OR REPLACE) but the name — and
/// therefore the re-emitter's call site — stays valid. Both the trigger and the
/// re-emitter regenerate from the same codegen, so their op shapes cannot drift.
pub fn fan_out_ops_function_name(source: &SyncSource) -> String {
    format!("bitdex_{}_fanout_ops", source.table.to_lowercase())
}

/// Field whose fan-out `set` must be guarded against NULL / FUTURE values.
///
/// A future `publishedAt` on a Post is a *scheduling* event, not a publish. The
/// per-image fan-out applies ops directly to already-ALIVE images (the engine's
/// deferred branch only quarantines future values on FRESH inserts). Because the
/// `isPublished` filter target is an `exists_boolean` shadow — a plain null-check
/// on `publishedAt` — a bare `set` with a future value flips the shadow TRUE and
/// lands the image at the top of the feed with a future `sortAt` (this leaked
/// ~26.5k slots in prod 2026-07-18). Emitting a `remove` instead flips the shadow
/// FALSE (image hidden) and is correct for BOTH unpublish (NULL) and scheduling
/// (FUTURE). The image is later activated by the overdue sweep (only where the
/// doc retained publishedAt) and PRIMARILY by the W1-3 re-emitter (civitai PR
/// #3231): once `publishedAt` enters `[now-15m, now]` the re-emitter re-emits a
/// past-value `set` that flips the shadow back TRUE. **The re-emitter flag being
/// ON is LOAD-BEARING for this population** — without it, alive-then-scheduled
/// images never activate.
const FUTURE_GUARDED_FIELD: &str = "publishedAt";

/// Build a single jsonb op element for a fan-out `set` payload.
///
/// For most fields this is an unconditional `set`. For [`FUTURE_GUARDED_FIELD`]
/// it is a `CASE` that emits a `remove` when the parent's value is NULL or FUTURE
/// (`> now()`) and a `set` only when the value is already past — keeping
/// scheduled/unpublished images hidden until the value actually becomes past.
/// `parent_row` is the SQL row alias the guard reads the raw column from (`_p`
/// for the shared INSERT function, `NEW` for the UPDATE-branch new-side).
fn fan_out_set_op_element(field_name: &str, parent_row: &str, value_expr: &str) -> String {
    if field_name == FUTURE_GUARDED_FIELD {
        format!(
            "    CASE WHEN {parent}.\"{field}\" IS NULL OR {parent}.\"{field}\" > now()\n\
             \x20     THEN jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb({expr}))\n\
             \x20     ELSE jsonb_build_object('op', 'set', 'field', '{field}', 'value', to_jsonb({expr}))\n\
             \x20   END",
            parent = parent_row,
            field = field_name,
            expr = value_expr,
        )
    } else {
        format!(
            "    jsonb_build_object('op', 'set', 'field', '{}', 'value', to_jsonb({}))",
            field_name, value_expr
        )
    }
}

/// Generate the shared op-payload function for a `fan_out_per_row` source:
///
/// ```sql
/// CREATE OR REPLACE FUNCTION bitdex_post_fanout_ops(_p "Post") RETURNS jsonb AS $$
///   SELECT jsonb_build_array(
///     jsonb_build_object('op','set','field','publishedAt','value', to_jsonb(...)),
///     ...
///   );
/// $$ LANGUAGE sql STABLE;
/// ```
///
/// This is the SINGLE source of the per-image op JSONB shape. The generated
/// trigger's INSERT path calls it, and the W1-3 re-emitter calls it via its own
/// `INSERT ... SELECT i."id", bitdex_post_fanout_ops(p) FROM "Image" i JOIN
/// "Post" p ...`. If the two paths built the JSONB independently they could
/// drift, and a re-emit would stop being a no-op — destroying the ≥99%-no-op
/// success signal. The payload references only the parent (Post) row, so the
/// function takes that row; `STABLE` lets PG evaluate it once per Post row.
pub fn generate_fan_out_ops_function_sql(source: &SyncSource, rows: &RowsFrom) -> String {
    let _ = rows; // reserved: future image-dependent payloads extend the signature
    let fn_name = fan_out_ops_function_name(source);
    let parent_row = "_p";
    let track_strings: Vec<String> = source.track_fields.as_deref().unwrap_or(&[])
        .iter().map(|tf| tf.to_track_string()).collect();
    let ops: Vec<String> = track_strings.iter().map(|f| {
        let (field_name, _insert_expr, template_expr) = parse_track_field(f);
        let expr = substitute_columns(&template_expr, parent_row);
        // publishedAt is guarded (future/null → remove) so a scheduled Post's
        // fan-out does not flip already-alive images visible; see
        // FUTURE_GUARDED_FIELD. All other fields emit a plain `set`.
        fan_out_set_op_element(&field_name, parent_row, &expr)
    }).collect();

    format!(
        "CREATE OR REPLACE FUNCTION {fn_name}({parent} \"{table}\") RETURNS jsonb AS $$\n\
         \x20 SELECT jsonb_build_array(\n\
         {ops}\n\
         \x20 );\n\
         $$ LANGUAGE sql STABLE;\n",
        fn_name = fn_name,
        parent = parent_row,
        table = source.table,
        ops = ops.join(",\n"),
    )
}

/// Stable name of a shared single-field op function for a `shared_ops_fields`
/// entry, e.g. `bitdex_image_sortat_ops` for target field `sortAtUnix` on table
/// `Image`. Strips a trailing `Unix` (case-insensitive) from the target before
/// lowercasing — `sortAtUnix` (the wire/storage name) names the function after
/// `sortAt` (the semantic field), matching the naming convention already used
/// for `fan_out_ops_function_name`. UNHASHED and stable so a re-emitter can call
/// it by a fixed name; CREATE OR REPLACE keeps that name valid across config
/// changes.
pub fn shared_field_ops_function_name(table: &str, target_field: &str) -> String {
    let stripped = target_field.strip_suffix("Unix")
        .or_else(|| target_field.strip_suffix("UNIX"))
        .unwrap_or(target_field);
    format!("bitdex_{}_{}_ops", table.to_lowercase(), stripped.to_lowercase())
}

/// Generate the shared single-field op function for a `shared_ops_fields` entry:
///
/// ```sql
/// CREATE OR REPLACE FUNCTION bitdex_image_sortat_ops(_i "Image") RETURNS jsonb AS $$
///   SELECT jsonb_build_array(
///     jsonb_build_object('op','set','field','sortAtUnix','value', to_jsonb(<expr>))
///   );
/// $$ LANGUAGE sql STABLE;
/// ```
///
/// Same pattern as `generate_fan_out_ops_function_sql`: ONE builder for the op
/// JSONB shape. The trigger's INSERT path calls it (via `|| fn(NEW)`); an
/// upstream re-emitter calls the identical function to re-assert sortAtUnix
/// with mechanical parity — same expression, same value, guaranteed by
/// construction rather than by two implementations happening to agree.
///
/// Errors if `target_field` isn't found among `track_fields`/`computed_fields`
/// — a `shared_ops_fields` entry naming a field the source doesn't track is a
/// config mistake, not something to silently ignore.
pub fn generate_shared_field_ops_function_sql(
    source: &SyncSource,
    target_field: &str,
) -> Result<String, String> {
    let parent_row = "_i";
    let track_strings: Vec<String> = source.track_fields.as_deref().unwrap_or(&[])
        .iter().map(|tf| tf.to_track_string()).collect();

    let template_expr = track_strings.iter()
        .map(|f| parse_track_field(f))
        .find(|(name, _, _)| name == target_field)
        .map(|(_, _, template)| template)
        .or_else(|| {
            source.computed_fields.as_deref().unwrap_or(&[]).iter()
                .find(|cf| cf.target == target_field)
                .map(|cf| cf.expression.clone())
        })
        .ok_or_else(|| format!(
            "sync source \"{}\": shared_ops_fields entry \"{target_field}\" not found \
             in track_fields or computed_fields",
            source.table
        ))?;

    let expr = substitute_columns(&template_expr, parent_row);
    let fn_name = shared_field_ops_function_name(&source.table, target_field);

    Ok(format!(
        "CREATE OR REPLACE FUNCTION {fn_name}({parent} \"{table}\") RETURNS jsonb AS $$\n\
         \x20 SELECT jsonb_build_array(\n\
         \x20   jsonb_build_object('op', 'set', 'field', '{target_field}', 'value', to_jsonb({expr}))\n\
         \x20 );\n\
         $$ LANGUAGE sql STABLE;\n",
        fn_name = fn_name,
        parent = parent_row,
        table = source.table,
        target_field = target_field,
        expr = expr,
    ))
}

/// Generate body for per-row materialized fan-out tables (Post → its Images).
///
/// Unlike `generate_fan_out_body` (which emits one `queryOpSet` row that BitDex
/// resolves against its own — possibly moved — index at apply time), this
/// enumerates the child rows *inside the owning PG transaction*:
///
/// ```sql
/// INSERT INTO "BitdexOps" (entity_id, ops)
/// SELECT c."id", _ops FROM "Image" c WHERE c."postId" = NEW."id";
/// ```
///
/// The op payload is identical to the fan-out's (per PR-M1 the Post fan-out
/// RETAINS the full {publishedAt, availability, postedToId} set — publishedAt
/// drives deferred-alive activation stamping AND the isPublished shadow, so it
/// must not be dropped). Ops are ordinary per-slot set/remove ops keyed on the
/// child slot; the ops processor needs no `queryOpSet` handling for them.
///
/// The INSERT `set` payload is built by the SHARED `bitdex_<table>_fanout_ops`
/// function (see `generate_fan_out_ops_function_sql`) so the W1-3 re-emitter can
/// call the identical shape — shape parity is the re-emitter's idempotency
/// contract. The UPDATE path emits live remove/set deltas (IS DISTINCT FROM on
/// the parent row) inline; the re-emitter only ever re-asserts the full `set`
/// payload, so only that shape needs to be shared.
///
/// Fires on INSERT and UPDATE, mirroring the fan-out. On Post INSERT the child
/// SELECT typically matches zero rows (images are inserted after the post) — a
/// harmless no-op.
fn generate_fan_out_per_row_body(source: &SyncSource, rows: &RowsFrom) -> String {
    let track_strings: Vec<String> = source.track_fields.as_deref().unwrap_or(&[])
        .iter().map(|tf| tf.to_track_string()).collect();
    let track_fields: Vec<&str> = track_strings.iter().map(|s| s.as_str()).collect();

    // The child-row enumeration shared by INSERT and UPDATE.
    // `SELECT c."<slot>", _ops FROM "<child>" c WHERE c."<fk>" = NEW."<parent_key>"`
    let select_insert = format!(
        "SELECT c.\"{slot}\", _ops FROM \"{child}\" c WHERE c.\"{fk}\" = NEW.\"{pk}\"",
        slot = rows.slot,
        child = rows.table,
        fk = rows.fk,
        pk = rows.parent_key,
    );

    let ops_fn = fan_out_ops_function_name(source);

    let mut body = String::from("DECLARE\n  _ops jsonb;\nBEGIN\n");

    // INSERT: emit the FULL `set` payload for every child row (new parent, no
    // prior state). The payload is built by the SHARED ops function so the
    // upstream re-emitter (W1-3) can call the identical shape — parity here is
    // the re-emitter's idempotency contract.
    body.push_str("  IF TG_OP = 'INSERT' THEN\n");
    body.push_str(&format!("    _ops := {ops_fn}(NEW);\n"));
    body.push_str(&format!(
        "    INSERT INTO \"BitdexOps\" (entity_id, ops)\n      {};\n",
        select_insert
    ));
    body.push_str("    RETURN NEW;\n");

    // UPDATE: emit remove/set pairs only for changed parent fields.
    body.push_str("  ELSIF TG_OP = 'UPDATE' THEN\n");
    body.push_str("    _ops := '[]'::jsonb;\n");
    for f in &track_fields {
        let (field_name, _insert_expr, template_expr) = parse_track_field(f);
        let old_expr = substitute_columns(&template_expr, "OLD");
        let new_expr = substitute_columns(&template_expr, "NEW");
        // The OLD-side is ALWAYS a `remove`, guarded field included, and that is
        // load-bearing beyond clearing the old value from the filter bitmaps:
        // `process_remove_op` clears sort-layer bits derived from the VALUE it is
        // handed, not "the field". publishedAt is a 32-bit sort field, so on an
        // alive slot the old-side remove is the only op that clears the previous
        // timestamp's bits. Dropping it leaves `OLD & ~NEW` resident in the
        // layer, and on an unpublish (NEW null, no bits to clear) leaves the old
        // timestamp resident in full with no later Set to heal it.
        //
        // That means the schedule case genuinely does emit TWO value-bearing
        // removes on one field with no Set — the shape whose ORDER decides which
        // schedule the engine arms at. The ordering guarantee lives in
        // `op_dedup::dedup_entity_ops`, which re-emits multi-value ops in
        // arrival order (it used to use AHashMap order, which made a reschedule
        // a coin flip: the losing arm published content at an abandoned earlier
        // schedule, or stripped the schedule so it never published at all —
        // captured in "BitdexOps" on 2026-08-18 as one row carrying
        // `remove publishedAt null` and `remove publishedAt <future>`).
        //
        // So this trigger's contract with the engine is: **the new side is
        // emitted LAST**. Anything that reorders these two ops re-opens the bug;
        // `test_multi_value_removes_keep_arrival_order` is what holds that line.
        //
        // The NEW-side is guarded ONLY for publishedAt: a future/null NEW value
        // produces a `remove`, not a `set`, so an alive image being SCHEDULED
        // (or unpublished) stays hidden rather than jumping to feed top with a
        // future sortAt. Non-guarded fields keep the plain `set` new-side.
        let new_side = if field_name == FUTURE_GUARDED_FIELD {
            format!(
                "        CASE WHEN NEW.\"{field}\" IS NULL OR NEW.\"{field}\" > now()\n\
                 \x20         THEN jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb({new}))\n\
                 \x20         ELSE jsonb_build_object('op', 'set', 'field', '{field}', 'value', to_jsonb({new}))\n\
                 \x20       END",
                field = field_name,
                new = new_expr,
            )
        } else {
            format!(
                "        jsonb_build_object('op', 'set', 'field', '{field}', 'value', to_jsonb({new}))",
                field = field_name,
                new = new_expr,
            )
        };
        body.push_str(&format!(
            "    IF ({old}) IS DISTINCT FROM ({new}) THEN\n\
             \x20     _ops := _ops || jsonb_build_array(\n\
             \x20       jsonb_build_object('op', 'remove', 'field', '{field}', 'value', to_jsonb({old})),\n\
             {new_side}\n\
             \x20     );\n\
             \x20   END IF;\n",
            old = old_expr,
            new = new_expr,
            field = field_name,
            new_side = new_side,
        ));
    }
    body.push_str("    IF jsonb_array_length(_ops) > 0 THEN\n");
    body.push_str(&format!(
        "      INSERT INTO \"BitdexOps\" (entity_id, ops)\n        {};\n",
        select_insert
    ));
    body.push_str("    END IF;\n");
    body.push_str("    RETURN NEW;\n");
    body.push_str("  END IF;\n");
    body.push_str("  RETURN COALESCE(NEW, OLD);\n");
    body.push_str("END;");

    body
}

/// Parse a track field string into (field_name, insert_expr, template_expr).
/// - `insert_expr`: Used in INSERT (no OLD/NEW prefix), e.g. `"nsfwLevel"`.
/// - `template_expr`: Used in UPDATE, contains `{col}` templates for substitute_columns.
///
/// Simple field: "nsfwLevel" → ("nsfwLevel", "\"nsfwLevel\"", "{nsfwLevel}")
/// Expression: "GREATEST({scannedAt}, {createdAt}) as existedAt"
///   → ("existedAt", "GREATEST(\"scannedAt\", \"createdAt\")", "GREATEST({scannedAt}, {createdAt})")
fn parse_track_field(field: &str) -> (String, String, String) {
    if let Some(as_pos) = field.to_lowercase().rfind(" as ") {
        let expr = &field[..as_pos].trim();
        let alias = &field[as_pos + 4..].trim();
        // insert_expr: {col} → "col"
        let insert_sql = expr.replace('{', "\"").replace('}', "\"");
        // template_expr: keep {col} for substitute_columns
        (alias.to_string(), insert_sql, expr.to_string())
    } else {
        // Simple field name
        let insert_sql = format!("\"{}\"", field);
        let template = format!("{{{}}}", field);
        (field.to_string(), insert_sql, template)
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

/// Build an EXECUTE-compatible SQL string with $N parameter placeholders.
///
/// Template: "SELECT json_agg(mv.id) FROM \"ModelVersion\" mv WHERE mv.\"modelId\" = {id}"
/// with prefix "NEW"
/// → ("SELECT json_agg(mv.id) FROM \"ModelVersion\" mv WHERE mv.\"modelId\" = $1", ["NEW.\"id\""])
fn build_execute_with_params(template: &str, prefix: &str) -> (String, Vec<String>) {
    let mut sql = String::new();
    let mut params: Vec<String> = Vec::new();
    let mut chars = template.chars().peekable();

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
            params.push(format!("{prefix}.\"{col}\""));
            sql.push_str(&format!("${}", params.len()));
        } else {
            sql.push(c);
        }
    }

    (sql, params)
}

/// Build a PG string concatenation expression from a query template.
///
/// Template: "postId eq {id}" with prefix "NEW"
/// → `'postId eq ' || NEW."id"::text`
///
/// Template: "modelVersionIds in [{ids}]" with prefix "NEW"
/// → `'modelVersionIds in [' || NEW."ids"::text || ']'`
fn build_query_concatenation(template: &str, prefix: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut current_literal = String::new();
    let mut chars = template.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '{' {
            // Flush current literal
            if !current_literal.is_empty() {
                parts.push(format!("'{}'", current_literal.replace('\'', "''")));
                current_literal.clear();
            }
            // Read column name
            let mut col = String::new();
            while let Some(&next) = chars.peek() {
                if next == '}' {
                    chars.next();
                    break;
                }
                col.push(chars.next().unwrap());
            }
            parts.push(format!("{}.\"{col}\"::text", prefix));
        } else {
            current_literal.push(c);
        }
    }
    // Flush remaining literal
    if !current_literal.is_empty() {
        parts.push(format!("'{}'", current_literal.replace('\'', "''")));
    }

    if parts.len() == 1 {
        parts[0].clone()
    } else {
        parts.join(" || ")
    }
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
        let (name, insert_expr, template_expr) = parse_track_field("nsfwLevel");
        assert_eq!(name, "nsfwLevel");
        assert_eq!(insert_expr, "\"nsfwLevel\"");
        assert_eq!(template_expr, "{nsfwLevel}");
        assert_eq!(substitute_columns(&template_expr, "OLD"), "OLD.\"nsfwLevel\"");
        assert_eq!(substitute_columns(&template_expr, "NEW"), "NEW.\"nsfwLevel\"");
    }

    #[test]
    fn test_parse_track_field_expression() {
        let (name, insert_expr, template_expr) = parse_track_field("GREATEST({scannedAt}, {createdAt}) as existedAt");
        assert_eq!(name, "existedAt");
        assert_eq!(insert_expr, "GREATEST(\"scannedAt\", \"createdAt\")");
        assert_eq!(template_expr, "GREATEST({scannedAt}, {createdAt})");
        assert_eq!(substitute_columns(&template_expr, "NEW"), "GREATEST(NEW.\"scannedAt\", NEW.\"createdAt\")");
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

    /// Parent-table lookup via subquery in a computed_field expression (audit
    /// 2026-07-07 Mode B: Image inherits Post.publishedAt at insert). The
    /// {col} templating must pass the surrounding subquery SQL through
    /// untouched, and the generated body must emit the Set on both INSERT and
    /// UPDATE (re-resolving when the image moves posts).
    #[test]
    fn test_computed_field_subquery_lookup() {
        let expr = r#"(SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = {postId})"#;
        let substituted = substitute_columns(expr, "NEW");
        assert_eq!(
            substituted,
            r#"(SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = NEW."postId")"#,
        );

        let mut source = test_source("Image");
        source.slot_field = Some("id".into());
        source.sets_alive = true;
        source.track_fields = Some(vec![TrackField::Simple("postId".into())]);
        source.computed_fields = Some(vec![TriggerComputedField {
            target: "publishedAt".into(),
            expression: expr.into(),
            value: None,
        }]);
        let sql = generate_trigger_sql(&source);
        // INSERT branch: set op with the NEW-substituted subquery.
        assert!(
            sql.contains(r#"'field', 'publishedAt', 'value', to_jsonb((SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = NEW."postId"))"#),
            "INSERT must emit publishedAt from the Post subquery:\n{sql}"
        );
        // UPDATE branch: IS DISTINCT FROM diff between OLD and NEW resolution.
        assert!(
            sql.contains(r#"WHERE p.id = OLD."postId") IS DISTINCT FROM"#)
                || sql.contains(r#"WHERE p.id = OLD."postId")) IS DISTINCT FROM"#),
            "UPDATE must diff the subquery between OLD and NEW postId:\n{sql}"
        );
    }

    // --- trigger DDL / catalog-mask agreement -------------------------------
    //
    // `trigger_type_mask` is what lets the boot path ask the catalog "is this
    // trigger already what we would install?" instead of re-issuing DDL that
    // takes an AccessExclusiveLock on a hot table. If the mask ever stops
    // agreeing with the events clause, that check silently answers the wrong
    // question — it would either skip DDL that was needed, or reinstall DDL
    // every boot and reintroduce the crash-loop it exists to prevent. So the
    // two encodings are tested against each other directly.

    /// Decode a tgtype mask back into the events clause it claims to describe.
    fn events_from_mask(mask: i16) -> String {
        const ROW: i16 = 1 << 0;
        const BEFORE: i16 = 1 << 1;
        const INSERT: i16 = 1 << 2;
        const DELETE: i16 = 1 << 3;
        const UPDATE: i16 = 1 << 4;

        assert_eq!(mask & ROW, ROW, "every emitted trigger is FOR EACH ROW");
        assert_eq!(mask & BEFORE, 0, "every emitted trigger is AFTER, not BEFORE");

        let mut events = Vec::new();
        if mask & INSERT != 0 { events.push("INSERT"); }
        if mask & UPDATE != 0 { events.push("UPDATE"); }
        if mask & DELETE != 0 { events.push("DELETE"); }
        format!("AFTER {}", events.join(" OR "))
    }

    #[test]
    fn trigger_type_mask_agrees_with_events_clause() {
        // Plain source: AFTER INSERT OR UPDATE.
        let plain = test_source("Image");
        assert_eq!(trigger_events(&plain), "AFTER INSERT OR UPDATE");
        assert_eq!(events_from_mask(trigger_type_mask(&plain)), "AFTER INSERT OR UPDATE");

        // on_delete set: AFTER INSERT OR UPDATE OR DELETE.
        let mut with_delete = test_source("Image");
        with_delete.on_delete = Some(OnDeleteValue::Bool(true));
        assert_eq!(trigger_events(&with_delete), "AFTER INSERT OR UPDATE OR DELETE");
        assert_eq!(
            events_from_mask(trigger_type_mask(&with_delete)),
            "AFTER INSERT OR UPDATE OR DELETE"
        );

        // Multi-value join table: AFTER INSERT OR DELETE, no UPDATE.
        let mut join_table = test_source("TagsOnImageNew");
        join_table.field = Some("tagIds".into());
        join_table.value_field = Some("tagId".into());
        assert_eq!(trigger_events(&join_table), "AFTER INSERT OR DELETE");
        assert_eq!(events_from_mask(trigger_type_mask(&join_table)), "AFTER INSERT OR DELETE");

        // `field` wins over on_delete — the join-table branch is checked first,
        // and the mask has to follow that same precedence.
        join_table.on_delete = Some(OnDeleteValue::Bool(true));
        assert_eq!(trigger_events(&join_table), "AFTER INSERT OR DELETE");
        assert_eq!(events_from_mask(trigger_type_mask(&join_table)), "AFTER INSERT OR DELETE");
    }

    /// The split halves must still concatenate to exactly what callers used to
    /// get, or the review file and every `generate_trigger_sql` assertion in
    /// this module would be silently describing something the boot path no
    /// longer emits.
    #[test]
    fn split_sql_halves_reassemble_to_the_whole() {
        for source in [
            test_source("Image"),
            post_fan_out_per_row_source(),
            image_source_with_sortat(),
        ] {
            let whole = generate_trigger_sql(&source);
            let functions = generate_trigger_functions_sql(&source);
            let ddl = generate_trigger_ddl_sql(&source);

            assert_eq!(whole, format!("{functions}\n{ddl}"));

            // The half that is safe to re-run every boot must not contain the
            // half that locks the table — that separation is the entire point.
            assert!(
                !functions.contains("CREATE TRIGGER") && !functions.contains("DROP TRIGGER"),
                "functions half must not carry trigger DDL:\n{functions}"
            );
            assert!(
                ddl.contains("CREATE TRIGGER") && ddl.contains("ENABLE ALWAYS TRIGGER"),
                "DDL half must install and always-enable the trigger:\n{ddl}"
            );
        }
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
            rows_from: None,
            shared_ops_fields: None,
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

    // ----------------------------------------------------------------------
    // W1-1: fan_out_per_row (per-image materialized Post fan-out) + sortAtUnix
    // ----------------------------------------------------------------------

    /// The exact sortAt track-field expression W2-1 wires onto the Image
    /// trigger. Emits the TARGET field name `sortAt` in **seconds**, with a
    /// COALESCE(GREATEST...) belt so rows the PG backfill hasn't reached still
    /// emit a correct value.
    ///
    /// CORRECTED 2026-07-18 (was `sortAtUnix` in milliseconds): data_schema
    /// source→target renames apply to DOCUMENT keys only, never to op field
    /// names — ops resolve raw against filter/sort TARGET names and unknown
    /// fields are silently ignored (ops_processor.rs process_set_op). Emitting
    /// the source name meant every steady-state sortAt op was dropped in prod.
    const SORTAT_EXPR: &str = "COALESCE(extract(epoch from {sortAt})::bigint, \
GREATEST(extract(epoch from (SELECT p.\"publishedAt\" FROM \"Post\" p WHERE p.id = {postId}))::bigint, \
extract(epoch from {scannedAt})::bigint, \
extract(epoch from {createdAt})::bigint)) as sortAt";

    /// Build the Post per-image fan-out source, retaining the FULL payload
    /// {publishedAt, availability, postedToId} exactly as the current queryOpSet
    /// fan-out (per PR-M1: publishedAt must NOT be dropped).
    fn post_fan_out_per_row_source() -> SyncSource {
        SyncSource {
            table_type: Some("fan_out_per_row".into()),
            rows_from: Some(RowsFrom {
                table: "Image".into(),
                fk: "postId".into(),
                slot: "id".into(),
                parent_key: "id".into(),
            }),
            track_fields: Some(vec![
                TrackField::Simple("extract(epoch from {publishedAt})::bigint as publishedAt".into()),
                TrackField::Simple("{availability}::text as availability".into()),
                TrackField::Mapped {
                    column: Some("modelVersionId".into()),
                    target: Some("postedToId".into()),
                    expression: None,
                },
            ]),
            ..test_source("Post")
        }
    }

    /// Extract the set of BitDex field names a generated trigger emits, by
    /// scanning for `'field', '<name>'` op markers.
    fn emitted_fields(sql: &str) -> std::collections::HashSet<String> {
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

    /// Snapshot of the generated per-row fan-out SQL: enumerates child rows
    /// transactionally (INSERT ... SELECT FROM the child, WHERE fk = NEW.pk),
    /// keyed on the child slot — and never routes through `queryOpSet`.
    #[test]
    fn test_fan_out_per_row_snapshot() {
        let sql = generate_trigger_sql(&post_fan_out_per_row_source());

        // Trigger fires on INSERT and UPDATE of the Post (same as today's fan-out),
        // never DELETE.
        assert!(sql.contains(r#"CREATE TRIGGER"#));
        assert!(sql.contains("AFTER INSERT OR UPDATE ON \"Post\""), "events:\n{sql}");
        assert!(!sql.contains("DELETE"), "per-row fan-out must not handle DELETE:\n{sql}");

        // Transactional per-child enumeration, keyed on the child slot.
        assert!(
            sql.contains(r#"INSERT INTO "BitdexOps" (entity_id, ops)"#),
            "must insert per-row ops:\n{sql}"
        );
        assert!(
            sql.contains(r#"SELECT c."id", _ops FROM "Image" c WHERE c."postId" = NEW."id""#),
            "must enumerate child rows transactionally:\n{sql}"
        );

        // The moving-index resolution mechanism is GONE for this trigger.
        assert!(
            !sql.contains("queryOpSet"),
            "per-row fan-out must not emit queryOpSet:\n{sql}"
        );

        // INSERT emits set ops; UPDATE diffs with IS DISTINCT FROM.
        assert!(sql.contains("IF TG_OP = 'INSERT' THEN"));
        assert!(sql.contains("ELSIF TG_OP = 'UPDATE' THEN"));
        assert!(sql.contains("IS DISTINCT FROM"), "UPDATE must diff parent fields:\n{sql}");
        assert!(
            sql.contains("IF jsonb_array_length(_ops) > 0 THEN"),
            "UPDATE must skip empty diffs:\n{sql}"
        );

        // The shared op-payload function is emitted and the INSERT path calls it.
        assert!(
            sql.contains(r#"CREATE OR REPLACE FUNCTION bitdex_post_fanout_ops(_p "Post") RETURNS jsonb"#),
            "shared ops function must be generated:\n{sql}"
        );
        assert!(sql.contains("LANGUAGE sql STABLE"), "ops function must be STABLE sql:\n{sql}");
        assert!(
            sql.contains("_ops := bitdex_post_fanout_ops(NEW);"),
            "INSERT path must call the shared ops function:\n{sql}"
        );
    }

    /// W1-3 parity contract: the per-image `set` payload lives in ONE named PG
    /// function that BOTH the generated trigger's INSERT path AND the re-emitter
    /// call. Because the shape has a single source, a re-emit re-asserting the
    /// same values is a byte-identical no-op set — the ≥99%-no-op success signal.
    /// This test pins the function's existence, stable name, and that a sample
    /// re-emitter INSERT..SELECT can bind to it.
    #[test]
    fn test_fan_out_per_row_shared_ops_function_parity() {
        let source = post_fan_out_per_row_source();
        let rows = source.rows_from.as_ref().unwrap();

        // Stable, unhashed name the re-emitter can hardcode.
        assert_eq!(fan_out_ops_function_name(&source), "bitdex_post_fanout_ops");

        let fn_sql = generate_fan_out_ops_function_sql(&source, rows);
        // Full `set` payload, parent-row parameterized, retaining all three fields.
        for expected in ["publishedAt", "availability", "postedToId"] {
            assert!(fn_sql.contains(expected), "function dropped '{expected}':\n{fn_sql}");
        }
        assert!(fn_sql.contains(r#"extract(epoch from _p."publishedAt")::bigint"#), "{fn_sql}");
        assert!(fn_sql.contains(r#"_p."availability"::text"#), "{fn_sql}");
        assert!(fn_sql.contains(r#"to_jsonb(_p."modelVersionId")"#), "{fn_sql}");

        // REAL parity check (not a locally-built literal): the CREATE FUNCTION
        // block embedded in the actual generated trigger SQL must be
        // byte-identical to what `generate_fan_out_ops_function_sql` produces in
        // isolation — i.e. there is genuinely only ONE builder of this payload,
        // not two code paths that happen to agree today.
        let trigger_sql = generate_trigger_sql(&source);
        assert!(
            trigger_sql.contains(&fn_sql),
            "trigger SQL's embedded ops function diverged from the standalone \
             generator output — the single-builder guarantee is broken.\n\
             standalone:\n{fn_sql}\ntrigger:\n{trigger_sql}"
        );

        // And the trigger's INSERT path calls it by the exact name the helper
        // returns — a re-emitter built from `fan_out_ops_function_name` +
        // `generate_fan_out_ops_function_sql` is calling the identical function
        // the trigger installs, not a name it guessed.
        assert!(
            trigger_sql.contains(&format!("_ops := {}(NEW);", fan_out_ops_function_name(&source))),
            "trigger INSERT path must call the function by its helper-derived name:\n{trigger_sql}"
        );
    }

    /// PR-M1 (PINNED): the per-image Post fan-out RETAINS the full payload —
    /// publishedAt drives deferred-alive activation stamping AND the isPublished
    /// shadow, so it must keep flowing per-image. Options A/B (which delete
    /// publishedAt) are PARKED.
    #[test]
    fn test_fan_out_per_row_retains_full_payload() {
        let sql = generate_trigger_sql(&post_fan_out_per_row_source());
        let fields = emitted_fields(&sql);
        for expected in ["publishedAt", "availability", "postedToId"] {
            assert!(
                fields.contains(expected),
                "per-row fan-out dropped '{expected}' (payload must be retained per PR-M1); got {fields:?}\n{sql}"
            );
        }
        // publishedAt is emitted via the epoch expression, not a bare column.
        assert!(
            sql.contains(r#"extract(epoch from NEW."publishedAt")::bigint"#),
            "publishedAt must be emitted as an epoch bigint:\n{sql}"
        );
    }

    /// PROD-BUG GUARD (2026-07-18 ~26.5k leaked slots): the fan-out `publishedAt`
    /// emission must be CONDITIONAL. A future/null value is a scheduling/unpublish
    /// event and MUST become a `remove` (flips the isPublished exists_boolean
    /// shadow false → image hidden); only an already-past value may `set` (flips
    /// it true → visible). Pinned in the shared INSERT function AND the UPDATE
    /// new-side. availability/postedToId stay UNCONDITIONAL — only publishedAt is
    /// guarded.
    #[test]
    fn test_fan_out_publishedat_future_guard() {
        let source = post_fan_out_per_row_source();
        let rows = source.rows_from.as_ref().unwrap();

        // --- Shared INSERT function (_p parent row) ---
        let fn_sql = generate_fan_out_ops_function_sql(&source, rows);

        // Guard predicate covers BOTH future (> now()) AND null (IS NULL).
        assert!(
            fn_sql.contains(r#"CASE WHEN _p."publishedAt" IS NULL OR _p."publishedAt" > now()"#),
            "INSERT publishedAt must be guarded on NULL/future:\n{fn_sql}"
        );
        // future/null → remove (THEN branch); past → set (ELSE branch).
        assert!(
            fn_sql.contains(r#"THEN jsonb_build_object('op', 'remove', 'field', 'publishedAt'"#),
            "future/null publishedAt must emit a remove:\n{fn_sql}"
        );
        assert!(
            fn_sql.contains(r#"ELSE jsonb_build_object('op', 'set', 'field', 'publishedAt'"#),
            "past publishedAt must emit a set:\n{fn_sql}"
        );
        // Only publishedAt is guarded — availability/postedToId stay plain `set`.
        assert!(
            !fn_sql.contains(r#"CASE WHEN _p."availability""#)
                && !fn_sql.contains(r#"CASE WHEN _p."modelVersionId""#),
            "only publishedAt may be guarded; other fields must be unconditional:\n{fn_sql}"
        );
        assert!(
            fn_sql.contains(r#"jsonb_build_object('op', 'set', 'field', 'availability'"#),
            "availability must remain an unconditional set:\n{fn_sql}"
        );

        // --- UPDATE new-side (NEW row) ---
        let trigger_sql = generate_trigger_sql(&source);
        assert!(
            trigger_sql.contains(r#"CASE WHEN NEW."publishedAt" IS NULL OR NEW."publishedAt" > now()"#),
            "UPDATE new-side publishedAt must be guarded on NULL/future:\n{trigger_sql}"
        );
        // The already-past (publish) case still emits remove(OLD) + set(NEW).
        assert!(
            trigger_sql.contains(&format!(
                "jsonb_build_object('op', 'remove', 'field', 'publishedAt', 'value', \
                 to_jsonb({}))",
                r#"extract(epoch from OLD."publishedAt")::bigint"#
            )),
            "the publish arm must still remove the OLD publishedAt value:\n{trigger_sql}"
        );
        // availability's UPDATE new-side is NOT guarded.
        assert!(
            !trigger_sql.contains(r#"CASE WHEN NEW."availability""#),
            "only publishedAt may be guarded on the UPDATE new-side:\n{trigger_sql}"
        );
    }

    /// ROOT-CAUSE GUARD (2026-08-18): the schedule arm emits two value-bearing
    /// removes on one field, and the NEW side must be emitted LAST.
    ///
    /// The engine reads the last op on the schedule field to decide when to
    /// activate the slot, and `op_dedup` re-emits these in arrival order, so
    /// emission order IS the answer. It used to be hash order — a reschedule
    /// resolved by coin flip: one arm published at the abandoned schedule
    /// (content visible early), the other stripped the schedule (published
    /// content stuck invisible). Captured live in "BitdexOps": one row,
    /// `remove publishedAt null` and `remove publishedAt 1787029800`, same image.
    ///
    /// The old-side remove cannot be dropped to make the row unambiguous:
    /// `process_remove_op` clears sort-layer bits derived from the value it is
    /// given, so on an alive slot it is the only op that clears the previous
    /// timestamp. Dropping it trades a hash-order bug for a stale-sort-bits bug.
    #[test]
    fn test_fan_out_publishedat_schedule_emits_old_side_then_new() {
        let trigger_sql = generate_trigger_sql(&post_fan_out_per_row_source());

        let old_remove = format!(
            "jsonb_build_object('op', 'remove', 'field', 'publishedAt', 'value', to_jsonb({}))",
            r#"extract(epoch from OLD."publishedAt")::bigint"#
        );
        let new_remove = format!(
            "jsonb_build_object('op', 'remove', 'field', 'publishedAt', 'value', to_jsonb({}))",
            r#"extract(epoch from NEW."publishedAt")::bigint"#
        );
        let new_set = format!(
            "jsonb_build_object('op', 'set', 'field', 'publishedAt', 'value', to_jsonb({}))",
            r#"extract(epoch from NEW."publishedAt")::bigint"#
        );

        // Isolate the publishedAt block of the UPDATE branch: from its
        // `IS DISTINCT FROM` guard to the `END IF;` that closes it.
        let update_at = trigger_sql
            .find("ELSIF TG_OP = 'UPDATE' THEN")
            .unwrap_or_else(|| panic!("no UPDATE branch:\n{trigger_sql}"));
        let update_branch = &trigger_sql[update_at..];
        let block_start = update_branch
            .find(r#"IF (extract(epoch from OLD."publishedAt")::bigint)"#)
            .unwrap_or_else(|| panic!("no publishedAt UPDATE block:\n{update_branch}"));
        let block = &update_branch[block_start..];
        let block_end = block
            .find("END IF;")
            .unwrap_or_else(|| panic!("publishedAt block unterminated:\n{block}"));
        let block = &block[..block_end];

        // The OLD-side remove fires on BOTH arms — it is outside the CASE.
        // Without it, `process_remove_op` never clears the previous timestamp's
        // sort-layer bits for an alive slot.
        let case_at = block
            .find("CASE WHEN")
            .unwrap_or_else(|| panic!("guarded CASE missing:\n{block}"));
        let before_case = &block[..case_at];
        assert!(
            before_case.contains(&old_remove),
            "the old-side remove must be emitted unconditionally, before the \
             guard — it is what clears the previous value's sort bits:\n{block}"
        );

        // ORDER CONTRACT: the old-side remove comes FIRST, the guarded new side
        // LAST. The engine takes the last op on this field as the current
        // schedule, and dedup preserves arrival order, so this order is the
        // answer to "which schedule wins".
        let old_at = block.find(&old_remove).expect("old-side remove");
        let new_at = block
            .find(&new_remove)
            .unwrap_or_else(|| panic!("guarded new-side remove missing:\n{block}"));
        assert!(
            old_at < new_at,
            "the NEW-side op must be emitted LAST — the engine reads the last op \
             on this field as the schedule:\n{block}"
        );

        // Both arms of the guard: future/null → remove, already-past → set.
        assert!(
            block.contains(&new_set),
            "the already-past arm must still be a set:\n{block}"
        );
        assert_eq!(
            block.matches("'field', 'publishedAt'").count(),
            3,
            "exactly three publishedAt ops: the old-side remove plus the two \
             arms of the guarded new side:\n{block}"
        );
    }

    /// [reviewer minor 2] `type: fan_out_per_row` without `rows_from` must fail
    /// loudly at parse time rather than silently falling through to
    /// `generate_direct_body` (dispatch is keyed on `rows_from.is_some()`, not
    /// on `table_type`, so this misconfiguration would otherwise generate a
    /// plausible-looking but wrong direct-table trigger with no error at all).
    #[test]
    fn test_fan_out_per_row_type_without_rows_from_rejected() {
        let yaml = r#"
sync_sources:
  - table: Post
    type: fan_out_per_row
    track_fields: [publishedAt]
"#;
        let err = SyncConfig::from_yaml(yaml).unwrap_err();
        assert!(err.contains("Post"), "{err}");
        assert!(err.contains("rows_from"), "{err}");
    }

    /// [reviewer minor 2] `rows_from` set but `type` missing/wrong must fail
    /// loudly — otherwise a typo'd or omitted `type: fan_out_per_row` silently
    /// routes through `generate_direct_body` and `rows_from` is dropped on the
    /// floor with no per-image ops ever emitted.
    #[test]
    fn test_rows_from_without_matching_type_rejected() {
        let yaml = r#"
sync_sources:
  - table: Post
    rows_from: { table: Image, fk: postId }
    track_fields: [publishedAt]
"#;
        let err = SyncConfig::from_yaml(yaml).unwrap_err();
        assert!(err.contains("fan_out_per_row"), "{err}");
    }

    /// [reviewer minor 2] `rows_from` combined with `field` (multi-value join
    /// dispatch) must be rejected — `generate_trigger_body` dispatches on
    /// `field` BEFORE `rows_from`, so today that combination would silently
    /// build a multi-value-join trigger and ignore `rows_from` entirely.
    #[test]
    fn test_rows_from_combined_with_field_rejected() {
        let yaml = r#"
sync_sources:
  - table: Post
    type: fan_out_per_row
    field: tagIds
    value_field: tagId
    rows_from: { table: Image, fk: postId }
"#;
        let err = SyncConfig::from_yaml(yaml).unwrap_err();
        assert!(err.contains("field"), "{err}");
    }

    /// [reviewer minor 2] `rows_from` combined with `query` (the query-resolved
    /// fan-out) is an ambiguous "which fan-out mechanism" config and must be
    /// rejected rather than silently picked by dispatch order.
    #[test]
    fn test_rows_from_combined_with_query_rejected() {
        let yaml = r#"
sync_sources:
  - table: Post
    type: fan_out_per_row
    query: "postId eq {id}"
    rows_from: { table: Image, fk: postId }
    track_fields: [publishedAt]
"#;
        let err = SyncConfig::from_yaml(yaml).unwrap_err();
        assert!(err.contains("query"), "{err}");
    }

    /// Sanity: the valid production shape parses cleanly (no false positives
    /// from the new validation).
    #[test]
    fn test_fan_out_per_row_valid_config_parses() {
        let yaml = r#"
sync_sources:
  - table: Post
    type: fan_out_per_row
    rows_from: { table: Image, fk: postId }
    track_fields: [publishedAt]
"#;
        assert!(SyncConfig::from_yaml(yaml).is_ok());
    }

    /// Config-to-behavior: the parser reads `type: fan_out_per_row` + `rows_from`
    /// from YAML and populates the runtime struct (team-standards §Config-to-Behavior).
    #[test]
    fn test_fan_out_per_row_yaml_parsing() {
        let yaml = r#"
sync_sources:
  - table: Post
    type: fan_out_per_row
    rows_from: { table: Image, fk: postId }
    track_fields:
      - { column: publishedAt, target: publishedAt, expression: "extract(epoch from {publishedAt})::bigint" }
      - { column: availability, target: availability, expression: "{availability}::text" }
      - { column: modelVersionId, target: postedToId }
"#;
        let config = SyncConfig::from_yaml(yaml).unwrap();
        let src = &config.sync_sources[0];
        assert_eq!(src.table_type.as_deref(), Some("fan_out_per_row"));
        let rows = src.rows_from.as_ref().expect("rows_from must parse");
        assert_eq!(rows.table, "Image");
        assert_eq!(rows.fk, "postId");
        // Defaults applied when omitted.
        assert_eq!(rows.slot, "id");
        assert_eq!(rows.parent_key, "id");

        // And the runtime struct actually drives codegen down the per-row path.
        let sql = generate_trigger_sql(src);
        assert!(sql.contains(r#"FROM "Image" c WHERE c."postId" = NEW."id""#), "{sql}");
        assert!(!sql.contains("queryOpSet"), "{sql}");
    }

    /// [AR-4-v2] units: the Image sortAt track field emits the TARGET name
    /// `sortAt` in SECONDS with a COALESCE(GREATEST...) fallback, and diffs on
    /// UPDATE. CORRECTED 2026-07-18: source-name (`sortAtUnix`, ms) emission was
    /// silently dropped by the engine — op fields resolve against TARGET names
    /// only; data_schema renames are doc-path only. Pins the chain at codegen.
    /// Build the Image source with the sortAt track field factored into
    /// the shared `bitdex_image_sortat_ops` function (production shape: the
    /// re-emitter calls this exact function to re-assert sortAt).
    fn image_source_with_sortat() -> SyncSource {
        SyncSource {
            slot_field: Some("id".into()),
            track_fields: Some(vec![
                TrackField::Simple("nsfwLevel".into()),
                TrackField::Simple(SORTAT_EXPR.into()),
            ]),
            shared_ops_fields: Some(vec!["sortAt".into()]),
            sets_alive: true,
            on_delete: Some(OnDeleteValue::String("delete_slot".into())),
            ..test_source("Image")
        }
    }

    /// The guarded field has TWO writers, and the invariant has to hold at both.
    ///
    /// On Image, `publishedAt` is a COMPUTED field — a correlated subquery on
    /// the parent Post — so the direct-table trigger writes it as well as the
    /// Post fan-out. The guard lived only in the fan-out codegen, leaving the
    /// direct UPDATE branch free to emit a bare `set publishedAt = <future>`.
    /// On an alive slot that flips the isPublished shadow true and lands the
    /// image at the top of the feed with a future sortAt — the exact leak the
    /// guard exists to prevent.
    ///
    /// The INSERT branch must stay unguarded: its future `set` is what puts a
    /// freshly-inserted scheduled image into the deferred map.
    #[test]
    fn test_computed_publishedat_guarded_on_update_not_insert() {
        let source = SyncSource {
            slot_field: Some("id".into()),
            track_fields: Some(vec![TrackField::Simple("nsfwLevel".into())]),
            computed_fields: Some(vec![
                TriggerComputedField {
                    target: "publishedAt".into(),
                    expression:
                        r#"(SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = {postId})"#
                            .into(),
                    value: None,
                },
                TriggerComputedField {
                    target: "existedAt".into(),
                    expression: "GREATEST(extract(epoch from {scannedAt})::bigint, extract(epoch from {createdAt})::bigint)".into(),
                    value: None,
                },
            ]),
            sets_alive: true,
            ..test_source("Image")
        };
        let sql = generate_trigger_sql(&source);

        let insert_at = sql.find("IF TG_OP = 'INSERT' THEN").expect("INSERT branch");
        // Anchor on the UPDATE branch's own first statement rather than a bare
        // `ELSE`, which would silently split on the wrong boundary if the
        // codegen ever grows an earlier one.
        let update_at = sql[insert_at..]
            .find("  ELSE\n    _ops := '[]'::jsonb;\n")
            .map(|i| i + insert_at)
            .expect("UPDATE branch");
        assert!(insert_at < update_at, "unexpected branch order:\n{sql}");
        let insert_branch = &sql[insert_at..update_at];
        let update_branch = &sql[update_at..];

        // INSERT: bare set, no guard — deferral depends on it.
        assert!(
            insert_branch.contains("jsonb_build_object('op', 'set', 'field', 'publishedAt'"),
            "INSERT must emit a plain publishedAt set:\n{insert_branch}"
        );
        assert!(
            !insert_branch.contains("CASE WHEN"),
            "INSERT must NOT be guarded — the future set is what defers the \
             slot:\n{insert_branch}"
        );

        // UPDATE: guarded new side, unconditional old-side remove before it.
        assert!(
            update_branch.contains("extract(epoch from now())::bigint"),
            "UPDATE publishedAt must be guarded against future values:\n{update_branch}"
        );
        let case_at = update_branch
            .find("CASE WHEN")
            .expect("guarded CASE in the UPDATE branch");
        assert!(
            update_branch[..case_at].contains(
                "jsonb_build_object('op', 'remove', 'field', 'publishedAt'"
            ),
            "the old-side remove must be emitted before the guard — it is what \
             clears the previous value's sort bits:\n{update_branch}"
        );
        let case_body = &update_branch[case_at..];
        let else_at = case_body.find("ELSE").expect("ELSE arm");
        let then_arm = &case_body[..else_at];
        assert_eq!(
            then_arm.matches("jsonb_build_object('op'").count(),
            1,
            "future/null arm must emit exactly one op:\n{then_arm}"
        );
        assert!(
            then_arm.contains("'op', 'remove', 'field', 'publishedAt'"),
            "future/null arm must be a remove:\n{then_arm}"
        );

        // Other computed fields stay unguarded.
        assert!(
            update_branch.contains(
                "jsonb_build_object('op', 'set', 'field', 'existedAt'"
            ),
            "non-guarded computed fields keep the plain pair:\n{update_branch}"
        );
        assert_eq!(
            update_branch.matches("CASE WHEN").count(),
            1,
            "only publishedAt may be guarded:\n{update_branch}"
        );
    }

    #[test]
    fn test_image_sortat_target_name_seconds_and_coalesce() {
        let source = image_source_with_sortat();
        let sql = generate_trigger_sql(&source);

        // Emitted under the TARGET field name `sortAt` — op fields resolve
        // against target names; data_schema renames are doc-path only.
        assert!(emitted_fields(&sql).contains("sortAt"), "{sql}");
        assert!(!emitted_fields(&sql).contains("sortAtUnix"), "must NOT emit the source name:\n{sql}");
        // Seconds — the engine stores sort layers in seconds; no ms conversion.
        assert!(!sql.contains("* 1000"), "sortAt must be seconds, not ms:\n{sql}");
        // INSERT branch: the shared function is called with NEW rather than the
        // expression being inlined a second time.
        assert!(
            sql.contains("_ops := _ops || bitdex_image_sortat_ops(NEW);"),
            "INSERT must call the shared sortat ops function:\n{sql}"
        );
        // The shared function itself contains the NEW... no — the function is
        // parameterized on the row (_i), so its body uses _i, not NEW/OLD.
        assert!(
            sql.contains(r#"COALESCE(extract(epoch from _i."sortAt")::bigint"#),
            "shared function body must be row-parameterized (_i), not NEW/OLD:\n{sql}"
        );
        assert!(
            sql.contains(r#"FROM "Post" p WHERE p.id = _i."postId""#),
            "fallback must read Post.publishedAt per-image:\n{sql}"
        );
        // UPDATE branch still diffs OLD vs NEW inline (the shared function is
        // only used to build the INSERT set-op / re-emitter payload).
        assert!(
            sql.contains(r#"COALESCE(extract(epoch from NEW."sortAt")::bigint"#),
            "UPDATE must diff the NEW resolution:\n{sql}"
        );
        assert!(
            sql.contains(r#"COALESCE(extract(epoch from OLD."sortAt")::bigint"#),
            "UPDATE must diff the OLD resolution:\n{sql}"
        );
        assert!(
            sql.contains(r#"WHERE p.id = OLD."postId""#),
            "UPDATE fallback must resolve against OLD row:\n{sql}"
        );
    }

    /// W1-3 parity for sortAt, same pattern as the Post fan-out's parity
    /// test: prove the re-emitter's call target is the SAME function the
    /// trigger installs, not a name/expression it reimplements and could drift
    /// from.
    #[test]
    fn test_image_sortat_shared_ops_function_parity() {
        let source = image_source_with_sortat();

        // Stable, unhashed name — same convention as bitdex_post_fanout_ops,
        // derived from the wire field name with the Unix suffix stripped.
        assert_eq!(
            shared_field_ops_function_name("Image", "sortAt"),
            "bitdex_image_sortat_ops"
        );

        let fn_sql = generate_shared_field_ops_function_sql(&source, "sortAt")
            .expect("sortAt is tracked, must resolve");
        assert!(fn_sql.contains(r#"CREATE OR REPLACE FUNCTION bitdex_image_sortat_ops(_i "Image") RETURNS jsonb"#), "{fn_sql}");
        assert!(fn_sql.contains("LANGUAGE sql STABLE"), "{fn_sql}");
        assert!(fn_sql.contains("'field', 'sortAt'"), "{fn_sql}");
        assert!(!fn_sql.contains("* 1000"), "shared function must emit seconds:\n{fn_sql}");

        // REAL parity: the function embedded in the generated trigger SQL is
        // byte-identical to the standalone generator's output.
        let trigger_sql = generate_trigger_sql(&source);
        assert!(
            trigger_sql.contains(&fn_sql),
            "trigger SQL's embedded sortat ops function diverged from the \
             standalone generator output.\nstandalone:\n{fn_sql}\ntrigger:\n{trigger_sql}"
        );
        assert!(
            trigger_sql.contains(&format!(
                "_ops := _ops || {}(NEW);",
                shared_field_ops_function_name("Image", "sortAt"),
            )),
            "trigger INSERT path must call the function by its helper-derived name:\n{trigger_sql}"
        );
    }

    /// A `shared_ops_fields` entry naming a field the source doesn't actually
    /// track must fail loudly at config parse time (fail-fast, matches the
    /// fan_out_per_row validation contract) rather than at codegen or, worse,
    /// silently produce a broken function referencing nothing.
    #[test]
    fn test_shared_ops_fields_unknown_target_rejected() {
        let yaml = r#"
sync_sources:
  - table: Image
    slot_field: id
    sets_alive: true
    track_fields: [nsfwLevel]
    shared_ops_fields: [doesNotExist]
"#;
        let err = SyncConfig::from_yaml(yaml).unwrap_err();
        assert!(err.contains("doesNotExist"), "error must name the bad field: {err}");
    }

    /// PR-m2 (disjointness): the Post per-row fan-out and the Image trigger must
    /// never emit the SAME field for one image. Disjointness is what makes
    /// double-emission safe — an overlap would let op_dedup LIFO-resolve the two
    /// writers nondeterministically.
    #[test]
    fn test_post_fanout_and_image_fields_disjoint() {
        let post_sql = generate_trigger_sql(&post_fan_out_per_row_source());

        let image = SyncSource {
            track_fields: Some(vec![
                TrackField::Simple("nsfwLevel".into()),
                TrackField::Simple("{type}::text as type".into()),
                TrackField::Simple("postId".into()),
                TrackField::Simple(SORTAT_EXPR.into()),
            ]),
            ..image_source_with_sortat()
        };
        let image_sql = generate_trigger_sql(&image);

        let post_fields = emitted_fields(&post_sql);
        let image_fields = emitted_fields(&image_sql);
        assert!(!post_fields.is_empty() && !image_fields.is_empty());

        let overlap: Vec<_> = post_fields.intersection(&image_fields).collect();
        assert!(
            overlap.is_empty(),
            "Post fan-out and Image trigger emit overlapping field(s) {overlap:?} — \
             double-emission would be LIFO-resolved nondeterministically.\n\
             Post: {post_fields:?}\nImage: {image_fields:?}"
        );
    }
}
