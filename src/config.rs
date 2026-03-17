use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{BitdexError, Result};
pub use crate::filter::FilterFieldType;

/// Top-level Bitdex V2 configuration.
///
/// Loaded from TOML or YAML files. Designed for future hot-reloadability:
/// all config sections are cheaply cloneable and can be swapped atomically
/// behind an `Arc<ArcSwap<Config>>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Filter field definitions.
    #[serde(default)]
    pub filter_fields: Vec<FilterFieldConfig>,

    /// Sort field definitions.
    #[serde(default)]
    pub sort_fields: Vec<SortFieldConfig>,

    /// Time bucket configuration for pre-computed time range filters.
    /// Decoupled from filter_fields to avoid creating per-value bitmaps
    /// for high-cardinality timestamp fields.
    #[serde(default)]
    pub time_buckets: Option<TimeBucketFieldConfig>,

    /// Maximum results per query (hard cap).
    #[serde(default = "default_max_page_size")]
    pub max_page_size: usize,

    /// Trie cache settings.
    #[serde(default)]
    pub cache: CacheConfig,

    /// Autovac interval in seconds.
    #[serde(default = "default_autovac_interval")]
    pub autovac_interval_secs: u64,

    /// Merge interval for versioned bitmaps, in milliseconds.
    #[serde(default = "default_merge_interval_ms")]
    pub merge_interval_ms: u64,

    /// Prometheus metrics port.
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,

    /// Flush interval for the concurrent engine's background flush thread, in microseconds.
    #[serde(default = "default_flush_interval_us")]
    pub flush_interval_us: u64,

    /// Bounded channel capacity for the write coalescer.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    /// Bitmap persistence and caching settings.
    #[serde(default)]
    pub storage: StorageConfig,

    /// Compaction threshold: percentage of stale tuples that triggers background
    /// compaction. Default 30 (compact when >30% stale). Set to 0 to disable
    /// compaction entirely (no worker thread, no staleness tracking on reads).
    #[serde(default = "default_compact_threshold_pct")]
    pub compact_threshold_pct: u64,

    /// Eviction sweep interval: check for idle values every N flush cycles.
    /// Default 1000 (~0.1s at 100μs flush). Lower values make eviction more
    /// responsive (useful for testing).
    #[serde(default = "default_eviction_sweep_interval")]
    pub eviction_sweep_interval: u64,

    /// Deferred alive: documents with a future timestamp in the specified field
    /// won't be marked alive until that time arrives. Only one field per document.
    #[serde(default)]
    pub deferred_alive: Option<DeferredAliveConfig>,

    /// Headless mode: skip all background threads (flush, merge, eviction).
    /// Used by bulk loaders that write directly to BitmapFs and don't need
    /// the engine's write pipeline. The engine still provides config, BitmapFs
    /// access, and docstore, but no background work runs.
    #[serde(default)]
    pub headless: bool,
}

fn default_max_page_size() -> usize {
    100
}
fn default_autovac_interval() -> u64 {
    3600
}
fn default_merge_interval_ms() -> u64 {
    5000
}
fn default_prometheus_port() -> u16 {
    9090
}
fn default_flush_interval_us() -> u64 {
    100
}
fn default_compact_threshold_pct() -> u64 {
    30
}
fn default_eviction_sweep_interval() -> u64 {
    1000
}
fn default_channel_capacity() -> usize {
    100_000
}
fn default_schema_version() -> u8 {
    1
}

/// Deferred alive configuration: defer a document's alive bit until a future timestamp.
///
/// The source field is read from the incoming document. If its value is in the future,
/// the slot's filter/sort bitmaps are set immediately but the alive bit is deferred
/// until the timestamp arrives. The flush thread activates due slots every cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeferredAliveConfig {
    /// The document field containing the activation timestamp (unix seconds).
    pub source_field: String,
    /// If true, the source value is in milliseconds and will be divided by 1000.
    #[serde(default)]
    pub ms_to_seconds: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            filter_fields: Vec::new(),
            sort_fields: Vec::new(),
            time_buckets: None,
            max_page_size: default_max_page_size(),
            cache: CacheConfig::default(),
            autovac_interval_secs: default_autovac_interval(),
            merge_interval_ms: default_merge_interval_ms(),
            prometheus_port: default_prometheus_port(),
            flush_interval_us: default_flush_interval_us(),
            channel_capacity: default_channel_capacity(),
            storage: StorageConfig::default(),
            eviction_sweep_interval: default_eviction_sweep_interval(),
            compact_threshold_pct: default_compact_threshold_pct(),
            deferred_alive: None,
            headless: false,
        }
    }
}

impl Config {
    /// Load configuration from a file. Format is detected from the file extension.
    ///
    /// Supported extensions: `.toml`, `.yaml`, `.yml`
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| BitdexError::Config(format!("failed to read {}: {e}", path.display())))?;

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        match ext {
            "toml" => Self::from_toml(&content),
            other => Err(BitdexError::Config(format!(
                "unsupported config file format: '{other}'"
            ))),
        }
    }

    /// Load configuration from a TOML string.
    pub fn from_toml(toml_str: &str) -> Result<Self> {
        let config: Config =
            toml::from_str(toml_str).map_err(|e| BitdexError::Config(format!("TOML parse error: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.max_page_size == 0 {
            return Err(BitdexError::Config(
                "max_page_size must be > 0".to_string(),
            ));
        }

        // Validate cache settings
        if self.cache.decay_rate <= 0.0 || self.cache.decay_rate > 1.0 {
            return Err(BitdexError::Config(
                "cache.decay_rate must be in (0.0, 1.0]".to_string(),
            ));
        }

        // Check for duplicate filter field names
        let mut filter_names = std::collections::HashSet::new();
        for f in &self.filter_fields {
            if f.name.is_empty() {
                return Err(BitdexError::Config(
                    "filter field name must not be empty".to_string(),
                ));
            }
            if !filter_names.insert(&f.name) {
                return Err(BitdexError::Config(format!(
                    "duplicate filter field: {}",
                    f.name
                )));
            }
            // Validate eviction: only on multi_value fields
            if let Some(ref eviction) = f.eviction {
                if f.field_type != FilterFieldType::MultiValue {
                    return Err(BitdexError::Config(format!(
                        "filter field '{}': eviction is only supported on multi_value fields",
                        f.name
                    )));
                }
                if eviction.idle_seconds <= 0.0 {
                    return Err(BitdexError::Config(format!(
                        "filter field '{}': eviction.idle_seconds must be > 0",
                        f.name
                    )));
                }
            }
            if let Some(behaviors) = &f.behaviors {
                // Validate range_buckets: unique names, non-zero durations
                let mut bucket_names = std::collections::HashSet::new();
                for bucket in &behaviors.range_buckets {
                    if bucket.name.is_empty() {
                        return Err(BitdexError::Config(format!(
                            "filter field '{}': bucket name must not be empty",
                            f.name
                        )));
                    }
                    if !bucket_names.insert(&bucket.name) {
                        return Err(BitdexError::Config(format!(
                            "filter field '{}': duplicate bucket name '{}'",
                            f.name, bucket.name
                        )));
                    }
                    if bucket.duration_secs == 0 {
                        return Err(BitdexError::Config(format!(
                            "filter field '{}', bucket '{}': duration_secs must be > 0",
                            f.name, bucket.name
                        )));
                    }
                    if bucket.refresh_interval_secs == 0 {
                        return Err(BitdexError::Config(format!(
                            "filter field '{}', bucket '{}': refresh_interval_secs must be > 0",
                            f.name, bucket.name
                        )));
                    }
                }
            }
        }

        // Validate top-level time_buckets
        if let Some(ref tb) = self.time_buckets {
            if tb.filter_field.is_empty() {
                return Err(BitdexError::Config(
                    "time_buckets.filter_field must not be empty".to_string(),
                ));
            }
            if tb.sort_field.is_empty() {
                return Err(BitdexError::Config(
                    "time_buckets.sort_field must not be empty".to_string(),
                ));
            }
            let mut bucket_names = std::collections::HashSet::new();
            for bucket in &tb.range_buckets {
                if bucket.name.is_empty() {
                    return Err(BitdexError::Config(
                        "time_buckets: bucket name must not be empty".to_string(),
                    ));
                }
                if !bucket_names.insert(&bucket.name) {
                    return Err(BitdexError::Config(format!(
                        "time_buckets: duplicate bucket name '{}'",
                        bucket.name
                    )));
                }
                if bucket.duration_secs == 0 {
                    return Err(BitdexError::Config(format!(
                        "time_buckets bucket '{}': duration_secs must be > 0",
                        bucket.name
                    )));
                }
                if bucket.refresh_interval_secs == 0 {
                    return Err(BitdexError::Config(format!(
                        "time_buckets bucket '{}': refresh_interval_secs must be > 0",
                        bucket.name
                    )));
                }
            }
        }

        // Validate deferred_alive config
        if let Some(ref da) = self.deferred_alive {
            if da.source_field.is_empty() {
                return Err(BitdexError::Config(
                    "deferred_alive.source_field must not be empty".to_string(),
                ));
            }
        }

        // Check for duplicate sort field names and validate bits
        let mut sort_names = std::collections::HashSet::new();
        for s in &self.sort_fields {
            if s.name.is_empty() {
                return Err(BitdexError::Config(
                    "sort field name must not be empty".to_string(),
                ));
            }
            if !sort_names.insert(&s.name) {
                return Err(BitdexError::Config(format!(
                    "duplicate sort field: {}",
                    s.name
                )));
            }
            if s.bits == 0 || s.bits > 64 {
                return Err(BitdexError::Config(format!(
                    "sort field '{}': bits must be 1-64, got {}",
                    s.name, s.bits
                )));
            }
        }

        Ok(())
    }
}

/// Trie cache configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum number of cached entries.
    #[serde(default = "default_cache_max_entries")]
    pub max_entries: usize,
    /// Exponential decay rate for hit stats (0.0, 1.0].
    #[serde(default = "default_cache_decay_rate")]
    pub decay_rate: f64,
    /// Target number of slots in a bound cache entry.
    /// Bound caches reduce sort working sets to approximately this many candidates.
    #[serde(default = "default_bound_target_size")]
    pub bound_target_size: usize,
    /// Maximum bound size before triggering a rebuild.
    /// When live maintenance grows a bound beyond this, the next query rebuilds it.
    #[serde(default = "default_bound_max_size")]
    pub bound_max_size: usize,
    /// Maximum number of bound cache entries before LRU eviction.
    #[serde(default = "default_bound_max_count")]
    pub bound_max_count: usize,
    /// Prefetch threshold: trigger background expansion when the user has consumed
    /// this fraction of the cached entries (0.0–1.0). Default 0.95 = 95% consumed.
    /// Set to 0.0 or 1.0 to disable prefetching.
    #[serde(default = "default_prefetch_threshold")]
    pub prefetch_threshold: f64,
    /// Preload all bound cache shards at startup instead of lazy-loading on first query.
    /// Eliminates cold-start latency for cached sorts. Default: true.
    #[serde(default = "default_preload_bounds")]
    pub preload_bounds: bool,
    /// Maximum maintenance work per flush (affected_entries x changed_slots).
    /// When exceeded, affected entries are marked for rebuild instead of
    /// per-slot evaluation. Default 500_000.
    /// Used as fallback when `max_maintenance_ms` is 0.
    #[serde(default = "default_max_maintenance_work")]
    pub max_maintenance_work: usize,
    /// Time budget for cache maintenance per flush cycle in milliseconds.
    /// When > 0, replaces the count-based `max_maintenance_work` budget.
    /// 0 = use count-based `max_maintenance_work` instead. Default: 10ms.
    #[serde(default = "default_max_maintenance_ms")]
    pub max_maintenance_ms: u64,
}

fn default_cache_max_entries() -> usize {
    10_000
}
fn default_cache_decay_rate() -> f64 {
    0.95
}
fn default_bound_target_size() -> usize {
    10_000
}
fn default_bound_max_size() -> usize {
    20_000
}
fn default_bound_max_count() -> usize {
    100
}
fn default_prefetch_threshold() -> f64 {
    0.95
}
fn default_preload_bounds() -> bool {
    true
}
fn default_max_maintenance_work() -> usize {
    500_000
}
fn default_max_maintenance_ms() -> u64 {
    10
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: default_cache_max_entries(),
            decay_rate: default_cache_decay_rate(),
            bound_target_size: default_bound_target_size(),
            bound_max_size: default_bound_max_size(),
            bound_max_count: default_bound_max_count(),
            prefetch_threshold: default_prefetch_threshold(),
            preload_bounds: default_preload_bounds(),
            max_maintenance_work: default_max_maintenance_work(),
            max_maintenance_ms: default_max_maintenance_ms(),
        }
    }
}

/// Configuration for bitmap persistence.
///
/// All bitmaps are stored as individual files on the filesystem.
/// The two-tier snapshot/cached distinction is gone — all bitmaps are
/// always in the ArcSwap snapshot, and the OS page cache handles
/// hot/cold management transparently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Path to the bitmap directory for filesystem persistence.
    /// If None, bitmaps are memory-only (no persistence).
    #[serde(default)]
    pub bitmap_path: Option<std::path::PathBuf>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            bitmap_path: None,
        }
    }
}

/// Configuration for a single filter field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterFieldConfig {
    pub name: String,
    pub field_type: FilterFieldType,
    /// Optional time-related behaviors (only valid for timestamp fields).
    #[serde(default)]
    pub behaviors: Option<FieldBehaviors>,
    /// Idle eviction config. Only meaningful on `multi_value` fields.
    /// Values untouched for `idle_seconds` are evicted from memory and
    /// re-loaded from BitmapFs on the next query.
    #[serde(default)]
    pub eviction: Option<EvictionConfig>,
    /// If true, load this field's bitmaps eagerly on startup instead of
    /// deferring to first query (lazy loading). Default: false.
    #[serde(default)]
    pub eager_load: bool,
}

/// Per-value idle eviction configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictionConfig {
    /// Evict values untouched for this many seconds.
    /// The flush thread converts this to flush cycles using observed cycle timing.
    pub idle_seconds: f64,
}

/// Time-related behaviors for timestamp fields.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FieldBehaviors {
    /// Pre-computed range buckets for this field (e.g., "24h", "7d", "30d").
    #[serde(default)]
    pub range_buckets: Vec<BucketConfig>,
    /// Sort field to use for time bucket value reconstruction.
    /// If not set, defaults to the filter field name itself.
    /// Useful when the filter field name differs from the sort field name
    /// (e.g., filter="sortAtUnix" but sort="sortAt").
    #[serde(default)]
    pub sort_field: Option<String>,
}

/// Configuration for a single time range bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketConfig {
    /// Human-readable name (used in cache keys, e.g., "24h", "7d").
    pub name: String,
    /// Duration of the bucket in seconds (e.g., 86400 for 24h).
    pub duration_secs: u64,
    /// How often to rebuild this bucket's bitmap, in seconds.
    pub refresh_interval_secs: u64,
}

/// Top-level time bucket configuration.
/// Maps a filter clause field name to a sort field for value reconstruction,
/// with pre-computed range buckets. This is separate from filter_fields
/// because timestamp fields are extremely high-cardinality and would create
/// millions of per-value bitmaps if registered as regular filter fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeBucketFieldConfig {
    /// The field name used in filter clauses (e.g., "sortAtUnix").
    /// Gte/Gt clauses targeting this field will be snapped to the nearest bucket.
    pub filter_field: String,
    /// The sort field to use for value reconstruction during bucket rebuilds.
    /// Must reference a registered sort field (e.g., "sortAt").
    pub sort_field: String,
    /// Pre-computed range buckets.
    pub range_buckets: Vec<BucketConfig>,
}

/// Configuration for a single sort field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortFieldConfig {
    pub name: String,
    /// Source type (e.g., "uint32", "int64").
    #[serde(default = "default_source_type")]
    pub source_type: String,
    /// Encoding: "linear" or "log" (log is future work).
    #[serde(default = "default_encoding")]
    pub encoding: String,
    /// Number of bitmap layers. Defaults to 32 for uint32.
    #[serde(default = "default_bits")]
    pub bits: u8,
    /// If true, load this field's bitmaps eagerly on startup instead of
    /// deferring to first query (lazy loading). Default: false.
    #[serde(default)]
    pub eager_load: bool,
}

fn default_source_type() -> String {
    "uint32".to_string()
}
fn default_encoding() -> String {
    "linear".to_string()
}
fn default_bits() -> u8 {
    32
}

// ---------------------------------------------------------------------------
// Data Schema — describes how to map raw NDJSON fields to engine Documents
// ---------------------------------------------------------------------------

/// Schema describing how raw NDJSON records map to engine documents.
/// Used by the generic loader to convert arbitrary JSON into Documents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSchema {
    /// Name of the JSON field containing the document ID.
    pub id_field: String,
    /// Current schema version. Increment when changing field defaults.
    /// Old documents encoded with previous schema versions are decoded using
    /// historical defaults stored in `meta/schema/v{n}.json`. Docs are lazily
    /// migrated to the current schema version on next write.
    #[serde(default = "default_schema_version")]
    pub schema_version: u8,
    /// Field mapping rules: source JSON → target engine field.
    #[serde(default)]
    pub fields: Vec<FieldMapping>,
}

impl Default for DataSchema {
    fn default() -> Self {
        Self {
            id_field: String::new(),
            schema_version: default_schema_version(),
            fields: Vec::new(),
        }
    }
}

/// Maps a single source JSON field to a target engine field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    /// Source field name in the raw JSON.
    pub source: String,
    /// Target field name in the engine Document.
    pub target: String,
    /// How to interpret/convert the value.
    pub value_type: FieldValueType,
    /// Fallback source field if the primary is missing.
    #[serde(default)]
    pub fallback: Option<String>,
    /// For `mapped_string`: map string values to integer IDs.
    #[serde(default)]
    pub string_map: Option<HashMap<String, i64>>,
    /// If true, this field is stored in docstore only (not bitmap-indexed).
    #[serde(default)]
    pub doc_only: bool,
    /// If true, this field is bitmap-indexed only (not stored in docstore).
    /// Inverse of `doc_only`. Use for fields populated by separate data sources
    /// (e.g., collectionIds from CollectionItem table) rather than inline document fields.
    #[serde(default)]
    pub filter_only: bool,
    /// If true, divide millisecond timestamp by 1000 to get seconds, then store as u32.
    /// Use for fields like sortAtUnix/publishedAtUnix that are in milliseconds.
    #[serde(default)]
    pub ms_to_seconds: bool,
    /// Legacy alias for ms_to_seconds. Deprecated — use ms_to_seconds instead.
    #[serde(default)]
    pub truncate_u32: bool,
    /// If true, string matching is case-sensitive. Default false (case-insensitive).
    /// Applies to MappedString fields: both ingest (string_map lookup) and query resolution.
    #[serde(default)]
    pub case_sensitive: bool,
    /// Default value for this field. Documents with this value will have the field
    /// elided on write. On read, missing fields are reconstructed from this default.
    #[serde(default, rename = "default")]
    pub default_value: Option<serde_json::Value>,
}

impl FieldMapping {
    /// Whether this field should convert ms timestamps to seconds.
    /// Accepts either the new `ms_to_seconds` or legacy `truncate_u32` flag.
    pub fn should_convert_ms(&self) -> bool {
        self.ms_to_seconds || self.truncate_u32
    }

    /// Resolve the raw JSON value for this field, trying source then fallback.
    /// Returns (value, apply_ms_to_seconds). When the fallback is used, ms_to_seconds
    /// is NOT applied since the fallback is assumed to already be in the target unit.
    pub fn resolve_raw<'a>(
        &self,
        json: &'a serde_json::Value,
    ) -> Option<(&'a serde_json::Value, bool)> {
        if let Some(v) = json.get(&self.source).filter(|v| !v.is_null()) {
            // Primary source has a non-null value
            Some((v, self.should_convert_ms()))
        } else if let Some(v) = self
            .fallback
            .as_ref()
            .and_then(|fb| json.get(fb))
            .filter(|v| !v.is_null())
        {
            // Fallback has a non-null value
            Some((v, false))
        } else if json.get(&self.source).is_some() {
            // Primary source is explicitly null (no fallback or fallback also null).
            // Return the null so callers can write defaults to the docstore.
            Some((json.get(&self.source).unwrap(), self.should_convert_ms()))
        } else {
            // Source field not present at all
            None
        }
    }
}

impl DataSchema {
    /// Validate the schema.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version < 1 {
            return Err(BitdexError::Config(
                "data_schema.schema_version must be >= 1".to_string(),
            ));
        }
        for mapping in &self.fields {
            if mapping.doc_only && mapping.filter_only {
                return Err(BitdexError::Config(format!(
                    "Field '{}' cannot be both doc_only and filter_only",
                    mapping.target,
                )));
            }
        }
        Ok(())
    }

    /// Look up a field mapping by its source name.
    pub fn field_by_source(&self, source: &str) -> Option<&FieldMapping> {
        self.fields.iter().find(|m| m.source == source)
    }

    /// Normalize string_map keys to lowercase for case-insensitive MappedString fields.
    /// Call once after deserialization, before use in loader/docstore/server.
    pub fn normalize_string_maps(&mut self) {
        for mapping in &mut self.fields {
            if mapping.value_type == FieldValueType::MappedString && !mapping.case_sensitive {
                if let Some(ref mut map) = mapping.string_map {
                    *map = map.drain().map(|(k, v)| (k.to_lowercase(), v)).collect();
                }
            }
        }
    }
}

/// How a field value should be interpreted during NDJSON loading.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FieldValueType {
    /// Numeric value → Value::Integer
    Integer,
    /// Boolean value → Value::Bool
    Boolean,
    /// String value → Value::String (doc-only, not bitmap-indexed)
    String,
    /// String mapped to integer via string_map → Value::Integer
    MappedString,
    /// Low-cardinality string: auto-builds dictionary as new values are encountered.
    /// No hardcoded string_map needed — the dictionary assigns integer keys automatically.
    /// Case-insensitive matching by default.
    LowCardinalityString,
    /// Array of integers → FieldValue::Multi
    IntegerArray,
    /// Computed boolean: true if the source field exists and is non-null, false otherwise.
    /// Useful for "isPublished", "hasBlockedFor", "isRemix", etc.
    ExistsBoolean,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.max_page_size, 100);
        assert_eq!(config.cache.max_entries, 10_000);
        assert_eq!(config.cache.decay_rate, 0.95);
        assert_eq!(config.autovac_interval_secs, 3600);
        assert_eq!(config.merge_interval_ms, 5000);
        assert_eq!(config.prometheus_port, 9090);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_toml_parsing() {
        let toml_str = r#"
max_page_size = 50
autovac_interval_secs = 7200
merge_interval_ms = 3000
prometheus_port = 9191

[cache]
max_entries = 5000
decay_rate = 0.9

[[filter_fields]]
name = "nsfwLevel"
field_type = "single_value"

[[filter_fields]]
name = "tagIds"
field_type = "multi_value"

[[filter_fields]]
name = "onSite"
field_type = "boolean"

[[sort_fields]]
name = "reactionCount"
source_type = "uint32"
encoding = "linear"
bits = 32

[[sort_fields]]
name = "sortAt"
source_type = "uint32"
bits = 32
"#;
        let config = Config::from_toml(toml_str).unwrap();
        assert_eq!(config.max_page_size, 50);
        assert_eq!(config.cache.max_entries, 5000);
        assert_eq!(config.cache.decay_rate, 0.9);
        assert_eq!(config.autovac_interval_secs, 7200);
        assert_eq!(config.merge_interval_ms, 3000);
        assert_eq!(config.prometheus_port, 9191);
        assert_eq!(config.filter_fields.len(), 3);
        assert_eq!(config.sort_fields.len(), 2);
        assert_eq!(config.filter_fields[0].name, "nsfwLevel");
        assert_eq!(config.filter_fields[0].field_type, FilterFieldType::SingleValue);
        assert_eq!(config.filter_fields[1].field_type, FilterFieldType::MultiValue);
        assert_eq!(config.filter_fields[2].field_type, FilterFieldType::Boolean);
        assert_eq!(config.sort_fields[0].bits, 32);
    }

    #[test]
    fn test_from_file_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
max_page_size = 42

[cache]
max_entries = 999
"#,
        )
        .unwrap();
        let config = Config::from_file(&path).unwrap();
        assert_eq!(config.max_page_size, 42);
        assert_eq!(config.cache.max_entries, 999);
    }

    #[test]
    fn test_from_file_unsupported_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.xml");
        std::fs::write(&path, "<config/>").unwrap();
        let err = Config::from_file(&path);
        assert!(err.is_err());
    }

    #[test]
    fn test_from_file_not_found() {
        let path = PathBuf::from("/nonexistent/config.toml");
        assert!(Config::from_file(&path).is_err());
    }

    #[test]
    fn test_validation_rejects_zero_page_size() {
        let config = Config {
            max_page_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_invalid_decay_rate_zero() {
        let mut config = Config::default();
        config.cache.decay_rate = 0.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_invalid_decay_rate_over_one() {
        let mut config = Config::default();
        config.cache.decay_rate = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_duplicate_filter_fields() {
        let config = Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "status".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
                FilterFieldConfig {
                    name: "status".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
            ],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_duplicate_sort_fields() {
        let config = Config {
            sort_fields: vec![
                SortFieldConfig {
                    name: "x".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                },
                SortFieldConfig {
                    name: "x".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                },
            ],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_empty_field_names() {
        let config = Config {
            filter_fields: vec![FilterFieldConfig {
                name: "".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = Config {
            sort_fields: vec![SortFieldConfig {
                name: "".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: false,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_invalid_bits() {
        let config = Config {
            sort_fields: vec![SortFieldConfig {
                name: "test".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 0,
                eager_load: false,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = Config {
            sort_fields: vec![SortFieldConfig {
                name: "test".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 65,
                eager_load: false,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_civitai_config_toml() {
        let toml_str = r#"
max_page_size = 100
autovac_interval_secs = 3600
merge_interval_ms = 5000
prometheus_port = 9090

[cache]
max_entries = 10000
decay_rate = 0.95

[[filter_fields]]
name = "nsfwLevel"
field_type = "single_value"

[[filter_fields]]
name = "tagIds"
field_type = "multi_value"

[[filter_fields]]
name = "userId"
field_type = "single_value"

[[filter_fields]]
name = "modelVersionIds"
field_type = "multi_value"

[[filter_fields]]
name = "onSite"
field_type = "boolean"

[[filter_fields]]
name = "hasMeta"
field_type = "boolean"

[[filter_fields]]
name = "type"
field_type = "single_value"

[[sort_fields]]
name = "reactionCount"
source_type = "uint32"
bits = 32

[[sort_fields]]
name = "sortAt"
source_type = "uint32"
bits = 32

[[sort_fields]]
name = "commentCount"
source_type = "uint32"
bits = 32

[[sort_fields]]
name = "collectedCount"
source_type = "uint32"
bits = 32

[[sort_fields]]
name = "id"
source_type = "uint32"
bits = 32
"#;
        let config = Config::from_toml(toml_str).unwrap();
        assert_eq!(config.filter_fields.len(), 7);
        assert_eq!(config.sort_fields.len(), 5);
        assert_eq!(config.cache.max_entries, 10_000);
        assert_eq!(config.cache.decay_rate, 0.95);
    }

    #[test]
    fn test_invalid_toml() {
        assert!(Config::from_toml("{{{{not valid").is_err());
    }

    #[test]
    fn test_serde_roundtrip_toml() {
        let config = Config {
            sort_fields: vec![SortFieldConfig {
                name: "score".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
                eager_load: false,
            }],
            filter_fields: vec![FilterFieldConfig {
                name: "status".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            }],
            ..Config::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let roundtrip = Config::from_toml(&toml_str).unwrap();
        assert_eq!(roundtrip.sort_fields.len(), 1);
        assert_eq!(roundtrip.sort_fields[0].name, "score");
        assert_eq!(roundtrip.filter_fields[0].field_type, FilterFieldType::SingleValue);
    }

    #[test]
    fn test_storage_config_defaults() {
        let sc = StorageConfig::default();
        assert!(sc.bitmap_path.is_none());
    }

    #[test]
    fn test_config_default_includes_storage() {
        let config = Config::default();
        assert!(config.storage.bitmap_path.is_none());
    }

    #[test]
    fn test_toml_with_storage_path() {
        let toml_str = r#"
[[filter_fields]]
name = "tagIds"
field_type = "multi_value"

[[filter_fields]]
name = "nsfwLevel"
field_type = "single_value"

[storage]
bitmap_path = "/tmp/bitmaps"
"#;
        let config = Config::from_toml(toml_str).unwrap();
        assert_eq!(
            config.storage.bitmap_path,
            Some(std::path::PathBuf::from("/tmp/bitmaps"))
        );
    }

    #[test]
    fn test_toml_without_storage_uses_defaults() {
        let toml_str = r#"
max_page_size = 50
"#;
        let config = Config::from_toml(toml_str).unwrap();
        assert!(config.storage.bitmap_path.is_none());
    }

    #[test]
    fn test_field_behaviors_toml_parsing() {
        let toml_str = r#"
[[filter_fields]]
name = "scheduledAt"
field_type = "single_value"

[[filter_fields.behaviors.range_buckets]]
name = "24h"
duration_secs = 86400
refresh_interval_secs = 60

[[filter_fields.behaviors.range_buckets]]
name = "7d"
duration_secs = 604800
refresh_interval_secs = 300

[deferred_alive]
source_field = "scheduledAt"
"#;
        let config = Config::from_toml(toml_str).unwrap();
        assert_eq!(config.filter_fields.len(), 1);
        let behaviors = config.filter_fields[0].behaviors.as_ref().unwrap();
        assert_eq!(behaviors.range_buckets.len(), 2);
        assert_eq!(behaviors.range_buckets[0].name, "24h");
        assert_eq!(behaviors.range_buckets[0].duration_secs, 86400);
        assert_eq!(behaviors.range_buckets[0].refresh_interval_secs, 60);
        assert_eq!(behaviors.range_buckets[1].name, "7d");
        assert_eq!(behaviors.range_buckets[1].duration_secs, 604800);
        assert_eq!(behaviors.range_buckets[1].refresh_interval_secs, 300);
        let da = config.deferred_alive.as_ref().unwrap();
        assert_eq!(da.source_field, "scheduledAt");
        assert!(!da.ms_to_seconds);
    }

    #[test]
    fn test_field_behaviors_defaults_to_none() {
        let toml_str = r#"
[[filter_fields]]
name = "nsfwLevel"
field_type = "single_value"
"#;
        let config = Config::from_toml(toml_str).unwrap();
        assert!(config.filter_fields[0].behaviors.is_none());
    }

    #[test]
    fn test_validation_rejects_empty_deferred_alive_source_field() {
        let config = Config {
            deferred_alive: Some(DeferredAliveConfig {
                source_field: "".into(),
                ms_to_seconds: false,
            }),
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_deferred_alive_config_parsing() {
        let toml_str = r#"
[deferred_alive]
source_field = "publishedAtUnix"
ms_to_seconds = true
"#;
        let config = Config::from_toml(toml_str).unwrap();
        let da = config.deferred_alive.as_ref().unwrap();
        assert_eq!(da.source_field, "publishedAtUnix");
        assert!(da.ms_to_seconds);
    }

    #[test]
    fn test_validation_rejects_duplicate_bucket_names() {
        let config = Config {
            filter_fields: vec![FilterFieldConfig {
                name: "scheduledAt".into(),
                field_type: FilterFieldType::SingleValue,

                behaviors: Some(FieldBehaviors {
                    range_buckets: vec![
                        BucketConfig {
                            name: "24h".into(),
                            duration_secs: 86400,
                            refresh_interval_secs: 60,
                        },
                        BucketConfig {
                            name: "24h".into(),
                            duration_secs: 86400,
                            refresh_interval_secs: 60,
                        },
                    ],
                    sort_field: None,
                }),
                eviction: None,
                eager_load: false,
            }],
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_zero_duration_secs() {
        let config = Config {
            filter_fields: vec![FilterFieldConfig {
                name: "scheduledAt".into(),
                field_type: FilterFieldType::SingleValue,

                behaviors: Some(FieldBehaviors {
                    range_buckets: vec![BucketConfig {
                        name: "bad".into(),
                        duration_secs: 0,
                        refresh_interval_secs: 60,
                    }],
                    sort_field: None,
                }),
                eviction: None,
                eager_load: false,
            }],
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_zero_refresh_interval_secs() {
        let config = Config {
            filter_fields: vec![FilterFieldConfig {
                name: "scheduledAt".into(),
                field_type: FilterFieldType::SingleValue,

                behaviors: Some(FieldBehaviors {
                    range_buckets: vec![BucketConfig {
                        name: "bad".into(),
                        duration_secs: 86400,
                        refresh_interval_secs: 0,
                    }],
                    sort_field: None,
                }),
                eviction: None,
                eager_load: false,
            }],
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_field_behaviors_serde_roundtrip_toml() {
        let config = Config {
            filter_fields: vec![FilterFieldConfig {
                name: "scheduledAt".into(),
                field_type: FilterFieldType::SingleValue,

                behaviors: Some(FieldBehaviors {
                    range_buckets: vec![BucketConfig {
                        name: "7d".into(),
                        duration_secs: 604800,
                        refresh_interval_secs: 300,
                    }],
                    sort_field: None,
                }),
                eviction: None,
                eager_load: false,
            }],
            deferred_alive: Some(DeferredAliveConfig {
                source_field: "scheduledAt".into(),
                ms_to_seconds: false,
            }),
            ..Config::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let roundtrip = Config::from_toml(&toml_str).unwrap();
        let behaviors = roundtrip.filter_fields[0].behaviors.as_ref().unwrap();
        assert_eq!(behaviors.range_buckets[0].name, "7d");
        assert_eq!(behaviors.range_buckets[0].duration_secs, 604800);
        let da = roundtrip.deferred_alive.as_ref().unwrap();
        assert_eq!(da.source_field, "scheduledAt");
    }

    #[test]
    fn test_data_schema_default_version() {
        let schema = DataSchema::default();
        assert_eq!(schema.schema_version, 1);
    }

    #[test]
    fn test_data_schema_version_from_json() {
        let json = r#"{"id_field": "id", "schema_version": 3, "fields": []}"#;
        let schema: DataSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.schema_version, 3);
    }

    #[test]
    fn test_data_schema_version_defaults_to_1_in_json() {
        let json = r#"{"id_field": "id", "fields": []}"#;
        let schema: DataSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.schema_version, 1);
    }

    #[test]
    fn test_data_schema_validates_version_zero() {
        let schema = DataSchema {
            schema_version: 0,
            ..DataSchema::default()
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn test_data_schema_validates_version_one() {
        let schema = DataSchema::default();
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn test_config_patch_roundtrip() {
        // Build a config with known filter/sort fields
        let mut config = Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: crate::filter::FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: crate::filter::FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: true,
                },
            ],
            sort_fields: vec![SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: false,
            }],
            cache: CacheConfig {
                max_entries: 5_000,
                ..CacheConfig::default()
            },
            ..Config::default()
        };

        // Simulate a partial patch: flip eager_load on nsfwLevel, update cache
        for fc in config.filter_fields.iter_mut() {
            if fc.name == "nsfwLevel" {
                fc.eager_load = true;
            }
        }
        for sc in config.sort_fields.iter_mut() {
            if sc.name == "reactionCount" {
                sc.eager_load = true;
            }
        }
        config.cache.max_entries = 20_000;
        config.cache.bound_target_size = 5_000;

        // Serialize to JSON and deserialize back
        let json = serde_json::to_string_pretty(&config).unwrap();
        let restored: Config = serde_json::from_str(&json).unwrap();

        // Verify the patched values survived roundtrip
        assert!(restored.filter_fields.iter().find(|f| f.name == "nsfwLevel").unwrap().eager_load);
        assert!(restored.filter_fields.iter().find(|f| f.name == "tagIds").unwrap().eager_load);
        assert!(restored.sort_fields[0].eager_load);
        assert_eq!(restored.cache.max_entries, 20_000);
        assert_eq!(restored.cache.bound_target_size, 5_000);
        // Ensure other defaults are preserved
        assert_eq!(restored.cache.decay_rate, CacheConfig::default().decay_rate);
        assert!(restored.validate().is_ok());
    }
}
