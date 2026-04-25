use thiserror::Error;

pub type Result<T> = std::result::Result<T, BitdexError>;

#[derive(Debug, Error)]
pub enum BitdexError {
    #[error("slot not found: {0}")]
    SlotNotFound(u32),

    #[error("field not found: {0}")]
    FieldNotFound(String),

    #[error("invalid value for field '{field}': {reason}")]
    InvalidValue { field: String, reason: String },

    #[error("schema error: {0}")]
    Schema(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("query parse error: {0}")]
    QueryParse(String),

    #[error("capacity exceeded: {0}")]
    CapacityExceeded(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Returned when a range scan exceeds the configured value-count cap.
    /// field: the filter field name.
    /// field_loaded_values: number of distinct values currently loaded in memory
    ///   for the field (may be less than total on-disk for per_value_lazy fields).
    /// cap: the configured limit (FilterFieldConfig::max_range_scan_values).
    #[error(
        "query too expensive: range scan on '{field}' has {field_loaded_values} loaded values \
         (cap: {cap}). Set max_range_scan_values in config to adjust or disable."
    )]
    QueryTooExpensive {
        field: String,
        /// In-memory loaded distinct value count at query time.
        /// Best-effort for per_value_lazy fields; exact for eager-loaded fields.
        field_loaded_values: usize,
        cap: usize,
    },
}
