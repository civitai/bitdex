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

    #[error("docstore error: {0}")]
    DocStore(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
