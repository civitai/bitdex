use ahash::AHashMap as HashMap;
use std::sync::Arc;

use crate::query::{BitdexQuery, ParseError, QueryParser};

/// Registry of query parsers keyed by format name.
///
/// The server uses this to route incoming queries to the correct parser based on
/// a `?format=` query parameter or header. Default format is "bitdex" (the original
/// JSON query format).
pub struct ParserRegistry {
    parsers: HashMap<String, Arc<dyn QueryParser>>,
    default_format: String,
}

impl ParserRegistry {
    pub fn new() -> Self {
        Self {
            parsers: HashMap::new(),
            default_format: "bitdex".to_string(),
        }
    }

    /// Register a parser for the given format name (e.g. "bitdex", "meilisearch", "compact").
    pub fn register(&mut self, format: impl Into<String>, parser: impl QueryParser + 'static) {
        self.parsers.insert(format.into(), Arc::new(parser));
    }

    /// Set which format is used when no format is specified.
    pub fn set_default(&mut self, format: impl Into<String>) {
        self.default_format = format.into();
    }

    /// Parse input using the named format, falling back to the default.
    pub fn parse(&self, format: Option<&str>, input: &[u8]) -> Result<BitdexQuery, ParseError> {
        let name = format.unwrap_or(&self.default_format);
        let parser = self
            .parsers
            .get(name)
            .ok_or_else(|| ParseError::new(format!("unknown query format '{name}'")))?;
        parser.parse(input)
    }

    /// List all registered format names.
    pub fn formats(&self) -> Vec<&str> {
        self.parsers.keys().map(|s| s.as_str()).collect()
    }

    /// Get the default format name.
    pub fn default_format(&self) -> &str {
        &self.default_format
    }
}

/// Build a registry with all built-in parsers.
pub fn default_registry() -> ParserRegistry {
    let mut registry = ParserRegistry::new();
    registry.register("bitdex", super::json::JsonQueryParser);
    registry.register("compact", super::compact::CompactQueryParser);
    registry.register("meilisearch", super::meilisearch::MeilisearchQueryParser);
    registry.set_default("bitdex");
    registry
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::json::JsonQueryParser;

    #[test]
    fn test_registry_basic() {
        let mut registry = ParserRegistry::new();
        registry.register("bitdex", JsonQueryParser);

        let q = registry
            .parse(Some("bitdex"), br#"{"limit": 10}"#)
            .unwrap();
        assert_eq!(q.limit, 10);
    }

    #[test]
    fn test_registry_default() {
        let mut registry = ParserRegistry::new();
        registry.register("bitdex", JsonQueryParser);
        registry.set_default("bitdex");

        let q = registry.parse(None, br#"{"limit": 20}"#).unwrap();
        assert_eq!(q.limit, 20);
    }

    #[test]
    fn test_registry_unknown_format() {
        let registry = ParserRegistry::new();
        let err = registry.parse(Some("unknown"), b"{}").unwrap_err();
        assert!(err.message.contains("unknown query format"));
    }

    #[test]
    fn test_default_registry_has_all_formats() {
        let registry = default_registry();
        let mut formats = registry.formats();
        formats.sort();
        assert!(formats.contains(&"bitdex"));
        assert!(formats.contains(&"compact"));
        assert!(formats.contains(&"meilisearch"));
    }
}
