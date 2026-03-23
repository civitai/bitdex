//! FieldHandler — pluggable field type registry for ShardStore document ops.
//!
//! Each field type (Scalar, MultiValue, Boolean) gets a handler that validates
//! operations and applies them to field values. Adding a new field type =
//! implement the trait, register it.
//!
//! The ops log doesn't care about field types — it stores bytes. The handler
//! interprets them. Validation happens before the op hits the log, and apply
//! happens on read (when reconstructing from ops).

use std::collections::HashMap;
use std::sync::Arc;

use crate::docstore::PackedValue;
use crate::shard_store_doc::DocOp;

// ---------------------------------------------------------------------------
// FieldHandler trait
// ---------------------------------------------------------------------------

/// The set of operation kinds a field handler supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Set,
    Append,
    Remove,
    Delete,
    Create,
}

/// Handles validation and application of ops for a specific field type.
///
/// Implementations: `ScalarHandler`, `MultiValueHandler`, `BooleanHandler`.
/// Adding a new field type = implement this trait and register it.
pub trait FieldHandler: Send + Sync {
    /// Which op kinds this handler accepts.
    fn valid_ops(&self) -> &[OpKind];

    /// Check if an op is valid for this field type.
    /// Returns an error message if invalid, None if ok.
    fn validate_op(&self, op_kind: OpKind, value: Option<&PackedValue>) -> Option<String>;

    /// The default value for this field type (used when field is absent).
    fn default_value(&self) -> PackedValue;

    /// A human-readable name for this field type.
    fn type_name(&self) -> &str;
}

// ---------------------------------------------------------------------------
// ScalarHandler — Integer, Float, String scalars
// ---------------------------------------------------------------------------

/// Handles scalar fields (Integer, Float, String).
/// Valid ops: Set only. Append/Remove are rejected.
pub struct ScalarHandler {
    default: PackedValue,
}

impl ScalarHandler {
    pub fn new(default: PackedValue) -> Self {
        ScalarHandler { default }
    }

    pub fn integer(default: i64) -> Self {
        ScalarHandler { default: PackedValue::I(default) }
    }

    pub fn string(default: String) -> Self {
        ScalarHandler { default: PackedValue::S(default) }
    }

    pub fn float(default: f64) -> Self {
        ScalarHandler { default: PackedValue::F(default) }
    }
}

impl FieldHandler for ScalarHandler {
    fn valid_ops(&self) -> &[OpKind] {
        &[OpKind::Set, OpKind::Delete, OpKind::Create]
    }

    fn validate_op(&self, op_kind: OpKind, _value: Option<&PackedValue>) -> Option<String> {
        match op_kind {
            OpKind::Set | OpKind::Delete | OpKind::Create => None,
            OpKind::Append => Some("cannot Append to a scalar field".into()),
            OpKind::Remove => Some("cannot Remove from a scalar field".into()),
        }
    }

    fn default_value(&self) -> PackedValue {
        self.default.clone()
    }

    fn type_name(&self) -> &str {
        "scalar"
    }
}

// ---------------------------------------------------------------------------
// MultiValueHandler — Integer arrays, mixed arrays
// ---------------------------------------------------------------------------

/// Handles multi-value fields (tags, model versions, etc.).
/// Valid ops: Set, Append, Remove.
pub struct MultiValueHandler {
    default: PackedValue,
}

impl MultiValueHandler {
    pub fn new(default: PackedValue) -> Self {
        MultiValueHandler { default }
    }

    pub fn integer_array() -> Self {
        MultiValueHandler { default: PackedValue::Mi(Vec::new()) }
    }

    pub fn mixed_array() -> Self {
        MultiValueHandler { default: PackedValue::Mm(Vec::new()) }
    }
}

impl FieldHandler for MultiValueHandler {
    fn valid_ops(&self) -> &[OpKind] {
        &[OpKind::Set, OpKind::Append, OpKind::Remove, OpKind::Delete, OpKind::Create]
    }

    fn validate_op(&self, _op_kind: OpKind, _value: Option<&PackedValue>) -> Option<String> {
        // All ops are valid for multi-value fields
        None
    }

    fn default_value(&self) -> PackedValue {
        self.default.clone()
    }

    fn type_name(&self) -> &str {
        "multi_value"
    }
}

// ---------------------------------------------------------------------------
// BooleanHandler — true/false fields
// ---------------------------------------------------------------------------

/// Handles boolean fields.
/// Valid ops: Set only. Append/Remove are rejected.
pub struct BooleanHandler {
    default: bool,
}

impl BooleanHandler {
    pub fn new(default: bool) -> Self {
        BooleanHandler { default }
    }
}

impl FieldHandler for BooleanHandler {
    fn valid_ops(&self) -> &[OpKind] {
        &[OpKind::Set, OpKind::Delete, OpKind::Create]
    }

    fn validate_op(&self, op_kind: OpKind, value: Option<&PackedValue>) -> Option<String> {
        match op_kind {
            OpKind::Set => {
                if let Some(pv) = value {
                    match pv {
                        PackedValue::B(_) => None,
                        _ => Some(format!("boolean field requires B value, got {:?}", pv)),
                    }
                } else {
                    None
                }
            }
            OpKind::Delete | OpKind::Create => None,
            OpKind::Append => Some("cannot Append to a boolean field".into()),
            OpKind::Remove => Some("cannot Remove from a boolean field".into()),
        }
    }

    fn default_value(&self) -> PackedValue {
        PackedValue::B(self.default)
    }

    fn type_name(&self) -> &str {
        "boolean"
    }
}

// ---------------------------------------------------------------------------
// FieldRegistry — maps field_idx to handler
// ---------------------------------------------------------------------------

/// Registry mapping field indices to their handlers.
///
/// Built from the schema at startup. Used by the doc write path to validate
/// ops before they hit the log, and by the read path to apply defaults.
pub struct FieldRegistry {
    handlers: HashMap<u16, Arc<dyn FieldHandler>>,
    names: HashMap<u16, String>,
}

impl FieldRegistry {
    pub fn new() -> Self {
        FieldRegistry {
            handlers: HashMap::new(),
            names: HashMap::new(),
        }
    }

    /// Register a field with its handler.
    pub fn register(&mut self, field_idx: u16, name: String, handler: Arc<dyn FieldHandler>) {
        self.handlers.insert(field_idx, handler);
        self.names.insert(field_idx, name);
    }

    /// Get the handler for a field.
    pub fn handler(&self, field_idx: u16) -> Option<&dyn FieldHandler> {
        self.handlers.get(&field_idx).map(|h| h.as_ref())
    }

    /// Get the field name by index.
    pub fn field_name(&self, field_idx: u16) -> Option<&str> {
        self.names.get(&field_idx).map(|s| s.as_str())
    }

    /// Validate a doc op against the registry.
    /// Returns None if valid, Some(error_message) if invalid.
    pub fn validate_op(&self, op: &DocOp) -> Option<String> {
        match op {
            DocOp::Set { field, value, .. } => {
                if let Some(handler) = self.handler(*field) {
                    handler.validate_op(OpKind::Set, Some(value))
                } else {
                    None // Unknown fields pass validation (extensible schema)
                }
            }
            DocOp::Append { field, value, .. } => {
                if let Some(handler) = self.handler(*field) {
                    handler.validate_op(OpKind::Append, Some(value))
                } else {
                    None
                }
            }
            DocOp::Remove { field, value, .. } => {
                if let Some(handler) = self.handler(*field) {
                    handler.validate_op(OpKind::Remove, Some(value))
                } else {
                    None
                }
            }
            DocOp::Delete { .. } => None, // Always valid
            DocOp::Create { .. } => None, // Always valid
        }
    }

    /// Number of registered fields.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// Get default values for all registered fields.
    pub fn defaults(&self) -> Vec<(u16, PackedValue)> {
        self.handlers.iter().map(|(&idx, handler)| {
            (idx, handler.default_value())
        }).collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_handler_validates_set() {
        let h = ScalarHandler::integer(0);
        assert!(h.validate_op(OpKind::Set, Some(&PackedValue::I(42))).is_none());
        assert!(h.validate_op(OpKind::Append, Some(&PackedValue::I(42))).is_some());
        assert!(h.validate_op(OpKind::Remove, Some(&PackedValue::I(42))).is_some());
    }

    #[test]
    fn test_multi_value_handler_validates_all() {
        let h = MultiValueHandler::integer_array();
        assert!(h.validate_op(OpKind::Set, None).is_none());
        assert!(h.validate_op(OpKind::Append, Some(&PackedValue::I(1))).is_none());
        assert!(h.validate_op(OpKind::Remove, Some(&PackedValue::I(1))).is_none());
    }

    #[test]
    fn test_boolean_handler_validates_type() {
        let h = BooleanHandler::new(false);
        assert!(h.validate_op(OpKind::Set, Some(&PackedValue::B(true))).is_none());
        assert!(h.validate_op(OpKind::Set, Some(&PackedValue::I(1))).is_some());
        assert!(h.validate_op(OpKind::Append, None).is_some());
    }

    #[test]
    fn test_registry_validate_op() {
        let mut reg = FieldRegistry::new();
        reg.register(0, "nsfwLevel".into(), Arc::new(ScalarHandler::integer(0)));
        reg.register(1, "tagIds".into(), Arc::new(MultiValueHandler::integer_array()));
        reg.register(2, "poi".into(), Arc::new(BooleanHandler::new(false)));

        // Valid: Set scalar
        assert!(reg.validate_op(&DocOp::Set {
            slot: 1, field: 0, value: PackedValue::I(2)
        }).is_none());

        // Valid: Append to multi-value
        assert!(reg.validate_op(&DocOp::Append {
            slot: 1, field: 1, value: PackedValue::I(42)
        }).is_none());

        // Invalid: Append to scalar
        assert!(reg.validate_op(&DocOp::Append {
            slot: 1, field: 0, value: PackedValue::I(42)
        }).is_some());

        // Invalid: Set integer on boolean field
        assert!(reg.validate_op(&DocOp::Set {
            slot: 1, field: 2, value: PackedValue::I(1)
        }).is_some());

        // Valid: Set boolean on boolean field
        assert!(reg.validate_op(&DocOp::Set {
            slot: 1, field: 2, value: PackedValue::B(true)
        }).is_none());

        // Unknown field passes (extensible schema)
        assert!(reg.validate_op(&DocOp::Set {
            slot: 1, field: 99, value: PackedValue::I(1)
        }).is_none());
    }

    #[test]
    fn test_registry_defaults() {
        let mut reg = FieldRegistry::new();
        reg.register(0, "nsfwLevel".into(), Arc::new(ScalarHandler::integer(1)));
        reg.register(1, "tagIds".into(), Arc::new(MultiValueHandler::integer_array()));
        reg.register(2, "poi".into(), Arc::new(BooleanHandler::new(false)));

        let defaults = reg.defaults();
        assert_eq!(defaults.len(), 3);
    }

    #[test]
    fn test_registry_field_name() {
        let mut reg = FieldRegistry::new();
        reg.register(0, "nsfwLevel".into(), Arc::new(ScalarHandler::integer(0)));

        assert_eq!(reg.field_name(0), Some("nsfwLevel"));
        assert_eq!(reg.field_name(99), None);
    }

    #[test]
    fn test_handler_type_names() {
        assert_eq!(ScalarHandler::integer(0).type_name(), "scalar");
        assert_eq!(MultiValueHandler::integer_array().type_name(), "multi_value");
        assert_eq!(BooleanHandler::new(false).type_name(), "boolean");
    }
}
