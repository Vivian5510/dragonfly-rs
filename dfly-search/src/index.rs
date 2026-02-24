//! Index schema placeholders for FT-like commands.

/// Field type used by search schema definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    /// Text token field.
    Text,
    /// Numeric field.
    Numeric,
    /// Tag/categorical field.
    Tag,
    /// Vector field.
    Vector,
    /// Geo field.
    Geo,
}

/// One schema field definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaField {
    /// Field name in source documents.
    pub name: String,
    /// Field semantic type.
    pub field_type: FieldType,
}
