//! Search query placeholders.

/// Parsed query descriptor placeholder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQuery {
    /// Index name targeted by this query.
    pub index: String,
    /// Query expression.
    pub expr: String,
}
