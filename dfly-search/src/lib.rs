//! Search subsystem abstractions.

pub mod index;
pub mod query;

/// Search subsystem bootstrap module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchModule {
    /// Whether FT command family is enabled.
    pub enabled: bool,
}

impl SearchModule {
    /// Creates the search module bootstrap object.
    #[must_use]
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}
