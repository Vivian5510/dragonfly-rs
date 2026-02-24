//! Tiered storage policy interfaces.

pub mod cooling;
pub mod policy;

/// Tiering subsystem bootstrap module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TieringModule {
    /// Whether tiered storage is enabled.
    pub enabled: bool,
}

impl TieringModule {
    /// Creates tiering module bootstrap object.
    #[must_use]
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}
