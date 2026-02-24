//! Replication and journal interface layer.

pub mod journal;
pub mod state;

/// Replication subsystem bootstrap module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationModule {
    /// Whether replication is enabled at bootstrap.
    pub enabled: bool,
}

impl ReplicationModule {
    /// Creates the replication module bootstrap object.
    #[must_use]
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}
