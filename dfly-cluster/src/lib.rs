//! Cluster routing and migration interfaces.

pub mod migration;
pub mod slot;

use dfly_common::config::ClusterMode;

/// Cluster subsystem bootstrap module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterModule {
    /// Startup cluster mode.
    pub mode: ClusterMode,
}

impl ClusterModule {
    /// Builds a cluster module from startup mode.
    #[must_use]
    pub fn new(mode: ClusterMode) -> Self {
        Self { mode }
    }
}
