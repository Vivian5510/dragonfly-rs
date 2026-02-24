//! Facade layer abstractions for protocol and connection lifecycle.

pub mod connection;
pub mod protocol;

use dfly_common::config::RuntimeConfig;

/// Facade subsystem bootstrap module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacadeModule {
    /// Main RESP listener port.
    pub redis_port: u16,
    /// Optional memcache listener port.
    pub memcached_port: Option<u16>,
}

impl FacadeModule {
    /// Builds the facade bootstrap model from process config.
    #[must_use]
    pub fn from_config(config: &RuntimeConfig) -> Self {
        Self {
            redis_port: config.redis_port,
            memcached_port: config.memcached_port,
        }
    }
}
