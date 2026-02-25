//! Facade layer abstractions for protocol and connection lifecycle.

pub mod connection;
pub mod proactor;
pub mod protocol;

use dfly_common::config::RuntimeConfig;

/// Facade subsystem bootstrap module.
#[derive(Debug)]
pub struct FacadeModule {
    /// Main RESP listener port.
    pub redis_port: u16,
    /// Optional memcache listener port.
    pub memcached_port: Option<u16>,
    /// Number of active network I/O workers in runtime path.
    pub io_thread_count: u16,
}

impl FacadeModule {
    /// Builds the facade bootstrap model from process config.
    #[must_use]
    pub fn from_config(config: &RuntimeConfig) -> Self {
        // Runtime currently uses one reactor service loop as the active I/O worker domain.
        // ProactorPool remains available for focused facade tests but is not bootstrapped here.
        let io_thread_count = 1;
        Self {
            redis_port: config.redis_port,
            memcached_port: config.memcached_port,
            io_thread_count,
        }
    }
}
