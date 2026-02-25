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
        // Reactor runtime maps accepted sockets across a fixed I/O worker set.
        let io_thread_count = config.shard_count.get().max(1);
        Self {
            redis_port: config.redis_port,
            memcached_port: config.memcached_port,
            io_thread_count,
        }
    }
}
