//! Facade layer abstractions for protocol and connection lifecycle.

pub mod connection;
pub mod proactor;
pub mod protocol;

use dfly_common::config::RuntimeConfig;
use proactor::ProactorPool;

/// Facade subsystem bootstrap module.
#[derive(Debug)]
pub struct FacadeModule {
    /// Main RESP listener port.
    pub redis_port: u16,
    /// Optional memcache listener port.
    pub memcached_port: Option<u16>,
    /// Number of active I/O proactor workers.
    pub io_thread_count: u16,
    /// Shared proactor pool used by connection ingress.
    pub proactor_pool: ProactorPool,
}

impl FacadeModule {
    /// Builds the facade bootstrap model from process config.
    #[must_use]
    pub fn from_config(config: &RuntimeConfig) -> Self {
        // Dragonfly keeps proactor and shard domains separately configurable.
        // This learning port keeps them aligned one-to-one for deterministic topology.
        let io_thread_count = config.shard_count.get().max(1);
        Self {
            redis_port: config.redis_port,
            memcached_port: config.memcached_port,
            io_thread_count,
            proactor_pool: ProactorPool::new(io_thread_count),
        }
    }
}
