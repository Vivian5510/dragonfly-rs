//! Runtime configuration shared by module bootstrap code.

use crate::ids::ShardCount;

/// Cluster operating mode aligned with Dragonfly's flag-level behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMode {
    /// Cluster disabled.
    Disabled,
    /// Emulated cluster mode enabled.
    Emulated,
    /// Real cluster mode enabled.
    Real,
}

/// Bootstrap configuration used by `dfly-server` during process startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Number of shard owners used by shared-nothing execution.
    pub shard_count: ShardCount,
    /// Main RESP listener port.
    pub redis_port: u16,
    /// Optional memcache text protocol port.
    pub memcached_port: Option<u16>,
    /// Max memory budget in bytes.
    pub max_memory_bytes: u64,
    /// Cluster mode for routing/slot behavior.
    pub cluster_mode: ClusterMode,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            shard_count: ShardCount::new(4).expect("literal shard count must be non-zero"),
            redis_port: 6379,
            memcached_port: None,
            max_memory_bytes: 0,
            cluster_mode: ClusterMode::Disabled,
        }
    }
}
