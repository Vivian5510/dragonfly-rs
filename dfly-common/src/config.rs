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
    /// Force using epoll backend even when `io_uring` probe succeeds.
    pub force_epoll: bool,
    /// Total number of connection I/O worker threads used by listener dispatch.
    ///
    /// A value of `0` means "auto", currently resolved from shard count.
    pub conn_io_threads: u16,
    /// Starting worker index for connection assignment.
    pub conn_io_thread_start: u16,
    /// Whether listener dispatch should first try peer-hash affinity before RR fallback.
    pub conn_use_peer_hash_affinity: bool,
    /// Whether controlled connection migration is enabled.
    pub migrate_connections: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            shard_count: ShardCount::new(4).expect("literal shard count must be non-zero"),
            redis_port: 6379,
            memcached_port: None,
            max_memory_bytes: 0,
            cluster_mode: ClusterMode::Disabled,
            force_epoll: false,
            conn_io_threads: 0,
            conn_io_thread_start: 0,
            conn_use_peer_hash_affinity: false,
            migrate_connections: false,
        }
    }
}
