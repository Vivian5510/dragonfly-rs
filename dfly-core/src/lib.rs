//! Core runtime abstractions shared by transaction, storage, and facade layers.

pub mod command;
pub mod runtime;
pub mod sharding;

use dfly_common::ids::ShardCount;
use sharding::HashTagShardResolver;

/// Core module bootstrap object.
///
/// In Unit 0 this struct only wires shard resolver policy. Later units add process-wide registries
/// and cross-shard execution orchestration.
#[derive(Debug, Clone)]
pub struct CoreModule {
    /// Resolver used to map keys into owning shards.
    pub resolver: HashTagShardResolver,
}

impl CoreModule {
    /// Creates the core bootstrap module from process config.
    #[must_use]
    pub fn new(shard_count: ShardCount) -> Self {
        Self {
            resolver: HashTagShardResolver::new(shard_count),
        }
    }
}
