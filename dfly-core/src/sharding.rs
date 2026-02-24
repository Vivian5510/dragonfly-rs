//! Shard routing abstractions.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use dfly_common::ids::{ShardCount, ShardId};

/// Resolves key ownership to a shard id.
pub trait ShardResolver {
    /// Returns the owner shard for the given key bytes.
    fn shard_for_key(&self, key: &[u8]) -> ShardId;
}

/// Hash-tag aware shard resolver placeholder.
///
/// Dragonfly's C++ code supports both cluster-slot and lock-tag based sharding behavior.
/// Unit 0 only installs a deterministic hash-based resolver. Unit 2+ extends this with
/// lock-tag extraction and cluster-slot modes.
#[derive(Debug, Clone)]
pub struct HashTagShardResolver {
    shard_count: ShardCount,
}

impl HashTagShardResolver {
    /// Builds a resolver for a specific shard count.
    #[must_use]
    pub fn new(shard_count: ShardCount) -> Self {
        Self { shard_count }
    }
}

impl ShardResolver for HashTagShardResolver {
    fn shard_for_key(&self, key: &[u8]) -> ShardId {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let shard = hasher.finish() % u64::from(self.shard_count.get());
        match ShardId::try_from(shard) {
            Ok(shard_id) => shard_id,
            Err(_) => unreachable!("modulo shard_count ensures shard id fits into u16"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HashTagShardResolver, ShardResolver};
    use dfly_common::ids::ShardCount;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    #[case(b"hello".as_slice(), 4)]
    #[case(b"dragonfly-key".as_slice(), 8)]
    #[case(b"another-key".as_slice(), 16)]
    fn resolver_returns_valid_shard_range(#[case] key: &[u8], #[case] shard_count: u16) {
        let resolver =
            HashTagShardResolver::new(ShardCount::new(shard_count).expect("literal is valid"));
        let shard = resolver.shard_for_key(key);
        assert_that!(shard < shard_count, eq(true));
    }

    #[rstest]
    #[case(b"dragonfly-key".as_slice())]
    #[case(b"same-key".as_slice())]
    fn resolver_is_deterministic_for_same_input(#[case] key: &[u8]) {
        let resolver = HashTagShardResolver::new(ShardCount::new(4).expect("literal is valid"));
        let first = resolver.shard_for_key(key);
        let second = resolver.shard_for_key(key);
        assert_that!(first, eq(second));
    }
}
