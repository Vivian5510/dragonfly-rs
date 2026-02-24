//! Shard routing abstractions.

use dfly_cluster::slot::key_slot;
use dfly_common::ids::{ShardCount, ShardId};

/// Resolves key ownership to a shard id.
pub trait ShardResolver {
    /// Returns the owner shard for the given key bytes.
    fn shard_for_key(&self, key: &[u8]) -> ShardId;
}

/// Hash-tag aware shard resolver.
///
/// Dragonfly routes keys by Redis hash-slot semantics (`{...}` hashtag extraction + CRC16).
/// We mirror that contract and then map slot ownership to shard threads by modulo shard count.
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

    /// Returns configured shard count.
    #[must_use]
    pub fn shard_count(&self) -> ShardCount {
        self.shard_count
    }
}

impl ShardResolver for HashTagShardResolver {
    fn shard_for_key(&self, key: &[u8]) -> ShardId {
        let slot = key_slot(key);
        let shard = u32::from(slot) % u32::from(self.shard_count.get());
        match ShardId::try_from(shard) {
            Ok(shard_id) => shard_id,
            Err(_) => unreachable!("slot modulo shard_count always fits into u16"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HashTagShardResolver, ShardResolver};
    use dfly_cluster::slot::key_slot;
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

    #[rstest]
    fn resolver_uses_hashtag_semantics_for_shard_routing() {
        let resolver = HashTagShardResolver::new(ShardCount::new(8).expect("literal is valid"));

        let first = resolver.shard_for_key(b"user:{42}:name");
        let second = resolver.shard_for_key(b"user:{42}:email");
        assert_that!(first, eq(second));

        let third = resolver.shard_for_key(b"user:{84}:name");
        assert_that!(first, not(eq(third)));
    }

    #[rstest]
    fn resolver_matches_slot_modulo_shard_count_contract() {
        let resolver = HashTagShardResolver::new(ShardCount::new(16).expect("literal is valid"));
        let key = b"dragonfly:{alpha}:key";
        let slot = key_slot(key);
        let expected = slot % 16;

        let shard = resolver.shard_for_key(key);
        assert_that!(shard, eq(expected));
    }
}
