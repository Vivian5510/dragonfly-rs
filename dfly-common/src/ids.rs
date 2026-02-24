//! Canonical identifier types used across runtime, storage, and replication.

/// Numeric shard identifier inside a single process.
pub type ShardId = u16;

/// Logical database index (Redis `SELECT` compatible model).
pub type DbIndex = u16;

/// Monotonic transaction identifier allocated by coordinators.
pub type TxId = u64;

/// Redis cluster slot identifier.
pub type SlotId = u16;

/// Upper bound of Redis-compatible hash slot space.
pub const MAX_SLOT_ID: SlotId = 0x3FFF;

/// Strongly typed shard-count wrapper to avoid passing raw integers around runtime APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardCount(u16);

impl ShardCount {
    /// Creates a validated shard-count value.
    ///
    /// Returns `None` for zero because a Dragonfly-style runtime always requires at least
    /// one owning shard.
    #[must_use]
    pub fn new(value: u16) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the inner count.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::ShardCount;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn shard_count_rejects_zero() {
        assert_that!(ShardCount::new(0), eq(None));
    }

    #[rstest]
    #[case(1)]
    #[case(8)]
    #[case(u16::MAX)]
    fn shard_count_accepts_positive_values(#[case] input: u16) {
        let count = ShardCount::new(input).expect("positive count must be valid");
        assert_that!(count.get(), eq(input));
    }
}
