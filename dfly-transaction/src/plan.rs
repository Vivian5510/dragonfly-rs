//! Transaction plan model.

use dfly_common::ids::{ShardId, TxId};

use dfly_core::command::CommandFrame;

/// Execution mode aligned with Dragonfly's high-level transaction modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Full global barrier mode.
    Global,
    /// Lock-ahead mode over known keys.
    LockAhead,
    /// Non-atomic mode for throughput-oriented scenarios.
    NonAtomic,
}

/// A single execution hop inside a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionHop {
    /// Commands grouped by destination shard for this hop.
    pub per_shard: Vec<(ShardId, CommandFrame)>,
}

/// Full transaction execution plan prepared by the coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionPlan {
    /// Monotonic transaction id.
    pub txid: TxId,
    /// Execution mode chosen for this transaction.
    pub mode: TransactionMode,
    /// Ordered hop sequence.
    pub hops: Vec<TransactionHop>,
    /// Full shard reservation footprint for this transaction.
    ///
    /// This set may be wider than `hops[*].per_shard[*].0` because some commands
    /// fan out at runtime (for example `MSET` on keys belonging to different shards).
    /// Scheduler queue reservation must use this full footprint to preserve isolation.
    pub touched_shards: Vec<ShardId>,
}
