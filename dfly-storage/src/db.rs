//! DB-slice style storage interfaces.

use dfly_common::error::DflyResult;
use dfly_common::ids::{DbIndex, ShardId};

use dfly_core::command::CommandFrame;

/// Logical keyspace slice owned by a shard.
pub trait DbSlice: Send + Sync {
    /// Returns owner shard id.
    fn shard_id(&self) -> ShardId;

    /// Applies one command into this shard-local slice.
    fn apply(&self, db: DbIndex, command: &CommandFrame) -> DflyResult<()>;
}
