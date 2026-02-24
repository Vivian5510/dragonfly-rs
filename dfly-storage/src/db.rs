//! DB-slice style storage interfaces.

use dfly_common::error::DflyResult;
use dfly_common::ids::{DbIndex, ShardId};

use dfly_core::command::CommandFrame;

/// Logical keyspace slice owned by a shard.
pub trait DbSlice: Send + Sync {
    /// Returns owner shard id.
    fn shard_id(&self) -> ShardId;

    /// Applies one command into this shard-local slice.
    ///
    /// # Errors
    ///
    /// Returns an error when command application fails due to invalid DB index,
    /// unsupported operation, or storage runtime constraints.
    fn apply(&self, db: DbIndex, command: &CommandFrame) -> DflyResult<()>;
}
