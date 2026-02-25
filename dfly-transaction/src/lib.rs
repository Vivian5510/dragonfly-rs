//! Transaction planning and scheduling interfaces.

pub mod plan;
pub mod scheduler;
pub mod session;

use dfly_common::ids::ShardCount;
use scheduler::InMemoryTransactionScheduler;

/// Transaction subsystem bootstrap module.
#[derive(Debug, Default)]
pub struct TransactionModule {
    /// Current scheduler implementation used by the server composition root.
    pub scheduler: InMemoryTransactionScheduler,
}

impl TransactionModule {
    /// Creates the transaction module with shard-scoped scheduler state.
    #[must_use]
    pub fn new(shard_count: ShardCount) -> Self {
        Self {
            scheduler: InMemoryTransactionScheduler::with_shard_count(shard_count),
        }
    }
}
