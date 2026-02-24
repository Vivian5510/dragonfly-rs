//! Transaction planning and scheduling interfaces.

pub mod plan;
pub mod scheduler;
pub mod session;

use scheduler::InMemoryTransactionScheduler;

/// Transaction subsystem bootstrap module.
#[derive(Debug, Default)]
pub struct TransactionModule {
    /// Current scheduler implementation used by the server composition root.
    pub scheduler: InMemoryTransactionScheduler,
}

impl TransactionModule {
    /// Creates the transaction module with an in-memory scheduler.
    #[must_use]
    pub fn new() -> Self {
        Self {
            scheduler: InMemoryTransactionScheduler::default(),
        }
    }
}
