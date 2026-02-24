//! Transaction planning and scheduling interfaces.

pub mod plan;
pub mod scheduler;

use scheduler::NoopTransactionScheduler;

/// Transaction subsystem bootstrap module.
#[derive(Debug, Default)]
pub struct TransactionModule {
    /// Current scheduler implementation used by the server composition root.
    pub scheduler: NoopTransactionScheduler,
}

impl TransactionModule {
    /// Creates the Unit 0 transaction module with a no-op scheduler placeholder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            scheduler: NoopTransactionScheduler,
        }
    }
}
