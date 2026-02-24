//! Scheduler interfaces for transaction execution.

use dfly_common::error::{DflyError, DflyResult};

use crate::plan::TransactionPlan;

/// Transaction scheduler abstraction.
pub trait TransactionScheduler: Send + Sync {
    /// Schedules a transaction for execution.
    ///
    /// # Errors
    ///
    /// Returns an error when the provided plan is invalid for scheduling
    /// or when runtime scheduling resources are unavailable.
    fn schedule(&self, plan: &TransactionPlan) -> DflyResult<()>;
}

/// Unit 0 scheduler placeholder.
#[derive(Debug, Default)]
pub struct NoopTransactionScheduler;

impl TransactionScheduler for NoopTransactionScheduler {
    fn schedule(&self, plan: &TransactionPlan) -> DflyResult<()> {
        if plan.hops.is_empty() {
            return Err(DflyError::InvalidState(
                "transaction plan must contain at least one hop",
            ));
        }
        Ok(())
    }
}
