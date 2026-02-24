//! Scheduler interfaces for transaction execution.

use std::collections::HashSet;

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
        for hop in &plan.hops {
            if hop.per_shard.is_empty() {
                return Err(DflyError::InvalidState(
                    "transaction hop must contain at least one command",
                ));
            }

            // A single hop models parallelizable work across shards, so each shard may
            // appear at most once inside the hop.
            let mut seen = HashSet::new();
            for (shard, _) in &hop.per_shard {
                if !seen.insert(*shard) {
                    return Err(DflyError::InvalidState(
                        "transaction hop contains duplicate shard",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{NoopTransactionScheduler, TransactionScheduler};
    use crate::plan::{TransactionHop, TransactionMode, TransactionPlan};
    use dfly_core::command::CommandFrame;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn scheduler_rejects_empty_hops() {
        let scheduler = NoopTransactionScheduler;
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::Global,
            hops: vec![TransactionHop {
                per_shard: Vec::new(),
            }],
        };

        let result = scheduler.schedule(&plan);
        assert_that!(result.is_err(), eq(true));
    }

    #[rstest]
    fn scheduler_rejects_duplicate_shards_in_one_hop() {
        let scheduler = NoopTransactionScheduler;
        let command = CommandFrame::new("PING", Vec::new());
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::Global,
            hops: vec![TransactionHop {
                per_shard: vec![(0, command.clone()), (0, command)],
            }],
        };

        let result = scheduler.schedule(&plan);
        assert_that!(result.is_err(), eq(true));
    }

    #[rstest]
    fn scheduler_accepts_well_formed_plan() {
        let scheduler = NoopTransactionScheduler;
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::Global,
            hops: vec![
                TransactionHop {
                    per_shard: vec![(0, CommandFrame::new("PING", Vec::new()))],
                },
                TransactionHop {
                    per_shard: vec![(1, CommandFrame::new("ECHO", vec![b"x".to_vec()]))],
                },
            ],
        };

        let result = scheduler.schedule(&plan);
        assert_that!(result.is_ok(), eq(true));
    }
}
