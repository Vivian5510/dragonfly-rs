//! Scheduler interfaces for transaction execution.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;

use dfly_common::error::{DflyError, DflyResult};
use dfly_common::ids::{ShardId, TxId};

use crate::plan::{TransactionMode, TransactionPlan};

/// Transaction scheduler abstraction.
pub trait TransactionScheduler: Send + Sync {
    /// Schedules a transaction for execution.
    ///
    /// # Errors
    ///
    /// Returns an error when the provided plan is invalid for scheduling
    /// or when runtime scheduling resources are unavailable.
    fn schedule(&self, plan: &TransactionPlan) -> DflyResult<()>;

    /// Concludes one transaction and releases all scheduling resources.
    ///
    /// # Errors
    ///
    /// Returns an error when scheduler state cannot be accessed.
    fn conclude(&self, txid: TxId) -> DflyResult<()> {
        let _ = txid;
        Ok(())
    }
}

/// In-memory scheduler with minimal Dragonfly-like queue/lock lifecycle.
///
/// The model intentionally mirrors two core runtime ideas:
/// 1. Each shard has one pending transaction queue.
/// 2. Key locks are kept for the lifetime of one active transaction.
///
/// The current learning implementation executes transactions synchronously right after scheduling,
/// so we allow only one active transaction per shard queue head at a time.
#[derive(Debug, Default)]
pub struct InMemoryTransactionScheduler {
    state: Mutex<SchedulerState>,
}

#[derive(Debug, Default)]
struct SchedulerState {
    /// Pending queue per shard, ordered by scheduling time.
    shard_queues: HashMap<ShardId, VecDeque<TxId>>,
    /// Exclusive key owner map `(shard, key) -> txid`.
    key_locks: HashMap<(ShardId, Vec<u8>), TxId>,
    /// Lease descriptors for active transactions.
    active: HashMap<TxId, ActiveLease>,
}

#[derive(Debug, Default)]
struct ActiveLease {
    /// Shards reserved by this transaction in scheduling queues.
    reserved_shards: Vec<ShardId>,
    /// Key locks held by this transaction.
    locked_keys: Vec<(ShardId, Vec<u8>)>,
}

impl TransactionScheduler for InMemoryTransactionScheduler {
    fn schedule(&self, plan: &TransactionPlan) -> DflyResult<()> {
        validate_plan_shape(plan)?;

        // Non-atomic mode is intentionally lock-free: commands are executed as independent reads.
        if plan.mode == TransactionMode::NonAtomic {
            return Ok(());
        }

        let mut touched_shards = HashSet::new();
        let mut touched_keys = HashSet::new();
        for hop in &plan.hops {
            for (shard, command) in &hop.per_shard {
                let _ = touched_shards.insert(*shard);
                if let Some(primary_key) = command.args.first() {
                    let _ = touched_keys.insert((*shard, primary_key.clone()));
                }
            }
        }

        let mut shards = touched_shards.into_iter().collect::<Vec<_>>();
        shards.sort_unstable();

        let mut keys = touched_keys.into_iter().collect::<Vec<_>>();
        keys.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));

        let mut state = self
            .state
            .lock()
            .map_err(|_| DflyError::InvalidState("transaction scheduler mutex is poisoned"))?;

        if state.active.contains_key(&plan.txid) {
            return Err(DflyError::InvalidState(
                "transaction txid is already active",
            ));
        }

        let mut reserved_shards = Vec::new();
        for shard in &shards {
            let queue = state.shard_queues.entry(*shard).or_default();

            // Unit-level model: a queued transaction is considered "active". If queue is non-empty,
            // next transaction must retry later instead of running immediately.
            if !queue.is_empty() {
                rollback_leases(&mut state, plan.txid, &reserved_shards, &[]);
                return Err(DflyError::InvalidState("shard queue is busy"));
            }

            queue.push_back(plan.txid);
            reserved_shards.push(*shard);
        }

        let mut locked_keys = Vec::new();
        for key_id in &keys {
            match state.key_locks.entry(key_id.clone()) {
                Entry::Vacant(slot) => {
                    slot.insert(plan.txid);
                    locked_keys.push(key_id.clone());
                }
                Entry::Occupied(owner) => {
                    if *owner.get() != plan.txid {
                        rollback_leases(&mut state, plan.txid, &reserved_shards, &locked_keys);
                        return Err(DflyError::InvalidState("key lock is busy"));
                    }
                }
            }
        }

        let _ = state.active.insert(
            plan.txid,
            ActiveLease {
                reserved_shards,
                locked_keys,
            },
        );
        Ok(())
    }

    fn conclude(&self, txid: TxId) -> DflyResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DflyError::InvalidState("transaction scheduler mutex is poisoned"))?;

        let Some(lease) = state.active.remove(&txid) else {
            // Non-atomic plans keep no scheduler lease, so this path is intentionally a no-op.
            return Ok(());
        };

        rollback_leases(&mut state, txid, &lease.reserved_shards, &lease.locked_keys);
        Ok(())
    }
}

fn validate_plan_shape(plan: &TransactionPlan) -> DflyResult<()> {
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

fn rollback_leases(
    state: &mut SchedulerState,
    txid: TxId,
    reserved_shards: &[ShardId],
    locked_keys: &[(ShardId, Vec<u8>)],
) {
    for key_id in locked_keys {
        if state
            .key_locks
            .get(key_id)
            .is_some_and(|owner| *owner == txid)
        {
            let _ = state.key_locks.remove(key_id);
        }
    }

    for shard in reserved_shards {
        let mut should_remove_queue = false;
        if let Some(queue) = state.shard_queues.get_mut(shard) {
            if let Some(position) = queue.iter().position(|queued_txid| *queued_txid == txid) {
                let _ = queue.remove(position);
            }
            should_remove_queue = queue.is_empty();
        }
        if should_remove_queue {
            let _ = state.shard_queues.remove(shard);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryTransactionScheduler, TransactionScheduler};
    use crate::plan::{TransactionHop, TransactionMode, TransactionPlan};
    use dfly_core::command::CommandFrame;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn scheduler_rejects_empty_hops() {
        let scheduler = InMemoryTransactionScheduler::default();
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
        let scheduler = InMemoryTransactionScheduler::default();
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
        let scheduler = InMemoryTransactionScheduler::default();
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

    #[rstest]
    fn scheduler_blocks_busy_shard_until_transaction_concludes() {
        let scheduler = InMemoryTransactionScheduler::default();
        let first = TransactionPlan {
            txid: 10,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new("SET", vec![b"k".to_vec(), b"1".to_vec()]),
                )],
            }],
        };
        let second = TransactionPlan {
            txid: 11,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new("SET", vec![b"k".to_vec(), b"2".to_vec()]),
                )],
            }],
        };

        assert_that!(scheduler.schedule(&first).is_ok(), eq(true));
        assert_that!(scheduler.schedule(&second).is_err(), eq(true));

        assert_that!(scheduler.conclude(first.txid).is_ok(), eq(true));
        assert_that!(scheduler.schedule(&second).is_ok(), eq(true));
    }

    #[rstest]
    fn scheduler_conclude_is_noop_for_unknown_txid() {
        let scheduler = InMemoryTransactionScheduler::default();
        assert_that!(scheduler.conclude(999).is_ok(), eq(true));
    }
}
