//! Scheduler interfaces for transaction execution.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use dfly_common::error::{DflyError, DflyResult};
use dfly_common::ids::{ShardId, TxId};
use dfly_core::{SingleKeyAccess, classify_single_key_access};

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

    /// Ensures the provided shards are not currently held by any pending transaction.
    ///
    /// Direct command execution uses this hook to avoid racing with in-flight transactions
    /// that already reserved the same shards. The default implementation is a no-op so
    /// schedulers don't have to implement it when they do not carry shard ownership state.
    ///
    /// # Errors
    ///
    /// Returns an error when scheduler implementation can prove one or more shards are
    /// currently busy and cannot accept direct command execution.
    fn ensure_shards_available(&self, _shards: &[ShardId]) -> DflyResult<()> {
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
#[derive(Debug)]
pub struct InMemoryTransactionScheduler {
    shard_queues: Mutex<HashMap<ShardId, Arc<Mutex<VecDeque<TxId>>>>>,
    key_locks: Mutex<KeyLockMap>,
    active: Mutex<ActiveLeaseMap>,
}

type KeyId = (ShardId, Vec<u8>);
type KeyAccessList = Vec<(KeyId, SingleKeyAccess)>;

type KeyLockMap = HashMap<KeyId, KeyLockState>;
type ActiveLeaseMap = HashMap<TxId, ActiveLease>;

#[derive(Debug, Default)]
struct KeyLockState {
    /// Shared readers currently holding the key.
    readers: HashSet<TxId>,
    /// Exclusive writer currently holding the key.
    writer: Option<TxId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyLease {
    key_id: KeyId,
    access: SingleKeyAccess,
}

#[derive(Debug, Default)]
struct ActiveLease {
    /// Shards reserved by this transaction in scheduling queues.
    reserved_shards: Vec<ShardId>,
    /// Key locks held by this transaction.
    key_leases: Vec<KeyLease>,
}

impl Default for InMemoryTransactionScheduler {
    fn default() -> Self {
        Self {
            shard_queues: Mutex::new(HashMap::new()),
            key_locks: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
        }
    }
}

impl TransactionScheduler for InMemoryTransactionScheduler {
    fn schedule(&self, plan: &TransactionPlan) -> DflyResult<()> {
        validate_plan_shape(plan)?;

        if plan.mode == TransactionMode::NonAtomic {
            // Non-atomic mode is intentionally lock-free: it validates command shape only and
            // does not reserve shard queues or key leases in scheduler state.
            validate_non_atomic_mode(plan)?;
            return Ok(());
        }

        let shards = validate_touched_shard_footprint(plan)?;

        let key_accesses = match plan.mode {
            TransactionMode::LockAhead => collect_lock_ahead_accesses(plan)?,
            // Global mode models a coarse barrier and does not need per-key locks.
            TransactionMode::Global => Vec::new(),
            TransactionMode::NonAtomic => {
                unreachable!("non-atomic mode returns before queue/lock scheduling")
            }
        };

        if self.is_active_txid(plan.txid)? {
            return Err(DflyError::InvalidState(
                "transaction txid is already active",
            ));
        }

        let reserved_shards = self.reserve_shards_for_tx(plan.txid, &shards)?;
        let key_leases = match self.acquire_key_leases_for_tx(plan.txid, key_accesses) {
            Ok(key_leases) => key_leases,
            Err(error) => {
                self.rollback_tx_leases(plan.txid, &reserved_shards, &[])?;
                return Err(error);
            }
        };

        let mut active = self
            .active
            .lock()
            .map_err(|_| DflyError::InvalidState("transaction scheduler mutex is poisoned"))?;
        if active.contains_key(&plan.txid) {
            drop(active);
            self.rollback_tx_leases(plan.txid, &reserved_shards, &key_leases)?;
            return Err(DflyError::InvalidState(
                "transaction txid is already active",
            ));
        }
        let _ = active.insert(
            plan.txid,
            ActiveLease {
                reserved_shards,
                key_leases,
            },
        );
        Ok(())
    }

    fn conclude(&self, txid: TxId) -> DflyResult<()> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| DflyError::InvalidState("transaction scheduler mutex is poisoned"))?;

        let Some(lease) = active.remove(&txid) else {
            // Non-atomic plans keep no scheduler lease, so this path is intentionally a no-op.
            return Ok(());
        };
        drop(active);

        self.rollback_tx_leases(txid, &lease.reserved_shards, &lease.key_leases)?;
        Ok(())
    }

    fn ensure_shards_available(&self, shards: &[ShardId]) -> DflyResult<()> {
        if shards.is_empty() {
            return Ok(());
        }

        let mut unique_shards = shards.to_vec();
        unique_shards.sort_unstable();
        unique_shards.dedup();

        for shard in unique_shards {
            if let Some(queue_handle) = self.shard_queue_handle_if_exists(shard)? {
                let queue = queue_handle.lock().map_err(|_| {
                    DflyError::InvalidState("transaction scheduler shard queue mutex is poisoned")
                })?;
                if !queue.is_empty() {
                    return Err(DflyError::InvalidState("transaction shard queue is busy"));
                }
            }
        }
        Ok(())
    }
}

impl InMemoryTransactionScheduler {
    fn is_active_txid(&self, txid: TxId) -> DflyResult<bool> {
        let active = self
            .active
            .lock()
            .map_err(|_| DflyError::InvalidState("transaction scheduler mutex is poisoned"))?;
        Ok(active.contains_key(&txid))
    }

    fn reserve_shards_for_tx(&self, txid: TxId, shards: &[ShardId]) -> DflyResult<Vec<ShardId>> {
        let mut reserved_shards = Vec::with_capacity(shards.len());
        for shard in shards {
            let queue_handle = self.shard_queue_handle(*shard)?;
            let mut queue = queue_handle.lock().map_err(|_| {
                DflyError::InvalidState("transaction scheduler shard queue mutex is poisoned")
            })?;

            // Unit-level model: a queued transaction is considered "active". If queue is non-empty,
            // next transaction must retry later instead of running immediately.
            if !queue.is_empty() {
                drop(queue);
                self.release_shard_reservations(txid, &reserved_shards)?;
                return Err(DflyError::InvalidState("shard queue is busy"));
            }

            queue.push_back(txid);
            reserved_shards.push(*shard);
        }
        Ok(reserved_shards)
    }

    fn acquire_key_leases_for_tx(
        &self,
        txid: TxId,
        key_accesses: KeyAccessList,
    ) -> DflyResult<Vec<KeyLease>> {
        let mut key_locks = self.key_locks.lock().map_err(|_| {
            DflyError::InvalidState("transaction scheduler key lock mutex is poisoned")
        })?;
        let mut key_leases = Vec::with_capacity(key_accesses.len());
        for (key_id, access) in key_accesses {
            let lock_state = key_locks.entry(key_id.clone()).or_default();
            if !try_acquire_key_lock(lock_state, txid, access) {
                for key_lease in &key_leases {
                    release_key_lease_from_map(&mut key_locks, txid, key_lease);
                }
                return Err(DflyError::InvalidState("key lock is busy"));
            }
            key_leases.push(KeyLease { key_id, access });
        }
        Ok(key_leases)
    }

    fn rollback_tx_leases(
        &self,
        txid: TxId,
        reserved_shards: &[ShardId],
        key_leases: &[KeyLease],
    ) -> DflyResult<()> {
        self.release_key_leases(txid, key_leases)?;
        self.release_shard_reservations(txid, reserved_shards)
    }

    fn release_key_leases(&self, txid: TxId, key_leases: &[KeyLease]) -> DflyResult<()> {
        if key_leases.is_empty() {
            return Ok(());
        }

        let mut key_locks = self.key_locks.lock().map_err(|_| {
            DflyError::InvalidState("transaction scheduler key lock mutex is poisoned")
        })?;
        for key_lease in key_leases {
            release_key_lease_from_map(&mut key_locks, txid, key_lease);
        }
        Ok(())
    }

    fn release_shard_reservations(
        &self,
        txid: TxId,
        reserved_shards: &[ShardId],
    ) -> DflyResult<()> {
        for shard in reserved_shards {
            let Some(queue_handle) = self.shard_queue_handle_if_exists(*shard)? else {
                continue;
            };
            let mut queue = queue_handle.lock().map_err(|_| {
                DflyError::InvalidState("transaction scheduler shard queue mutex is poisoned")
            })?;
            if let Some(position) = queue.iter().position(|queued_txid| *queued_txid == txid) {
                let _ = queue.remove(position);
            }
        }
        Ok(())
    }

    fn shard_queue_handle(&self, shard: ShardId) -> DflyResult<Arc<Mutex<VecDeque<TxId>>>> {
        let mut shard_queues = self.shard_queues.lock().map_err(|_| {
            DflyError::InvalidState("transaction scheduler shard queue mutex is poisoned")
        })?;
        Ok(shard_queues
            .entry(shard)
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone())
    }

    fn shard_queue_handle_if_exists(
        &self,
        shard: ShardId,
    ) -> DflyResult<Option<Arc<Mutex<VecDeque<TxId>>>>> {
        let shard_queues = self.shard_queues.lock().map_err(|_| {
            DflyError::InvalidState("transaction scheduler shard queue mutex is poisoned")
        })?;
        Ok(shard_queues.get(&shard).cloned())
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

fn validate_touched_shard_footprint(plan: &TransactionPlan) -> DflyResult<Vec<ShardId>> {
    if plan.touched_shards.is_empty() {
        return Err(DflyError::InvalidState(
            "transaction plan must declare touched shard footprint",
        ));
    }

    let mut touched = plan.touched_shards.clone();
    touched.sort_unstable();

    for duplicate_pair in touched.windows(2) {
        if duplicate_pair[0] == duplicate_pair[1] {
            return Err(DflyError::InvalidState(
                "transaction plan touched_shards contains duplicates",
            ));
        }
    }

    for hop in &plan.hops {
        for (shard, _) in &hop.per_shard {
            if touched.binary_search(shard).is_err() {
                return Err(DflyError::InvalidState(
                    "transaction plan shard is missing from touched_shards",
                ));
            }
        }
    }

    Ok(touched)
}

fn validate_non_atomic_mode(plan: &TransactionPlan) -> DflyResult<()> {
    for hop in &plan.hops {
        for (_, command) in &hop.per_shard {
            if classify_single_key_access(command) != Some(SingleKeyAccess::Read) {
                return Err(DflyError::InvalidState(
                    "non-atomic mode requires read-only single-key commands",
                ));
            }
        }
    }
    Ok(())
}

fn collect_lock_ahead_accesses(plan: &TransactionPlan) -> DflyResult<KeyAccessList> {
    let mut by_key = HashMap::<KeyId, SingleKeyAccess>::new();
    for hop in &plan.hops {
        for (shard, command) in &hop.per_shard {
            let Some(access) = classify_single_key_access(command) else {
                return Err(DflyError::InvalidState(
                    "lock-ahead mode requires single-key commands",
                ));
            };
            let Some(primary_key) = command.args.first() else {
                return Err(DflyError::InvalidState(
                    "single-key command is missing key argument",
                ));
            };
            by_key
                .entry((*shard, primary_key.clone()))
                .and_modify(|current| {
                    if *current == SingleKeyAccess::Read && access == SingleKeyAccess::Write {
                        *current = SingleKeyAccess::Write;
                    }
                })
                .or_insert(access);
        }
    }

    let mut accesses = by_key.into_iter().collect::<Vec<_>>();
    accesses.sort_by(|(left_key, _), (right_key, _)| {
        left_key
            .0
            .cmp(&right_key.0)
            .then(left_key.1.cmp(&right_key.1))
    });
    Ok(accesses)
}

fn try_acquire_key_lock(
    lock_state: &mut KeyLockState,
    txid: TxId,
    access: SingleKeyAccess,
) -> bool {
    match access {
        SingleKeyAccess::Read => {
            if lock_state.writer.is_some_and(|owner| owner != txid) {
                return false;
            }
            let _ = lock_state.readers.insert(txid);
            true
        }
        SingleKeyAccess::Write => {
            if lock_state.writer.is_some_and(|owner| owner != txid) {
                return false;
            }
            if lock_state.readers.iter().any(|reader| *reader != txid) {
                return false;
            }
            lock_state.writer = Some(txid);
            true
        }
    }
}

fn release_key_lease_from_map(key_locks: &mut KeyLockMap, txid: TxId, lease: &KeyLease) {
    let mut remove_state = false;
    if let Some(lock_state) = key_locks.get_mut(&lease.key_id) {
        match lease.access {
            SingleKeyAccess::Read => {
                let _ = lock_state.readers.remove(&txid);
            }
            SingleKeyAccess::Write => {
                if lock_state.writer == Some(txid) {
                    lock_state.writer = None;
                }
                let _ = lock_state.readers.remove(&txid);
            }
        }
        remove_state = lock_state.writer.is_none() && lock_state.readers.is_empty();
    }
    if remove_state {
        let _ = key_locks.remove(&lease.key_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryTransactionScheduler, TransactionScheduler};
    use crate::plan::{TransactionHop, TransactionMode, TransactionPlan};
    use dfly_common::error::DflyError;
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
            touched_shards: vec![0],
        };

        let result = scheduler.schedule(&plan);
        assert_that!(result.is_err(), eq(true));
    }

    #[rstest]
    fn scheduler_rejects_empty_touched_shard_footprint() {
        let scheduler = InMemoryTransactionScheduler::default();
        let plan = TransactionPlan {
            txid: 2,
            mode: TransactionMode::Global,
            hops: vec![TransactionHop {
                per_shard: vec![(0, CommandFrame::new("PING", Vec::new()))],
            }],
            touched_shards: Vec::new(),
        };

        let result = scheduler.schedule(&plan);
        assert_that!(result.is_err(), eq(true));
    }

    #[rstest]
    fn scheduler_rejects_hop_shard_missing_from_touched_footprint() {
        let scheduler = InMemoryTransactionScheduler::default();
        let plan = TransactionPlan {
            txid: 3,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    1,
                    CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]),
                )],
            }],
            touched_shards: vec![0],
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
            touched_shards: vec![0],
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
            touched_shards: vec![0, 1],
        };

        let result = scheduler.schedule(&plan);
        assert_that!(result.is_ok(), eq(true));
    }

    #[rstest]
    fn scheduler_rejects_non_atomic_write_commands() {
        let scheduler = InMemoryTransactionScheduler::default();
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::NonAtomic,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]),
                )],
            }],
            touched_shards: vec![0],
        };

        let result = scheduler.schedule(&plan);
        assert_that!(result.is_err(), eq(true));
    }

    #[rstest]
    fn scheduler_rejects_non_atomic_multi_key_commands() {
        let scheduler = InMemoryTransactionScheduler::default();
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::NonAtomic,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new("EXISTS", vec![b"k1".to_vec(), b"k2".to_vec()]),
                )],
            }],
            touched_shards: vec![0],
        };

        let result = scheduler.schedule(&plan);
        assert_that!(result.is_err(), eq(true));
    }

    #[rstest]
    fn scheduler_keeps_non_atomic_mode_lock_free() {
        let scheduler = InMemoryTransactionScheduler::default();
        let non_atomic = TransactionPlan {
            txid: 10,
            mode: TransactionMode::NonAtomic,
            hops: vec![TransactionHop {
                per_shard: vec![(0, CommandFrame::new("GET", vec![b"k".to_vec()]))],
            }],
            touched_shards: vec![0],
        };
        let lock_ahead = TransactionPlan {
            txid: 11,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]),
                )],
            }],
            touched_shards: vec![0],
        };

        assert_that!(scheduler.schedule(&non_atomic).is_ok(), eq(true));
        assert_that!(scheduler.schedule(&lock_ahead).is_ok(), eq(true));
    }

    #[rstest]
    fn scheduler_rejects_lockahead_for_non_single_key_commands() {
        let scheduler = InMemoryTransactionScheduler::default();
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new("RENAME", vec![b"src".to_vec(), b"dst".to_vec()]),
                )],
            }],
            touched_shards: vec![0],
        };

        let result = scheduler.schedule(&plan);
        assert_that!(result.is_err(), eq(true));
    }

    #[rstest]
    fn scheduler_accepts_lockahead_set_with_extra_options() {
        let scheduler = InMemoryTransactionScheduler::default();
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new(
                        "SET",
                        vec![
                            b"k".to_vec(),
                            b"v".to_vec(),
                            b"NX".to_vec(),
                            b"GET".to_vec(),
                        ],
                    ),
                )],
            }],
            touched_shards: vec![0],
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
            touched_shards: vec![0],
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
            touched_shards: vec![0],
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

    #[rstest]
    fn scheduler_global_mode_reserves_full_runtime_shard_footprint() {
        let scheduler = InMemoryTransactionScheduler::default();
        let global = TransactionPlan {
            txid: 20,
            mode: TransactionMode::Global,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new(
                        "MSET",
                        vec![
                            b"cross:0".to_vec(),
                            b"a".to_vec(),
                            b"cross:1".to_vec(),
                            b"b".to_vec(),
                        ],
                    ),
                )],
            }],
            touched_shards: vec![0, 1],
        };
        let shard_one_writer = TransactionPlan {
            txid: 21,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    1,
                    CommandFrame::new("SET", vec![b"cross:1".to_vec(), b"next".to_vec()]),
                )],
            }],
            touched_shards: vec![1],
        };

        assert_that!(scheduler.schedule(&global).is_ok(), eq(true));
        assert_that!(scheduler.schedule(&shard_one_writer).is_err(), eq(true));
        assert_that!(scheduler.conclude(global.txid).is_ok(), eq(true));
        assert_that!(scheduler.schedule(&shard_one_writer).is_ok(), eq(true));
    }

    #[rstest]
    fn scheduler_detects_busy_shard_queue_in_ensure() {
        let scheduler = InMemoryTransactionScheduler::default();
        let plan = TransactionPlan {
            txid: 33,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]),
                )],
            }],
            touched_shards: vec![0],
        };

        assert_that!(scheduler.schedule(&plan).is_ok(), eq(true));
        let Err(error) = scheduler.ensure_shards_available(&plan.touched_shards) else {
            panic!("expected shard to stay busy while transaction is active");
        };
        assert_eq!(
            error,
            DflyError::InvalidState("transaction shard queue is busy")
        );

        assert_that!(scheduler.conclude(plan.txid).is_ok(), eq(true));
        assert_that!(
            scheduler
                .ensure_shards_available(&plan.touched_shards)
                .is_ok(),
            eq(true)
        );
    }

    #[rstest]
    fn scheduler_ensure_shards_available_deduplicates_input() {
        let scheduler = InMemoryTransactionScheduler::default();
        let shards = vec![2_u16, 2_u16, 1_u16, 1_u16];
        assert_that!(scheduler.ensure_shards_available(&shards).is_ok(), eq(true));
    }

    #[rstest]
    fn scheduler_rejects_duplicate_active_txid_even_on_other_shards() {
        let scheduler = InMemoryTransactionScheduler::default();
        let first = TransactionPlan {
            txid: 77,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    0,
                    CommandFrame::new("SET", vec![b"k:one".to_vec(), b"v1".to_vec()]),
                )],
            }],
            touched_shards: vec![0],
        };
        let duplicate_txid = TransactionPlan {
            txid: 77,
            mode: TransactionMode::LockAhead,
            hops: vec![TransactionHop {
                per_shard: vec![(
                    1,
                    CommandFrame::new("SET", vec![b"k:two".to_vec(), b"v2".to_vec()]),
                )],
            }],
            touched_shards: vec![1],
        };

        assert_that!(scheduler.schedule(&first).is_ok(), eq(true));
        assert_that!(scheduler.schedule(&duplicate_txid).is_err(), eq(true));
    }
}
