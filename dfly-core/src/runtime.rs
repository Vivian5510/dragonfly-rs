//! Runtime envelopes for coordinator-to-shard dispatch.

use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use dfly_common::error::DflyError;
use dfly_common::error::DflyResult;
use dfly_common::ids::{DbIndex, ShardCount, ShardId};

use crate::command::{CommandFrame, CommandReply};

/// Unit of work sent from a coordinator to a specific shard owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEnvelope {
    /// Destination shard owning the affected keys.
    pub target_shard: ShardId,
    /// Logical DB namespace (`SELECT`).
    pub db: DbIndex,
    /// Whether this envelope should execute command logic in worker context.
    ///
    /// Fanout envelopes used as shard barriers keep this flag `false`.
    pub execute_on_worker: bool,
    /// Wire-equivalent command data.
    pub command: CommandFrame,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueuedEnvelope {
    sequence: u64,
    envelope: RuntimeEnvelope,
}

#[derive(Debug, Default)]
struct ShardExecutionInner {
    processed: Vec<RuntimeEnvelope>,
    processed_sequence: u64,
    replies_by_sequence: std::collections::HashMap<u64, CommandReply>,
}

#[derive(Debug, Default)]
struct ShardExecutionState {
    inner: Mutex<ShardExecutionInner>,
    changed: Condvar,
}

/// Abstract runtime backend that receives coordinator work.
pub trait ShardRuntime: Send + Sync {
    /// Number of shard workers served by this runtime.
    fn shard_count(&self) -> ShardCount;

    /// Submits one envelope to a destination shard.
    ///
    /// # Errors
    ///
    /// Returns an error when runtime state cannot accept this envelope
    /// (for example shard is unavailable or queue is closed).
    fn submit(&self, envelope: RuntimeEnvelope) -> DflyResult<()>;
}

/// One in-memory shard runtime using one worker thread per shard queue.
///
/// The design mirrors Dragonfly's shared-nothing execution boundary:
/// each shard owns a queue and a dedicated execution domain. This unit keeps
/// execution payload minimal by recording envelopes into per-shard logs.
pub struct InMemoryShardRuntime {
    shard_count: ShardCount,
    senders: Vec<mpsc::Sender<QueuedEnvelope>>,
    execution_per_shard: Arc<Vec<ShardExecutionState>>,
    submitted_sequence_per_shard: Vec<Mutex<u64>>,
    workers: Vec<thread::JoinHandle<()>>,
}

type RuntimeExecutor = dyn Fn(&RuntimeEnvelope) -> CommandReply + Send + Sync;

impl std::fmt::Debug for InMemoryShardRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryShardRuntime")
            .field("shard_count", &self.shard_count)
            .field("senders", &self.senders.len())
            .field("execution_per_shard", &self.execution_per_shard.len())
            .field(
                "submitted_sequence_per_shard",
                &self.submitted_sequence_per_shard.len(),
            )
            .field("workers", &self.workers.len())
            .finish()
    }
}

impl InMemoryShardRuntime {
    /// Creates one runtime with one worker thread per shard.
    #[must_use]
    pub fn new(shard_count: ShardCount) -> Self {
        Self::new_with_executor(shard_count, |_envelope| {
            // Default runtime keeps compatibility with unit tests that only verify queueing.
            CommandReply::SimpleString("QUEUED".to_owned())
        })
    }

    /// Creates one runtime with one worker thread per shard and custom worker executor.
    #[must_use]
    pub fn new_with_executor<F>(shard_count: ShardCount, executor: F) -> Self
    where
        F: Fn(&RuntimeEnvelope) -> CommandReply + Send + Sync + 'static,
    {
        let shard_len = usize::from(shard_count.get());
        let execution_per_shard = Arc::new(
            (0..shard_len)
                .map(|_| ShardExecutionState::default())
                .collect::<Vec<_>>(),
        );
        let submitted_sequence_per_shard = (0..shard_len)
            .map(|_| Mutex::new(0_u64))
            .collect::<Vec<_>>();
        let executor = Arc::new(executor);

        let mut senders = Vec::with_capacity(shard_len);
        let mut workers = Vec::with_capacity(shard_len);
        for shard in 0..shard_len {
            let (sender, receiver) = mpsc::channel::<QueuedEnvelope>();
            senders.push(sender);

            let execution = Arc::clone(&execution_per_shard);
            let shard_executor = Arc::clone(&executor);
            let handle = thread::spawn(move || {
                shard_worker_loop(shard, receiver, &execution, &*shard_executor);
            });
            workers.push(handle);
        }

        Self {
            shard_count,
            senders,
            execution_per_shard,
            submitted_sequence_per_shard,
            workers,
        }
    }

    /// Returns and clears all processed envelopes for one shard.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range or when
    /// the shard log mutex is poisoned.
    pub fn drain_processed_for_shard(&self, shard: ShardId) -> DflyResult<Vec<RuntimeEnvelope>> {
        let Some(state) = self.execution_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let mut guard = state
            .inner
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        Ok(std::mem::take(&mut guard.processed))
    }

    /// Returns current number of processed envelopes for one shard.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range or when
    /// the shard log mutex is poisoned.
    pub fn processed_count(&self, shard: ShardId) -> DflyResult<usize> {
        let Some(state) = self.execution_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let guard = state
            .inner
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        Ok(guard.processed.len())
    }

    /// Waits until one shard has processed at least `minimum` envelopes.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range or when
    /// the shard log mutex is poisoned.
    pub fn wait_for_processed_count(
        &self,
        shard: ShardId,
        minimum: usize,
        timeout: Duration,
    ) -> DflyResult<bool> {
        let Some(state) = self.execution_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let guard = state
            .inner
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        let (guard, _) = state
            .changed
            .wait_timeout_while(guard, timeout, |inner| inner.processed.len() < minimum)
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        Ok(guard.processed.len() >= minimum)
    }

    /// Submits one envelope and returns per-shard submission sequence.
    ///
    /// The returned sequence can be used with `wait_for_processed_sequence`
    /// to build precise hop barriers that are independent from unrelated traffic.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when target shard is out of range
    /// or when the target queue is closed.
    pub fn submit_with_sequence(&self, envelope: RuntimeEnvelope) -> DflyResult<u64> {
        let shard = usize::from(envelope.target_shard);
        let Some(sender) = self.senders.get(shard) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };
        let Some(sequence_counter) = self.submitted_sequence_per_shard.get(shard) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        // Keep one per-shard submit lock so sequence assignment matches real queue ingress order.
        // This avoids virtual sequence gaps where one producer reserves a number before enqueue.
        let mut sequence_guard = sequence_counter
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime sequence mutex is poisoned"))?;
        let sequence = (*sequence_guard).saturating_add(1);
        sender
            .send(QueuedEnvelope { sequence, envelope })
            .map_err(|_| DflyError::InvalidState("shard queue is closed"))?;
        *sequence_guard = sequence;
        Ok(sequence)
    }

    /// Waits until one shard has processed one specific submission sequence.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range.
    pub fn wait_for_processed_sequence(
        &self,
        shard: ShardId,
        sequence: u64,
        timeout: Duration,
    ) -> DflyResult<bool> {
        let Some(state) = self.execution_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let guard = state
            .inner
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        let (guard, _) = state
            .changed
            .wait_timeout_while(guard, timeout, |inner| inner.processed_sequence < sequence)
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        Ok(guard.processed_sequence >= sequence)
    }

    /// Returns and removes one worker reply for a specific processed sequence.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range or when
    /// the shard log mutex is poisoned.
    pub fn take_processed_reply(
        &self,
        shard: ShardId,
        sequence: u64,
    ) -> DflyResult<Option<CommandReply>> {
        let Some(state) = self.execution_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let mut guard = state
            .inner
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        Ok(guard.replies_by_sequence.remove(&sequence))
    }
}

impl ShardRuntime for InMemoryShardRuntime {
    fn shard_count(&self) -> ShardCount {
        self.shard_count
    }

    fn submit(&self, envelope: RuntimeEnvelope) -> DflyResult<()> {
        let _ = self.submit_with_sequence(envelope)?;
        Ok(())
    }
}

impl Drop for InMemoryShardRuntime {
    fn drop(&mut self) {
        // Close all producer handles first so worker loops can observe queue closure.
        self.senders.clear();

        // Join worker threads to avoid background tasks surviving beyond runtime scope.
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

fn shard_worker_loop(
    shard: usize,
    receiver: mpsc::Receiver<QueuedEnvelope>,
    execution_per_shard: &[ShardExecutionState],
    executor: &RuntimeExecutor,
) {
    for queued in receiver {
        let envelope = queued.envelope;
        // Keep one explicit shard check so the queue ownership contract is visible
        // and invalid routing data does not pollute another shard log.
        if usize::from(envelope.target_shard) != shard {
            continue;
        }

        let reply = if envelope.execute_on_worker {
            executor(&envelope)
        } else {
            CommandReply::SimpleString("QUEUED".to_owned())
        };
        if let Some(state) = execution_per_shard.get(shard)
            && let Ok(mut guard) = state.inner.lock()
        {
            guard.processed.push(envelope);
            guard.processed_sequence = queued.sequence;
            let _ = guard.replies_by_sequence.insert(queued.sequence, reply);
            state.changed.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryShardRuntime, RuntimeEnvelope, ShardRuntime};
    use crate::command::{CommandFrame, CommandReply};
    use dfly_common::ids::ShardCount;
    use googletest::prelude::*;
    use rstest::rstest;
    use std::time::Duration;

    #[rstest]
    fn runtime_rejects_out_of_range_target_shard() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"));
        let envelope = RuntimeEnvelope {
            target_shard: 2,
            db: 0,
            execute_on_worker: false,
            command: CommandFrame::new("PING", Vec::new()),
        };

        let result = runtime.submit(envelope);
        assert_that!(result.is_err(), eq(true));
    }

    #[rstest]
    fn runtime_dispatches_to_target_shard_worker() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"));
        let envelope = RuntimeEnvelope {
            target_shard: 1,
            db: 0,
            execute_on_worker: false,
            command: CommandFrame::new("GET", vec![b"k".to_vec()]),
        };

        assert_that!(runtime.submit(envelope.clone()).is_ok(), eq(true));
        assert_that!(
            runtime
                .wait_for_processed_count(1, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );

        let processed = runtime
            .drain_processed_for_shard(1)
            .expect("drain should succeed");
        assert_that!(&processed, eq(&vec![envelope]));
    }

    #[rstest]
    fn runtime_keeps_processed_logs_isolated_between_shards() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"));

        let shard_zero = RuntimeEnvelope {
            target_shard: 0,
            db: 0,
            execute_on_worker: false,
            command: CommandFrame::new("GET", vec![b"a".to_vec()]),
        };
        let shard_one = RuntimeEnvelope {
            target_shard: 1,
            db: 0,
            execute_on_worker: false,
            command: CommandFrame::new("GET", vec![b"b".to_vec()]),
        };

        assert_that!(runtime.submit(shard_zero.clone()).is_ok(), eq(true));
        assert_that!(runtime.submit(shard_one.clone()).is_ok(), eq(true));
        assert_that!(
            runtime
                .wait_for_processed_count(0, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );
        assert_that!(
            runtime
                .wait_for_processed_count(1, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );

        let first = runtime
            .drain_processed_for_shard(0)
            .expect("drain should succeed");
        let second = runtime
            .drain_processed_for_shard(1)
            .expect("drain should succeed");
        assert_that!(&first, eq(&vec![shard_zero]));
        assert_that!(&second, eq(&vec![shard_one]));
    }

    #[rstest]
    fn runtime_reports_processed_count_per_shard() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"));
        let first = RuntimeEnvelope {
            target_shard: 0,
            db: 0,
            execute_on_worker: false,
            command: CommandFrame::new("GET", vec![b"a".to_vec()]),
        };
        let second = RuntimeEnvelope {
            target_shard: 0,
            db: 0,
            execute_on_worker: false,
            command: CommandFrame::new("GET", vec![b"b".to_vec()]),
        };

        assert_that!(runtime.submit(first).is_ok(), eq(true));
        assert_that!(runtime.submit(second).is_ok(), eq(true));
        assert_that!(
            runtime
                .wait_for_processed_count(0, 2, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );
        assert_that!(
            runtime.processed_count(0).expect("count should succeed"),
            eq(2)
        );
    }

    #[rstest]
    fn runtime_waits_for_specific_submission_sequence() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"));

        let first = RuntimeEnvelope {
            target_shard: 0,
            db: 0,
            execute_on_worker: false,
            command: CommandFrame::new("GET", vec![b"a".to_vec()]),
        };
        let second = RuntimeEnvelope {
            target_shard: 0,
            db: 0,
            execute_on_worker: false,
            command: CommandFrame::new("GET", vec![b"b".to_vec()]),
        };

        let first_sequence = runtime
            .submit_with_sequence(first)
            .expect("submission should succeed");
        let second_sequence = runtime
            .submit_with_sequence(second)
            .expect("submission should succeed");
        assert_that!(first_sequence < second_sequence, eq(true));

        assert_that!(
            runtime
                .wait_for_processed_sequence(0, second_sequence, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );
        assert_that!(
            runtime.processed_count(0).expect("count should succeed") >= 2,
            eq(true)
        );
    }

    #[rstest]
    fn runtime_submission_sequences_are_contiguous_per_shard() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"));

        let first = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 1,
                db: 0,
                execute_on_worker: false,
                command: CommandFrame::new("GET", vec![b"a".to_vec()]),
            })
            .expect("submission should succeed");
        let second = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 1,
                db: 0,
                execute_on_worker: false,
                command: CommandFrame::new("GET", vec![b"b".to_vec()]),
            })
            .expect("submission should succeed");
        let third = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 1,
                db: 0,
                execute_on_worker: false,
                command: CommandFrame::new("GET", vec![b"c".to_vec()]),
            })
            .expect("submission should succeed");

        assert_that!(first, eq(1_u64));
        assert_that!(second, eq(2_u64));
        assert_that!(third, eq(3_u64));
    }

    #[rstest]
    fn runtime_worker_executor_publishes_reply_for_sequence() {
        let runtime = InMemoryShardRuntime::new_with_executor(
            ShardCount::new(2).expect("count should be valid"),
            |envelope| {
                CommandReply::BulkString(
                    format!(
                        "db{}:{}:{}",
                        envelope.db, envelope.target_shard, envelope.command.name
                    )
                    .into_bytes(),
                )
            },
        );

        let sequence = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 1,
                db: 7,
                execute_on_worker: true,
                command: CommandFrame::new("GET", vec![b"k".to_vec()]),
            })
            .expect("submission should succeed");
        assert_that!(
            runtime
                .wait_for_processed_sequence(1, sequence, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );

        let reply = runtime
            .take_processed_reply(1, sequence)
            .expect("reply fetch should succeed");
        assert_that!(
            &reply,
            eq(&Some(CommandReply::BulkString(b"db7:1:GET".to_vec())))
        );
    }
}
