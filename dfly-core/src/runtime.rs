//! Runtime envelopes for coordinator-to-shard dispatch.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use dfly_common::error::DflyError;
use dfly_common::error::DflyResult;
use dfly_common::ids::{ShardCount, ShardId};

use crate::command::CommandFrame;

/// Unit of work sent from a coordinator to a specific shard owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEnvelope {
    /// Destination shard owning the affected keys.
    pub target_shard: ShardId,
    /// Wire-equivalent command data.
    pub command: CommandFrame,
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
#[derive(Debug)]
pub struct InMemoryShardRuntime {
    shard_count: ShardCount,
    senders: Vec<mpsc::Sender<RuntimeEnvelope>>,
    executed_per_shard: Arc<Vec<Mutex<Vec<RuntimeEnvelope>>>>,
    submitted_sequence_per_shard: Vec<AtomicU64>,
    processed_sequence_per_shard: Arc<Vec<AtomicU64>>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl InMemoryShardRuntime {
    /// Creates one runtime with one worker thread per shard.
    #[must_use]
    pub fn new(shard_count: ShardCount) -> Self {
        let shard_len = usize::from(shard_count.get());
        let executed_per_shard = Arc::new(
            (0..shard_len)
                .map(|_| Mutex::new(Vec::new()))
                .collect::<Vec<_>>(),
        );
        let submitted_sequence_per_shard = (0..shard_len)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let processed_sequence_per_shard = Arc::new(
            (0..shard_len)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>(),
        );

        let mut senders = Vec::with_capacity(shard_len);
        let mut workers = Vec::with_capacity(shard_len);
        for shard in 0..shard_len {
            let (sender, receiver) = mpsc::channel::<RuntimeEnvelope>();
            senders.push(sender);

            let executed = Arc::clone(&executed_per_shard);
            let processed = Arc::clone(&processed_sequence_per_shard);
            let handle =
                thread::spawn(move || shard_worker_loop(shard, receiver, &executed, &processed));
            workers.push(handle);
        }

        Self {
            shard_count,
            senders,
            executed_per_shard,
            submitted_sequence_per_shard,
            processed_sequence_per_shard,
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
        let Some(bucket) = self.executed_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let mut guard = bucket
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        Ok(std::mem::take(&mut *guard))
    }

    /// Returns current number of processed envelopes for one shard.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range or when
    /// the shard log mutex is poisoned.
    pub fn processed_count(&self, shard: ShardId) -> DflyResult<usize> {
        let Some(bucket) = self.executed_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let guard = bucket
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        Ok(guard.len())
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
        let Some(bucket) = self.executed_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let deadline = Instant::now() + timeout;
        loop {
            let reached = bucket
                .lock()
                .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?
                .len()
                >= minimum;
            if reached {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            thread::sleep(Duration::from_millis(1));
        }
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

        let sequence = sequence_counter
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        sender
            .send(envelope)
            .map_err(|_| DflyError::InvalidState("shard queue is closed"))?;
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
        let Some(processed_counter) = self.processed_sequence_per_shard.get(usize::from(shard))
        else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let deadline = Instant::now() + timeout;
        loop {
            if processed_counter.load(Ordering::Acquire) >= sequence {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            thread::sleep(Duration::from_millis(1));
        }
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
    receiver: mpsc::Receiver<RuntimeEnvelope>,
    executed_per_shard: &[Mutex<Vec<RuntimeEnvelope>>],
    processed_sequence_per_shard: &[AtomicU64],
) {
    for envelope in receiver {
        // Keep one explicit shard check so the queue ownership contract is visible
        // and invalid routing data does not pollute another shard log.
        if usize::from(envelope.target_shard) != shard {
            continue;
        }
        if let Some(bucket) = executed_per_shard.get(shard)
            && let Ok(mut guard) = bucket.lock()
        {
            guard.push(envelope);
        }
        if let Some(processed_counter) = processed_sequence_per_shard.get(shard) {
            let _ = processed_counter.fetch_add(1, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryShardRuntime, RuntimeEnvelope, ShardRuntime};
    use crate::command::CommandFrame;
    use dfly_common::ids::ShardCount;
    use googletest::prelude::*;
    use rstest::rstest;
    use std::time::Duration;

    #[rstest]
    fn runtime_rejects_out_of_range_target_shard() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"));
        let envelope = RuntimeEnvelope {
            target_shard: 2,
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
            command: CommandFrame::new("GET", vec![b"a".to_vec()]),
        };
        let shard_one = RuntimeEnvelope {
            target_shard: 1,
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
            command: CommandFrame::new("GET", vec![b"a".to_vec()]),
        };
        let second = RuntimeEnvelope {
            target_shard: 0,
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
            command: CommandFrame::new("GET", vec![b"a".to_vec()]),
        };
        let second = RuntimeEnvelope {
            target_shard: 0,
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
}
