//! Runtime envelopes for coordinator-to-shard dispatch.

use std::sync::Arc;
use std::sync::Mutex;
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
pub struct InMemoryShardRuntime {
    shard_count: ShardCount,
    senders: Vec<mpsc::Sender<RuntimeEnvelope>>,
    executed_per_shard: Arc<Vec<Mutex<Vec<RuntimeEnvelope>>>>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl InMemoryShardRuntime {
    /// Creates one runtime with one worker thread per shard.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when spawning one worker thread fails.
    pub fn new(shard_count: ShardCount) -> DflyResult<Self> {
        let shard_len = usize::from(shard_count.get());
        let executed_per_shard = Arc::new(
            (0..shard_len)
                .map(|_| Mutex::new(Vec::new()))
                .collect::<Vec<_>>(),
        );

        let mut senders = Vec::with_capacity(shard_len);
        let mut workers = Vec::with_capacity(shard_len);
        for shard in 0..shard_len {
            let (sender, receiver) = mpsc::channel::<RuntimeEnvelope>();
            senders.push(sender);

            let executed = Arc::clone(&executed_per_shard);
            let worker_name = format!("dfly-shard-runtime-{shard}");
            let handle = thread::Builder::new()
                .name(worker_name)
                .spawn(move || shard_worker_loop(shard, receiver, &executed))
                .map_err(|error| DflyError::Io(error.to_string()))?;
            workers.push(handle);
        }

        Ok(Self {
            shard_count,
            senders,
            executed_per_shard,
            workers,
        })
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
}

impl ShardRuntime for InMemoryShardRuntime {
    fn shard_count(&self) -> ShardCount {
        self.shard_count
    }

    fn submit(&self, envelope: RuntimeEnvelope) -> DflyResult<()> {
        let shard = usize::from(envelope.target_shard);
        let Some(sender) = self.senders.get(shard) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        sender
            .send(envelope)
            .map_err(|_| DflyError::InvalidState("shard queue is closed"))
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
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"))
            .expect("runtime should initialize");
        let envelope = RuntimeEnvelope {
            target_shard: 2,
            command: CommandFrame::new("PING", Vec::new()),
        };

        let result = runtime.submit(envelope);
        assert_that!(result.is_err(), eq(true));
    }

    #[rstest]
    fn runtime_dispatches_to_target_shard_worker() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"))
            .expect("runtime should initialize");
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
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"))
            .expect("runtime should initialize");

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
}
