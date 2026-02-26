//! Runtime envelopes for coordinator-to-shard dispatch.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use dfly_common::error::DflyError;
use dfly_common::error::DflyResult;
use dfly_common::ids::{DbIndex, ShardCount, ShardId};
use tokio::runtime::Builder as TokioBuilder;
use tokio::sync::{mpsc, watch};
use tokio::task::LocalSet;

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

#[derive(Debug, Default)]
struct ShardQueueMetrics {
    pending: AtomicUsize,
    peak_pending: AtomicUsize,
    pending_bytes: AtomicUsize,
    peak_pending_bytes: AtomicUsize,
    blocked_submitters: AtomicUsize,
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
///
/// Unlike the initial blocking-loop version, each shard thread now hosts a
/// coroutine executor (`tokio` current-thread runtime + `LocalSet`) so command
/// execution can follow Dragonfly's "thread + fiber" shape.
pub struct InMemoryShardRuntime {
    shard_count: ShardCount,
    queue_soft_limit: usize,
    senders: Vec<mpsc::UnboundedSender<QueuedEnvelope>>,
    execution_per_shard: Arc<Vec<ShardExecutionState>>,
    queue_metrics_per_shard: Arc<Vec<ShardQueueMetrics>>,
    submitted_sequence_per_shard: Vec<Mutex<u64>>,
    workers: Vec<thread::JoinHandle<()>>,
}

type RuntimeExecutor = dyn Fn(&RuntimeEnvelope) -> CommandReply + Send + Sync;
type RuntimeMaintenance = dyn Fn(ShardId) + Send + Sync;
const SHARD_WORKER_YIELD_INTERVAL: usize = 64;
const SHARD_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_QUEUE_SOFT_LIMIT: usize = usize::MAX;

fn update_peak_pending(peak: &AtomicUsize, candidate: usize) {
    let mut observed = peak.load(Ordering::Acquire);
    while candidate > observed {
        match peak.compare_exchange_weak(observed, candidate, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(current) => observed = current,
        }
    }
}

fn envelope_queue_bytes(envelope: &RuntimeEnvelope) -> usize {
    envelope
        .command
        .args
        .iter()
        .fold(envelope.command.name.len(), |total, arg| {
            total.saturating_add(arg.len())
        })
}

impl std::fmt::Debug for InMemoryShardRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryShardRuntime")
            .field("shard_count", &self.shard_count)
            .field("queue_soft_limit", &self.queue_soft_limit)
            .field("senders", &self.senders.len())
            .field("execution_per_shard", &self.execution_per_shard.len())
            .field(
                "queue_metrics_per_shard",
                &self.queue_metrics_per_shard.len(),
            )
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
        let executor: Arc<RuntimeExecutor> = Arc::new(executor);
        Self::new_internal(
            shard_count,
            &executor,
            None,
            SHARD_MAINTENANCE_INTERVAL,
            DEFAULT_QUEUE_SOFT_LIMIT,
        )
    }

    /// Creates one runtime with custom worker executor and bounded submit queue pressure limit.
    ///
    /// Calls to `submit_with_sequence` block while per-shard pending depth is at least
    /// `queue_soft_limit`.
    #[must_use]
    pub fn new_with_executor_and_queue_limit<F>(
        shard_count: ShardCount,
        queue_soft_limit: usize,
        executor: F,
    ) -> Self
    where
        F: Fn(&RuntimeEnvelope) -> CommandReply + Send + Sync + 'static,
    {
        let executor: Arc<RuntimeExecutor> = Arc::new(executor);
        Self::new_internal(
            shard_count,
            &executor,
            None,
            SHARD_MAINTENANCE_INTERVAL,
            queue_soft_limit.max(1),
        )
    }

    /// Creates one runtime with custom worker executor and shard maintenance callback.
    #[must_use]
    pub fn new_with_executor_and_maintenance<F, M>(
        shard_count: ShardCount,
        executor: F,
        maintenance: M,
    ) -> Self
    where
        F: Fn(&RuntimeEnvelope) -> CommandReply + Send + Sync + 'static,
        M: Fn(ShardId) + Send + Sync + 'static,
    {
        Self::new_with_executor_and_maintenance_interval(
            shard_count,
            executor,
            maintenance,
            SHARD_MAINTENANCE_INTERVAL,
        )
    }

    fn new_with_executor_and_maintenance_interval<F, M>(
        shard_count: ShardCount,
        executor: F,
        maintenance: M,
        maintenance_interval: Duration,
    ) -> Self
    where
        F: Fn(&RuntimeEnvelope) -> CommandReply + Send + Sync + 'static,
        M: Fn(ShardId) + Send + Sync + 'static,
    {
        let executor: Arc<RuntimeExecutor> = Arc::new(executor);
        let maintenance: Arc<RuntimeMaintenance> = Arc::new(maintenance);
        Self::new_internal(
            shard_count,
            &executor,
            Some(&maintenance),
            maintenance_interval,
            DEFAULT_QUEUE_SOFT_LIMIT,
        )
    }

    fn new_internal(
        shard_count: ShardCount,
        executor: &Arc<RuntimeExecutor>,
        maintenance: Option<&Arc<RuntimeMaintenance>>,
        maintenance_interval: Duration,
        queue_soft_limit: usize,
    ) -> Self {
        let shard_len = usize::from(shard_count.get());
        let execution_per_shard = Arc::new(
            (0..shard_len)
                .map(|_| ShardExecutionState::default())
                .collect::<Vec<_>>(),
        );
        let queue_metrics_per_shard = Arc::new(
            (0..shard_len)
                .map(|_| ShardQueueMetrics::default())
                .collect::<Vec<_>>(),
        );
        let submitted_sequence_per_shard = (0..shard_len)
            .map(|_| Mutex::new(0_u64))
            .collect::<Vec<_>>();

        let mut senders = Vec::with_capacity(shard_len);
        let mut workers = Vec::with_capacity(shard_len);
        for shard in 0..shard_len {
            // Coordinator-to-shard ingress stays lock-free and per-shard, same
            // ownership boundary as Dragonfly's per-engine-shard dispatch.
            let (sender, receiver) = mpsc::unbounded_channel::<QueuedEnvelope>();
            senders.push(sender);

            let execution = Arc::clone(&execution_per_shard);
            let queue_metrics = Arc::clone(&queue_metrics_per_shard);
            let shard_executor = Arc::clone(executor);
            let shard_maintenance = maintenance.map(Arc::clone);
            let handle = thread::spawn(move || {
                shard_worker_thread_main(
                    shard,
                    receiver,
                    execution,
                    queue_metrics,
                    shard_executor,
                    shard_maintenance,
                    maintenance_interval,
                );
            });
            workers.push(handle);
        }

        Self {
            shard_count,
            queue_soft_limit,
            senders,
            execution_per_shard,
            queue_metrics_per_shard,
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

    /// Returns current number of unconsumed worker replies buffered for one shard.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range or when
    /// the shard log mutex is poisoned.
    pub fn pending_reply_count(&self, shard: ShardId) -> DflyResult<usize> {
        let Some(state) = self.execution_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let guard = state
            .inner
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        Ok(guard.replies_by_sequence.len())
    }

    /// Returns current number of queued envelopes that have not reached execution yet.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range.
    pub fn pending_queue_depth(&self, shard: ShardId) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };
        Ok(metrics.pending.load(Ordering::Acquire))
    }

    /// Returns maximum observed pending queue depth for one shard.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range.
    pub fn peak_pending_queue_depth(&self, shard: ShardId) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };
        Ok(metrics.peak_pending.load(Ordering::Acquire))
    }

    /// Returns current number of pending payload bytes queued for one shard.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range.
    pub fn pending_queue_bytes(&self, shard: ShardId) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };
        Ok(metrics.pending_bytes.load(Ordering::Acquire))
    }

    /// Returns maximum observed pending payload bytes for one shard queue.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range.
    pub fn peak_pending_queue_bytes(&self, shard: ShardId) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };
        Ok(metrics.peak_pending_bytes.load(Ordering::Acquire))
    }

    /// Returns current number of submitters blocked on queue pressure for one shard.
    ///
    /// Counter semantics mirror Dragonfly task queues where blocked-submitter metrics
    /// represent in-flight contention, not a cumulative historical total.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range.
    pub fn blocked_submitters(&self, shard: ShardId) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };
        Ok(metrics.blocked_submitters.load(Ordering::Acquire))
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
        let Some(execution_state) = self.execution_per_shard.get(shard) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };
        let Some(queue_metrics) = self.queue_metrics_per_shard.get(shard) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };
        let Some(sequence_counter) = self.submitted_sequence_per_shard.get(shard) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };
        let payload_bytes = envelope_queue_bytes(&envelope);

        let queue_soft_limit = self.queue_soft_limit;
        let mut blocked_submitter_marked = false;
        while queue_soft_limit != usize::MAX
            && queue_metrics.pending.load(Ordering::Acquire) >= queue_soft_limit
        {
            if !blocked_submitter_marked {
                let _ = queue_metrics
                    .blocked_submitters
                    .fetch_add(1, Ordering::AcqRel);
                blocked_submitter_marked = true;
            }
            let guard = execution_state
                .inner
                .lock()
                .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
            let _guard = execution_state
                .changed
                .wait_while(guard, |_inner| {
                    queue_metrics.pending.load(Ordering::Acquire) >= queue_soft_limit
                })
                .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        }
        if blocked_submitter_marked {
            let _ = queue_metrics.blocked_submitters.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |blocked| Some(blocked.saturating_sub(1)),
            );
        }

        // Keep one per-shard submit lock so sequence assignment matches real queue ingress order.
        // This avoids virtual sequence gaps where one producer reserves a number before enqueue.
        let mut sequence_guard = sequence_counter
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime sequence mutex is poisoned"))?;
        let sequence = (*sequence_guard).saturating_add(1);
        let pending_depth = queue_metrics.pending.fetch_add(1, Ordering::AcqRel) + 1;
        update_peak_pending(&queue_metrics.peak_pending, pending_depth);
        let pending_bytes = queue_metrics
            .pending_bytes
            .fetch_add(payload_bytes, Ordering::AcqRel)
            + payload_bytes;
        update_peak_pending(&queue_metrics.peak_pending_bytes, pending_bytes);
        if sender.send(QueuedEnvelope { sequence, envelope }).is_err() {
            let _ = queue_metrics.pending.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |pending| Some(pending.saturating_sub(1)),
            );
            let _ = queue_metrics.pending_bytes.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |pending| Some(pending.saturating_sub(payload_bytes)),
            );
            return Err(DflyError::InvalidState("shard queue is closed"));
        }
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

    /// Returns one point-in-time snapshot of per-shard processed sequence values.
    ///
    /// Index `i` in the returned vector corresponds to shard `i`.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when any shard execution-state mutex is poisoned.
    pub fn snapshot_processed_sequences(&self) -> DflyResult<Vec<u64>> {
        let mut snapshot = Vec::with_capacity(self.execution_per_shard.len());
        for state in self.execution_per_shard.iter() {
            let guard = state
                .inner
                .lock()
                .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
            snapshot.push(guard.processed_sequence);
        }
        Ok(snapshot)
    }

    /// Blocks until one shard has processed one specific submission sequence.
    ///
    /// Dragonfly transaction barriers are open-ended synchronization points
    /// between coordinator and shard execution. This method mirrors that
    /// behavior without imposing arbitrary wall-clock timeouts.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when shard id is out of range.
    pub fn wait_until_processed_sequence(&self, shard: ShardId, sequence: u64) -> DflyResult<()> {
        let Some(state) = self.execution_per_shard.get(usize::from(shard)) else {
            return Err(DflyError::InvalidState("target shard is out of range"));
        };

        let guard = state
            .inner
            .lock()
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        let _guard = state
            .changed
            .wait_while(guard, |inner| inner.processed_sequence < sequence)
            .map_err(|_| DflyError::InvalidState("runtime log mutex is poisoned"))?;
        Ok(())
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

fn shard_worker_thread_main(
    shard: usize,
    receiver: mpsc::UnboundedReceiver<QueuedEnvelope>,
    execution_per_shard: Arc<Vec<ShardExecutionState>>,
    queue_metrics_per_shard: Arc<Vec<ShardQueueMetrics>>,
    executor: Arc<RuntimeExecutor>,
    maintenance: Option<Arc<RuntimeMaintenance>>,
    maintenance_interval: Duration,
) {
    let Ok(runtime) = TokioBuilder::new_current_thread().enable_time().build() else {
        // Keep failure mode contained to this worker thread. Producers will
        // observe a closed queue through `submit_with_sequence`.
        return;
    };
    let local_set = LocalSet::new();
    runtime.block_on(local_set.run_until(async move {
        // Dragonfly uses multiple fibers per shard thread (queueing + execution).
        // We mirror that shape with ingress/execution fibers and one maintenance fiber.
        let (execution_sender, execution_receiver) = mpsc::unbounded_channel::<QueuedEnvelope>();
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);

        let ingress_fiber =
            tokio::task::spawn_local(shard_ingress_fiber(receiver, execution_sender));
        let execution_fiber = tokio::task::spawn_local(shard_execution_fiber(
            shard,
            execution_receiver,
            execution_per_shard,
            queue_metrics_per_shard,
            executor,
        ));
        let maintenance_fiber = maintenance.map(|maintenance| {
            tokio::task::spawn_local(shard_maintenance_fiber(
                shard,
                maintenance_interval,
                maintenance,
                shutdown_receiver,
            ))
        });
        let _ = ingress_fiber.await;
        let _ = execution_fiber.await;
        let _ = shutdown_sender.send(true);
        if let Some(maintenance_fiber) = maintenance_fiber {
            let _ = maintenance_fiber.await;
        }
    }));
}

async fn shard_ingress_fiber(
    mut receiver: mpsc::UnboundedReceiver<QueuedEnvelope>,
    execution_sender: mpsc::UnboundedSender<QueuedEnvelope>,
) {
    while let Some(queued) = receiver.recv().await {
        if execution_sender.send(queued).is_err() {
            break;
        }
    }
}

async fn shard_execution_fiber(
    shard: usize,
    mut receiver: mpsc::UnboundedReceiver<QueuedEnvelope>,
    execution_per_shard: Arc<Vec<ShardExecutionState>>,
    queue_metrics_per_shard: Arc<Vec<ShardQueueMetrics>>,
    executor: Arc<RuntimeExecutor>,
) {
    let mut processed_since_yield = 0_usize;
    while let Some(queued) = receiver.recv().await {
        let envelope = queued.envelope;
        let payload_bytes = envelope_queue_bytes(&envelope);
        // Keep one explicit shard check so the queue ownership contract is visible
        // and invalid routing data does not pollute another shard log.
        if usize::from(envelope.target_shard) != shard {
            if let Some(metrics) = queue_metrics_per_shard.get(shard) {
                let _ =
                    metrics
                        .pending
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                            Some(pending.saturating_sub(1))
                        });
                let _ = metrics.pending_bytes.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |pending| Some(pending.saturating_sub(payload_bytes)),
                );
            }
            if let Some(state) = execution_per_shard.get(shard) {
                state.changed.notify_all();
            }
            continue;
        }

        // Dragonfly executes transaction/dispatch callbacks inside fibers running
        // on shard-owned threads. Spawning a local task here keeps that boundary:
        // queue consumer coroutine -> command coroutine within same shard thread.
        let reply = if envelope.execute_on_worker {
            Some(execute_envelope_in_worker_fiber(&executor, &envelope).await)
        } else {
            None
        };
        if let Some(metrics) = queue_metrics_per_shard.get(shard) {
            let _ = metrics
                .pending
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                    Some(pending.saturating_sub(1))
                });
            let _ = metrics.pending_bytes.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |pending| Some(pending.saturating_sub(payload_bytes)),
            );
        }
        if let Some(state) = execution_per_shard.get(shard) {
            let mut guard = state
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.processed.push(envelope);
            guard.processed_sequence = queued.sequence;
            if let Some(reply) = reply {
                let _ = guard.replies_by_sequence.insert(queued.sequence, reply);
            }
            state.changed.notify_all();
        }

        processed_since_yield = processed_since_yield.saturating_add(1);
        // Cooperative yielding matches Dragonfly's explicit ThisFiber::Yield points
        // that prevent one hot queue from starving other shard-local fibers.
        if processed_since_yield >= SHARD_WORKER_YIELD_INTERVAL {
            processed_since_yield = 0;
            tokio::task::yield_now().await;
        }
    }
}

async fn shard_maintenance_fiber(
    shard: usize,
    interval: Duration,
    maintenance: Arc<RuntimeMaintenance>,
    mut shutdown: watch::Receiver<bool>,
) {
    let Ok(shard_id) = u16::try_from(shard) else {
        return;
    };

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            () = tokio::time::sleep(interval) => {
                maintenance(shard_id);
            }
        }
    }
}

async fn execute_envelope_in_worker_fiber(
    executor: &Arc<RuntimeExecutor>,
    envelope: &RuntimeEnvelope,
) -> CommandReply {
    let envelope_for_fiber = envelope.clone();
    let executor_for_fiber = Arc::clone(executor);
    match tokio::task::spawn_local(async move { executor_for_fiber(&envelope_for_fiber) }).await {
        Ok(reply) => reply,
        Err(_join_error) => CommandReply::Error("runtime worker fiber failed".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryShardRuntime, RuntimeEnvelope, SHARD_WORKER_YIELD_INTERVAL, ShardRuntime};
    use crate::command::{CommandFrame, CommandReply};
    use dfly_common::ids::ShardCount;
    use googletest::prelude::*;
    use rstest::rstest;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
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
    fn runtime_tracks_pending_queue_depth_and_peak() {
        let queued_payload_bytes = b"SET".len() + b"k1".len() + b"v1".len();
        let runtime = InMemoryShardRuntime::new_with_executor(
            ShardCount::new(1).expect("count should be valid"),
            |_envelope| {
                thread::sleep(Duration::from_millis(40));
                CommandReply::SimpleString("QUEUED".to_owned())
            },
        );

        let first_sequence = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 0,
                db: 0,
                execute_on_worker: true,
                command: CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            })
            .expect("submission should succeed");
        thread::sleep(Duration::from_millis(5));
        let second_sequence = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 0,
                db: 0,
                execute_on_worker: true,
                command: CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
            })
            .expect("submission should succeed");
        let third_sequence = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 0,
                db: 0,
                execute_on_worker: true,
                command: CommandFrame::new("SET", vec![b"k3".to_vec(), b"v3".to_vec()]),
            })
            .expect("submission should succeed");
        assert_that!(third_sequence > second_sequence, eq(true));
        assert_that!(second_sequence > first_sequence, eq(true));

        assert_that!(
            runtime
                .peak_pending_queue_depth(0)
                .expect("peak should succeed")
                >= 2,
            eq(true)
        );
        assert_that!(
            runtime
                .peak_pending_queue_bytes(0)
                .expect("peak bytes should succeed")
                >= queued_payload_bytes.saturating_mul(2),
            eq(true)
        );
        assert_that!(
            runtime
                .wait_until_processed_sequence(0, third_sequence)
                .is_ok(),
            eq(true)
        );
        assert_that!(
            runtime
                .pending_queue_depth(0)
                .expect("pending should succeed"),
            eq(0_usize)
        );
        assert_that!(
            runtime
                .pending_queue_bytes(0)
                .expect("pending bytes should succeed"),
            eq(0_usize)
        );
    }

    #[rstest]
    fn runtime_counts_blocked_submitters_when_queue_backlogs() {
        let runtime = InMemoryShardRuntime::new_with_executor_and_queue_limit(
            ShardCount::new(1).expect("count should be valid"),
            1_usize,
            |_envelope| {
                thread::sleep(Duration::from_millis(60));
                CommandReply::SimpleString("QUEUED".to_owned())
            },
        );
        let first_sequence = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 0,
                db: 0,
                execute_on_worker: true,
                command: CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            })
            .expect("first submission should succeed");
        std::thread::scope(|scope| {
            let submitter = scope.spawn(|| {
                runtime.submit_with_sequence(RuntimeEnvelope {
                    target_shard: 0,
                    db: 0,
                    execute_on_worker: true,
                    command: CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
                })
            });

            let mut observed_blocked = false;
            for _ in 0..80 {
                if runtime
                    .blocked_submitters(0)
                    .expect("blocked counter should succeed")
                    > 0
                {
                    observed_blocked = true;
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }
            assert_that!(observed_blocked, eq(true));

            let second_sequence = submitter
                .join()
                .expect("submitter thread should complete")
                .expect("second submission should succeed");
            assert_that!(second_sequence > first_sequence, eq(true));
            assert_that!(
                runtime
                    .wait_until_processed_sequence(0, second_sequence)
                    .is_ok(),
                eq(true)
            );
        });
        assert_that!(
            runtime
                .blocked_submitters(0)
                .expect("blocked counter should succeed"),
            eq(0_usize)
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
    fn runtime_wait_until_processed_sequence_blocks_until_barrier_reached() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(2).expect("count should be valid"));
        let first = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 0,
                db: 0,
                execute_on_worker: false,
                command: CommandFrame::new("GET", vec![b"a".to_vec()]),
            })
            .expect("submission should succeed");
        let second = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 0,
                db: 0,
                execute_on_worker: false,
                command: CommandFrame::new("GET", vec![b"b".to_vec()]),
            })
            .expect("submission should succeed");
        assert_that!(second > first, eq(true));

        assert_that!(
            runtime.wait_until_processed_sequence(0, second).is_ok(),
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
        assert_that!(
            runtime
                .pending_reply_count(1)
                .expect("pending reply count should succeed"),
            eq(1_usize)
        );

        let reply = runtime
            .take_processed_reply(1, sequence)
            .expect("reply fetch should succeed");
        assert_that!(
            &reply,
            eq(&Some(CommandReply::BulkString(b"db7:1:GET".to_vec())))
        );
        assert_that!(
            runtime
                .pending_reply_count(1)
                .expect("pending reply count should succeed"),
            eq(0_usize)
        );
    }

    #[rstest]
    fn runtime_barrier_envelope_does_not_store_reply_payload() {
        let worker_calls = Arc::new(AtomicUsize::new(0));
        let worker_calls_for_executor = Arc::clone(&worker_calls);
        let runtime = InMemoryShardRuntime::new_with_executor(
            ShardCount::new(2).expect("count should be valid"),
            move |_envelope| {
                worker_calls_for_executor.fetch_add(1, Ordering::AcqRel);
                CommandReply::SimpleString("OK".to_owned())
            },
        );

        let sequence = runtime
            .submit_with_sequence(RuntimeEnvelope {
                target_shard: 1,
                db: 0,
                execute_on_worker: false,
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
        assert_that!(&reply, eq(&None));
        assert_that!(worker_calls.load(Ordering::Acquire), eq(0_usize));
    }

    #[rstest]
    fn runtime_worker_executor_panic_isolated_by_fiber_boundary() {
        let runtime = InMemoryShardRuntime::new_with_executor(
            ShardCount::new(2).expect("count should be valid"),
            |_envelope| {
                panic!("expected panic from test executor");
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
            eq(&Some(CommandReply::Error(
                "runtime worker fiber failed".to_owned()
            )))
        );
    }

    #[rstest]
    fn runtime_execution_fiber_yields_without_losing_sequence_order() {
        let runtime = InMemoryShardRuntime::new(ShardCount::new(1).expect("count should be valid"));
        let batch = SHARD_WORKER_YIELD_INTERVAL.saturating_add(8);
        for index in 0..batch {
            let submitted = runtime.submit_with_sequence(RuntimeEnvelope {
                target_shard: 0,
                db: 0,
                execute_on_worker: false,
                command: CommandFrame::new("GET", vec![format!("k{index}").into_bytes()]),
            });
            assert_that!(submitted.is_ok(), eq(true));
        }

        assert_that!(
            runtime
                .wait_for_processed_count(0, batch, Duration::from_millis(500))
                .expect("wait should succeed"),
            eq(true)
        );
        assert_that!(
            runtime.processed_count(0).expect("count should succeed"),
            eq(batch)
        );
    }

    #[rstest]
    fn runtime_maintenance_fiber_executes_on_each_shard() {
        let counters = Arc::new(
            (0..2)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<AtomicUsize>>(),
        );
        let counters_for_maintenance = Arc::clone(&counters);
        let runtime = InMemoryShardRuntime::new_with_executor_and_maintenance_interval(
            ShardCount::new(2).expect("count should be valid"),
            |_envelope| CommandReply::SimpleString("QUEUED".to_owned()),
            move |shard| {
                if let Some(counter) = counters_for_maintenance.get(usize::from(shard)) {
                    let _ = counter.fetch_add(1, Ordering::Relaxed);
                }
            },
            Duration::from_millis(5),
        );

        let mut observed = false;
        for _ in 0..40 {
            if counters
                .iter()
                .all(|counter| counter.load(Ordering::Relaxed) > 0)
            {
                observed = true;
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert_that!(observed, eq(true));
        drop(runtime);
    }
}
