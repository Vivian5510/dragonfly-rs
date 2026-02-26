//! Proactor-style I/O worker pool for connection parsing.
//!
//! Dragonfly binds each accepted connection to one proactor thread.
//! This module mirrors that model by assigning a stable I/O worker per connection and
//! executing parser advancement on that worker thread.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use dfly_common::error::{DflyError, DflyResult};
use tokio::runtime::Builder as TokioBuilder;
use tokio::sync::mpsc;
use tokio::task::LocalSet;

use crate::connection::{ConnectionContext, ConnectionState};
use crate::protocol::ParsedCommand;

#[derive(Debug)]
enum ParseRequest {
    Detached {
        sequence: u64,
        parser: ConnectionState,
        bytes: PooledBytes,
    },
    Bound {
        sequence: u64,
        connection_id: u64,
        context: Option<ConnectionContext>,
        bytes: PooledBytes,
    },
    Release {
        sequence: u64,
        connection_id: u64,
    },
}

#[derive(Debug, Default)]
struct IoWorkerQueueMetrics {
    pending: AtomicUsize,
    peak_pending: AtomicUsize,
    pending_bytes: AtomicUsize,
    peak_pending_bytes: AtomicUsize,
    blocked_submitters: AtomicUsize,
}

#[derive(Debug, Default)]
struct IoWorkerQueueState {
    gate: Mutex<()>,
    changed: Condvar,
}

#[derive(Debug)]
struct IoWorkerBufferPool {
    recycled: Mutex<Vec<Vec<u8>>>,
    max_cached_buffers: usize,
}

impl Default for IoWorkerBufferPool {
    fn default() -> Self {
        Self {
            recycled: Mutex::new(Vec::new()),
            max_cached_buffers: 64,
        }
    }
}

impl IoWorkerBufferPool {
    fn acquire(&self, min_capacity: usize) -> Vec<u8> {
        let mut recycled = self
            .recycled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while let Some(mut buffer) = recycled.pop() {
            if buffer.capacity() >= min_capacity {
                buffer.clear();
                return buffer;
            }
        }
        Vec::with_capacity(min_capacity)
    }

    fn recycle(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        let mut recycled = self
            .recycled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if recycled.len() >= self.max_cached_buffers {
            return;
        }
        recycled.push(buffer);
    }

    #[cfg(test)]
    fn cached_count(&self) -> usize {
        self.recycled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

#[derive(Debug)]
struct PooledBytes {
    bytes: Vec<u8>,
    pool: Arc<IoWorkerBufferPool>,
}

impl PooledBytes {
    fn from_slice(pool: Arc<IoWorkerBufferPool>, input: &[u8]) -> Self {
        let mut bytes = pool.acquire(input.len());
        bytes.extend_from_slice(input);
        Self { bytes, pool }
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for PooledBytes {
    fn drop(&mut self) {
        let bytes = std::mem::take(&mut self.bytes);
        self.pool.recycle(bytes);
    }
}

#[derive(Debug, Default)]
struct IoWorkerCompletionInner {
    replies_by_sequence: std::collections::HashMap<u64, ProactorParseCompletion>,
}

#[derive(Debug, Default)]
struct IoWorkerCompletionState {
    inner: Mutex<IoWorkerCompletionInner>,
    changed: Condvar,
}

/// Stable connection-to-I/O-worker binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionAffinity {
    /// Monotonic logical connection id allocated by server-side accept path.
    pub connection_id: u64,
    /// I/O worker index that owns parse progress for this connection.
    pub io_worker: u16,
}

/// Result of one parser advancement step executed on an I/O worker.
#[derive(Debug)]
pub struct ProactorParseBatch {
    /// Parser state after consuming the provided input chunk.
    pub parser: ConnectionState,
    /// Parsed commands completed by this chunk before parser returned `Incomplete` or an error.
    pub commands: Vec<ParsedCommand>,
    /// Optional protocol error observed after parsing a command prefix.
    pub parse_error: Option<DflyError>,
    /// Worker index that executed this parse step.
    pub io_worker: u16,
}

/// Result of one parser advancement for an already bound connection.
#[derive(Debug)]
pub struct ProactorBoundParseBatch {
    /// Parsed commands completed by this chunk before parser returned `Incomplete` or an error.
    pub commands: Vec<ParsedCommand>,
    /// Optional protocol error observed after parsing a command prefix.
    pub parse_error: Option<DflyError>,
    /// Worker index that executed this parse step.
    pub io_worker: u16,
}

#[derive(Debug)]
enum ProactorParseCompletion {
    Detached(ProactorParseBatch),
    Bound(ProactorBoundParseBatch),
    Released,
}

/// In-memory parser-worker pool using one worker thread per I/O queue.
///
/// Each worker hosts a current-thread Tokio runtime and executes parse callbacks inside local
/// tasks, matching Dragonfly's "thread + fiber" shape at the network ingress boundary.
pub struct ProactorPool {
    worker_count: u16,
    queue_soft_limit: usize,
    senders: Vec<mpsc::UnboundedSender<ParseRequest>>,
    queue_metrics_per_worker: Arc<Vec<IoWorkerQueueMetrics>>,
    queue_state_per_worker: Arc<Vec<IoWorkerQueueState>>,
    buffer_pool_per_worker: Arc<Vec<Arc<IoWorkerBufferPool>>>,
    completion_per_worker: Arc<Vec<IoWorkerCompletionState>>,
    submitted_sequence_per_worker: Vec<Mutex<u64>>,
    workers: Vec<thread::JoinHandle<()>>,
}
const PROACTOR_WORKER_YIELD_INTERVAL: usize = 64;
const DEFAULT_QUEUE_SOFT_LIMIT: usize = usize::MAX;

/// Compatibility alias clarifying that this pool is parser-focused, not the network acceptor.
pub type ParserWorkerPool = ProactorPool;

fn update_peak_pending(peak: &AtomicUsize, candidate: usize) {
    let mut observed = peak.load(Ordering::Acquire);
    while candidate > observed {
        match peak.compare_exchange(observed, candidate, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(next_observed) => observed = next_observed,
        }
    }
}

impl std::fmt::Debug for ProactorPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProactorPool")
            .field("worker_count", &self.worker_count)
            .field("queue_soft_limit", &self.queue_soft_limit)
            .field("senders", &self.senders.len())
            .field(
                "queue_metrics_per_worker",
                &self.queue_metrics_per_worker.len(),
            )
            .field("queue_state_per_worker", &self.queue_state_per_worker.len())
            .field("buffer_pool_per_worker", &self.buffer_pool_per_worker.len())
            .field("completion_per_worker", &self.completion_per_worker.len())
            .field(
                "submitted_sequence_per_worker",
                &self.submitted_sequence_per_worker.len(),
            )
            .field("workers", &self.workers.len())
            .finish()
    }
}

impl ProactorPool {
    /// Creates an I/O worker pool.
    ///
    /// `worker_count == 0` is normalized to `1`.
    #[must_use]
    pub fn new(worker_count: u16) -> Self {
        Self::new_with_queue_limit(worker_count, DEFAULT_QUEUE_SOFT_LIMIT)
    }

    /// Creates an I/O worker pool with bounded parse queue pressure.
    ///
    /// Calls to `parse_on_worker` block while per-worker pending depth is at least
    /// `queue_soft_limit`.
    #[must_use]
    pub fn new_with_queue_limit(worker_count: u16, queue_soft_limit: usize) -> Self {
        let worker_count = worker_count.max(1);
        let worker_len = usize::from(worker_count);
        let queue_metrics_per_worker = Arc::new(
            (0..worker_len)
                .map(|_| IoWorkerQueueMetrics::default())
                .collect::<Vec<_>>(),
        );
        let queue_state_per_worker = Arc::new(
            (0..worker_len)
                .map(|_| IoWorkerQueueState::default())
                .collect::<Vec<_>>(),
        );
        let buffer_pool_per_worker = Arc::new(
            (0..worker_len)
                .map(|_| Arc::new(IoWorkerBufferPool::default()))
                .collect::<Vec<_>>(),
        );
        let completion_per_worker = Arc::new(
            (0..worker_len)
                .map(|_| IoWorkerCompletionState::default())
                .collect::<Vec<_>>(),
        );
        let submitted_sequence_per_worker = (0..worker_len)
            .map(|_| Mutex::new(0_u64))
            .collect::<Vec<_>>();
        let mut senders = Vec::with_capacity(worker_len);
        let mut workers = Vec::with_capacity(worker_len);

        for worker in 0..worker_len {
            let (sender, receiver) = mpsc::unbounded_channel::<ParseRequest>();
            senders.push(sender);
            let io_worker = u16::try_from(worker).unwrap_or(0);
            let queue_metrics = Arc::clone(&queue_metrics_per_worker);
            let queue_state = Arc::clone(&queue_state_per_worker);
            let completion = Arc::clone(&completion_per_worker);
            let handle = thread::spawn(move || {
                proactor_worker_thread_main(
                    io_worker,
                    receiver,
                    queue_metrics,
                    queue_state,
                    completion,
                );
            });
            workers.push(handle);
        }

        Self {
            worker_count,
            queue_soft_limit: queue_soft_limit.max(1),
            senders,
            queue_metrics_per_worker,
            queue_state_per_worker,
            buffer_pool_per_worker,
            completion_per_worker,
            submitted_sequence_per_worker,
            workers,
        }
    }

    /// Returns the number of I/O workers in this pool.
    #[must_use]
    pub fn worker_count(&self) -> u16 {
        self.worker_count
    }

    /// Returns current pending parse queue depth for one I/O worker.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when worker id is out of range.
    pub fn pending_queue_depth(&self, io_worker: u16) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        Ok(metrics.pending.load(Ordering::Acquire))
    }

    /// Returns maximum observed pending parse queue depth for one I/O worker.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when worker id is out of range.
    pub fn peak_pending_queue_depth(&self, io_worker: u16) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        Ok(metrics.peak_pending.load(Ordering::Acquire))
    }

    /// Returns current pending parse queue bytes for one I/O worker.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when worker id is out of range.
    pub fn pending_queue_bytes(&self, io_worker: u16) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        Ok(metrics.pending_bytes.load(Ordering::Acquire))
    }

    /// Returns maximum observed pending parse queue bytes for one I/O worker.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when worker id is out of range.
    pub fn peak_pending_queue_bytes(&self, io_worker: u16) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        Ok(metrics.peak_pending_bytes.load(Ordering::Acquire))
    }

    /// Returns current number of submitters blocked by parse queue pressure.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when worker id is out of range.
    pub fn blocked_submitters(&self, io_worker: u16) -> DflyResult<usize> {
        let Some(metrics) = self.queue_metrics_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        Ok(metrics.blocked_submitters.load(Ordering::Acquire))
    }

    /// Returns deterministic connection affinity.
    ///
    /// Dragonfly keeps each connection pinned to one proactor for parser/socket ownership.
    /// We mirror that by hashing the logical connection id into worker space.
    #[must_use]
    pub fn bind_connection(&self, connection_id: u64) -> ConnectionAffinity {
        let worker_mod = connection_id % u64::from(self.worker_count);
        let io_worker = u16::try_from(worker_mod).unwrap_or(0);
        ConnectionAffinity {
            connection_id,
            io_worker,
        }
    }

    /// Advances one connection parser on a specific I/O worker.
    ///
    /// The returned `commands` field carries protocol parse result while preserving parser state.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when the worker id is invalid or the worker queue/reply
    /// channel is closed.
    pub fn parse_on_worker(
        &self,
        io_worker: u16,
        parser: ConnectionState,
        bytes: &[u8],
    ) -> DflyResult<ProactorParseBatch> {
        let bytes = self.allocate_request_bytes(io_worker, bytes)?;
        let request_bytes = bytes.len();
        let request = ParseRequest::Detached {
            sequence: 0,
            parser,
            bytes,
        };
        let sequence = self.submit_parse_request(io_worker, request, request_bytes)?;
        match self.wait_for_parse_completion(io_worker, sequence)? {
            ProactorParseCompletion::Detached(reply) => Ok(reply),
            ProactorParseCompletion::Bound(_) | ProactorParseCompletion::Released => Err(
                DflyError::InvalidState("io worker completion type mismatch"),
            ),
        }
    }

    /// Advances parser state for one connection pinned to its I/O worker.
    ///
    /// The parser buffer is retained inside the worker by `connection_id`, so callers
    /// only provide incremental bytes and connection context.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when the worker id is invalid or queue/completion
    /// state is unavailable.
    pub fn parse_on_worker_for_connection(
        &self,
        affinity: ConnectionAffinity,
        context: &ConnectionContext,
        bytes: &[u8],
    ) -> DflyResult<ProactorBoundParseBatch> {
        let bytes = self.allocate_request_bytes(affinity.io_worker, bytes)?;
        let request_bytes = bytes.len();
        let request = ParseRequest::Bound {
            sequence: 0,
            connection_id: affinity.connection_id,
            context: Some(context.clone()),
            bytes,
        };
        let sequence = self.submit_parse_request(affinity.io_worker, request, request_bytes)?;
        match self.wait_for_parse_completion(affinity.io_worker, sequence)? {
            ProactorParseCompletion::Bound(reply) => Ok(reply),
            ProactorParseCompletion::Detached(_) | ProactorParseCompletion::Released => Err(
                DflyError::InvalidState("io worker completion type mismatch"),
            ),
        }
    }

    /// Advances parser state for one already-initialized bound connection.
    ///
    /// This fast path mirrors Dragonfly's per-connection parser ownership where metadata updates
    /// are infrequent and most reads only push incremental bytes into worker-local parser state.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when worker id is invalid or queue/completion
    /// state is unavailable.
    pub fn parse_on_worker_for_bound_connection(
        &self,
        affinity: ConnectionAffinity,
        bytes: &[u8],
    ) -> DflyResult<ProactorBoundParseBatch> {
        let bytes = self.allocate_request_bytes(affinity.io_worker, bytes)?;
        let request_bytes = bytes.len();
        let request = ParseRequest::Bound {
            sequence: 0,
            connection_id: affinity.connection_id,
            context: None,
            bytes,
        };
        let sequence = self.submit_parse_request(affinity.io_worker, request, request_bytes)?;
        match self.wait_for_parse_completion(affinity.io_worker, sequence)? {
            ProactorParseCompletion::Bound(reply) => Ok(reply),
            ProactorParseCompletion::Detached(_) | ProactorParseCompletion::Released => Err(
                DflyError::InvalidState("io worker completion type mismatch"),
            ),
        }
    }

    /// Releases worker-owned parser state for one bound connection.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when worker id is invalid or queue/completion
    /// state is unavailable.
    pub fn release_connection(&self, affinity: ConnectionAffinity) -> DflyResult<()> {
        let request = ParseRequest::Release {
            sequence: 0,
            connection_id: affinity.connection_id,
        };
        let sequence = self.submit_parse_request(affinity.io_worker, request, 0)?;
        match self.wait_for_parse_completion(affinity.io_worker, sequence)? {
            ProactorParseCompletion::Released => Ok(()),
            ProactorParseCompletion::Detached(_) | ProactorParseCompletion::Bound(_) => Err(
                DflyError::InvalidState("io worker completion type mismatch"),
            ),
        }
    }

    fn allocate_request_bytes(&self, io_worker: u16, bytes: &[u8]) -> DflyResult<PooledBytes> {
        let Some(pool) = self.buffer_pool_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        Ok(PooledBytes::from_slice(Arc::clone(pool), bytes))
    }

    fn submit_parse_request(
        &self,
        io_worker: u16,
        request: ParseRequest,
        request_bytes: usize,
    ) -> DflyResult<u64> {
        let Some(sender) = self.senders.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        let Some(queue_metrics) = self.queue_metrics_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        let Some(queue_state) = self.queue_state_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        let Some(sequence_counter) = self
            .submitted_sequence_per_worker
            .get(usize::from(io_worker))
        else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };

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
            let guard = queue_state
                .gate
                .lock()
                .map_err(|_| DflyError::InvalidState("io worker queue mutex is poisoned"))?;
            let _guard = queue_state
                .changed
                .wait_while(guard, |_inner| {
                    queue_metrics.pending.load(Ordering::Acquire) >= queue_soft_limit
                })
                .map_err(|_| DflyError::InvalidState("io worker queue mutex is poisoned"))?;
        }
        if blocked_submitter_marked {
            let _ = queue_metrics.blocked_submitters.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |blocked| Some(blocked.saturating_sub(1)),
            );
        }

        let pending_depth = queue_metrics.pending.fetch_add(1, Ordering::AcqRel) + 1;
        update_peak_pending(&queue_metrics.peak_pending, pending_depth);
        let pending_bytes = queue_metrics
            .pending_bytes
            .fetch_add(request_bytes, Ordering::AcqRel)
            + request_bytes;
        update_peak_pending(&queue_metrics.peak_pending_bytes, pending_bytes);

        let mut sequence_guard = sequence_counter
            .lock()
            .map_err(|_| DflyError::InvalidState("io worker sequence mutex is poisoned"))?;
        let sequence = (*sequence_guard).saturating_add(1);
        let request = match request {
            ParseRequest::Detached { parser, bytes, .. } => ParseRequest::Detached {
                sequence,
                parser,
                bytes,
            },
            ParseRequest::Bound {
                connection_id,
                context,
                bytes,
                ..
            } => ParseRequest::Bound {
                sequence,
                connection_id,
                context,
                bytes,
            },
            ParseRequest::Release { connection_id, .. } => ParseRequest::Release {
                sequence,
                connection_id,
            },
        };
        sender.send(request).map_err(|_| {
            let _ = queue_metrics.pending.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |pending| Some(pending.saturating_sub(1)),
            );
            let _ = queue_metrics.pending_bytes.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |pending| Some(pending.saturating_sub(request_bytes)),
            );
            queue_state.changed.notify_all();
            DflyError::InvalidState("io worker queue is closed")
        })?;
        *sequence_guard = sequence;
        Ok(sequence)
    }

    fn wait_for_parse_completion(
        &self,
        io_worker: u16,
        sequence: u64,
    ) -> DflyResult<ProactorParseCompletion> {
        let Some(completion_state) = self.completion_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        let completion = completion_state
            .inner
            .lock()
            .map_err(|_| DflyError::InvalidState("io worker completion mutex is poisoned"))?;
        let mut completion = completion_state
            .changed
            .wait_while(completion, |inner| {
                !inner.replies_by_sequence.contains_key(&sequence)
            })
            .map_err(|_| DflyError::InvalidState("io worker completion mutex is poisoned"))?;
        completion
            .replies_by_sequence
            .remove(&sequence)
            .ok_or(DflyError::InvalidState(
                "io worker completion for sequence is missing",
            ))
    }

    #[cfg(test)]
    fn pooled_request_buffer_count(&self, io_worker: u16) -> usize {
        self.buffer_pool_per_worker
            .get(usize::from(io_worker))
            .map_or(0, |pool| pool.cached_count())
    }
}

impl Drop for ProactorPool {
    fn drop(&mut self) {
        self.senders.clear();
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

fn proactor_worker_thread_main(
    io_worker: u16,
    receiver: mpsc::UnboundedReceiver<ParseRequest>,
    queue_metrics_per_worker: Arc<Vec<IoWorkerQueueMetrics>>,
    queue_state_per_worker: Arc<Vec<IoWorkerQueueState>>,
    completion_per_worker: Arc<Vec<IoWorkerCompletionState>>,
) {
    let Ok(runtime) = TokioBuilder::new_current_thread().enable_time().build() else {
        return;
    };

    let local_set = LocalSet::new();
    runtime.block_on(local_set.run_until(async move {
        // Mirror Dragonfly's explicit ingress and execution fibers inside one proactor thread.
        let (dispatch_sender, dispatch_receiver) = mpsc::unbounded_channel::<ParseRequest>();
        let ingress_fiber =
            tokio::task::spawn_local(proactor_ingress_fiber(receiver, dispatch_sender));
        let parse_fiber = tokio::task::spawn_local(proactor_parse_fiber(
            io_worker,
            dispatch_receiver,
            queue_metrics_per_worker,
            queue_state_per_worker,
            completion_per_worker,
        ));
        let _ = ingress_fiber.await;
        let _ = parse_fiber.await;
    }));
}

async fn proactor_ingress_fiber(
    mut receiver: mpsc::UnboundedReceiver<ParseRequest>,
    dispatch_sender: mpsc::UnboundedSender<ParseRequest>,
) {
    while let Some(request) = receiver.recv().await {
        if dispatch_sender.send(request).is_err() {
            break;
        }
    }
}

async fn proactor_parse_fiber(
    io_worker: u16,
    mut receiver: mpsc::UnboundedReceiver<ParseRequest>,
    queue_metrics_per_worker: Arc<Vec<IoWorkerQueueMetrics>>,
    queue_state_per_worker: Arc<Vec<IoWorkerQueueState>>,
    completion_per_worker: Arc<Vec<IoWorkerCompletionState>>,
) {
    let mut parsers_by_connection = std::collections::HashMap::<u64, ConnectionState>::new();
    let mut processed_since_yield = 0_usize;
    while let Some(request) = receiver.recv().await {
        handle_parse_request(
            io_worker,
            request,
            &queue_metrics_per_worker,
            &queue_state_per_worker,
            &completion_per_worker,
            &mut parsers_by_connection,
        );

        processed_since_yield = processed_since_yield.saturating_add(1);
        if processed_since_yield >= PROACTOR_WORKER_YIELD_INTERVAL {
            processed_since_yield = 0;
            tokio::task::yield_now().await;
        }
    }
}

fn handle_parse_request(
    io_worker: u16,
    request: ParseRequest,
    queue_metrics_per_worker: &Arc<Vec<IoWorkerQueueMetrics>>,
    queue_state_per_worker: &Arc<Vec<IoWorkerQueueState>>,
    completion_per_worker: &Arc<Vec<IoWorkerCompletionState>>,
    parsers_by_connection: &mut std::collections::HashMap<u64, ConnectionState>,
) {
    match request {
        ParseRequest::Detached {
            sequence,
            mut parser,
            bytes,
        } => {
            let request_bytes = bytes.len();
            parser.feed_bytes(bytes.as_slice());
            let (commands, parse_error) = drain_commands(&mut parser);
            finalize_parse_request(
                io_worker,
                sequence,
                request_bytes,
                ProactorParseCompletion::Detached(ProactorParseBatch {
                    parser,
                    commands,
                    parse_error,
                    io_worker,
                }),
                queue_metrics_per_worker,
                queue_state_per_worker,
                completion_per_worker,
            );
        }
        ParseRequest::Bound {
            sequence,
            connection_id,
            context,
            bytes,
        } => {
            let request_bytes = bytes.len();
            let parser = match parsers_by_connection.entry(connection_id) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let parser = entry.into_mut();
                    if let Some(context) = context {
                        parser.context = context;
                    }
                    parser
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let Some(context) = context else {
                        finalize_parse_request(
                            io_worker,
                            sequence,
                            request_bytes,
                            ProactorParseCompletion::Bound(ProactorBoundParseBatch {
                                commands: Vec::new(),
                                parse_error: Some(DflyError::InvalidState(
                                    "bound connection parser state is missing",
                                )),
                                io_worker,
                            }),
                            queue_metrics_per_worker,
                            queue_state_per_worker,
                            completion_per_worker,
                        );
                        return;
                    };
                    entry.insert(ConnectionState::new(context))
                }
            };
            parser.feed_bytes(bytes.as_slice());
            let (commands, parse_error) = drain_commands(parser);
            finalize_parse_request(
                io_worker,
                sequence,
                request_bytes,
                ProactorParseCompletion::Bound(ProactorBoundParseBatch {
                    commands,
                    parse_error,
                    io_worker,
                }),
                queue_metrics_per_worker,
                queue_state_per_worker,
                completion_per_worker,
            );
        }
        ParseRequest::Release {
            sequence,
            connection_id,
        } => {
            let _ = parsers_by_connection.remove(&connection_id);
            finalize_parse_request(
                io_worker,
                sequence,
                0,
                ProactorParseCompletion::Released,
                queue_metrics_per_worker,
                queue_state_per_worker,
                completion_per_worker,
            );
        }
    }
}

fn finalize_parse_request(
    io_worker: u16,
    sequence: u64,
    request_bytes: usize,
    completion_reply: ProactorParseCompletion,
    queue_metrics_per_worker: &Arc<Vec<IoWorkerQueueMetrics>>,
    queue_state_per_worker: &Arc<Vec<IoWorkerQueueState>>,
    completion_per_worker: &Arc<Vec<IoWorkerCompletionState>>,
) {
    if let Some(queue_metrics) = queue_metrics_per_worker.get(usize::from(io_worker)) {
        let _ =
            queue_metrics
                .pending
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                    Some(pending.saturating_sub(1))
                });
        let _ = queue_metrics.pending_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |pending| Some(pending.saturating_sub(request_bytes)),
        );
    }
    if let Some(queue_state) = queue_state_per_worker.get(usize::from(io_worker)) {
        queue_state.changed.notify_all();
    }
    if let Some(completion_state) = completion_per_worker.get(usize::from(io_worker)) {
        let mut completion = completion_state
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = completion
            .replies_by_sequence
            .insert(sequence, completion_reply);
        completion_state.changed.notify_all();
    }
}

fn drain_commands(parser: &mut ConnectionState) -> (Vec<ParsedCommand>, Option<DflyError>) {
    let mut commands = Vec::new();
    loop {
        match parser.try_pop_command() {
            Ok(Some(command)) => commands.push(command),
            Ok(None) => return (commands, None),
            Err(error) => return (commands, Some(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProactorPool;
    use crate::connection::{ConnectionContext, ConnectionState};
    use crate::protocol::ClientProtocol;
    use googletest::prelude::*;
    use rstest::rstest;
    use std::thread;
    use std::time::{Duration, Instant};

    fn resp_parser() -> ConnectionState {
        ConnectionState::new(ConnectionContext {
            protocol: ClientProtocol::Resp,
            db_index: 0,
            privileged: false,
        })
    }

    #[rstest]
    fn proactor_pool_binds_connection_ids_to_stable_workers() {
        let pool = ProactorPool::new(4);
        let first = pool.bind_connection(42);
        let second = pool.bind_connection(42);
        let third = pool.bind_connection(43);

        assert_that!(&first, eq(&second));
        assert_that!(first.io_worker < pool.worker_count(), eq(true));
        assert_that!(third.io_worker < pool.worker_count(), eq(true));
    }

    #[rstest]
    fn proactor_pool_parses_connection_chunks_on_bound_worker() {
        let pool = ProactorPool::new(2);
        let affinity = pool.bind_connection(7);
        let first = pool
            .parse_on_worker(
                affinity.io_worker,
                resp_parser(),
                b"*2\r\n$4\r\nECHO\r\n$5\r\nhe",
            )
            .expect("first parse step should reach worker");
        assert_that!(first.io_worker, eq(affinity.io_worker));
        let first_commands = first.commands;
        assert_that!(first_commands.is_empty(), eq(true));
        assert_that!(first.parse_error.is_none(), eq(true));
        assert_that!(first.parser.pending_bytes() > 0, eq(true));

        let second = pool
            .parse_on_worker(affinity.io_worker, first.parser, b"llo\r\n")
            .expect("second parse step should reach worker");
        assert_that!(second.io_worker, eq(affinity.io_worker));
        let parsed_commands = second.commands;
        assert_that!(parsed_commands.len(), eq(1_usize));
        assert_that!(second.parse_error.is_none(), eq(true));
        assert_that!(parsed_commands[0].name.as_str(), eq("ECHO"));
        assert_that!(&parsed_commands[0].args, eq(&vec![b"hello".to_vec()]));
    }

    #[rstest]
    fn proactor_pool_keeps_bound_connection_parser_state_on_worker() {
        let pool = ProactorPool::new(2);
        let affinity = pool.bind_connection(11);
        let context = ConnectionContext {
            protocol: ClientProtocol::Resp,
            db_index: 0,
            privileged: false,
        };

        let first = pool
            .parse_on_worker_for_connection(affinity, &context, b"*2\r\n$4\r\nECHO\r\n$5\r\nhe")
            .expect("first parse step should reach worker");
        assert_that!(first.io_worker, eq(affinity.io_worker));
        assert_that!(first.commands.is_empty(), eq(true));
        assert_that!(first.parse_error.is_none(), eq(true));

        let second = pool
            .parse_on_worker_for_connection(affinity, &context, b"llo\r\n")
            .expect("second parse step should reach worker");
        assert_that!(second.io_worker, eq(affinity.io_worker));
        assert_that!(second.commands.len(), eq(1_usize));
        assert_that!(second.parse_error.is_none(), eq(true));
        assert_that!(second.commands[0].name.as_str(), eq("ECHO"));
        assert_that!(&second.commands[0].args, eq(&vec![b"hello".to_vec()]));
    }

    #[rstest]
    fn proactor_pool_isolates_bound_parsers_by_connection_id() {
        let pool = ProactorPool::new(1);
        let first_affinity = pool.bind_connection(1);
        let second_affinity = pool.bind_connection(2);
        let context = ConnectionContext {
            protocol: ClientProtocol::Resp,
            db_index: 0,
            privileged: false,
        };

        let first_partial = pool
            .parse_on_worker_for_connection(
                first_affinity,
                &context,
                b"*2\r\n$4\r\nECHO\r\n$5\r\nhe",
            )
            .expect("partial parse should reach worker");
        assert_that!(first_partial.commands.is_empty(), eq(true));

        let second_complete = pool
            .parse_on_worker_for_connection(second_affinity, &context, b"*1\r\n$4\r\nPING\r\n")
            .expect("second connection parse should reach worker");
        assert_that!(second_complete.commands.len(), eq(1_usize));
        assert_that!(second_complete.commands[0].name.as_str(), eq("PING"));

        let first_complete = pool
            .parse_on_worker_for_connection(first_affinity, &context, b"llo\r\n")
            .expect("first connection completion should parse");
        assert_that!(first_complete.commands.len(), eq(1_usize));
        assert_that!(first_complete.commands[0].name.as_str(), eq("ECHO"));
        assert_that!(
            &first_complete.commands[0].args,
            eq(&vec![b"hello".to_vec()])
        );
    }

    #[rstest]
    fn proactor_pool_release_connection_drops_bound_parser_state() {
        let pool = ProactorPool::new(1);
        let affinity = pool.bind_connection(77);
        let context = ConnectionContext {
            protocol: ClientProtocol::Resp,
            db_index: 0,
            privileged: false,
        };

        let partial = pool
            .parse_on_worker_for_connection(affinity, &context, b"*2\r\n$4\r\nECHO\r\n$5\r\nhe")
            .expect("partial parse should reach worker");
        assert_that!(partial.commands.is_empty(), eq(true));

        assert_that!(pool.release_connection(affinity).is_ok(), eq(true));

        let parsed = pool
            .parse_on_worker_for_connection(affinity, &context, b"*1\r\n$4\r\nPING\r\n")
            .expect("fresh parser after release should parse full command");
        assert_that!(parsed.commands.len(), eq(1_usize));
        assert_that!(parsed.commands[0].name.as_str(), eq("PING"));
        assert_that!(parsed.parse_error.is_none(), eq(true));
    }

    #[rstest]
    fn proactor_pool_bound_fast_path_uses_existing_worker_parser_state() {
        let pool = ProactorPool::new(1);
        let affinity = pool.bind_connection(88);
        let context = ConnectionContext {
            protocol: ClientProtocol::Resp,
            db_index: 0,
            privileged: false,
        };

        let partial = pool
            .parse_on_worker_for_connection(affinity, &context, b"*2\r\n$4\r\nECHO\r\n$5\r\nhe")
            .expect("partial parse should initialize worker parser");
        assert_that!(partial.commands.is_empty(), eq(true));
        assert_that!(partial.parse_error.is_none(), eq(true));

        let completed = pool
            .parse_on_worker_for_bound_connection(affinity, b"llo\r\n")
            .expect("fast path should continue worker parser buffer");
        assert_that!(completed.commands.len(), eq(1_usize));
        assert_that!(completed.commands[0].name.as_str(), eq("ECHO"));
        assert_that!(&completed.commands[0].args, eq(&vec![b"hello".to_vec()]));
        assert_that!(completed.parse_error.is_none(), eq(true));
    }

    #[rstest]
    fn proactor_pool_bound_fast_path_requires_existing_worker_parser_state() {
        let pool = ProactorPool::new(1);
        let affinity = pool.bind_connection(99);

        let parsed = pool
            .parse_on_worker_for_bound_connection(affinity, b"*1\r\n$4\r\nPING\r\n")
            .expect("request should still complete with parser-state error");
        assert_that!(parsed.commands.is_empty(), eq(true));
        let error = parsed
            .parse_error
            .expect("missing parser state should be surfaced as parse error");
        assert_that!(
            format!("{error}").contains("bound connection parser state is missing"),
            eq(true)
        );
    }

    #[rstest]
    fn proactor_pool_returns_protocol_errors_from_worker() {
        let pool = ProactorPool::new(1);
        let parsed = pool
            .parse_on_worker(0, resp_parser(), b"*1\r\n$A\r\nPING\r\n")
            .expect("request should be delivered to worker");
        let command_error = parsed.parse_error.expect("malformed bulk length must fail");
        assert_that!(parsed.commands.is_empty(), eq(true));
        assert_that!(
            format!("{command_error}").contains("protocol error"),
            eq(true)
        );
    }

    #[rstest]
    fn proactor_pool_tracks_pending_queue_depth_and_peak() {
        let pool = ProactorPool::new(1);
        let parsed = pool
            .parse_on_worker(0, resp_parser(), b"*1\r\n$4\r\nPING\r\n")
            .expect("request should be delivered to worker");
        assert_that!(parsed.commands.len(), eq(1_usize));
        assert_that!(
            pool.pending_queue_depth(0)
                .expect("pending depth should be available"),
            eq(0_usize)
        );
        assert_that!(
            pool.pending_queue_bytes(0)
                .expect("pending bytes should be available"),
            eq(0_usize)
        );
        assert_that!(
            pool.peak_pending_queue_depth(0)
                .expect("peak depth should be available")
                >= 1,
            eq(true)
        );
        assert_that!(
            pool.peak_pending_queue_bytes(0)
                .expect("peak bytes should be available")
                >= b"*1\r\n$4\r\nPING\r\n".len(),
            eq(true)
        );
        assert_that!(
            pool.blocked_submitters(0)
                .expect("blocked submitters should be available"),
            eq(0_usize)
        );
    }

    #[rstest]
    fn proactor_pool_recycles_request_buffers_per_worker() {
        let pool = ProactorPool::new(1);
        let before = pool.pooled_request_buffer_count(0);
        assert_that!(before, eq(0_usize));

        let _ = pool
            .parse_on_worker(0, resp_parser(), b"*1\r\n$4\r\nPING\r\n")
            .expect("first parse should succeed");
        let deadline = Instant::now() + Duration::from_millis(200);
        let mut after_first = pool.pooled_request_buffer_count(0);
        while after_first < 1 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
            after_first = pool.pooled_request_buffer_count(0);
        }
        assert_that!(after_first >= 1, eq(true));

        let _ = pool
            .parse_on_worker(0, resp_parser(), b"*1\r\n$4\r\nPING\r\n")
            .expect("second parse should succeed");
        let after_second = pool.pooled_request_buffer_count(0);
        assert_that!(after_second >= 1, eq(true));
    }
}
