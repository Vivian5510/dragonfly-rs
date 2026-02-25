//! Proactor-style I/O worker pool for connection parsing.
//!
//! Dragonfly binds each accepted connection to one proactor thread.
//! This module mirrors that model by assigning a stable I/O worker per connection and
//! executing parser advancement on that worker thread.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use dfly_common::error::{DflyError, DflyResult};
use tokio::runtime::Builder as TokioBuilder;
use tokio::sync::mpsc;
use tokio::task::LocalSet;

use crate::connection::ConnectionState;
use crate::protocol::ParsedCommand;

#[derive(Debug)]
struct ParseRequest {
    parser: ConnectionState,
    bytes: Vec<u8>,
    response: std_mpsc::Sender<ProactorParseBatch>,
}

#[derive(Debug, Default)]
struct IoWorkerQueueMetrics {
    pending: AtomicUsize,
    peak_pending: AtomicUsize,
    blocked_submitters: AtomicUsize,
}

#[derive(Debug, Default)]
struct IoWorkerQueueState {
    gate: Mutex<()>,
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

/// In-memory proactor pool using one worker thread per I/O queue.
///
/// Each worker hosts a current-thread Tokio runtime and executes parse callbacks inside local
/// tasks, matching Dragonfly's "thread + fiber" shape at the network ingress boundary.
pub struct ProactorPool {
    worker_count: u16,
    queue_soft_limit: usize,
    senders: Vec<mpsc::UnboundedSender<ParseRequest>>,
    queue_metrics_per_worker: Arc<Vec<IoWorkerQueueMetrics>>,
    queue_state_per_worker: Arc<Vec<IoWorkerQueueState>>,
    workers: Vec<thread::JoinHandle<()>>,
}
const PROACTOR_WORKER_YIELD_INTERVAL: usize = 64;
const DEFAULT_QUEUE_SOFT_LIMIT: usize = usize::MAX;

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
        let mut senders = Vec::with_capacity(worker_len);
        let mut workers = Vec::with_capacity(worker_len);

        for worker in 0..worker_len {
            let (sender, receiver) = mpsc::unbounded_channel::<ParseRequest>();
            senders.push(sender);
            let io_worker = u16::try_from(worker).unwrap_or(0);
            let queue_metrics = Arc::clone(&queue_metrics_per_worker);
            let queue_state = Arc::clone(&queue_state_per_worker);
            let handle = thread::spawn(move || {
                proactor_worker_thread_main(io_worker, receiver, queue_metrics, queue_state);
            });
            workers.push(handle);
        }

        Self {
            worker_count,
            queue_soft_limit: queue_soft_limit.max(1),
            senders,
            queue_metrics_per_worker,
            queue_state_per_worker,
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
        let Some(sender) = self.senders.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        let Some(queue_metrics) = self.queue_metrics_per_worker.get(usize::from(io_worker)) else {
            return Err(DflyError::InvalidState("io worker is out of range"));
        };
        let Some(queue_state) = self.queue_state_per_worker.get(usize::from(io_worker)) else {
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

        let (response_tx, response_rx) = std_mpsc::channel::<ProactorParseBatch>();
        sender
            .send(ParseRequest {
                parser,
                bytes: bytes.to_vec(),
                response: response_tx,
            })
            .map_err(|_| {
                let _ = queue_metrics.pending.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |pending| Some(pending.saturating_sub(1)),
                );
                queue_state.changed.notify_all();
                DflyError::InvalidState("io worker queue is closed")
            })?;
        response_rx.recv().map_err(|_| {
            let _ = queue_metrics.pending.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |pending| Some(pending.saturating_sub(1)),
            );
            queue_state.changed.notify_all();
            DflyError::InvalidState("io worker reply channel is closed")
        })
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
) {
    let mut processed_since_yield = 0_usize;
    while let Some(request) = receiver.recv().await {
        let queue_metrics = Arc::clone(&queue_metrics_per_worker);
        let queue_state = Arc::clone(&queue_state_per_worker);
        let _ = tokio::task::spawn_local(async move {
            handle_parse_request(io_worker, request, &queue_metrics, &queue_state);
        })
        .await;

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
) {
    let ParseRequest {
        mut parser,
        bytes,
        response,
    } = request;
    parser.feed_bytes(&bytes);
    let (commands, parse_error) = drain_commands(&mut parser);
    if let Some(queue_metrics) = queue_metrics_per_worker.get(usize::from(io_worker)) {
        let _ =
            queue_metrics
                .pending
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                    Some(pending.saturating_sub(1))
                });
    }
    if let Some(queue_state) = queue_state_per_worker.get(usize::from(io_worker)) {
        queue_state.changed.notify_all();
    }
    let _ = response.send(ProactorParseBatch {
        parser,
        commands,
        parse_error,
        io_worker,
    });
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
            pool.peak_pending_queue_depth(0)
                .expect("peak depth should be available")
                >= 1,
            eq(true)
        );
        assert_that!(
            pool.blocked_submitters(0)
                .expect("blocked submitters should be available"),
            eq(0_usize)
        );
    }
}
