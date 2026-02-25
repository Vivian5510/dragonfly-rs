//! Proactor-style I/O worker pool for connection parsing.
//!
//! Dragonfly binds each accepted connection to one proactor thread.
//! This module mirrors that model by assigning a stable I/O worker per connection and
//! executing parser advancement on that worker thread.

use std::sync::mpsc as std_mpsc;
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
    /// Parsed commands completed by this chunk (or protocol error).
    pub commands: DflyResult<Vec<ParsedCommand>>,
    /// Worker index that executed this parse step.
    pub io_worker: u16,
}

/// In-memory proactor pool using one worker thread per I/O queue.
///
/// Each worker hosts a current-thread Tokio runtime and executes parse callbacks inside local
/// tasks, matching Dragonfly's "thread + fiber" shape at the network ingress boundary.
pub struct ProactorPool {
    worker_count: u16,
    senders: Vec<mpsc::UnboundedSender<ParseRequest>>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for ProactorPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProactorPool")
            .field("worker_count", &self.worker_count)
            .field("senders", &self.senders.len())
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
        let worker_count = worker_count.max(1);
        let worker_len = usize::from(worker_count);
        let mut senders = Vec::with_capacity(worker_len);
        let mut workers = Vec::with_capacity(worker_len);

        for worker in 0..worker_len {
            let (sender, receiver) = mpsc::unbounded_channel::<ParseRequest>();
            senders.push(sender);
            let io_worker = u16::try_from(worker).unwrap_or(0);
            let handle = thread::spawn(move || proactor_worker_thread_main(io_worker, receiver));
            workers.push(handle);
        }

        Self {
            worker_count,
            senders,
            workers,
        }
    }

    /// Returns the number of I/O workers in this pool.
    #[must_use]
    pub fn worker_count(&self) -> u16 {
        self.worker_count
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
        let (response_tx, response_rx) = std_mpsc::channel::<ProactorParseBatch>();
        sender
            .send(ParseRequest {
                parser,
                bytes: bytes.to_vec(),
                response: response_tx,
            })
            .map_err(|_| DflyError::InvalidState("io worker queue is closed"))?;
        response_rx
            .recv()
            .map_err(|_| DflyError::InvalidState("io worker reply channel is closed"))
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

fn proactor_worker_thread_main(io_worker: u16, receiver: mpsc::UnboundedReceiver<ParseRequest>) {
    let Ok(runtime) = TokioBuilder::new_current_thread().enable_time().build() else {
        return;
    };

    let local_set = LocalSet::new();
    runtime.block_on(local_set.run_until(async move {
        let parse_fiber = tokio::task::spawn_local(proactor_parse_fiber(io_worker, receiver));
        let _ = parse_fiber.await;
    }));
}

async fn proactor_parse_fiber(io_worker: u16, mut receiver: mpsc::UnboundedReceiver<ParseRequest>) {
    while let Some(request) = receiver.recv().await {
        let _ = tokio::task::spawn_local(async move {
            handle_parse_request(io_worker, request);
        })
        .await;
    }
}

fn handle_parse_request(io_worker: u16, request: ParseRequest) {
    let ParseRequest {
        mut parser,
        bytes,
        response,
    } = request;
    parser.feed_bytes(&bytes);
    let commands = drain_commands(&mut parser);
    let _ = response.send(ProactorParseBatch {
        parser,
        commands,
        io_worker,
    });
}

fn drain_commands(parser: &mut ConnectionState) -> DflyResult<Vec<ParsedCommand>> {
    let mut commands = Vec::new();
    loop {
        let parsed = parser.try_pop_command()?;
        let Some(command) = parsed else {
            break;
        };
        commands.push(command);
    }
    Ok(commands)
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
        let first_commands = first
            .commands
            .expect("first parse step should not fail protocol validation");
        assert_that!(first_commands.is_empty(), eq(true));
        assert_that!(first.parser.pending_bytes() > 0, eq(true));

        let second = pool
            .parse_on_worker(affinity.io_worker, first.parser, b"llo\r\n")
            .expect("second parse step should reach worker");
        assert_that!(second.io_worker, eq(affinity.io_worker));
        let parsed_commands = second
            .commands
            .expect("second parse step should decode one command");
        assert_that!(parsed_commands.len(), eq(1_usize));
        assert_that!(parsed_commands[0].name.as_str(), eq("ECHO"));
        assert_that!(&parsed_commands[0].args, eq(&vec![b"hello".to_vec()]));
    }

    #[rstest]
    fn proactor_pool_returns_protocol_errors_from_worker() {
        let pool = ProactorPool::new(1);
        let parsed = pool
            .parse_on_worker(0, resp_parser(), b"*1\r\n$A\r\nPING\r\n")
            .expect("request should be delivered to worker");
        let command_error = parsed
            .commands
            .expect_err("malformed bulk length must fail");
        assert_that!(
            format!("{command_error}").contains("protocol error"),
            eq(true)
        );
    }
}
