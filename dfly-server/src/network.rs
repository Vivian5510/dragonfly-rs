//! Reactor-style network event loop for RESP and Memcache ingress.
//!
//! Dragonfly keeps socket ownership in dedicated I/O threads and advances parsing/execution
//! from readiness events. This module provides the same shape for `dragonfly-rs`:
//! per-protocol listeners + per-connection state machine driven by `mio::Poll`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dfly_common::error::{DflyError, DflyResult};
use dfly_facade::net_proactor::{MultiplexApi, MultiplexSelection};
use dfly_facade::protocol::{ClientProtocol, ParsedCommand};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
#[cfg(target_os = "linux")]
use std::net::TcpStream as StdTcpStream;
#[cfg(target_os = "linux")]
use std::os::fd::OwnedFd;
#[cfg(target_os = "linux")]
use tokio::sync::mpsc::{
    UnboundedReceiver as TokioUnboundedReceiver, UnboundedSender as TokioUnboundedSender,
    unbounded_channel as tokio_unbounded_channel,
};
#[cfg(target_os = "linux")]
use tokio::sync::oneshot;
#[cfg(target_os = "linux")]
use tokio_uring::buf::BoundedBuf;
#[cfg(target_os = "linux")]
use tokio_uring::net::TcpStream as IoUringTcpStream;

use crate::app::{
    AppExecutor, ParsedCommandExecution, RuntimeReplyTicket, ServerApp, ServerConnection,
};
#[cfg(test)]
use crate::ingress::ingress_connection_bytes;

const RESP_LISTENER_TOKEN: Token = Token(0);
const MEMCACHE_LISTENER_TOKEN: Token = Token(1);
const CONNECTION_TOKEN_START: usize = 2;
const READ_CHUNK_BYTES: usize = 8192;
const DEFAULT_WRITE_HIGH_WATERMARK_BYTES: usize = 256 * 1024;
const DEFAULT_WRITE_LOW_WATERMARK_BYTES: usize = 128 * 1024;
const DEFAULT_MAX_PENDING_REQUESTS_PER_CONNECTION: usize = 256;
const DEFAULT_MAX_PENDING_REQUESTS_PER_WORKER: usize = 8192;
const DEFAULT_REBALANCE_SCAN_INTERVAL_TICKS: usize = 64;
const WORKER_READ_BUDGET_PER_EVENT: usize = 8;
const WORKER_PARSE_BUDGET_PER_READ: usize = 32;
const WORKER_WRITE_BUDGET_BYTES_PER_EVENT: usize = 64 * 1024;
const WRITE_BUFFER_COMPACT_THRESHOLD_BYTES: usize = 64 * 1024;
#[cfg(target_os = "linux")]
const IO_URING_DEFERRED_REAP_MAX_WAIT: Duration = Duration::from_millis(500);

/// Top-level network engine over one selected multiplex backend.
pub struct NetEngine {
    selection: MultiplexSelection,
    backend: NetEngineBackend,
}

enum NetEngineBackend {
    Mio(MioBackend),
    #[cfg(target_os = "linux")]
    IoUring(IoUringBackend),
}

impl NetEngine {
    /// Binds listeners and worker runtime for one selected backend.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when listener or worker startup fails.
    pub fn bind_with_memcache(
        resp_addr: SocketAddr,
        memcache_addr: Option<SocketAddr>,
        config: ServerReactorConfig,
        selection: MultiplexSelection,
        app_executor: &AppExecutor,
    ) -> DflyResult<Self> {
        #[cfg(target_os = "linux")]
        let mut resolved_selection = selection;
        #[cfg(not(target_os = "linux"))]
        let resolved_selection = selection;
        let backend = match resolved_selection.api {
            MultiplexApi::Mio => NetEngineBackend::Mio(MioBackend::bind_with_memcache(
                resp_addr,
                memcache_addr,
                config,
                app_executor,
            )?),
            #[cfg(target_os = "linux")]
            MultiplexApi::IoUring => {
                match IoUringBackend::bind_with_memcache(resp_addr, memcache_addr, config, app_executor)
                {
                    Ok(backend) => NetEngineBackend::IoUring(backend),
                    Err(error) => {
                        resolved_selection.api = MultiplexApi::Mio;
                        resolved_selection.fallback_reason = Some(format!(
                            "io_uring backend init failed, fallback to mio: {error}"
                        ));
                        NetEngineBackend::Mio(MioBackend::bind_with_memcache(
                            resp_addr,
                            memcache_addr,
                            config,
                            app_executor,
                        )?)
                    }
                }
            }
            #[cfg(not(target_os = "linux"))]
            MultiplexApi::IoUring => {
                return Err(DflyError::InvalidConfig(
                    "io_uring backend is Linux-only; select mio backend",
                ));
            }
        };
        Ok(Self {
            selection: resolved_selection,
            backend,
        })
    }

    /// Runs one poll cycle on the selected backend.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when backend polling fails.
    pub fn poll_once(&mut self, timeout: Option<Duration>) -> DflyResult<usize> {
        match &mut self.backend {
            NetEngineBackend::Mio(backend) => backend.poll_once(timeout),
            #[cfg(target_os = "linux")]
            NetEngineBackend::IoUring(backend) => backend.poll_once(timeout),
        }
    }

    /// Runs the selected backend event loop forever.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when backend polling fails.
    pub fn run(&mut self) -> DflyResult<()> {
        match &mut self.backend {
            NetEngineBackend::Mio(_) => loop {
                let _ = self.poll_once(Some(Duration::from_millis(10)))?;
            },
            #[cfg(target_os = "linux")]
            NetEngineBackend::IoUring(backend) => backend.run(),
        }
    }

    /// Returns selected multiplex backend metadata for startup logs.
    #[must_use]
    pub fn selection(&self) -> &MultiplexSelection {
        &self.selection
    }
}

/// Top-level pool runtime matching Dragonfly's listener/proactor composition.
pub struct NetworkProactorPool {
    engine: NetEngine,
}

impl NetworkProactorPool {
    /// Binds one network pool from listener addresses, backend selection and runtime config.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when listener or worker startup fails.
    pub fn bind_with_memcache(
        resp_addr: SocketAddr,
        memcache_addr: Option<SocketAddr>,
        config: ServerReactorConfig,
        selection: MultiplexSelection,
        app_executor: &AppExecutor,
    ) -> DflyResult<Self> {
        let engine = NetEngine::bind_with_memcache(
            resp_addr,
            memcache_addr,
            config,
            selection,
            app_executor,
        )?;
        Ok(Self { engine })
    }

    /// Returns selected backend metadata.
    #[must_use]
    pub fn selection(&self) -> &MultiplexSelection {
        self.engine.selection()
    }

    /// Runs network pool until shutdown.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when backend polling fails.
    pub fn run(&mut self) -> DflyResult<()> {
        self.engine.run()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerReactorConfig {
    pub max_events: usize,
    pub write_high_watermark_bytes: usize,
    pub write_low_watermark_bytes: usize,
    pub io_worker_count: usize,
    pub io_worker_dispatch_start: usize,
    pub io_worker_dispatch_count: usize,
    pub use_peer_hash_dispatch: bool,
    pub enable_connection_migration: bool,
    pub max_pending_requests_per_connection: usize,
    pub max_pending_requests_per_worker: usize,
}

impl ServerReactorConfig {
    #[must_use]
    pub fn normalized_max_events(self) -> usize {
        self.max_events.max(64)
    }

    #[must_use]
    pub fn normalized_io_worker_count(self) -> usize {
        self.io_worker_count.max(1)
    }

    #[must_use]
    pub fn normalized_dispatch_start(self, worker_count: usize) -> usize {
        if worker_count == 0 {
            return 0;
        }
        self.io_worker_dispatch_start % worker_count
    }

    #[must_use]
    pub fn normalized_dispatch_count(self, worker_count: usize) -> usize {
        if worker_count == 0 {
            return 1;
        }
        let requested = if self.io_worker_dispatch_count == 0 {
            worker_count
        } else {
            self.io_worker_dispatch_count
        };
        requested.clamp(1, worker_count)
    }

    pub fn normalized_backpressure_watermarks(self) -> DflyResult<(usize, usize)> {
        let high = if self.write_high_watermark_bytes == 0 {
            DEFAULT_WRITE_HIGH_WATERMARK_BYTES
        } else {
            self.write_high_watermark_bytes
        };
        let low = if self.write_low_watermark_bytes == 0 {
            DEFAULT_WRITE_LOW_WATERMARK_BYTES
        } else {
            self.write_low_watermark_bytes
        };
        if low >= high {
            return Err(DflyError::InvalidConfig(
                "backpressure low watermark must be smaller than high watermark",
            ));
        }
        Ok((high, low))
    }

    #[must_use]
    pub fn normalized_pending_request_limits(self) -> (usize, usize) {
        let per_connection = if self.max_pending_requests_per_connection == 0 {
            DEFAULT_MAX_PENDING_REQUESTS_PER_CONNECTION
        } else {
            self.max_pending_requests_per_connection
        };
        let per_worker = if self.max_pending_requests_per_worker == 0 {
            DEFAULT_MAX_PENDING_REQUESTS_PER_WORKER
        } else {
            self.max_pending_requests_per_worker
        };
        (per_connection.max(1), per_worker.max(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionLifecycle {
    Active,
    Draining,
    Closing,
}

#[derive(Debug)]
struct ReactorListener {
    token: Token,
    protocol: ClientProtocol,
    socket: TcpListener,
}

#[cfg(test)]
mod single_reactor;
#[cfg(test)]
use single_reactor::{ReactorConnection, ServerReactor};

const THREADED_IO_WORKER_POLL_TIMEOUT: Duration = Duration::from_millis(5);

type MigrationAdoptResult = Result<(), Box<MigratedWorkerConnection>>;
type MigrationAdoptSender = Sender<MigrationAdoptResult>;
type MigrationAdoptReceiver = Receiver<MigrationAdoptResult>;

#[derive(Debug)]
struct MigrationDispatchFailure {
    error: DflyError,
    connection: Box<MigratedWorkerConnection>,
}

#[derive(Debug)]
enum WorkerCommand {
    AddConnection {
        connection_id: u64,
        protocol: ClientProtocol,
        socket: TcpStream,
    },
    TakeConnectionForMigration {
        connection_id: u64,
        response: Sender<Result<MigratedWorkerConnection, String>>,
    },
    AdoptMigratedConnection {
        connection: Box<MigratedWorkerConnection>,
        response: MigrationAdoptSender,
    },
    Shutdown,
}

#[derive(Debug)]
enum WorkerEvent {
    ConnectionClosed { connection_id: u64 },
}

#[derive(Debug)]
struct MigratedWorkerConnection {
    connection: WorkerConnection,
}

#[derive(Debug)]
struct WorkerConnection {
    connection_id: u64,
    socket: TcpStream,
    logical: ServerConnection,
    write_buffer: Vec<u8>,
    write_cursor: usize,
    next_request_id: u64,
    pending_request_count: usize,
    pending_runtime_requests: Vec<PendingRuntimeRequest>,
    reply_order: ConnectionReplyOrder,
    lifecycle: ConnectionLifecycle,
    read_paused_by_backpressure: bool,
    read_paused_by_command_backpressure: bool,
    interest: Interest,
}

impl WorkerConnection {
    fn new(connection_id: u64, socket: TcpStream, protocol: ClientProtocol) -> Self {
        Self {
            connection_id,
            socket,
            logical: ServerApp::new_connection(protocol),
            write_buffer: Vec::new(),
            write_cursor: 0,
            next_request_id: 1,
            pending_request_count: 0,
            pending_runtime_requests: Vec::new(),
            reply_order: ConnectionReplyOrder::new(),
            lifecycle: ConnectionLifecycle::Active,
            read_paused_by_backpressure: false,
            read_paused_by_command_backpressure: false,
            interest: Interest::READABLE,
        }
    }

    fn on_peer_closed_or_error(&mut self) {
        if self.lifecycle == ConnectionLifecycle::Active {
            self.lifecycle = ConnectionLifecycle::Draining;
        }
    }

    fn mark_draining(&mut self) {
        if self.lifecycle == ConnectionLifecycle::Active {
            self.lifecycle = ConnectionLifecycle::Draining;
        }
    }

    fn mark_closing(&mut self) {
        self.lifecycle = ConnectionLifecycle::Closing;
    }

    fn can_read(&self) -> bool {
        self.lifecycle == ConnectionLifecycle::Active
            && !self.read_paused_by_backpressure
            && !self.read_paused_by_command_backpressure
    }

    fn should_try_flush(&self) -> bool {
        !write_buffer_is_empty(&self.write_buffer, self.write_cursor)
    }

    fn should_close_now(&self) -> bool {
        self.lifecycle == ConnectionLifecycle::Closing
            || (self.lifecycle == ConnectionLifecycle::Draining
                && write_buffer_is_empty(&self.write_buffer, self.write_cursor))
    }

    fn update_backpressure_state(&mut self, high_watermark: usize, low_watermark: usize) {
        let buffered_bytes = write_buffer_pending_len(&self.write_buffer, self.write_cursor);
        if self.read_paused_by_backpressure {
            if buffered_bytes <= low_watermark {
                self.read_paused_by_backpressure = false;
            }
            return;
        }
        if buffered_bytes >= high_watermark {
            self.read_paused_by_backpressure = true;
        }
    }

    fn update_command_backpressure_state(
        &mut self,
        pending_for_worker: usize,
        max_pending_per_connection: usize,
        max_pending_per_worker: usize,
    ) {
        self.read_paused_by_command_backpressure = self.pending_request_count
            >= max_pending_per_connection
            || pending_for_worker >= max_pending_per_worker;
    }
}

#[derive(Debug)]
struct WorkerIoState {
    connections: HashMap<Token, WorkerConnection>,
    loop_state: WorkerLoopState,
}

#[derive(Debug)]
struct WorkerLoopState {
    pending_requests_for_worker: usize,
    active_parse_tokens: VecDeque<Token>,
    in_active_parse_set: HashSet<Token>,
    active_deferred_tokens: VecDeque<Token>,
    in_active_deferred_set: HashSet<Token>,
    detached_runtime_tickets: Vec<RuntimeReplyTicket>,
    live_connection_count: Arc<AtomicUsize>,
    next_token: usize,
}

impl WorkerIoState {
    fn new(live_connection_count: Arc<AtomicUsize>) -> Self {
        Self {
            connections: HashMap::new(),
            loop_state: WorkerLoopState {
                pending_requests_for_worker: 0,
                active_parse_tokens: VecDeque::new(),
                in_active_parse_set: HashSet::new(),
                active_deferred_tokens: VecDeque::new(),
                in_active_deferred_set: HashSet::new(),
                detached_runtime_tickets: Vec::new(),
                live_connection_count,
                next_token: CONNECTION_TOKEN_START,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BackpressureThresholds {
    high: usize,
    low: usize,
    max_pending_per_connection: usize,
    max_pending_per_worker: usize,
}

fn resolve_backpressure_thresholds(config: ServerReactorConfig) -> DflyResult<BackpressureThresholds> {
    let (write_high_watermark, write_low_watermark) = config.normalized_backpressure_watermarks()?;
    let (max_pending_per_connection, max_pending_per_worker) = config.normalized_pending_request_limits();
    Ok(BackpressureThresholds {
        high: write_high_watermark,
        low: write_low_watermark,
        max_pending_per_connection: max_pending_per_connection.max(1),
        max_pending_per_worker: max_pending_per_worker.max(1),
    })
}

fn bind_acceptor_listeners(
    acceptor_poll: &Poll,
    resp_addr: SocketAddr,
    memcache_addr: Option<SocketAddr>,
) -> DflyResult<Vec<ReactorListener>> {
    let mut listeners = Vec::with_capacity(if memcache_addr.is_some() { 2 } else { 1 });
    listeners.push(bind_acceptor_listener(
        acceptor_poll,
        resp_addr,
        RESP_LISTENER_TOKEN,
        ClientProtocol::Resp,
        "RESP",
    )?);
    if let Some(addr) = memcache_addr {
        listeners.push(bind_acceptor_listener(
            acceptor_poll,
            addr,
            MEMCACHE_LISTENER_TOKEN,
            ClientProtocol::Memcache,
            "Memcache",
        )?);
    }
    Ok(listeners)
}

fn bind_acceptor_listener(
    acceptor_poll: &Poll,
    addr: SocketAddr,
    token: Token,
    protocol: ClientProtocol,
    protocol_name: &str,
) -> DflyResult<ReactorListener> {
    let mut listener = TcpListener::bind(addr)
        .map_err(|error| DflyError::Io(format!("bind {protocol_name} listener failed: {error}")))?;
    acceptor_poll
        .registry()
        .register(&mut listener, token, Interest::READABLE)
        .map_err(|error| {
            DflyError::Io(format!(
                "register {protocol_name} listener in poll failed: {error}"
            ))
        })?;
    Ok(ReactorListener {
        token,
        protocol,
        socket: listener,
    })
}

#[derive(Debug)]
struct PendingRuntimeRequest {
    request_id: u64,
    ticket: RuntimeReplyTicket,
}

#[derive(Debug)]
struct ConnectionReplyOrder {
    next_request_id: u64,
    completed: BTreeMap<u64, Option<Vec<u8>>>,
}

impl ConnectionReplyOrder {
    fn new() -> Self {
        Self {
            next_request_id: 1,
            completed: BTreeMap::new(),
        }
    }
}

struct WorkerHandle {
    sender: Sender<WorkerCommand>,
    join: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy)]
struct WorkerDispatchWindow {
    start: usize,
    count: usize,
}

#[derive(Debug, Default)]
struct WorkerOwnedConnections {
    live: HashSet<u64>,
    round_robin: VecDeque<u64>,
}

impl WorkerOwnedConnections {
    fn insert(&mut self, connection_id: u64) {
        if self.live.insert(connection_id) {
            self.round_robin.push_back(connection_id);
        }
    }

    fn remove(&mut self, connection_id: u64) {
        let _ = self.live.remove(&connection_id);
    }

    fn next_candidate(&mut self, migrated_connections: &HashSet<u64>) -> Option<u64> {
        let turn_len = self.round_robin.len();
        for _ in 0..turn_len {
            let connection_id = self.round_robin.pop_front()?;
            if self.live.contains(&connection_id) {
                self.round_robin.push_back(connection_id);
                if !migrated_connections.contains(&connection_id) {
                    return Some(connection_id);
                }
            }
        }
        None
    }
}

fn write_buffer_pending_len(write_buffer: &[u8], write_cursor: usize) -> usize {
    write_buffer.len().saturating_sub(write_cursor)
}

fn write_buffer_is_empty(write_buffer: &[u8], write_cursor: usize) -> bool {
    write_buffer_pending_len(write_buffer, write_cursor) == 0
}

fn write_buffer_maybe_compact(write_buffer: &mut Vec<u8>, write_cursor: &mut usize) {
    if *write_cursor == 0 {
        return;
    }
    if *write_cursor >= write_buffer.len() {
        write_buffer.clear();
        *write_cursor = 0;
        return;
    }
    if *write_cursor >= WRITE_BUFFER_COMPACT_THRESHOLD_BYTES
        && (*write_cursor).saturating_mul(2) >= write_buffer.len()
    {
        let _ = write_buffer.drain(..*write_cursor);
        *write_cursor = 0;
    }
}

fn write_buffer_append(write_buffer: &mut Vec<u8>, write_cursor: &mut usize, payload: &[u8]) {
    write_buffer_maybe_compact(write_buffer, write_cursor);
    write_buffer.extend_from_slice(payload);
}

fn write_buffer_pending_slice(write_buffer: &[u8], write_cursor: usize) -> &[u8] {
    if write_cursor >= write_buffer.len() {
        &[]
    } else {
        &write_buffer[write_cursor..]
    }
}

fn write_buffer_advance(write_buffer: &mut Vec<u8>, write_cursor: &mut usize, written: usize) {
    let pending = write_buffer_pending_len(write_buffer, *write_cursor);
    let consumed = written.min(pending);
    *write_cursor = write_cursor.saturating_add(consumed);
    write_buffer_maybe_compact(write_buffer, write_cursor);
}

fn connection_owner_upsert(
    connection_owner_by_id: &mut HashMap<u64, usize>,
    connection_counts_by_worker: &mut [usize],
    owned_connections_by_worker: &mut [WorkerOwnedConnections],
    connection_id: u64,
    worker_id: usize,
) {
    let previous_owner = connection_owner_by_id.insert(connection_id, worker_id);
    if let Some(previous_worker) = previous_owner
        && previous_worker < connection_counts_by_worker.len()
    {
        connection_counts_by_worker[previous_worker] =
            connection_counts_by_worker[previous_worker].saturating_sub(1);
        if let Some(owned) = owned_connections_by_worker.get_mut(previous_worker) {
            owned.remove(connection_id);
        }
    }
    if worker_id < connection_counts_by_worker.len() {
        connection_counts_by_worker[worker_id] =
            connection_counts_by_worker[worker_id].saturating_add(1);
        if let Some(owned) = owned_connections_by_worker.get_mut(worker_id) {
            owned.insert(connection_id);
        }
    }
}

fn connection_owner_remove(
    connection_owner_by_id: &mut HashMap<u64, usize>,
    connection_counts_by_worker: &mut [usize],
    owned_connections_by_worker: &mut [WorkerOwnedConnections],
    connection_id: u64,
) -> Option<usize> {
    let owner = connection_owner_by_id.remove(&connection_id)?;
    if owner < connection_counts_by_worker.len() {
        connection_counts_by_worker[owner] = connection_counts_by_worker[owner].saturating_sub(1);
        if let Some(owned) = owned_connections_by_worker.get_mut(owner) {
            owned.remove(connection_id);
        }
    }
    Some(owner)
}

fn rebalance_source_and_target(connection_counts_by_worker: &[usize]) -> Option<(usize, usize)> {
    let (source_worker, source_count) = connection_counts_by_worker
        .iter()
        .enumerate()
        .max_by_key(|(_, count)| **count)
        .map(|(index, count)| (index, *count))?;
    let (target_worker, target_count) = connection_counts_by_worker
        .iter()
        .enumerate()
        .min_by_key(|(_, count)| **count)
        .map(|(index, count)| (index, *count))?;
    if source_count <= target_count.saturating_add(1) {
        return None;
    }
    Some((source_worker, target_worker))
}

fn select_rebalance_connection_id(
    owned_connections_by_worker: &mut [WorkerOwnedConnections],
    source_worker: usize,
    migrated_connections: &HashSet<u64>,
) -> Option<u64> {
    owned_connections_by_worker
        .get_mut(source_worker)
        .and_then(|owned| owned.next_candidate(migrated_connections))
}

/// Acceptor + fixed I/O worker runtime.
///
/// The acceptor thread only accepts/dispatches sockets and coordinates command execution against
/// `ServerApp`. Every socket/parser/outbuf stays on one worker thread.
pub struct ThreadedServerReactor {
    acceptor_poll: Poll,
    acceptor_events: Events,
    ready_listener_indexes: Vec<usize>,
    listeners: Vec<ReactorListener>,
    workers: Vec<WorkerHandle>,
    worker_events: Receiver<WorkerEvent>,
    #[cfg(test)]
    worker_connection_counts: Vec<Arc<AtomicUsize>>,
    dispatch_window: WorkerDispatchWindow,
    next_rr_offset: usize,
    use_peer_hash_dispatch: bool,
    migration_enabled: bool,
    rebalance_scan_ticks: usize,
    rebalance_scan_interval_ticks: usize,
    rebalance_dirty: bool,
    next_connection_id: u64,
    connection_owner_by_id: HashMap<u64, usize>,
    connection_counts_by_worker: Vec<usize>,
    owned_connections_by_worker: Vec<WorkerOwnedConnections>,
    migrated_connections: HashSet<u64>,
}

impl ThreadedServerReactor {
    /// Binds one RESP listener and starts worker threads.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` if listener bind/registration or worker spawn fails.
    #[cfg(test)]
    pub fn bind(
        addr: SocketAddr,
        config: ServerReactorConfig,
        app_executor: &AppExecutor,
    ) -> DflyResult<Self> {
        Self::bind_with_memcache(addr, None, config, app_executor)
    }

    /// Binds RESP listener and optional Memcache listener and starts worker threads.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` if listener bind/registration or worker spawn fails.
    #[allow(clippy::too_many_lines)]
    pub fn bind_with_memcache(
        resp_addr: SocketAddr,
        memcache_addr: Option<SocketAddr>,
        config: ServerReactorConfig,
        app_executor: &AppExecutor,
    ) -> DflyResult<Self> {
        let acceptor_poll =
            Poll::new().map_err(|error| DflyError::Io(format!("create poll failed: {error}")))?;
        let listeners = bind_acceptor_listeners(&acceptor_poll, resp_addr, memcache_addr)?;
        let backpressure = resolve_backpressure_thresholds(config)?;
        let worker_count = config.normalized_io_worker_count();
        let dispatch_window = WorkerDispatchWindow {
            start: config.normalized_dispatch_start(worker_count),
            count: config.normalized_dispatch_count(worker_count),
        };
        let max_events = config.normalized_max_events();
        let (worker_event_sender, worker_event_receiver) = mpsc::channel::<WorkerEvent>();

        let mut workers = Vec::with_capacity(worker_count);
        #[cfg(test)]
        let mut worker_connection_counts = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let (command_sender, command_receiver) = mpsc::channel::<WorkerCommand>();
            let executor = app_executor.clone();
            let live_connection_count = Arc::new(AtomicUsize::new(0));
            let event_sender = worker_event_sender.clone();
            #[cfg(test)]
            worker_connection_counts.push(Arc::clone(&live_connection_count));
            let join = thread::Builder::new()
                .name(format!("dfly-io-worker-{worker_id}"))
                .spawn(move || {
                    io_worker_thread_main(
                        &command_receiver,
                        max_events,
                        backpressure,
                        &executor,
                        live_connection_count,
                        &event_sender,
                    );
                })
                .map_err(|error| {
                    DflyError::Io(format!("spawn io worker {worker_id} failed: {error}"))
                })?;
            workers.push(WorkerHandle {
                sender: command_sender,
                join: Some(join),
            });
        }

        Ok(Self {
            acceptor_poll,
            acceptor_events: Events::with_capacity(max_events),
            ready_listener_indexes: Vec::with_capacity(max_events),
            listeners,
            workers,
            worker_events: worker_event_receiver,
            #[cfg(test)]
            worker_connection_counts,
            dispatch_window,
            next_rr_offset: 0,
            use_peer_hash_dispatch: config.use_peer_hash_dispatch,
            migration_enabled: config.enable_connection_migration,
            rebalance_scan_ticks: 0,
            rebalance_scan_interval_ticks: DEFAULT_REBALANCE_SCAN_INTERVAL_TICKS,
            rebalance_dirty: false,
            next_connection_id: 1,
            connection_owner_by_id: HashMap::new(),
            connection_counts_by_worker: vec![0; worker_count],
            owned_connections_by_worker: (0..worker_count)
                .map(|_| WorkerOwnedConnections::default())
                .collect(),
            migrated_connections: HashSet::new(),
        })
    }

    /// Processes one acceptor readiness cycle and dispatches accepted sockets to I/O workers.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when polling, accepting, or worker IPC fails.
    pub fn poll_once(&mut self, timeout: Option<Duration>) -> DflyResult<usize> {
        self.acceptor_poll
            .poll(&mut self.acceptor_events, timeout)
            .map_err(|error| DflyError::Io(format!("poll wait failed: {error}")))?;
        let mut observed = 0_usize;
        self.ready_listener_indexes.clear();
        for event in &self.acceptor_events {
            observed = observed.saturating_add(1);
            if let Some(listener_index) = self.listener_index(event.token()) {
                self.ready_listener_indexes.push(listener_index);
            }
        }
        let mut accepted = 0_usize;
        let ready_count = self.ready_listener_indexes.len();
        for index in 0..ready_count {
            let listener_index = self.ready_listener_indexes[index];
            accepted = accepted.saturating_add(self.accept_new_connections(listener_index)?);
        }
        self.drain_worker_events();
        self.maybe_rebalance_connections_if_needed();
        Ok(observed.saturating_add(accepted))
    }

    #[cfg(test)]
    fn local_addr(&self) -> DflyResult<SocketAddr> {
        self.listener_local_addr(ClientProtocol::Resp)
    }

    #[cfg(test)]
    fn memcache_local_addr(&self) -> DflyResult<SocketAddr> {
        self.listener_local_addr(ClientProtocol::Memcache)
    }

    #[cfg(test)]
    fn worker_connection_counts(&self) -> Vec<usize> {
        self.worker_connection_counts
            .iter()
            .map(|count| count.load(Ordering::Acquire))
            .collect::<Vec<_>>()
    }

    #[cfg(test)]
    fn active_connection_ids(&self) -> Vec<u64> {
        self.connection_owner_by_id
            .keys()
            .copied()
            .collect::<Vec<_>>()
    }

    #[cfg(test)]
    fn connection_owner(&self, connection_id: u64) -> Option<usize> {
        self.connection_owner_by_id.get(&connection_id).copied()
    }

    fn listener_index(&self, token: Token) -> Option<usize> {
        self.listeners
            .iter()
            .position(|listener| listener.token == token)
    }

    #[cfg(test)]
    fn listener_local_addr(&self, protocol: ClientProtocol) -> DflyResult<SocketAddr> {
        let Some(listener) = self
            .listeners
            .iter()
            .find(|listener| listener.protocol == protocol)
        else {
            return Err(DflyError::InvalidState(
                "network listener protocol is not enabled",
            ));
        };

        listener
            .socket
            .local_addr()
            .map_err(|error| DflyError::Io(format!("query local address failed: {error}")))
    }

    fn accept_new_connections(&mut self, listener_index: usize) -> DflyResult<usize> {
        let protocol = self
            .listeners
            .get(listener_index)
            .map(|listener| listener.protocol)
            .ok_or(DflyError::InvalidState(
                "reactor listener index is out of range",
            ))?;

        let mut accepted = 0_usize;
        loop {
            let accept_result = {
                let listener =
                    self.listeners
                        .get_mut(listener_index)
                        .ok_or(DflyError::InvalidState(
                            "reactor listener index is out of range",
                        ))?;
                listener.socket.accept()
            };

            match accept_result {
                Ok((socket, _peer)) => {
                    let _ = socket.set_nodelay(true);
                    let connection_id = self.allocate_connection_id();
                    let worker_id = self.allocate_worker_index(&socket);
                    self.dispatch_to_worker(
                        worker_id,
                        WorkerCommand::AddConnection {
                            connection_id,
                            protocol,
                            socket,
                        },
                    )?;
                    self.upsert_connection_owner(connection_id, worker_id);
                    self.mark_rebalance_dirty();
                    accepted = accepted.saturating_add(1);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    return Err(DflyError::Io(format!("accept connection failed: {error}")));
                }
            }
        }
        Ok(accepted)
    }

    fn dispatch_to_worker(&self, worker_id: usize, command: WorkerCommand) -> DflyResult<()> {
        let Some(worker) = self.workers.get(worker_id) else {
            return Err(DflyError::InvalidState("io worker index is out of range"));
        };
        worker.sender.send(command).map_err(|_| {
            DflyError::Io(format!(
                "send command to io worker {worker_id} failed because worker is unavailable"
            ))
        })
    }

    fn allocate_connection_id(&mut self) -> u64 {
        let connection_id = self.next_connection_id;
        self.next_connection_id = self.next_connection_id.saturating_add(1);
        connection_id
    }

    fn upsert_connection_owner(&mut self, connection_id: u64, worker_id: usize) {
        connection_owner_upsert(
            &mut self.connection_owner_by_id,
            &mut self.connection_counts_by_worker,
            &mut self.owned_connections_by_worker,
            connection_id,
            worker_id,
        );
    }

    fn remove_connection_owner(&mut self, connection_id: u64) -> Option<usize> {
        connection_owner_remove(
            &mut self.connection_owner_by_id,
            &mut self.connection_counts_by_worker,
            &mut self.owned_connections_by_worker,
            connection_id,
        )
    }

    fn allocate_worker_index(&mut self, socket: &TcpStream) -> usize {
        if self.use_peer_hash_dispatch
            && let Some(worker_id) = self.select_worker_by_peer_hash(socket)
        {
            return worker_id;
        }
        let offset = self.next_rr_offset % self.dispatch_window.count.max(1);
        self.next_rr_offset = (self.next_rr_offset + 1) % self.dispatch_window.count.max(1);
        (self.dispatch_window.start + offset) % self.workers.len().max(1)
    }

    fn select_worker_by_peer_hash(&self, socket: &TcpStream) -> Option<usize> {
        // Current "affinity" mode is stable peer-hash dispatch, not SO_INCOMING_CPU lookup.
        // This keeps selection deterministic without unsafe platform-specific socket calls.
        let peer = socket.peer_addr().ok()?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        peer.hash(&mut hasher);
        let bucket = usize::try_from(hasher.finish()).ok()?;
        let offset = bucket % self.dispatch_window.count.max(1);
        Some((self.dispatch_window.start + offset) % self.workers.len().max(1))
    }

    fn drain_worker_events(&mut self) {
        while let Ok(WorkerEvent::ConnectionClosed { connection_id }) =
            self.worker_events.try_recv()
        {
            if self.remove_connection_owner(connection_id).is_some() {
                self.mark_rebalance_dirty();
            }
            let _ = self.migrated_connections.remove(&connection_id);
        }
    }

    fn mark_rebalance_dirty(&mut self) {
        self.rebalance_dirty = true;
        // Start with an immediate scan after topology changes, then fall back to throttled scans.
        self.rebalance_scan_ticks = self.rebalance_scan_interval_ticks;
    }

    fn maybe_rebalance_connections_if_needed(&mut self) {
        if !self.rebalance_dirty || !self.migration_enabled {
            return;
        }
        self.rebalance_scan_ticks = self.rebalance_scan_ticks.saturating_add(1);
        if self.rebalance_scan_ticks < self.rebalance_scan_interval_ticks {
            return;
        }
        self.rebalance_scan_ticks = 0;
        self.rebalance_dirty = self.maybe_rebalance_connections().unwrap_or(true);
    }

    fn dispatch_adopt_for_migration(
        &self,
        worker_id: usize,
        migrated: MigratedWorkerConnection,
    ) -> Result<MigrationAdoptReceiver, MigrationDispatchFailure> {
        let Some(worker) = self.workers.get(worker_id) else {
            return Err(MigrationDispatchFailure {
                error: DflyError::InvalidState("io worker index is out of range"),
                connection: Box::new(migrated),
            });
        };
        let (response_sender, response_receiver) = mpsc::channel::<MigrationAdoptResult>();
        let command = WorkerCommand::AdoptMigratedConnection {
            connection: Box::new(migrated),
            response: response_sender,
        };
        match worker.sender.send(command) {
            Ok(()) => Ok(response_receiver),
            Err(send_error) => {
                let WorkerCommand::AdoptMigratedConnection { connection, .. } = send_error.0 else {
                    unreachable!("send error must return the same command variant");
                };
                Err(MigrationDispatchFailure {
                    error: DflyError::Io(format!(
                        "send migration adopt command to io worker {worker_id} failed because worker is unavailable"
                    )),
                    connection,
                })
            }
        }
    }

    fn rollback_migration_to_source(
        &self,
        source_worker: usize,
        migrated: MigratedWorkerConnection,
    ) -> DflyResult<()> {
        let receiver = match self.dispatch_adopt_for_migration(source_worker, migrated) {
            Ok(receiver) => receiver,
            Err(error) => {
                return Err(DflyError::Io(format!(
                    "migration rollback failed before reaching source worker: {}",
                    error.error
                )));
            }
        };
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(DflyError::Io(
                "migration rollback failed because source worker rejected restored connection"
                    .to_owned(),
            )),
            Err(_) => Err(DflyError::InvalidState(
                "migration rollback timed out while restoring source worker",
            )),
        }
    }

    fn maybe_rebalance_connections(&mut self) -> Result<bool, ()> {
        if !self.migration_enabled
            || self.workers.len() < 2
            || self.connection_owner_by_id.len() < 2
        {
            return Ok(false);
        }
        let Some((source_worker, target_worker)) =
            rebalance_source_and_target(&self.connection_counts_by_worker)
        else {
            return Ok(false);
        };

        let Some(connection_id) = select_rebalance_connection_id(
            &mut self.owned_connections_by_worker,
            source_worker,
            &self.migrated_connections,
        )
        else {
            return Ok(false);
        };
        self.migrate_connection(connection_id, target_worker)
            .map(|()| true)
            .map_err(|_| ())
    }

    /// Migrates one live connection to a target worker using constrained Dragonfly-style rules.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when migration is disabled, the connection is unknown,
    /// migration already happened for this connection, or source/target worker handoff fails.
    pub fn migrate_connection(
        &mut self,
        connection_id: u64,
        target_worker: usize,
    ) -> DflyResult<()> {
        if !self.migration_enabled {
            return Err(DflyError::InvalidState(
                "connection migration is disabled by config",
            ));
        }
        if self.migrated_connections.contains(&connection_id) {
            return Err(DflyError::InvalidState(
                "connection has already been migrated once",
            ));
        }
        let Some(source_worker) = self.connection_owner_by_id.get(&connection_id).copied() else {
            return Err(DflyError::InvalidState(
                "connection is not tracked by acceptor",
            ));
        };
        if source_worker == target_worker {
            return Ok(());
        }
        if target_worker >= self.workers.len() {
            return Err(DflyError::InvalidState(
                "target worker index is out of range",
            ));
        }

        let (reply_sender, reply_receiver) =
            mpsc::channel::<Result<MigratedWorkerConnection, String>>();
        self.dispatch_to_worker(
            source_worker,
            WorkerCommand::TakeConnectionForMigration {
                connection_id,
                response: reply_sender,
            },
        )?;
        let migrated = reply_receiver
            .recv_timeout(Duration::from_secs(1))
            .map_err(|_| {
                DflyError::InvalidState("migration source worker timed out while detaching")
            })?
            .map_err(|message| {
                DflyError::Io(format!(
                    "migration source worker rejected handoff: {message}"
                ))
            })?;
        let adopt_receiver = match self.dispatch_adopt_for_migration(target_worker, migrated) {
            Ok(receiver) => receiver,
            Err(dispatch_error) => {
                self.rollback_migration_to_source(source_worker, *dispatch_error.connection)?;
                return Err(dispatch_error.error);
            }
        };
        match adopt_receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(())) => {}
            Ok(Err(rollback_connection)) => {
                self.rollback_migration_to_source(source_worker, *rollback_connection)?;
                return Err(DflyError::Io(
                    "migration target worker rejected handoff".to_owned(),
                ));
            }
            Err(_) => {
                return Err(DflyError::InvalidState(
                    "migration target worker timed out while adopting connection",
                ));
            }
        }
        self.upsert_connection_owner(connection_id, target_worker);
        let _ = self.migrated_connections.insert(connection_id);
        self.mark_rebalance_dirty();
        Ok(())
    }
}

/// `mio` backend implementation for the top-level network engine.
pub struct MioBackend {
    reactor: ThreadedServerReactor,
}

impl MioBackend {
    /// Binds listeners/workers for the mio backend.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when listener or worker startup fails.
    pub fn bind_with_memcache(
        resp_addr: SocketAddr,
        memcache_addr: Option<SocketAddr>,
        config: ServerReactorConfig,
        app_executor: &AppExecutor,
    ) -> DflyResult<Self> {
        Ok(Self {
            reactor: ThreadedServerReactor::bind_with_memcache(
                resp_addr,
                memcache_addr,
                config,
                app_executor,
            )?,
        })
    }

    /// Runs one poll cycle of the mio backend.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when polling fails.
    pub fn poll_once(&mut self, timeout: Option<Duration>) -> DflyResult<usize> {
        self.reactor.poll_once(timeout)
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
enum IoUringWorkerCommand {
    AddConnection {
        connection_id: u64,
        protocol: ClientProtocol,
        socket: StdTcpStream,
    },
    TakeConnectionForMigration {
        connection_id: u64,
        response: Sender<Result<IoUringMigratedConnection, String>>,
    },
    AdoptMigratedConnection {
        connection: Box<IoUringMigratedConnection>,
        response: IoUringMigrationAdoptSender,
    },
    Shutdown,
}

#[cfg(target_os = "linux")]
type IoUringMigrationAdoptResult = Result<(), Box<IoUringMigratedConnection>>;
#[cfg(target_os = "linux")]
type IoUringMigrationAdoptSender = Sender<IoUringMigrationAdoptResult>;
#[cfg(target_os = "linux")]
type IoUringMigrationAdoptReceiver = Receiver<IoUringMigrationAdoptResult>;
#[cfg(target_os = "linux")]
type IoUringDetachedRuntimeTickets = Arc<std::sync::Mutex<Vec<RuntimeReplyTicket>>>;

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct IoUringConnectionTaskContext {
    app_executor: AppExecutor,
    backpressure: BackpressureThresholds,
    worker_event_sender: Sender<IoUringWorkerEvent>,
    worker_pending_requests: Arc<AtomicUsize>,
    detached_runtime_tickets: IoUringDetachedRuntimeTickets,
    closed_sender: TokioUnboundedSender<u64>,
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct IoUringWorkerLoopContext {
    app_executor: AppExecutor,
    backpressure: BackpressureThresholds,
    worker_event_sender: Sender<IoUringWorkerEvent>,
    worker_pending_requests: Arc<AtomicUsize>,
    detached_runtime_tickets: IoUringDetachedRuntimeTickets,
    closed_sender: TokioUnboundedSender<u64>,
}

#[cfg(target_os = "linux")]
impl IoUringWorkerLoopContext {
    fn to_connection_task_context(&self) -> IoUringConnectionTaskContext {
        IoUringConnectionTaskContext {
            app_executor: self.app_executor.clone(),
            backpressure: self.backpressure,
            worker_event_sender: self.worker_event_sender.clone(),
            worker_pending_requests: Arc::clone(&self.worker_pending_requests),
            detached_runtime_tickets: Arc::clone(&self.detached_runtime_tickets),
            closed_sender: self.closed_sender.clone(),
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct IoUringMigrationDispatchFailure {
    error: DflyError,
    connection: Box<IoUringMigratedConnection>,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
enum IoUringWorkerEvent {
    ConnectionClosed { connection_id: u64 },
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
enum IoUringConnectionControl {
    Migrate {
        response: oneshot::Sender<Result<IoUringMigratedConnection, String>>,
    },
    Shutdown,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct IoUringMigratedConnection {
    connection_id: u64,
    socket: StdTcpStream,
    logical: ServerConnection,
    write_buffer: Vec<u8>,
    write_cursor: usize,
    next_request_id: u64,
    pending_request_count: usize,
    pending_runtime_requests: Vec<PendingRuntimeRequest>,
    reply_order: ConnectionReplyOrder,
    read_paused_by_backpressure: bool,
    read_paused_by_command_backpressure: bool,
    draining: bool,
}

#[cfg(target_os = "linux")]
impl IoUringMigratedConnection {
    fn new(connection_id: u64, socket: StdTcpStream, protocol: ClientProtocol) -> Self {
        Self {
            connection_id,
            socket,
            logical: ServerApp::new_connection(protocol),
            write_buffer: Vec::new(),
            write_cursor: 0,
            next_request_id: 1,
            pending_request_count: 0,
            pending_runtime_requests: Vec::new(),
            reply_order: ConnectionReplyOrder::new(),
            read_paused_by_backpressure: false,
            read_paused_by_command_backpressure: false,
            draining: false,
        }
    }

    fn can_read(&self) -> bool {
        !self.draining && !self.read_paused_by_backpressure && !self.read_paused_by_command_backpressure
    }

    fn should_close_now(&self) -> bool {
        self.draining
            && self.pending_request_count == 0
            && write_buffer_is_empty(&self.write_buffer, self.write_cursor)
    }

    fn update_backpressure_state(&mut self, high_watermark: usize, low_watermark: usize) {
        let buffered_bytes = write_buffer_pending_len(&self.write_buffer, self.write_cursor);
        if self.read_paused_by_backpressure {
            if buffered_bytes <= low_watermark {
                self.read_paused_by_backpressure = false;
            }
            return;
        }
        if buffered_bytes >= high_watermark {
            self.read_paused_by_backpressure = true;
        }
    }

    fn update_command_backpressure_state(
        &mut self,
        pending_for_worker: usize,
        max_pending_per_connection: usize,
        max_pending_per_worker: usize,
    ) {
        self.read_paused_by_command_backpressure = self.pending_request_count
            >= max_pending_per_connection
            || pending_for_worker >= max_pending_per_worker;
    }
}

#[cfg(target_os = "linux")]
struct IoUringWorkerHandle {
    sender: TokioUnboundedSender<IoUringWorkerCommand>,
    join: Option<JoinHandle<()>>,
}

#[cfg(target_os = "linux")]
/// Linux `io_uring` backend implementation.
///
/// The acceptor remains readiness-driven (`mio`), while each worker runs one
/// `tokio-uring` runtime and owns socket read/write/completion loops.
pub struct IoUringBackend {
    acceptor_poll: Poll,
    acceptor_events: Events,
    ready_listener_indexes: Vec<usize>,
    listeners: Vec<ReactorListener>,
    workers: Vec<IoUringWorkerHandle>,
    worker_events: Receiver<IoUringWorkerEvent>,
    dispatch_window: WorkerDispatchWindow,
    next_rr_offset: usize,
    use_peer_hash_dispatch: bool,
    migration_enabled: bool,
    rebalance_scan_ticks: usize,
    rebalance_scan_interval_ticks: usize,
    rebalance_dirty: bool,
    next_connection_id: u64,
    connection_owner_by_id: HashMap<u64, usize>,
    connection_counts_by_worker: Vec<usize>,
    owned_connections_by_worker: Vec<WorkerOwnedConnections>,
    migrated_connections: HashSet<u64>,
}

#[cfg(target_os = "linux")]
impl IoUringBackend {
    /// Binds listeners/workers for the `io_uring` backend.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidConfig` or `DflyError::Io` when setup fails.
    pub fn bind_with_memcache(
        resp_addr: SocketAddr,
        memcache_addr: Option<SocketAddr>,
        config: ServerReactorConfig,
        app_executor: &AppExecutor,
    ) -> DflyResult<Self> {
        let acceptor_poll =
            Poll::new().map_err(|error| DflyError::Io(format!("create poll failed: {error}")))?;
        let listeners = bind_acceptor_listeners(&acceptor_poll, resp_addr, memcache_addr)?;
        let backpressure = resolve_backpressure_thresholds(config)?;
        let worker_count = config.normalized_io_worker_count();
        let dispatch_window = WorkerDispatchWindow {
            start: config.normalized_dispatch_start(worker_count),
            count: config.normalized_dispatch_count(worker_count),
        };
        let max_events = config.normalized_max_events();
        let (worker_event_sender, worker_event_receiver) = mpsc::channel::<IoUringWorkerEvent>();
        let workers = Self::spawn_workers(worker_count, app_executor, backpressure, &worker_event_sender)?;

        Ok(Self {
            acceptor_poll,
            acceptor_events: Events::with_capacity(max_events),
            ready_listener_indexes: Vec::with_capacity(max_events),
            listeners,
            workers,
            worker_events: worker_event_receiver,
            dispatch_window,
            next_rr_offset: 0,
            use_peer_hash_dispatch: config.use_peer_hash_dispatch,
            migration_enabled: config.enable_connection_migration,
            rebalance_scan_ticks: 0,
            rebalance_scan_interval_ticks: DEFAULT_REBALANCE_SCAN_INTERVAL_TICKS,
            rebalance_dirty: false,
            next_connection_id: 1,
            connection_owner_by_id: HashMap::new(),
            connection_counts_by_worker: vec![0; worker_count],
            owned_connections_by_worker: (0..worker_count)
                .map(|_| WorkerOwnedConnections::default())
                .collect(),
            migrated_connections: HashSet::new(),
        })
    }

    fn spawn_workers(
        worker_count: usize,
        app_executor: &AppExecutor,
        backpressure: BackpressureThresholds,
        worker_event_sender: &Sender<IoUringWorkerEvent>,
    ) -> DflyResult<Vec<IoUringWorkerHandle>> {
        let mut workers = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let (sender, receiver) = tokio_unbounded_channel::<IoUringWorkerCommand>();
            let worker_app = app_executor.clone();
            let worker_backpressure = backpressure;
            let worker_event_sender = worker_event_sender.clone();
            let worker_pending_requests = Arc::new(AtomicUsize::new(0));
            let join = thread::Builder::new()
                .name(format!("dfly-io-uring-worker-{worker_id}"))
                .spawn(move || {
                    io_uring_worker_thread_main(
                        receiver,
                        worker_app,
                        worker_backpressure,
                        worker_event_sender,
                        worker_pending_requests,
                    );
                })
                .map_err(|error| {
                    DflyError::Io(format!("spawn io_uring worker {worker_id} failed: {error}"))
                })?;
            workers.push(IoUringWorkerHandle {
                sender,
                join: Some(join),
            });
        }
        Ok(workers)
    }

    /// Runs one acceptor cycle and dispatches accepted sockets to `io_uring` workers.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when polling/accept/dispatch fails.
    pub fn poll_once(&mut self, timeout: Option<Duration>) -> DflyResult<usize> {
        self.acceptor_poll
            .poll(&mut self.acceptor_events, timeout)
            .map_err(|error| DflyError::Io(format!("poll wait failed: {error}")))?;
        let mut observed = 0_usize;
        self.ready_listener_indexes.clear();
        for event in &self.acceptor_events {
            observed = observed.saturating_add(1);
            if let Some(listener_index) = self.listener_index(event.token()) {
                self.ready_listener_indexes.push(listener_index);
            }
        }
        let mut accepted = 0_usize;
        let ready_count = self.ready_listener_indexes.len();
        for index in 0..ready_count {
            let listener_index = self.ready_listener_indexes[index];
            accepted = accepted.saturating_add(self.accept_new_connections(listener_index)?);
        }
        self.drain_worker_events();
        self.maybe_rebalance_connections_if_needed();
        Ok(observed.saturating_add(accepted))
    }

    /// Runs accept loop and dispatches accepted sockets to `io_uring` workers.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when acceptor poll/accept/dispatch fails.
    pub fn run(&mut self) -> DflyResult<()> {
        loop {
            let _ = self.poll_once(Some(Duration::from_millis(10)))?;
        }
    }

    fn listener_index(&self, token: Token) -> Option<usize> {
        self.listeners
            .iter()
            .position(|listener| listener.token == token)
    }

    fn accept_new_connections(&mut self, listener_index: usize) -> DflyResult<usize> {
        let protocol = self
            .listeners
            .get(listener_index)
            .map(|listener| listener.protocol)
            .ok_or(DflyError::InvalidState(
                "io_uring listener index is out of range",
            ))?;
        let mut accepted = 0_usize;

        loop {
            let accept_result = {
                let listener =
                    self.listeners
                        .get_mut(listener_index)
                        .ok_or(DflyError::InvalidState(
                            "io_uring listener index is out of range",
                        ))?;
                listener.socket.accept()
            };

            match accept_result {
                Ok((socket, _peer)) => {
                    let _ = socket.set_nodelay(true);
                    let connection_id = self.allocate_connection_id();
                    let worker_id = self.allocate_worker_index(&socket);
                    let socket = mio_stream_into_std(socket);
                    self.dispatch_to_worker(
                        worker_id,
                        IoUringWorkerCommand::AddConnection {
                            connection_id,
                            protocol,
                            socket,
                        },
                    )?;
                    self.upsert_connection_owner(connection_id, worker_id);
                    self.mark_rebalance_dirty();
                    accepted = accepted.saturating_add(1);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(accepted);
                }
                Err(error) => {
                    return Err(DflyError::Io(format!("accept connection failed: {error}")));
                }
            }
        }
    }

    fn dispatch_to_worker(
        &self,
        worker_id: usize,
        command: IoUringWorkerCommand,
    ) -> DflyResult<()> {
        let Some(worker) = self.workers.get(worker_id) else {
            return Err(DflyError::InvalidState(
                "io_uring worker index is out of range",
            ));
        };
        worker.sender.send(command).map_err(|_| {
            DflyError::Io(format!(
                "send command to io_uring worker {worker_id} failed because worker is unavailable"
            ))
        })
    }

    fn allocate_connection_id(&mut self) -> u64 {
        let connection_id = self.next_connection_id;
        self.next_connection_id = self.next_connection_id.saturating_add(1);
        connection_id
    }

    fn upsert_connection_owner(&mut self, connection_id: u64, worker_id: usize) {
        connection_owner_upsert(
            &mut self.connection_owner_by_id,
            &mut self.connection_counts_by_worker,
            &mut self.owned_connections_by_worker,
            connection_id,
            worker_id,
        );
    }

    fn remove_connection_owner(&mut self, connection_id: u64) -> Option<usize> {
        connection_owner_remove(
            &mut self.connection_owner_by_id,
            &mut self.connection_counts_by_worker,
            &mut self.owned_connections_by_worker,
            connection_id,
        )
    }

    fn allocate_worker_index(&mut self, socket: &TcpStream) -> usize {
        if self.use_peer_hash_dispatch
            && let Some(worker_id) = self.select_worker_by_peer_hash(socket)
        {
            return worker_id;
        }
        let offset = self.next_rr_offset % self.dispatch_window.count.max(1);
        self.next_rr_offset = (self.next_rr_offset + 1) % self.dispatch_window.count.max(1);
        (self.dispatch_window.start + offset) % self.workers.len().max(1)
    }

    fn select_worker_by_peer_hash(&self, socket: &TcpStream) -> Option<usize> {
        let peer = socket.peer_addr().ok()?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        peer.hash(&mut hasher);
        let span = self.dispatch_window.count.max(1);
        let span_u64 = u64::try_from(span).ok()?;
        let bucket_u64 = hasher.finish() % span_u64;
        let bucket = usize::try_from(bucket_u64).ok()?;
        Some((self.dispatch_window.start + bucket) % self.workers.len().max(1))
    }

    fn drain_worker_events(&mut self) {
        while let Ok(IoUringWorkerEvent::ConnectionClosed { connection_id }) =
            self.worker_events.try_recv()
        {
            if self.remove_connection_owner(connection_id).is_some() {
                self.mark_rebalance_dirty();
            }
            let _ = self.migrated_connections.remove(&connection_id);
        }
    }

    fn mark_rebalance_dirty(&mut self) {
        self.rebalance_dirty = true;
        // Start with an immediate scan after topology changes, then fall back to throttled scans.
        self.rebalance_scan_ticks = self.rebalance_scan_interval_ticks;
    }

    fn maybe_rebalance_connections_if_needed(&mut self) {
        if !self.rebalance_dirty || !self.migration_enabled {
            return;
        }
        self.rebalance_scan_ticks = self.rebalance_scan_ticks.saturating_add(1);
        if self.rebalance_scan_ticks < self.rebalance_scan_interval_ticks {
            return;
        }
        self.rebalance_scan_ticks = 0;
        self.rebalance_dirty = self.maybe_rebalance_connections().unwrap_or(true);
    }

    fn dispatch_adopt_for_migration(
        &self,
        worker_id: usize,
        migrated: IoUringMigratedConnection,
    ) -> Result<IoUringMigrationAdoptReceiver, IoUringMigrationDispatchFailure> {
        let Some(worker) = self.workers.get(worker_id) else {
            return Err(IoUringMigrationDispatchFailure {
                error: DflyError::InvalidState("io_uring worker index is out of range"),
                connection: Box::new(migrated),
            });
        };
        let (response_sender, response_receiver) = mpsc::channel::<IoUringMigrationAdoptResult>();
        let command = IoUringWorkerCommand::AdoptMigratedConnection {
            connection: Box::new(migrated),
            response: response_sender,
        };
        match worker.sender.send(command) {
            Ok(()) => Ok(response_receiver),
            Err(send_error) => {
                let IoUringWorkerCommand::AdoptMigratedConnection { connection, .. } = send_error.0
                else {
                    unreachable!("send error must return the same command variant");
                };
                Err(IoUringMigrationDispatchFailure {
                    error: DflyError::Io(format!(
                        "send migration adopt command to io_uring worker {worker_id} failed because worker is unavailable"
                    )),
                    connection,
                })
            }
        }
    }

    fn rollback_migration_to_source(
        &self,
        source_worker: usize,
        migrated: IoUringMigratedConnection,
    ) -> DflyResult<()> {
        let receiver = match self.dispatch_adopt_for_migration(source_worker, migrated) {
            Ok(receiver) => receiver,
            Err(error) => {
                return Err(DflyError::Io(format!(
                    "migration rollback failed before reaching source io_uring worker: {}",
                    error.error
                )));
            }
        };
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(DflyError::Io(
                "migration rollback failed because source io_uring worker rejected restored connection"
                    .to_owned(),
            )),
            Err(_) => Err(DflyError::InvalidState(
                "migration rollback timed out while restoring source io_uring worker",
            )),
        }
    }

    fn maybe_rebalance_connections(&mut self) -> Result<bool, ()> {
        if !self.migration_enabled
            || self.workers.len() < 2
            || self.connection_owner_by_id.len() < 2
        {
            return Ok(false);
        }
        let Some((source_worker, target_worker)) =
            rebalance_source_and_target(&self.connection_counts_by_worker)
        else {
            return Ok(false);
        };

        let Some(connection_id) = select_rebalance_connection_id(
            &mut self.owned_connections_by_worker,
            source_worker,
            &self.migrated_connections,
        )
        else {
            return Ok(false);
        };
        self.migrate_connection(connection_id, target_worker)
            .map(|()| true)
            .map_err(|_| ())
    }

    /// Migrates one live connection to a target worker using constrained Dragonfly-style rules.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when migration is disabled, the connection is unknown,
    /// migration already happened for this connection, or source/target worker handoff fails.
    pub fn migrate_connection(
        &mut self,
        connection_id: u64,
        target_worker: usize,
    ) -> DflyResult<()> {
        if !self.migration_enabled {
            return Err(DflyError::InvalidState(
                "connection migration is disabled by config",
            ));
        }
        if self.migrated_connections.contains(&connection_id) {
            return Err(DflyError::InvalidState(
                "connection has already been migrated once",
            ));
        }
        let Some(source_worker) = self.connection_owner_by_id.get(&connection_id).copied() else {
            return Err(DflyError::InvalidState(
                "connection is not tracked by io_uring acceptor",
            ));
        };
        if source_worker == target_worker {
            return Ok(());
        }
        if target_worker >= self.workers.len() {
            return Err(DflyError::InvalidState(
                "target io_uring worker index is out of range",
            ));
        }

        let (reply_sender, reply_receiver) =
            mpsc::channel::<Result<IoUringMigratedConnection, String>>();
        self.dispatch_to_worker(
            source_worker,
            IoUringWorkerCommand::TakeConnectionForMigration {
                connection_id,
                response: reply_sender,
            },
        )?;
        let migrated = reply_receiver
            .recv_timeout(Duration::from_secs(1))
            .map_err(|_| {
                DflyError::InvalidState(
                    "migration source io_uring worker timed out while detaching",
                )
            })?
            .map_err(|message| {
                DflyError::Io(format!(
                    "migration source io_uring worker rejected handoff: {message}"
                ))
            })?;
        let adopt_receiver = match self.dispatch_adopt_for_migration(target_worker, migrated) {
            Ok(receiver) => receiver,
            Err(dispatch_error) => {
                self.rollback_migration_to_source(source_worker, *dispatch_error.connection)?;
                return Err(dispatch_error.error);
            }
        };
        match adopt_receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(())) => {}
            Ok(Err(rollback_connection)) => {
                self.rollback_migration_to_source(source_worker, *rollback_connection)?;
                return Err(DflyError::Io(
                    "migration target io_uring worker rejected handoff".to_owned(),
                ));
            }
            Err(_) => {
                return Err(DflyError::InvalidState(
                    "migration target io_uring worker timed out while adopting connection",
                ));
            }
        }
        self.upsert_connection_owner(connection_id, target_worker);
        let _ = self.migrated_connections.insert(connection_id);
        self.mark_rebalance_dirty();
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Drop for IoUringBackend {
    fn drop(&mut self) {
        for worker in &self.workers {
            let _ = worker.sender.send(IoUringWorkerCommand::Shutdown);
        }
        for worker in &mut self.workers {
            if let Some(join) = worker.join.take() {
                let _ = join.join();
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn mio_stream_into_std(stream: TcpStream) -> StdTcpStream {
    let owned: OwnedFd = stream.into();
    owned.into()
}

#[cfg(target_os = "linux")]
fn io_uring_worker_thread_main(
    mut receiver: TokioUnboundedReceiver<IoUringWorkerCommand>,
    app_executor: AppExecutor,
    backpressure: BackpressureThresholds,
    worker_event_sender: Sender<IoUringWorkerEvent>,
    worker_pending_requests: Arc<AtomicUsize>,
) {
    tokio_uring::start(async move {
        let mut controls = HashMap::<u64, TokioUnboundedSender<IoUringConnectionControl>>::new();
        let (closed_sender, mut closed_receiver) = tokio_unbounded_channel::<u64>();
        let detached_runtime_tickets: IoUringDetachedRuntimeTickets =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let context = IoUringWorkerLoopContext {
            app_executor,
            backpressure,
            worker_event_sender,
            worker_pending_requests,
            detached_runtime_tickets,
            closed_sender,
        };

        loop {
            io_uring_reap_detached_runtime_replies(
                &context.app_executor,
                &context.detached_runtime_tickets,
            );
            io_uring_prune_closed_controls(&mut controls, &mut closed_receiver);
            let command = match tokio::time::timeout(Duration::from_millis(5), receiver.recv()).await {
                Ok(Some(command)) => command,
                Ok(None) => break,
                Err(_) => continue,
            };
            if !io_uring_dispatch_worker_command(command, &mut controls, &context).await {
                break;
            }
        }
    });
}

#[cfg(target_os = "linux")]
fn io_uring_prune_closed_controls(
    controls: &mut HashMap<u64, TokioUnboundedSender<IoUringConnectionControl>>,
    closed_receiver: &mut TokioUnboundedReceiver<u64>,
) {
    while let Ok(connection_id) = closed_receiver.try_recv() {
        let _ = controls.remove(&connection_id);
    }
}

#[cfg(target_os = "linux")]
async fn io_uring_dispatch_worker_command(
    command: IoUringWorkerCommand,
    controls: &mut HashMap<u64, TokioUnboundedSender<IoUringConnectionControl>>,
    context: &IoUringWorkerLoopContext,
) -> bool {
    match command {
        IoUringWorkerCommand::AddConnection {
            connection_id,
            protocol,
            socket,
        } => {
            if controls.contains_key(&connection_id) {
                let _ = context
                    .worker_event_sender
                    .send(IoUringWorkerEvent::ConnectionClosed { connection_id });
                return true;
            }
            let control = spawn_io_uring_connection_task(
                IoUringMigratedConnection::new(connection_id, socket, protocol),
                context.to_connection_task_context(),
            );
            let _ = controls.insert(connection_id, control);
            true
        }
        IoUringWorkerCommand::TakeConnectionForMigration {
            connection_id,
            response,
        } => {
            io_uring_handle_take_connection_for_migration(connection_id, response, controls).await;
            true
        }
        IoUringWorkerCommand::AdoptMigratedConnection {
            connection,
            response,
        } => {
            let connection = *connection;
            let connection_id = connection.connection_id;
            if controls.contains_key(&connection_id) {
                let _ = response.send(Err(Box::new(connection)));
                return true;
            }
            let control =
                spawn_io_uring_connection_task(connection, context.to_connection_task_context());
            let _ = controls.insert(connection_id, control);
            let _ = response.send(Ok(()));
            true
        }
        IoUringWorkerCommand::Shutdown => {
            for control in controls.values() {
                let _ = control.send(IoUringConnectionControl::Shutdown);
            }
            false
        }
    }
}

#[cfg(target_os = "linux")]
async fn io_uring_handle_take_connection_for_migration(
    connection_id: u64,
    response: Sender<Result<IoUringMigratedConnection, String>>,
    controls: &mut HashMap<u64, TokioUnboundedSender<IoUringConnectionControl>>,
) {
    let Some(control) = controls.remove(&connection_id) else {
        let _ = response.send(Err("connection is not tracked by io_uring worker".to_owned()));
        return;
    };
    let (migrate_sender, migrate_receiver) =
        oneshot::channel::<Result<IoUringMigratedConnection, String>>();
    if control
        .send(IoUringConnectionControl::Migrate {
            response: migrate_sender,
        })
        .is_err()
    {
        let _ = response.send(Err(
            "connection task is unavailable while migrating".to_owned(),
        ));
        return;
    }
    let migration = match tokio::time::timeout(Duration::from_secs(1), migrate_receiver).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("connection task dropped migration response".to_owned()),
        Err(_) => Err("io_uring connection task timed out during migration".to_owned()),
    };
    match migration {
        Ok(connection) => {
            let _ = response.send(Ok(connection));
        }
        Err(message) => {
            let _ = controls.insert(connection_id, control);
            let _ = response.send(Err(message));
        }
    }
}

#[cfg(target_os = "linux")]
fn spawn_io_uring_connection_task(
    connection: IoUringMigratedConnection,
    context: IoUringConnectionTaskContext,
) -> TokioUnboundedSender<IoUringConnectionControl> {
    let (control_sender, control_receiver) = tokio_unbounded_channel::<IoUringConnectionControl>();
    tokio_uring::spawn(async move {
        io_uring_connection_main(connection, control_receiver, context).await;
    });
    control_sender
}

#[cfg(target_os = "linux")]
fn io_uring_stream_from_connection_socket(
    socket: &StdTcpStream,
) -> Result<IoUringTcpStream, std::io::Error> {
    let cloned = socket.try_clone()?;
    Ok(IoUringTcpStream::from_std(cloned))
}

#[cfg(target_os = "linux")]
async fn io_uring_connection_main(
    mut connection: IoUringMigratedConnection,
    mut control_receiver: TokioUnboundedReceiver<IoUringConnectionControl>,
    context: IoUringConnectionTaskContext,
) {
    let Ok(stream) = io_uring_stream_from_connection_socket(&connection.socket) else {
        io_uring_finalize_connection_with_context(&mut connection, &context).await;
        return;
    };

    loop {
        let mut should_shutdown = false;
        loop {
            match control_receiver.try_recv() {
                Ok(IoUringConnectionControl::Migrate { response }) => {
                    if can_migrate_io_uring_connection(&connection) {
                        let _ = response.send(Ok(connection));
                        return;
                    }
                    let _ = response.send(Err("connection state does not allow migration".to_owned()));
                }
                Ok(IoUringConnectionControl::Shutdown)
                | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    should_shutdown = true;
                    break;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            }
        }
        if should_shutdown {
            break;
        }

        let made_progress = io_uring_progress_connection_turn(&stream, &context, &mut connection).await;
        if connection.should_close_now() {
            break;
        }
        if !made_progress {
            tokio::task::yield_now().await;
        }
    }
    io_uring_finalize_connection_with_context(&mut connection, &context).await;
}

#[cfg(target_os = "linux")]
async fn io_uring_progress_connection_turn(
    stream: &IoUringTcpStream,
    context: &IoUringConnectionTaskContext,
    connection: &mut IoUringMigratedConnection,
) -> bool {
    let mut made_progress = false;
    if io_uring_drain_pending_runtime_replies(
        &context.app_executor,
        connection,
        context.backpressure,
        &context.worker_pending_requests,
    ) {
        made_progress = true;
    }

    let pending_for_worker = context.worker_pending_requests.load(Ordering::Acquire);
    connection.update_command_backpressure_state(
        pending_for_worker,
        context.backpressure.max_pending_per_connection,
        context.backpressure.max_pending_per_worker,
    );
    if io_uring_drain_parsed_commands(
        &context.app_executor,
        connection,
        context.backpressure,
        &context.worker_pending_requests,
    ) {
        made_progress = true;
    }
    if io_uring_flush_connection_writes(stream, connection, context.backpressure).await {
        made_progress = true;
    }
    if connection.can_read() {
        match io_uring_connection_has_readable_bytes(&connection.socket) {
            Ok(true) => {
                if io_uring_read_connection_bytes(
                    stream,
                    &context.app_executor,
                    connection,
                    context.backpressure,
                    &context.worker_pending_requests,
                )
                .await
                {
                    made_progress = true;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Ok(false) | Err(_) => {
                connection.draining = true;
                made_progress = true;
            }
        }
    }
    made_progress
}

#[cfg(target_os = "linux")]
async fn io_uring_finalize_connection_with_context(
    connection: &mut IoUringMigratedConnection,
    context: &IoUringConnectionTaskContext,
) {
    let connection_id = connection.connection_id;
    io_uring_finalize_connection(
        &context.app_executor,
        connection,
        context.backpressure,
        &context.worker_pending_requests,
        &context.worker_event_sender,
        &context.detached_runtime_tickets,
    )
    .await;
    let _ = context.closed_sender.send(connection_id);
}

#[cfg(target_os = "linux")]
async fn io_uring_finalize_connection(
    app_executor: &AppExecutor,
    connection: &mut IoUringMigratedConnection,
    backpressure: BackpressureThresholds,
    worker_pending_requests: &Arc<AtomicUsize>,
    worker_event_sender: &Sender<IoUringWorkerEvent>,
    detached_runtime_tickets: &IoUringDetachedRuntimeTickets,
) {
    if !connection.pending_runtime_requests.is_empty() {
        io_uring_reap_pending_runtime_replies(
            app_executor,
            connection,
            backpressure,
            worker_pending_requests,
            IO_URING_DEFERRED_REAP_MAX_WAIT,
        )
        .await;
        if !connection.pending_runtime_requests.is_empty() {
            let mut detached = detached_runtime_tickets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            detached.extend(
                connection
                    .pending_runtime_requests
                    .drain(..)
                    .map(|pending| pending.ticket),
            );
        }
    }
    if connection.pending_request_count > 0 {
        let pending = connection.pending_request_count;
        connection.pending_request_count = 0;
        let _ = worker_pending_requests.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(pending))
        });
    }
    app_executor.disconnect_connection(&mut connection.logical);
    let _ = worker_event_sender.send(IoUringWorkerEvent::ConnectionClosed {
        connection_id: connection.connection_id,
    });
}

#[cfg(target_os = "linux")]
fn io_uring_connection_has_readable_bytes(socket: &StdTcpStream) -> Result<bool, std::io::Error> {
    let mut probe = [0_u8; 1];
    match socket.peek(&mut probe) {
        Ok(0) => Ok(false),
        Ok(_) => Ok(true),
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "linux")]
async fn io_uring_read_connection_bytes(
    stream: &IoUringTcpStream,
    app_executor: &AppExecutor,
    connection: &mut IoUringMigratedConnection,
    backpressure: BackpressureThresholds,
    worker_pending_requests: &Arc<AtomicUsize>,
) -> bool {
    if !connection.can_read() {
        return false;
    }
    let read_input = vec![0_u8; READ_CHUNK_BYTES];
    let (read_result, read_output) = stream.read(read_input).await;
    match read_result {
        Ok(0) => {
            connection.draining = true;
            true
        }
        Ok(read_len) => {
            if read_len == 0 {
                return false;
            }
            connection.logical.parser.feed_bytes(&read_output[..read_len]);
            let _ = io_uring_drain_parsed_commands(
                app_executor,
                connection,
                backpressure,
                worker_pending_requests,
            );
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
        Err(_) => {
            connection.draining = true;
            false
        }
    }
}

#[cfg(target_os = "linux")]
async fn io_uring_flush_connection_writes(
    stream: &IoUringTcpStream,
    connection: &mut IoUringMigratedConnection,
    backpressure: BackpressureThresholds,
) -> bool {
    if write_buffer_is_empty(&connection.write_buffer, connection.write_cursor) {
        return false;
    }
    if let Ok(written) = io_uring_flush_write_buffer_budget(
        stream,
        &mut connection.write_buffer,
        &mut connection.write_cursor,
        WORKER_WRITE_BUDGET_BYTES_PER_EVENT,
    )
    .await
    {
        connection.update_backpressure_state(backpressure.high, backpressure.low);
        written > 0
    } else {
        connection.draining = true;
        false
    }
}

#[cfg(target_os = "linux")]
fn io_uring_drain_parsed_commands(
    app_executor: &AppExecutor,
    connection: &mut IoUringMigratedConnection,
    backpressure: BackpressureThresholds,
    worker_pending_requests: &Arc<AtomicUsize>,
) -> bool {
    let mut progressed = false;
    let mut remaining_budget = WORKER_PARSE_BUDGET_PER_READ;
    while remaining_budget > 0 {
        remaining_budget = remaining_budget.saturating_sub(1);
        let pending_for_worker = worker_pending_requests.load(Ordering::Acquire);
        connection.update_command_backpressure_state(
            pending_for_worker,
            backpressure.max_pending_per_connection,
            backpressure.max_pending_per_worker,
        );
        if !connection.can_read() {
            break;
        }
        match connection.logical.parser.try_pop_command() {
            Ok(Some(parsed)) => {
                progressed = true;
                let request_id = connection.next_request_id;
                connection.next_request_id = connection.next_request_id.saturating_add(1);
                io_uring_mark_request_submitted(connection, worker_pending_requests);
                match app_executor.execute_parsed_command_deferred(&mut connection.logical, parsed) {
                    ParsedCommandExecution::Immediate(reply) => {
                        io_uring_record_completed_request(
                            connection,
                            request_id,
                            reply,
                            backpressure,
                            worker_pending_requests,
                        );
                    }
                    ParsedCommandExecution::Deferred(ticket) => {
                        connection
                            .pending_runtime_requests
                            .push(PendingRuntimeRequest { request_id, ticket });
                    }
                }
            }
            Ok(None) => break,
            Err(error) => {
                progressed = true;
                let request_id = connection.next_request_id;
                connection.next_request_id = connection.next_request_id.saturating_add(1);
                io_uring_mark_request_submitted(connection, worker_pending_requests);
                let protocol = connection.logical.context.protocol;
                let reply = protocol_parse_error_reply(protocol, &error);
                io_uring_record_completed_request(
                    connection,
                    request_id,
                    Some(reply),
                    backpressure,
                    worker_pending_requests,
                );
                if protocol == ClientProtocol::Resp {
                    connection.draining = true;
                    break;
                }
                if !connection.logical.parser.recover_after_protocol_error() {
                    break;
                }
            }
        }
    }
    progressed
}

#[cfg(target_os = "linux")]
fn io_uring_drain_pending_runtime_replies(
    app_executor: &AppExecutor,
    connection: &mut IoUringMigratedConnection,
    backpressure: BackpressureThresholds,
    worker_pending_requests: &Arc<AtomicUsize>,
) -> bool {
    if connection.pending_runtime_requests.is_empty() {
        return false;
    }
    let processed_snapshot = match app_executor.runtime_processed_sequences_snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            while let Some(pending) = connection.pending_runtime_requests.pop() {
                let reply = runtime_dispatch_error_reply(pending.ticket.protocol, &error);
                io_uring_record_completed_request(
                    connection,
                    pending.request_id,
                    Some(reply),
                    backpressure,
                    worker_pending_requests,
                );
            }
            return true;
        }
    };
    let mut progressed = false;
    let mut index = 0_usize;
    while index < connection.pending_runtime_requests.len() {
        let ready_result = {
            let pending = &mut connection.pending_runtime_requests[index];
            app_executor.runtime_reply_ticket_ready_with_snapshot(&mut pending.ticket, &processed_snapshot)
        };
        let ready = match ready_result {
            Ok(ready) => ready,
            Err(error) => {
                let pending = connection.pending_runtime_requests.swap_remove(index);
                let reply = runtime_dispatch_error_reply(pending.ticket.protocol, &error);
                io_uring_record_completed_request(
                    connection,
                    pending.request_id,
                    Some(reply),
                    backpressure,
                    worker_pending_requests,
                );
                progressed = true;
                continue;
            }
        };
        if !ready {
            index = index.saturating_add(1);
            continue;
        }
        let mut pending = connection.pending_runtime_requests.swap_remove(index);
        let reply = match app_executor.take_runtime_reply_ticket_ready(&mut pending.ticket) {
            Ok(reply) => Some(reply),
            Err(error) => Some(runtime_dispatch_error_reply(pending.ticket.protocol, &error)),
        };
        io_uring_record_completed_request(
            connection,
            pending.request_id,
            reply,
            backpressure,
            worker_pending_requests,
        );
        progressed = true;
    }
    progressed
}

#[cfg(target_os = "linux")]
async fn io_uring_reap_pending_runtime_replies(
    app_executor: &AppExecutor,
    connection: &mut IoUringMigratedConnection,
    backpressure: BackpressureThresholds,
    worker_pending_requests: &Arc<AtomicUsize>,
    max_wait: Duration,
) {
    let deadline = std::time::Instant::now() + max_wait;
    while !connection.pending_runtime_requests.is_empty() {
        let progressed = io_uring_drain_pending_runtime_replies(
            app_executor,
            connection,
            backpressure,
            worker_pending_requests,
        );
        if !progressed {
            if std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

#[cfg(target_os = "linux")]
fn io_uring_reap_detached_runtime_replies(
    app_executor: &AppExecutor,
    detached_runtime_tickets: &IoUringDetachedRuntimeTickets,
) {
    let mut detached = {
        let mut guard = detached_runtime_tickets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.is_empty() {
            return;
        }
        std::mem::take(&mut *guard)
    };

    let processed_snapshot = app_executor.runtime_processed_sequences_snapshot().ok();
    let mut pending = Vec::new();
    for mut ticket in detached.drain(..) {
        let ready = match &processed_snapshot {
            Some(snapshot) => app_executor
                .runtime_reply_ticket_ready_with_snapshot(&mut ticket, snapshot)
                .unwrap_or(true),
            None => app_executor.runtime_reply_ticket_ready(&mut ticket).unwrap_or(true),
        };
        if !ready {
            pending.push(ticket);
            continue;
        }
        let _ = match &processed_snapshot {
            Some(_) => app_executor.take_runtime_reply_ticket_ready(&mut ticket),
            None => app_executor.take_runtime_reply_ticket(&mut ticket),
        };
    }

    if !pending.is_empty() {
        let mut guard = detached_runtime_tickets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.extend(pending);
    }
}

#[cfg(target_os = "linux")]
fn io_uring_mark_request_submitted(
    connection: &mut IoUringMigratedConnection,
    worker_pending_requests: &Arc<AtomicUsize>,
) {
    connection.pending_request_count = connection.pending_request_count.saturating_add(1);
    let _ = worker_pending_requests.fetch_add(1, Ordering::AcqRel);
}

#[cfg(target_os = "linux")]
fn io_uring_complete_request(
    connection: &mut IoUringMigratedConnection,
    worker_pending_requests: &Arc<AtomicUsize>,
) {
    if connection.pending_request_count > 0 {
        connection.pending_request_count = connection.pending_request_count.saturating_sub(1);
    }
    let _ = worker_pending_requests.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(1))
    });
}

#[cfg(target_os = "linux")]
fn io_uring_record_completed_request(
    connection: &mut IoUringMigratedConnection,
    request_id: u64,
    reply: Option<Vec<u8>>,
    backpressure: BackpressureThresholds,
    worker_pending_requests: &Arc<AtomicUsize>,
) {
    let _ = connection.reply_order.completed.insert(request_id, reply);
    loop {
        let Some(reply) = connection
            .reply_order
            .completed
            .remove(&connection.reply_order.next_request_id)
        else {
            break;
        };
        connection.reply_order.next_request_id =
            connection.reply_order.next_request_id.saturating_add(1);
        if let Some(reply) = reply {
            write_buffer_append(&mut connection.write_buffer, &mut connection.write_cursor, &reply);
        }
        io_uring_complete_request(connection, worker_pending_requests);
    }
    connection.update_backpressure_state(backpressure.high, backpressure.low);
    let pending_for_worker = worker_pending_requests.load(Ordering::Acquire);
    connection.update_command_backpressure_state(
        pending_for_worker,
        backpressure.max_pending_per_connection,
        backpressure.max_pending_per_worker,
    );
}

#[cfg(target_os = "linux")]
async fn io_uring_flush_write_buffer_budget(
    stream: &IoUringTcpStream,
    write_buffer: &mut Vec<u8>,
    write_cursor: &mut usize,
    write_budget_bytes: usize,
) -> Result<usize, std::io::Error> {
    write_buffer_maybe_compact(write_buffer, write_cursor);
    if write_buffer_is_empty(write_buffer, *write_cursor) {
        return Ok(0);
    }
    let mut remaining_budget = write_budget_bytes.max(1);
    let mut written_total = 0_usize;
    while !write_buffer_is_empty(write_buffer, *write_cursor) && remaining_budget > 0 {
        let chunk_len = remaining_budget.min(write_buffer_pending_len(write_buffer, *write_cursor));
        let payload = std::mem::take(write_buffer);
        let slice = payload.slice(*write_cursor..(*write_cursor).saturating_add(chunk_len));
        let (result, slice) = stream.write(slice).submit().await;
        *write_buffer = slice.into_inner();
        match result {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "io_uring write returned zero bytes",
                ));
            }
            Ok(written) => {
                let bounded_written = written.min(chunk_len);
                write_buffer_advance(write_buffer, write_cursor, bounded_written);
                written_total = written_total.saturating_add(bounded_written);
                remaining_budget = remaining_budget.saturating_sub(bounded_written);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => return Err(error),
        }
    }
    Ok(written_total)
}

#[cfg(target_os = "linux")]
fn can_migrate_io_uring_connection(connection: &IoUringMigratedConnection) -> bool {
    !connection.draining
        && connection.pending_request_count == 0
        && connection.pending_runtime_requests.is_empty()
        && connection.logical.parser.pending_bytes() == 0
        && connection.reply_order.completed.is_empty()
        && write_buffer_is_empty(&connection.write_buffer, connection.write_cursor)
}

impl Drop for ThreadedServerReactor {
    fn drop(&mut self) {
        for worker in &self.workers {
            let _ = worker.sender.send(WorkerCommand::Shutdown);
        }
        for worker in &mut self.workers {
            if let Some(join) = worker.join.take() {
                let _ = join.join();
            }
        }
    }
}

fn io_worker_thread_main(
    command_receiver: &Receiver<WorkerCommand>,
    max_events: usize,
    backpressure: BackpressureThresholds,
    app_executor: &AppExecutor,
    live_connection_count: Arc<AtomicUsize>,
    worker_event_sender: &Sender<WorkerEvent>,
) {
    let Ok(mut poll) = Poll::new() else {
        return;
    };
    let mut events = Events::with_capacity(max_events);
    let mut io_state = WorkerIoState::new(live_connection_count);

    loop {
        if !drain_worker_commands(&poll, command_receiver, &mut io_state) {
            return;
        }
        progress_worker_parse_backlog(
            &poll,
            app_executor,
            &mut io_state,
            backpressure,
            worker_event_sender,
        );
        progress_worker_runtime_replies(
            &poll,
            app_executor,
            &mut io_state,
            backpressure,
            worker_event_sender,
        );

        if poll
            .poll(&mut events, Some(THREADED_IO_WORKER_POLL_TIMEOUT))
            .is_err()
        {
            return;
        }
        for event in &events {
            handle_worker_connection_event(
                &poll,
                WorkerEventFlags {
                    token: event.token(),
                    readable: event.is_readable(),
                    writable: event.is_writable(),
                    closed_or_error: event.is_read_closed() || event.is_write_closed() || event.is_error(),
                },
                app_executor,
                &mut io_state,
                backpressure,
                worker_event_sender,
            );
        }
        progress_worker_parse_backlog(
            &poll,
            app_executor,
            &mut io_state,
            backpressure,
            worker_event_sender,
        );
        progress_worker_runtime_replies(
            &poll,
            app_executor,
            &mut io_state,
            backpressure,
            worker_event_sender,
        );
        reap_detached_runtime_replies(app_executor, &mut io_state);
    }
}

fn drain_worker_commands(
    poll: &Poll,
    command_receiver: &Receiver<WorkerCommand>,
    io_state: &mut WorkerIoState,
) -> bool {
    loop {
        match command_receiver.try_recv() {
            Ok(WorkerCommand::AddConnection {
                connection_id,
                protocol,
                mut socket,
            }) => {
                let token = Token(io_state.loop_state.next_token);
                io_state.loop_state.next_token = io_state.loop_state.next_token.saturating_add(1);
                if poll
                    .registry()
                    .register(&mut socket, token, Interest::READABLE)
                    .is_err()
                {
                    continue;
                }
                let _ = io_state.connections.insert(
                    token,
                    WorkerConnection::new(connection_id, socket, protocol),
                );
                let _ = io_state
                    .loop_state
                    .live_connection_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            Ok(WorkerCommand::TakeConnectionForMigration {
                connection_id,
                response,
            }) => {
                let migration = take_worker_connection_for_migration(poll, connection_id, io_state);
                let _ = response.send(migration);
            }
            Ok(WorkerCommand::AdoptMigratedConnection {
                connection,
                response,
            }) => {
                let adopted = adopt_migrated_worker_connection(poll, *connection, io_state);
                let _ = response.send(adopted);
            }
            Ok(WorkerCommand::Shutdown) | Err(TryRecvError::Disconnected) => return false,
            Err(TryRecvError::Empty) => return true,
        }
    }
}

fn progress_worker_parse_backlog(
    poll: &Poll,
    app_executor: &AppExecutor,
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
    worker_event_sender: &Sender<WorkerEvent>,
) {
    let active_count = io_state.loop_state.active_parse_tokens.len();
    for _ in 0..active_count {
        let Some(token) = io_state.loop_state.active_parse_tokens.pop_front() else {
            break;
        };
        let _ = io_state.loop_state.in_active_parse_set.remove(&token);

        let mut should_close = false;
        {
            let (connections, loop_state) = (&mut io_state.connections, &mut io_state.loop_state);
            let Some(connection) = connections.get_mut(&token) else {
                continue;
            };

            let mut should_requeue_parse = false;
            if connection.can_read() {
                match drain_worker_parsed_commands(
                    token,
                    connection,
                    app_executor,
                    loop_state,
                    backpressure,
                    WORKER_PARSE_BUDGET_PER_READ,
                ) {
                    ParseDrainAction::StopReadTurn => {
                        should_requeue_parse = true;
                    }
                    ParseDrainAction::CloseConnection => {
                        should_close = true;
                    }
                    ParseDrainAction::ContinueParserDrain | ParseDrainAction::ContinueSocketRead => {}
                }
            } else if connection.read_paused_by_command_backpressure
                && connection.logical.parser.pending_bytes() > 0
            {
                // Keep parse backlog active while command backpressure is armed so parser progress
                // resumes as soon as pending request counters drop.
                should_requeue_parse = true;
            }

            if !should_close {
                drain_connection_pending_runtime_replies(
                    connection,
                    app_executor,
                    loop_state,
                    backpressure,
                );
                if connection.should_try_flush() {
                    flush_worker_connection_writes(
                        connection,
                        backpressure.high,
                        backpressure.low,
                        WORKER_WRITE_BUDGET_BYTES_PER_EVENT,
                    );
                }
                if connection.should_close_now() || refresh_worker_interest(poll, token, connection).is_err() {
                    should_close = true;
                } else {
                    if should_requeue_parse {
                        enqueue_active_parse_connection(loop_state, token);
                    }
                    if !connection.pending_runtime_requests.is_empty() {
                        enqueue_active_deferred_connection(loop_state, token);
                    }
                }
            }
        }

        if should_close
            && let Some(connection) = io_state.connections.remove(&token)
        {
            close_worker_connection(
                poll,
                token,
                connection,
                &mut io_state.loop_state,
                app_executor,
                worker_event_sender,
            );
        }
    }
}

fn progress_worker_runtime_replies(
    poll: &Poll,
    app_executor: &AppExecutor,
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
    worker_event_sender: &Sender<WorkerEvent>,
) {
    let active_count = io_state.loop_state.active_deferred_tokens.len();
    for _ in 0..active_count {
        let Some(token) = io_state.loop_state.active_deferred_tokens.pop_front() else {
            break;
        };
        let _ = io_state.loop_state.in_active_deferred_set.remove(&token);

        let mut should_close = false;
        {
            let (connections, loop_state) = (&mut io_state.connections, &mut io_state.loop_state);
            let Some(connection) = connections.get_mut(&token) else {
                continue;
            };

            drain_connection_pending_runtime_replies(connection, app_executor, loop_state, backpressure);
            let has_pending_runtime = !connection.pending_runtime_requests.is_empty();
            if connection.should_try_flush() {
                flush_worker_connection_writes(
                    connection,
                    backpressure.high,
                    backpressure.low,
                    WORKER_WRITE_BUDGET_BYTES_PER_EVENT,
                );
            }
            if connection.should_close_now() || refresh_worker_interest(poll, token, connection).is_err() {
                should_close = true;
            } else if has_pending_runtime {
                enqueue_active_deferred_connection(loop_state, token);
            }
        }

        if should_close
            && let Some(connection) = io_state.connections.remove(&token)
        {
            close_worker_connection(
                poll,
                token,
                connection,
                &mut io_state.loop_state,
                app_executor,
                worker_event_sender,
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WorkerEventFlags {
    token: Token,
    readable: bool,
    writable: bool,
    closed_or_error: bool,
}

fn handle_worker_connection_event(
    poll: &Poll,
    event: WorkerEventFlags,
    app_executor: &AppExecutor,
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
    worker_event_sender: &Sender<WorkerEvent>,
) {
    let mut should_close = false;
    {
        let (connections, loop_state) = (&mut io_state.connections, &mut io_state.loop_state);
        let Some(connection) = connections.get_mut(&event.token) else {
            return;
        };

        if event.closed_or_error {
            connection.on_peer_closed_or_error();
        }

        if event.readable && connection.can_read() {
            read_worker_connection_bytes(
                event.token,
                connection,
                app_executor,
                loop_state,
                backpressure,
            );
            drain_connection_pending_runtime_replies(connection, app_executor, loop_state, backpressure);
        }
        if event.writable && connection.should_try_flush() {
            flush_worker_connection_writes(
                connection,
                backpressure.high,
                backpressure.low,
                WORKER_WRITE_BUDGET_BYTES_PER_EVENT,
            );
        }

        if connection.should_close_now()
            || refresh_worker_interest(poll, event.token, connection).is_err()
        {
            should_close = true;
        }
    }

    if should_close
        && let Some(connection) = io_state.connections.remove(&event.token)
    {
        close_worker_connection(
            poll,
            event.token,
            connection,
            &mut io_state.loop_state,
            app_executor,
            worker_event_sender,
        );
    }
}

fn read_worker_connection_bytes(
    token: Token,
    connection: &mut WorkerConnection,
    app_executor: &AppExecutor,
    loop_state: &mut WorkerLoopState,
    backpressure: BackpressureThresholds,
) {
    let mut chunk = [0_u8; READ_CHUNK_BYTES];
    let mut remaining_reads = WORKER_READ_BUDGET_PER_EVENT;
    while remaining_reads > 0 {
        remaining_reads = remaining_reads.saturating_sub(1);
        match connection.socket.read(&mut chunk) {
            Ok(0) => {
                connection.mark_draining();
                return;
            }
            Ok(read_len) => {
                connection.logical.parser.feed_bytes(&chunk[..read_len]);
                match drain_worker_parsed_commands(
                    token,
                    connection,
                    app_executor,
                    loop_state,
                    backpressure,
                    WORKER_PARSE_BUDGET_PER_READ,
                ) {
                    ParseDrainAction::ContinueParserDrain
                    | ParseDrainAction::ContinueSocketRead => {}
                    ParseDrainAction::StopReadTurn => {
                        enqueue_active_parse_connection(loop_state, token);
                        return;
                    }
                    ParseDrainAction::CloseConnection => return,
                }
                if connection.read_paused_by_backpressure {
                    // Once backpressure is armed we must stop draining socket bytes in
                    // this readiness turn and leave remaining input in kernel buffers.
                    return;
                }
                if connection.read_paused_by_command_backpressure {
                    // Do not keep parsing new requests while command queue limits are exceeded.
                    return;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
            Err(_error) => {
                connection.mark_closing();
                return;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParseDrainAction {
    ContinueParserDrain,
    ContinueSocketRead,
    StopReadTurn,
    CloseConnection,
}

fn drain_worker_parsed_commands(
    token: Token,
    connection: &mut WorkerConnection,
    app_executor: &AppExecutor,
    loop_state: &mut WorkerLoopState,
    backpressure: BackpressureThresholds,
    parse_budget: usize,
) -> ParseDrainAction {
    let mut remaining_budget = parse_budget;
    loop {
        if remaining_budget == 0 {
            return ParseDrainAction::StopReadTurn;
        }
        remaining_budget = remaining_budget.saturating_sub(1);
        if worker_command_backpressure_armed(connection, loop_state, backpressure) {
            return ParseDrainAction::StopReadTurn;
        }
        match connection.logical.parser.try_pop_command() {
            Ok(Some(parsed)) => {
                if !execute_worker_parsed_command(
                    token,
                    connection,
                    app_executor,
                    loop_state,
                    backpressure,
                    parsed,
                ) {
                    return ParseDrainAction::CloseConnection;
                }
            }
            Ok(None) => return ParseDrainAction::ContinueSocketRead,
            Err(error) => {
                match handle_worker_parse_error(connection, loop_state, backpressure, &error) {
                    ParseDrainAction::ContinueParserDrain => {}
                    action => return action,
                }
            }
        }
    }
}

fn worker_command_backpressure_armed(
    connection: &mut WorkerConnection,
    loop_state: &WorkerLoopState,
    backpressure: BackpressureThresholds,
) -> bool {
    let armed = connection.pending_request_count >= backpressure.max_pending_per_connection
        || loop_state.pending_requests_for_worker >= backpressure.max_pending_per_worker;
    if armed {
        connection.update_command_backpressure_state(
            loop_state.pending_requests_for_worker,
            backpressure.max_pending_per_connection,
            backpressure.max_pending_per_worker,
        );
    }
    armed
}

fn execute_worker_parsed_command(
    token: Token,
    connection: &mut WorkerConnection,
    app_executor: &AppExecutor,
    loop_state: &mut WorkerLoopState,
    backpressure: BackpressureThresholds,
    parsed: ParsedCommand,
) -> bool {
    let request_id = connection.next_request_id;
    connection.next_request_id = connection.next_request_id.saturating_add(1);
    mark_worker_request_submitted(connection, &mut loop_state.pending_requests_for_worker);

    match app_executor.execute_parsed_command_deferred(&mut connection.logical, parsed) {
        ParsedCommandExecution::Immediate(reply) => {
            record_completed_worker_request(connection, request_id, reply, loop_state, backpressure);
            true
        }
        ParsedCommandExecution::Deferred(ticket) => {
            connection
                .pending_runtime_requests
                .push(PendingRuntimeRequest { request_id, ticket });
            enqueue_active_deferred_connection(loop_state, token);
            connection.update_command_backpressure_state(
                loop_state.pending_requests_for_worker,
                backpressure.max_pending_per_connection,
                backpressure.max_pending_per_worker,
            );
            true
        }
    }
}

fn drain_connection_pending_runtime_replies(
    connection: &mut WorkerConnection,
    app_executor: &AppExecutor,
    loop_state: &mut WorkerLoopState,
    backpressure: BackpressureThresholds,
) {
    if connection.pending_runtime_requests.is_empty() {
        return;
    }
    let processed_snapshot = match app_executor.runtime_processed_sequences_snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            while let Some(pending) = connection.pending_runtime_requests.pop() {
                let reply = runtime_dispatch_error_reply(pending.ticket.protocol, &error);
                record_completed_worker_request(
                    connection,
                    pending.request_id,
                    Some(reply),
                    loop_state,
                    backpressure,
                );
            }
            return;
        }
    };

    let mut index = 0_usize;
    while index < connection.pending_runtime_requests.len() {
        let ready_result = {
            let pending = &mut connection.pending_runtime_requests[index];
            app_executor
                .runtime_reply_ticket_ready_with_snapshot(&mut pending.ticket, &processed_snapshot)
        };
        let ready = match ready_result {
            Ok(ready) => ready,
            Err(error) => {
                let pending = connection.pending_runtime_requests.swap_remove(index);
                let reply = runtime_dispatch_error_reply(pending.ticket.protocol, &error);
                record_completed_worker_request(
                    connection,
                    pending.request_id,
                    Some(reply),
                    loop_state,
                    backpressure,
                );
                continue;
            }
        };
        if !ready {
            index = index.saturating_add(1);
            continue;
        }

        let mut pending = connection.pending_runtime_requests.swap_remove(index);
        let reply = match app_executor.take_runtime_reply_ticket_ready(&mut pending.ticket) {
            Ok(reply) => Some(reply),
            Err(error) => Some(runtime_dispatch_error_reply(
                pending.ticket.protocol,
                &error,
            )),
        };
        record_completed_worker_request(
            connection,
            pending.request_id,
            reply,
            loop_state,
            backpressure,
        );
    }
}

fn enqueue_active_parse_connection(loop_state: &mut WorkerLoopState, token: Token) {
    if loop_state.in_active_parse_set.insert(token) {
        loop_state.active_parse_tokens.push_back(token);
    }
}

fn enqueue_active_deferred_connection(loop_state: &mut WorkerLoopState, token: Token) {
    if loop_state.in_active_deferred_set.insert(token) {
        loop_state.active_deferred_tokens.push_back(token);
    }
}

fn record_completed_worker_request(
    connection: &mut WorkerConnection,
    request_id: u64,
    reply: Option<Vec<u8>>,
    loop_state: &mut WorkerLoopState,
    backpressure: BackpressureThresholds,
) {
    let _ = connection.reply_order.completed.insert(request_id, reply);
    flush_worker_ordered_replies(connection, loop_state, backpressure);
}

fn flush_worker_ordered_replies(
    connection: &mut WorkerConnection,
    loop_state: &mut WorkerLoopState,
    backpressure: BackpressureThresholds,
) {
    loop {
        let Some(reply) = connection
            .reply_order
            .completed
            .remove(&connection.reply_order.next_request_id)
        else {
            break;
        };
        connection.reply_order.next_request_id =
            connection.reply_order.next_request_id.saturating_add(1);
        if let Some(reply) = reply {
            write_buffer_append(&mut connection.write_buffer, &mut connection.write_cursor, &reply);
        }
        complete_worker_request(connection, &mut loop_state.pending_requests_for_worker);
    }
    connection.update_backpressure_state(backpressure.high, backpressure.low);
    connection.update_command_backpressure_state(
        loop_state.pending_requests_for_worker,
        backpressure.max_pending_per_connection,
        backpressure.max_pending_per_worker,
    );
}

fn handle_worker_parse_error(
    connection: &mut WorkerConnection,
    loop_state: &mut WorkerLoopState,
    backpressure: BackpressureThresholds,
    error: &DflyError,
) -> ParseDrainAction {
    let request_id = connection.next_request_id;
    connection.next_request_id = connection.next_request_id.saturating_add(1);
    mark_worker_request_submitted(connection, &mut loop_state.pending_requests_for_worker);
    let protocol = connection.logical.context.protocol;
    let reply = protocol_parse_error_reply(protocol, error);
    record_completed_worker_request(connection, request_id, Some(reply), loop_state, backpressure);

    if protocol == ClientProtocol::Memcache {
        if !connection.logical.parser.recover_after_protocol_error() {
            return ParseDrainAction::StopReadTurn;
        }
        return ParseDrainAction::ContinueParserDrain;
    }

    connection.mark_draining();
    ParseDrainAction::StopReadTurn
}

fn mark_worker_request_submitted(
    connection: &mut WorkerConnection,
    pending_requests_for_worker: &mut usize,
) {
    connection.pending_request_count = connection.pending_request_count.saturating_add(1);
    *pending_requests_for_worker = pending_requests_for_worker.saturating_add(1);
}

fn complete_worker_request(
    connection: &mut WorkerConnection,
    pending_requests_for_worker: &mut usize,
) {
    if connection.pending_request_count > 0 {
        connection.pending_request_count = connection.pending_request_count.saturating_sub(1);
    }
    if *pending_requests_for_worker > 0 {
        *pending_requests_for_worker = pending_requests_for_worker.saturating_sub(1);
    }
}

fn protocol_parse_error_reply(protocol: ClientProtocol, error: &DflyError) -> Vec<u8> {
    match protocol {
        ClientProtocol::Resp => format!("-ERR {error}\r\n").into_bytes(),
        ClientProtocol::Memcache => format!("CLIENT_ERROR {error}\r\n").into_bytes(),
    }
}

fn runtime_dispatch_error_reply(protocol: ClientProtocol, error: &DflyError) -> Vec<u8> {
    match protocol {
        ClientProtocol::Resp => format!("-ERR runtime dispatch failed: {error}\r\n").into_bytes(),
        ClientProtocol::Memcache => {
            format!("SERVER_ERROR runtime dispatch failed: {error}\r\n").into_bytes()
        }
    }
}

fn reap_detached_runtime_replies(app_executor: &AppExecutor, io_state: &mut WorkerIoState) {
    if io_state.loop_state.detached_runtime_tickets.is_empty() {
        return;
    }
    let processed_snapshot = app_executor.runtime_processed_sequences_snapshot().ok();
    let mut index = 0_usize;
    while index < io_state.loop_state.detached_runtime_tickets.len() {
        let ready = {
            let ticket = &mut io_state.loop_state.detached_runtime_tickets[index];
            match &processed_snapshot {
                Some(snapshot) => app_executor
                    .runtime_reply_ticket_ready_with_snapshot(ticket, snapshot)
                    .unwrap_or(true),
                None => app_executor
                    .runtime_reply_ticket_ready(ticket)
                    .unwrap_or(true),
            }
        };
        if !ready {
            index = index.saturating_add(1);
            continue;
        }
        let mut ticket = io_state.loop_state.detached_runtime_tickets.swap_remove(index);
        let _ = match &processed_snapshot {
            Some(_) => app_executor.take_runtime_reply_ticket_ready(&mut ticket),
            None => app_executor.take_runtime_reply_ticket(&mut ticket),
        };
    }
}

fn flush_worker_connection_writes(
    connection: &mut WorkerConnection,
    write_high_watermark: usize,
    write_low_watermark: usize,
    write_budget_bytes: usize,
) {
    let mut remaining_budget = write_budget_bytes.max(1);
    while !write_buffer_is_empty(&connection.write_buffer, connection.write_cursor) && remaining_budget > 0
    {
        match connection
            .socket
            .write(write_buffer_pending_slice(&connection.write_buffer, connection.write_cursor))
        {
            Ok(0) => {
                connection.mark_closing();
                return;
            }
            Ok(written) => {
                remaining_budget = remaining_budget.saturating_sub(written);
                write_buffer_advance(
                    &mut connection.write_buffer,
                    &mut connection.write_cursor,
                    written,
                );
                connection.update_backpressure_state(write_high_watermark, write_low_watermark);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
            Err(_error) => {
                connection.mark_closing();
                return;
            }
        }
    }
}

fn refresh_worker_interest(
    poll: &Poll,
    token: Token,
    connection: &mut WorkerConnection,
) -> DflyResult<()> {
    let mut next_interest = if connection.can_read() {
        Interest::READABLE
    } else {
        Interest::WRITABLE
    };
    if connection.should_try_flush() {
        next_interest |= Interest::WRITABLE;
    }
    if next_interest == connection.interest {
        return Ok(());
    }
    poll.registry()
        .reregister(&mut connection.socket, token, next_interest)
        .map_err(|error| {
            DflyError::Io(format!("refresh connection poll interest failed: {error}"))
        })?;
    connection.interest = next_interest;
    Ok(())
}

fn close_worker_connection(
    poll: &Poll,
    token: Token,
    mut connection: WorkerConnection,
    loop_state: &mut WorkerLoopState,
    app_executor: &AppExecutor,
    worker_event_sender: &Sender<WorkerEvent>,
) {
    let _ = loop_state.in_active_parse_set.remove(&token);
    let _ = loop_state.in_active_deferred_set.remove(&token);
    if connection.pending_request_count > 0 {
        loop_state.pending_requests_for_worker = loop_state
            .pending_requests_for_worker
            .saturating_sub(connection.pending_request_count);
    }
    for pending in connection.pending_runtime_requests.drain(..) {
        loop_state.detached_runtime_tickets.push(pending.ticket);
    }
    let _ = poll.registry().deregister(&mut connection.socket);
    app_executor.disconnect_connection(&mut connection.logical);
    let _ = loop_state.live_connection_count.fetch_update(
        Ordering::AcqRel,
        Ordering::Acquire,
        |current| Some(current.saturating_sub(1)),
    );
    let _ = worker_event_sender.send(WorkerEvent::ConnectionClosed {
        connection_id: connection.connection_id,
    });
}

fn can_migrate_connection(connection: &WorkerConnection) -> bool {
    connection.lifecycle == ConnectionLifecycle::Active
        && connection.pending_request_count == 0
        && connection.pending_runtime_requests.is_empty()
        && connection.logical.parser.pending_bytes() == 0
        && connection.reply_order.completed.is_empty()
        && write_buffer_is_empty(&connection.write_buffer, connection.write_cursor)
}

fn take_worker_connection_for_migration(
    poll: &Poll,
    connection_id: u64,
    io_state: &mut WorkerIoState,
) -> Result<MigratedWorkerConnection, String> {
    let Some((&token, _)) = io_state
        .connections
        .iter()
        .find(|(_, connection)| connection.connection_id == connection_id)
    else {
        return Err("connection is not owned by worker".to_owned());
    };
    let Some(mut connection) = io_state.connections.remove(&token) else {
        return Err("connection disappeared during migration".to_owned());
    };
    if !can_migrate_connection(&connection) {
        let _ = io_state.connections.insert(token, connection);
        return Err("connection state does not allow migration".to_owned());
    }
    let _ = io_state.loop_state.in_active_parse_set.remove(&token);
    let _ = io_state.loop_state.in_active_deferred_set.remove(&token);
    let _ = poll.registry().deregister(&mut connection.socket);
    let _ = io_state.loop_state.live_connection_count.fetch_update(
        Ordering::AcqRel,
        Ordering::Acquire,
        |current| Some(current.saturating_sub(1)),
    );
    Ok(MigratedWorkerConnection { connection })
}

fn adopt_migrated_worker_connection(
    poll: &Poll,
    mut migrated: MigratedWorkerConnection,
    io_state: &mut WorkerIoState,
) -> MigrationAdoptResult {
    let token = Token(io_state.loop_state.next_token);
    io_state.loop_state.next_token = io_state.loop_state.next_token.saturating_add(1);
    migrated.connection.interest = if migrated.connection.can_read() {
        Interest::READABLE
    } else {
        Interest::WRITABLE
    };
    if migrated.connection.should_try_flush() {
        migrated.connection.interest |= Interest::WRITABLE;
    }
    if poll
        .registry()
        .register(
            &mut migrated.connection.socket,
            token,
            migrated.connection.interest,
        )
        .is_err()
    {
        return Err(Box::new(migrated));
    }
    let _ = io_state.connections.insert(token, migrated.connection);
    let _ = io_state
        .loop_state
        .live_connection_count
        .fetch_add(1, Ordering::AcqRel);
    Ok(())
}

#[cfg(test)]
#[path = "network/tests.rs"]
mod tests;

