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

/// Top-level network engine over one selected multiplex backend.
pub struct NetEngine {
    selection: MultiplexSelection,
    backend: NetEngineBackend,
}

enum NetEngineBackend {
    Epoll(EpollBackend),
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
        let backend = match selection.api {
            MultiplexApi::Epoll => NetEngineBackend::Epoll(EpollBackend::bind_with_memcache(
                resp_addr,
                memcache_addr,
                config,
                app_executor,
            )?),
            #[cfg(target_os = "linux")]
            MultiplexApi::IoUring => NetEngineBackend::IoUring(IoUringBackend::bind_with_memcache(
                resp_addr,
                memcache_addr,
                config,
                app_executor,
            )?),
            #[cfg(not(target_os = "linux"))]
            MultiplexApi::IoUring => {
                return Err(DflyError::InvalidConfig(
                    "io_uring backend is Linux-only; select epoll backend",
                ));
            }
        };
        Ok(Self { selection, backend })
    }

    /// Runs one poll cycle on the selected backend.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` when backend polling fails.
    pub fn poll_once(&mut self, timeout: Option<Duration>) -> DflyResult<usize> {
        match &mut self.backend {
            NetEngineBackend::Epoll(backend) => backend.poll_once(timeout),
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
            NetEngineBackend::Epoll(_) => loop {
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

#[derive(Debug, Clone, Copy)]
struct EventSnapshot {
    token: Token,
    flags: u8,
}

const EVENT_FLAG_READABLE: u8 = 1_u8 << 0;
const EVENT_FLAG_WRITABLE: u8 = 1_u8 << 1;
const EVENT_FLAG_READ_CLOSED: u8 = 1_u8 << 2;
const EVENT_FLAG_WRITE_CLOSED: u8 = 1_u8 << 3;
const EVENT_FLAG_ERROR: u8 = 1_u8 << 4;

impl EventSnapshot {
    fn from_mio_event(event: &mio::event::Event) -> Self {
        let mut flags = 0_u8;
        if event.is_readable() {
            flags |= EVENT_FLAG_READABLE;
        }
        if event.is_writable() {
            flags |= EVENT_FLAG_WRITABLE;
        }
        if event.is_read_closed() {
            flags |= EVENT_FLAG_READ_CLOSED;
        }
        if event.is_write_closed() {
            flags |= EVENT_FLAG_WRITE_CLOSED;
        }
        if event.is_error() {
            flags |= EVENT_FLAG_ERROR;
        }
        Self {
            token: event.token(),
            flags,
        }
    }

    fn readable(self) -> bool {
        (self.flags & EVENT_FLAG_READABLE) != 0
    }

    fn writable(self) -> bool {
        (self.flags & EVENT_FLAG_WRITABLE) != 0
    }

    fn closed_or_error(self) -> bool {
        (self.flags & (EVENT_FLAG_READ_CLOSED | EVENT_FLAG_WRITE_CLOSED | EVENT_FLAG_ERROR)) != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionLifecycle {
    Active,
    Draining,
    Closing,
}

#[cfg(test)]
#[derive(Debug)]
struct ReactorConnection {
    socket: TcpStream,
    logical: ServerConnection,
    write_buffer: Vec<u8>,
    lifecycle: ConnectionLifecycle,
    read_paused_by_backpressure: bool,
    interest: Interest,
}

#[cfg(test)]
impl ReactorConnection {
    fn new(socket: TcpStream, protocol: ClientProtocol) -> Self {
        let logical = ServerApp::new_connection(protocol);
        Self {
            socket,
            logical,
            write_buffer: Vec::new(),
            lifecycle: ConnectionLifecycle::Active,
            read_paused_by_backpressure: false,
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
        self.lifecycle == ConnectionLifecycle::Active && !self.read_paused_by_backpressure
    }

    fn should_try_flush(&self) -> bool {
        !self.write_buffer.is_empty()
    }

    fn should_close_now(&self) -> bool {
        self.lifecycle == ConnectionLifecycle::Closing
            || (self.lifecycle == ConnectionLifecycle::Draining && self.write_buffer.is_empty())
    }

    fn update_backpressure_state(&mut self, high_watermark: usize, low_watermark: usize) {
        if self.read_paused_by_backpressure {
            if self.write_buffer.len() <= low_watermark {
                self.read_paused_by_backpressure = false;
            }
            return;
        }
        if self.write_buffer.len() >= high_watermark {
            self.read_paused_by_backpressure = true;
        }
    }
}

#[derive(Debug)]
struct ReactorListener {
    token: Token,
    protocol: ClientProtocol,
    socket: TcpListener,
}

/// One reactor instance managing protocol listeners and all accepted connections.
#[cfg(test)]
#[derive(Debug)]
pub struct ServerReactor {
    poll: Poll,
    events: Events,
    listeners: Vec<ReactorListener>,
    next_token: usize,
    write_high_watermark: usize,
    write_low_watermark: usize,
    connections: HashMap<Token, ReactorConnection>,
}

#[cfg(test)]
impl ServerReactor {
    /// Binds one RESP listener and registers it in the reactor poller.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` if the listener or poll registration fails.
    pub fn bind(addr: SocketAddr, config: ServerReactorConfig) -> DflyResult<Self> {
        Self::bind_with_memcache(addr, None, config)
    }

    /// Binds RESP listener and optional Memcache listener in one reactor.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` if any listener bind/registration fails.
    pub fn bind_with_memcache(
        resp_addr: SocketAddr,
        memcache_addr: Option<SocketAddr>,
        config: ServerReactorConfig,
    ) -> DflyResult<Self> {
        let poll =
            Poll::new().map_err(|error| DflyError::Io(format!("create poll failed: {error}")))?;
        let mut listeners = Vec::with_capacity(if memcache_addr.is_some() { 2 } else { 1 });
        let (write_high_watermark, write_low_watermark) =
            config.normalized_backpressure_watermarks()?;

        let mut resp_listener = TcpListener::bind(resp_addr)
            .map_err(|error| DflyError::Io(format!("bind RESP listener failed: {error}")))?;
        poll.registry()
            .register(&mut resp_listener, RESP_LISTENER_TOKEN, Interest::READABLE)
            .map_err(|error| {
                DflyError::Io(format!("register RESP listener in poll failed: {error}"))
            })?;
        listeners.push(ReactorListener {
            token: RESP_LISTENER_TOKEN,
            protocol: ClientProtocol::Resp,
            socket: resp_listener,
        });

        if let Some(addr) = memcache_addr {
            let mut memcache_listener = TcpListener::bind(addr).map_err(|error| {
                DflyError::Io(format!("bind Memcache listener failed: {error}"))
            })?;
            poll.registry()
                .register(
                    &mut memcache_listener,
                    MEMCACHE_LISTENER_TOKEN,
                    Interest::READABLE,
                )
                .map_err(|error| {
                    DflyError::Io(format!(
                        "register Memcache listener in poll failed: {error}"
                    ))
                })?;
            listeners.push(ReactorListener {
                token: MEMCACHE_LISTENER_TOKEN,
                protocol: ClientProtocol::Memcache,
                socket: memcache_listener,
            });
        }

        Ok(Self {
            poll,
            events: Events::with_capacity(config.normalized_max_events()),
            listeners,
            next_token: CONNECTION_TOKEN_START,
            write_high_watermark,
            write_low_watermark,
            connections: HashMap::new(),
        })
    }

    /// Processes one readiness cycle and executes accepted/ready commands.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` if polling, socket registration, or socket I/O fails.
    pub fn poll_once(
        &mut self,
        app: &mut ServerApp,
        timeout: Option<Duration>,
    ) -> DflyResult<usize> {
        self.poll
            .poll(&mut self.events, timeout)
            .map_err(|error| DflyError::Io(format!("poll wait failed: {error}")))?;
        let snapshots = self
            .events
            .iter()
            .map(EventSnapshot::from_mio_event)
            .collect::<Vec<_>>();

        for snapshot in &snapshots {
            if let Some(listener_index) = self.listener_index(snapshot.token) {
                self.accept_new_connections(listener_index)?;
                continue;
            }
            self.handle_connection_event(app, *snapshot)?;
        }

        Ok(snapshots.len())
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
    fn connection_count(&self) -> usize {
        self.connections.len()
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

    fn accept_new_connections(&mut self, listener_index: usize) -> DflyResult<()> {
        let protocol = self
            .listeners
            .get(listener_index)
            .map(|listener| listener.protocol)
            .ok_or(DflyError::InvalidState(
                "reactor listener index is out of range",
            ))?;

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
                Ok((mut socket, _peer)) => {
                    let token = self.allocate_connection_token();
                    self.poll
                        .registry()
                        .register(&mut socket, token, Interest::READABLE)
                        .map_err(|error| {
                            DflyError::Io(format!(
                                "register accepted connection in poll failed: {error}"
                            ))
                        })?;
                    let _ = socket.set_nodelay(true);
                    let _ = self
                        .connections
                        .insert(token, ReactorConnection::new(socket, protocol));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => {
                    return Err(DflyError::Io(format!("accept connection failed: {error}")));
                }
            }
        }
    }

    fn handle_connection_event(
        &mut self,
        app: &mut ServerApp,
        snapshot: EventSnapshot,
    ) -> DflyResult<()> {
        let Some(mut connection) = self.connections.remove(&snapshot.token) else {
            return Ok(());
        };

        if snapshot.closed_or_error() {
            connection.on_peer_closed_or_error();
        }

        if snapshot.readable() && connection.can_read() {
            Self::read_connection_bytes(
                app,
                &mut connection,
                self.write_high_watermark,
                self.write_low_watermark,
            );
        }
        if snapshot.writable() && connection.should_try_flush() {
            Self::flush_connection_writes(
                &mut connection,
                self.write_high_watermark,
                self.write_low_watermark,
            );
        }

        if connection.should_close_now() {
            self.close_connection(app, snapshot.token, connection)?;
            return Ok(());
        }

        self.refresh_connection_interest(snapshot.token, &mut connection)?;
        let _ = self.connections.insert(snapshot.token, connection);
        Ok(())
    }

    fn read_connection_bytes(
        app: &mut ServerApp,
        connection: &mut ReactorConnection,
        write_high_watermark: usize,
        write_low_watermark: usize,
    ) {
        let mut chunk = [0_u8; READ_CHUNK_BYTES];
        loop {
            match connection.socket.read(&mut chunk) {
                Ok(0) => {
                    connection.mark_draining();
                    return;
                }
                Ok(read_len) => {
                    match ingress_connection_bytes(app, &mut connection.logical, &chunk[..read_len])
                    {
                        Ok(replies) => {
                            for reply in replies {
                                connection.write_buffer.extend_from_slice(&reply);
                                connection.update_backpressure_state(
                                    write_high_watermark,
                                    write_low_watermark,
                                );
                            }
                            if connection.read_paused_by_backpressure {
                                // Once backpressure is armed we must stop draining socket bytes in
                                // this readiness turn and leave remaining input in kernel buffers.
                                return;
                            }
                        }
                        Err(error) => {
                            let parse_error = protocol_parse_error_reply(
                                connection.logical.context.protocol,
                                &error,
                            );
                            connection.write_buffer.extend_from_slice(&parse_error);
                            if connection.logical.context.protocol == ClientProtocol::Resp {
                                connection.mark_draining();
                            }
                            connection.update_backpressure_state(
                                write_high_watermark,
                                write_low_watermark,
                            );
                            return;
                        }
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

    fn flush_connection_writes(
        connection: &mut ReactorConnection,
        write_high_watermark: usize,
        write_low_watermark: usize,
    ) {
        while !connection.write_buffer.is_empty() {
            match connection.socket.write(connection.write_buffer.as_slice()) {
                Ok(0) => {
                    connection.mark_closing();
                    return;
                }
                Ok(written) => {
                    let _ = connection.write_buffer.drain(..written);
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

    fn refresh_connection_interest(
        &self,
        token: Token,
        connection: &mut ReactorConnection,
    ) -> DflyResult<()> {
        let mut next_interest = if connection.can_read() {
            Interest::READABLE
        } else {
            Interest::WRITABLE
        };
        if !connection.write_buffer.is_empty() {
            next_interest |= Interest::WRITABLE;
        }
        if next_interest == connection.interest {
            return Ok(());
        }

        self.poll
            .registry()
            .reregister(&mut connection.socket, token, next_interest)
            .map_err(|error| {
                DflyError::Io(format!("refresh connection poll interest failed: {error}"))
            })?;
        connection.interest = next_interest;
        Ok(())
    }

    fn close_connection(
        &self,
        app: &mut ServerApp,
        token: Token,
        mut connection: ReactorConnection,
    ) -> DflyResult<()> {
        self.poll
            .registry()
            .deregister(&mut connection.socket)
            .map_err(|error| {
                DflyError::Io(format!(
                    "deregister closed connection {} failed: {error}",
                    token.0
                ))
            })?;
        app.disconnect_connection(&mut connection.logical);
        Ok(())
    }

    fn allocate_connection_token(&mut self) -> Token {
        let token = Token(self.next_token);
        self.next_token = self.next_token.saturating_add(1);
        token
    }
}

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
        !self.write_buffer.is_empty()
    }

    fn should_close_now(&self) -> bool {
        self.lifecycle == ConnectionLifecycle::Closing
            || (self.lifecycle == ConnectionLifecycle::Draining && self.write_buffer.is_empty())
    }

    fn update_backpressure_state(&mut self, high_watermark: usize, low_watermark: usize) {
        if self.read_paused_by_backpressure {
            if self.write_buffer.len() <= low_watermark {
                self.read_paused_by_backpressure = false;
            }
            return;
        }
        if self.write_buffer.len() >= high_watermark {
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
            pending_requests_for_worker: 0,
            active_parse_tokens: VecDeque::new(),
            in_active_parse_set: HashSet::new(),
            active_deferred_tokens: VecDeque::new(),
            in_active_deferred_set: HashSet::new(),
            detached_runtime_tickets: Vec::new(),
            live_connection_count,
            next_token: CONNECTION_TOKEN_START,
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

/// Acceptor + fixed I/O worker runtime.
///
/// The acceptor thread only accepts/dispatches sockets and coordinates command execution against
/// `ServerApp`. Every socket/parser/outbuf stays on one worker thread.
pub struct ThreadedServerReactor {
    acceptor_poll: Poll,
    acceptor_events: Events,
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
        let mut listeners = Vec::with_capacity(if memcache_addr.is_some() { 2 } else { 1 });
        let (write_high_watermark, write_low_watermark) =
            config.normalized_backpressure_watermarks()?;
        let (max_pending_per_connection, max_pending_per_worker) =
            config.normalized_pending_request_limits();
        let worker_count = config.normalized_io_worker_count();
        let dispatch_window = WorkerDispatchWindow {
            start: config.normalized_dispatch_start(worker_count),
            count: config.normalized_dispatch_count(worker_count),
        };
        let max_events = config.normalized_max_events();
        let (worker_event_sender, worker_event_receiver) = mpsc::channel::<WorkerEvent>();

        let mut resp_listener = TcpListener::bind(resp_addr)
            .map_err(|error| DflyError::Io(format!("bind RESP listener failed: {error}")))?;
        acceptor_poll
            .registry()
            .register(&mut resp_listener, RESP_LISTENER_TOKEN, Interest::READABLE)
            .map_err(|error| {
                DflyError::Io(format!("register RESP listener in poll failed: {error}"))
            })?;
        listeners.push(ReactorListener {
            token: RESP_LISTENER_TOKEN,
            protocol: ClientProtocol::Resp,
            socket: resp_listener,
        });

        if let Some(addr) = memcache_addr {
            let mut memcache_listener = TcpListener::bind(addr).map_err(|error| {
                DflyError::Io(format!("bind Memcache listener failed: {error}"))
            })?;
            acceptor_poll
                .registry()
                .register(
                    &mut memcache_listener,
                    MEMCACHE_LISTENER_TOKEN,
                    Interest::READABLE,
                )
                .map_err(|error| {
                    DflyError::Io(format!(
                        "register Memcache listener in poll failed: {error}"
                    ))
                })?;
            listeners.push(ReactorListener {
                token: MEMCACHE_LISTENER_TOKEN,
                protocol: ClientProtocol::Memcache,
                socket: memcache_listener,
            });
        }

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
                        BackpressureThresholds {
                            high: write_high_watermark,
                            low: write_low_watermark,
                            max_pending_per_connection,
                            max_pending_per_worker,
                        },
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
        let snapshots = self
            .acceptor_events
            .iter()
            .map(EventSnapshot::from_mio_event)
            .collect::<Vec<_>>();

        let mut accepted = 0_usize;
        for snapshot in &snapshots {
            if let Some(listener_index) = self.listener_index(snapshot.token) {
                accepted = accepted.saturating_add(self.accept_new_connections(listener_index)?);
            }
        }
        self.drain_worker_events();
        self.maybe_rebalance_connections_if_needed();
        Ok(snapshots.len().saturating_add(accepted))
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
                    let _ = self.connection_owner_by_id.insert(connection_id, worker_id);
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
            if self.connection_owner_by_id.remove(&connection_id).is_some() {
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
        let mut counts = vec![0_usize; self.workers.len()];
        for owner in self.connection_owner_by_id.values().copied() {
            if let Some(count) = counts.get_mut(owner) {
                *count = count.saturating_add(1);
            }
        }
        let Some((source_worker, source_count)) = counts
            .iter()
            .enumerate()
            .max_by_key(|(_, count)| **count)
            .map(|(index, count)| (index, *count))
        else {
            return Ok(false);
        };
        let Some((target_worker, target_count)) = counts
            .iter()
            .enumerate()
            .min_by_key(|(_, count)| **count)
            .map(|(index, count)| (index, *count))
        else {
            return Ok(false);
        };
        if source_count <= target_count.saturating_add(1) {
            return Ok(false);
        }

        let Some(connection_id) =
            self.connection_owner_by_id
                .iter()
                .find_map(|(connection_id, owner)| {
                    if *owner == source_worker && !self.migrated_connections.contains(connection_id)
                    {
                        Some(*connection_id)
                    } else {
                        None
                    }
                })
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
        let _ = self
            .connection_owner_by_id
            .insert(connection_id, target_worker);
        let _ = self.migrated_connections.insert(connection_id);
        self.mark_rebalance_dirty();
        Ok(())
    }
}

/// `epoll` backend implementation for the top-level network engine.
pub struct EpollBackend {
    reactor: ThreadedServerReactor,
}

impl EpollBackend {
    /// Binds listeners/workers for the epoll backend.
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

    /// Runs one poll cycle of the epoll backend.
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
    read_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    draining: bool,
}

#[cfg(target_os = "linux")]
impl IoUringMigratedConnection {
    fn new(connection_id: u64, socket: StdTcpStream, protocol: ClientProtocol) -> Self {
        Self {
            connection_id,
            socket,
            logical: ServerApp::new_connection(protocol),
            read_buffer: vec![0_u8; READ_CHUNK_BYTES],
            write_buffer: Vec::new(),
            draining: false,
        }
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
    migrated_connections: HashSet<u64>,
}

#[cfg(target_os = "linux")]
impl IoUringBackend {
    /// Binds listeners/workers for the io_uring backend.
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
        let mut listeners = Vec::with_capacity(if memcache_addr.is_some() { 2 } else { 1 });
        let (write_high_watermark, write_low_watermark) =
            config.normalized_backpressure_watermarks()?;
        let (max_pending_per_connection, max_pending_per_worker) =
            config.normalized_pending_request_limits();
        let worker_count = config.normalized_io_worker_count();
        let dispatch_window = WorkerDispatchWindow {
            start: config.normalized_dispatch_start(worker_count),
            count: config.normalized_dispatch_count(worker_count),
        };
        let max_events = config.normalized_max_events();
        let (worker_event_sender, worker_event_receiver) = mpsc::channel::<IoUringWorkerEvent>();

        let mut resp_listener = TcpListener::bind(resp_addr)
            .map_err(|error| DflyError::Io(format!("bind RESP listener failed: {error}")))?;
        acceptor_poll
            .registry()
            .register(&mut resp_listener, RESP_LISTENER_TOKEN, Interest::READABLE)
            .map_err(|error| {
                DflyError::Io(format!("register RESP listener in poll failed: {error}"))
            })?;
        listeners.push(ReactorListener {
            token: RESP_LISTENER_TOKEN,
            protocol: ClientProtocol::Resp,
            socket: resp_listener,
        });

        if let Some(addr) = memcache_addr {
            let mut memcache_listener = TcpListener::bind(addr).map_err(|error| {
                DflyError::Io(format!("bind Memcache listener failed: {error}"))
            })?;
            acceptor_poll
                .registry()
                .register(
                    &mut memcache_listener,
                    MEMCACHE_LISTENER_TOKEN,
                    Interest::READABLE,
                )
                .map_err(|error| {
                    DflyError::Io(format!(
                        "register Memcache listener in poll failed: {error}"
                    ))
                })?;
            listeners.push(ReactorListener {
                token: MEMCACHE_LISTENER_TOKEN,
                protocol: ClientProtocol::Memcache,
                socket: memcache_listener,
            });
        }

        let backpressure = BackpressureThresholds {
            high: write_high_watermark,
            low: write_low_watermark,
            max_pending_per_connection: max_pending_per_connection.max(1),
            max_pending_per_worker: max_pending_per_worker.max(1),
        };

        let mut workers = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let (sender, receiver) = tokio_unbounded_channel::<IoUringWorkerCommand>();
            let worker_app = app_executor.clone();
            let worker_backpressure = backpressure;
            let worker_event_sender = worker_event_sender.clone();
            let join = thread::Builder::new()
                .name(format!("dfly-io-uring-worker-{worker_id}"))
                .spawn(move || {
                    io_uring_worker_thread_main(
                        receiver,
                        worker_app,
                        worker_backpressure,
                        worker_event_sender,
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

        Ok(Self {
            acceptor_poll,
            acceptor_events: Events::with_capacity(max_events),
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
            migrated_connections: HashSet::new(),
        })
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
        let snapshots = self
            .acceptor_events
            .iter()
            .map(EventSnapshot::from_mio_event)
            .collect::<Vec<_>>();
        let mut accepted = 0_usize;
        for snapshot in &snapshots {
            if let Some(listener_index) = self.listener_index(snapshot.token) {
                accepted = accepted.saturating_add(self.accept_new_connections(listener_index)?);
            }
        }
        self.drain_worker_events();
        self.maybe_rebalance_connections_if_needed();
        Ok(snapshots.len().saturating_add(accepted))
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
                    let _ = self.connection_owner_by_id.insert(connection_id, worker_id);
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
            if self.connection_owner_by_id.remove(&connection_id).is_some() {
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
        let mut counts = vec![0_usize; self.workers.len()];
        for owner in self.connection_owner_by_id.values().copied() {
            if let Some(count) = counts.get_mut(owner) {
                *count = count.saturating_add(1);
            }
        }
        let Some((source_worker, source_count)) = counts
            .iter()
            .enumerate()
            .max_by_key(|(_, count)| **count)
            .map(|(index, count)| (index, *count))
        else {
            return Ok(false);
        };
        let Some((target_worker, target_count)) = counts
            .iter()
            .enumerate()
            .min_by_key(|(_, count)| **count)
            .map(|(index, count)| (index, *count))
        else {
            return Ok(false);
        };
        if source_count <= target_count.saturating_add(1) {
            return Ok(false);
        }

        let Some(connection_id) =
            self.connection_owner_by_id
                .iter()
                .find_map(|(connection_id, owner)| {
                    if *owner == source_worker && !self.migrated_connections.contains(connection_id)
                    {
                        Some(*connection_id)
                    } else {
                        None
                    }
                })
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
        let _ = self
            .connection_owner_by_id
            .insert(connection_id, target_worker);
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
) {
    tokio_uring::start(async move {
        let mut controls = HashMap::<u64, TokioUnboundedSender<IoUringConnectionControl>>::new();
        while let Some(command) = receiver.recv().await {
            match command {
                IoUringWorkerCommand::AddConnection {
                    connection_id,
                    protocol,
                    socket,
                } => {
                    let control = spawn_io_uring_connection_task(
                        IoUringMigratedConnection::new(connection_id, socket, protocol),
                        app_executor.clone(),
                        backpressure,
                        worker_event_sender.clone(),
                    );
                    let _ = controls.insert(connection_id, control);
                }
                IoUringWorkerCommand::TakeConnectionForMigration {
                    connection_id,
                    response,
                } => {
                    let Some(control) = controls.remove(&connection_id) else {
                        let _ = response.send(Err(
                            "connection is not tracked by io_uring worker".to_owned()
                        ));
                        continue;
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
                        continue;
                    }
                    let migration = match tokio::time::timeout(
                        Duration::from_secs(1),
                        migrate_receiver,
                    )
                    .await
                    {
                        Ok(Ok(result)) => result,
                        Ok(Err(_)) => Err("connection task dropped migration response".to_owned()),
                        Err(_) => {
                            Err("io_uring connection task timed out during migration".to_owned())
                        }
                    };
                    let _ = response.send(migration);
                }
                IoUringWorkerCommand::AdoptMigratedConnection {
                    connection,
                    response,
                } => {
                    let connection = *connection;
                    let connection_id = connection.connection_id;
                    if controls.contains_key(&connection_id) {
                        let _ = response.send(Err(Box::new(connection)));
                        continue;
                    }
                    let control = spawn_io_uring_connection_task(
                        connection,
                        app_executor.clone(),
                        backpressure,
                        worker_event_sender.clone(),
                    );
                    let _ = controls.insert(connection_id, control);
                    let _ = response.send(Ok(()));
                }
                IoUringWorkerCommand::Shutdown => {
                    for control in controls.values() {
                        let _ = control.send(IoUringConnectionControl::Shutdown);
                    }
                    break;
                }
            }
        }
    });
}

#[cfg(target_os = "linux")]
fn spawn_io_uring_connection_task(
    connection: IoUringMigratedConnection,
    app_executor: AppExecutor,
    backpressure: BackpressureThresholds,
    worker_event_sender: Sender<IoUringWorkerEvent>,
) -> TokioUnboundedSender<IoUringConnectionControl> {
    let (control_sender, control_receiver) = tokio_unbounded_channel::<IoUringConnectionControl>();
    tokio_uring::spawn(async move {
        io_uring_connection_main(
            connection,
            app_executor,
            backpressure,
            control_receiver,
            worker_event_sender,
        )
        .await;
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
    app_executor: AppExecutor,
    backpressure: BackpressureThresholds,
    mut control_receiver: TokioUnboundedReceiver<IoUringConnectionControl>,
    worker_event_sender: Sender<IoUringWorkerEvent>,
) {
    let stream = match io_uring_stream_from_connection_socket(&connection.socket) {
        Ok(stream) => stream,
        Err(_) => {
            app_executor.disconnect_connection(&mut connection.logical);
            let _ = worker_event_sender.send(IoUringWorkerEvent::ConnectionClosed {
                connection_id: connection.connection_id,
            });
            return;
        }
    };

    loop {
        match control_receiver.try_recv() {
            Ok(IoUringConnectionControl::Migrate { response }) => {
                let _ = response.send(Ok(connection));
                return;
            }
            Ok(IoUringConnectionControl::Shutdown) => {
                break;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }

        if !connection.draining {
            let read_input = std::mem::take(&mut connection.read_buffer);
            let (read_result, read_output) = stream.read(read_input).await;
            connection.read_buffer = read_output;
            match read_result {
                Ok(0) => {
                    connection.draining = true;
                }
                Ok(read_len) => {
                    connection
                        .logical
                        .parser
                        .feed_bytes(&connection.read_buffer[..read_len]);
                    io_uring_drain_parsed_commands(
                        &stream,
                        &app_executor,
                        &mut connection,
                        backpressure,
                    )
                    .await;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::task::yield_now().await;
                }
                Err(_) => {
                    break;
                }
            }
        }

        if !connection.write_buffer.is_empty()
            && io_uring_flush_write_buffer(&stream, &mut connection.write_buffer)
                .await
                .is_err()
        {
            break;
        }

        if connection.draining {
            break;
        }
    }

    app_executor.disconnect_connection(&mut connection.logical);
    let _ = worker_event_sender.send(IoUringWorkerEvent::ConnectionClosed {
        connection_id: connection.connection_id,
    });
}

#[cfg(target_os = "linux")]
async fn io_uring_drain_parsed_commands(
    stream: &IoUringTcpStream,
    app_executor: &AppExecutor,
    connection: &mut IoUringMigratedConnection,
    backpressure: BackpressureThresholds,
) {
    loop {
        match connection.logical.parser.try_pop_command() {
            Ok(Some(parsed)) => match app_executor
                .execute_parsed_command_deferred(&mut connection.logical, parsed)
            {
                ParsedCommandExecution::Immediate(reply) => {
                    if let Some(reply) = reply {
                        connection.write_buffer.extend_from_slice(&reply);
                    }
                }
                ParsedCommandExecution::Deferred(mut ticket) => {
                    let reply = io_uring_wait_runtime_reply(app_executor, &mut ticket).await;
                    connection.write_buffer.extend_from_slice(&reply);
                }
            },
            Ok(None) => break,
            Err(error) => {
                let protocol = connection.logical.context.protocol;
                connection
                    .write_buffer
                    .extend_from_slice(&protocol_parse_error_reply(protocol, &error));
                if protocol == ClientProtocol::Resp {
                    connection.draining = true;
                    break;
                }
                if !connection.logical.parser.recover_after_protocol_error() {
                    break;
                }
            }
        }

        if connection.write_buffer.len() >= backpressure.high {
            if io_uring_flush_write_buffer(stream, &mut connection.write_buffer)
                .await
                .is_err()
            {
                connection.draining = true;
            }
            if connection.write_buffer.len() > backpressure.low {
                break;
            }
        }
    }
}

#[cfg(target_os = "linux")]
async fn io_uring_wait_runtime_reply(
    app_executor: &AppExecutor,
    ticket: &mut RuntimeReplyTicket,
) -> Vec<u8> {
    loop {
        match app_executor.runtime_reply_ticket_ready(ticket) {
            Ok(true) => {
                return match app_executor.take_runtime_reply_ticket(ticket) {
                    Ok(reply) => reply,
                    Err(error) => runtime_dispatch_error_reply(ticket.protocol, &error),
                };
            }
            Ok(false) => {
                tokio::task::yield_now().await;
            }
            Err(error) => {
                return runtime_dispatch_error_reply(ticket.protocol, &error);
            }
        }
    }
}

#[cfg(target_os = "linux")]
async fn io_uring_flush_write_buffer(
    stream: &IoUringTcpStream,
    write_buffer: &mut Vec<u8>,
) -> Result<(), std::io::Error> {
    if write_buffer.is_empty() {
        return Ok(());
    }
    let payload = std::mem::take(write_buffer);
    let (result, remaining) = stream.write_all(payload).await;
    *write_buffer = remaining;
    result
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
        let snapshots = events
            .iter()
            .map(EventSnapshot::from_mio_event)
            .collect::<Vec<_>>();
        for snapshot in &snapshots {
            handle_worker_connection_event(
                &poll,
                *snapshot,
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
                let token = Token(io_state.next_token);
                io_state.next_token = io_state.next_token.saturating_add(1);
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
    let active_count = io_state.active_parse_tokens.len();
    for _ in 0..active_count {
        let Some(token) = io_state.active_parse_tokens.pop_front() else {
            break;
        };
        let _ = io_state.in_active_parse_set.remove(&token);
        let Some(mut connection) = io_state.connections.remove(&token) else {
            continue;
        };

        let mut should_requeue_parse = false;
        if connection.can_read() {
            match drain_worker_parsed_commands(
                token,
                &mut connection,
                app_executor,
                io_state,
                backpressure,
                WORKER_PARSE_BUDGET_PER_READ,
            ) {
                ParseDrainAction::StopReadTurn => {
                    should_requeue_parse = true;
                }
                ParseDrainAction::CloseConnection => {
                    close_worker_connection(
                        poll,
                        token,
                        connection,
                        io_state,
                        app_executor,
                        worker_event_sender,
                    );
                    continue;
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

        drain_connection_pending_runtime_replies(
            &mut connection,
            app_executor,
            io_state,
            backpressure,
        );
        if connection.should_try_flush() {
            flush_worker_connection_writes(
                &mut connection,
                backpressure.high,
                backpressure.low,
                WORKER_WRITE_BUDGET_BYTES_PER_EVENT,
            );
        }
        if connection.should_close_now() {
            close_worker_connection(
                poll,
                token,
                connection,
                io_state,
                app_executor,
                worker_event_sender,
            );
            continue;
        }
        if refresh_worker_interest(poll, token, &mut connection).is_err() {
            close_worker_connection(
                poll,
                token,
                connection,
                io_state,
                app_executor,
                worker_event_sender,
            );
            continue;
        }

        if should_requeue_parse {
            enqueue_active_parse_connection(io_state, token);
        }
        if !connection.pending_runtime_requests.is_empty() {
            enqueue_active_deferred_connection(io_state, token);
        }
        let _ = io_state.connections.insert(token, connection);
    }
}

fn progress_worker_runtime_replies(
    poll: &Poll,
    app_executor: &AppExecutor,
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
    worker_event_sender: &Sender<WorkerEvent>,
) {
    let active_count = io_state.active_deferred_tokens.len();
    for _ in 0..active_count {
        let Some(token) = io_state.active_deferred_tokens.pop_front() else {
            break;
        };
        let _ = io_state.in_active_deferred_set.remove(&token);
        let Some(mut connection) = io_state.connections.remove(&token) else {
            continue;
        };
        drain_connection_pending_runtime_replies(
            &mut connection,
            app_executor,
            io_state,
            backpressure,
        );
        let has_pending_runtime = !connection.pending_runtime_requests.is_empty();
        if connection.should_try_flush() {
            flush_worker_connection_writes(
                &mut connection,
                backpressure.high,
                backpressure.low,
                WORKER_WRITE_BUDGET_BYTES_PER_EVENT,
            );
        }
        if connection.should_close_now() {
            close_worker_connection(
                poll,
                token,
                connection,
                io_state,
                app_executor,
                worker_event_sender,
            );
            continue;
        }
        if refresh_worker_interest(poll, token, &mut connection).is_err() {
            close_worker_connection(
                poll,
                token,
                connection,
                io_state,
                app_executor,
                worker_event_sender,
            );
            continue;
        }
        if has_pending_runtime {
            enqueue_active_deferred_connection(io_state, token);
        }
        let _ = io_state.connections.insert(token, connection);
    }
}

fn handle_worker_connection_event(
    poll: &Poll,
    snapshot: EventSnapshot,
    app_executor: &AppExecutor,
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
    worker_event_sender: &Sender<WorkerEvent>,
) {
    let Some(mut connection) = io_state.connections.remove(&snapshot.token) else {
        return;
    };

    if snapshot.closed_or_error() {
        connection.on_peer_closed_or_error();
    }

    if snapshot.readable() && connection.can_read() {
        read_worker_connection_bytes(
            snapshot.token,
            &mut connection,
            app_executor,
            io_state,
            backpressure,
        );
        drain_connection_pending_runtime_replies(
            &mut connection,
            app_executor,
            io_state,
            backpressure,
        );
    }
    if snapshot.writable() && connection.should_try_flush() {
        flush_worker_connection_writes(
            &mut connection,
            backpressure.high,
            backpressure.low,
            WORKER_WRITE_BUDGET_BYTES_PER_EVENT,
        );
    }

    if connection.should_close_now() {
        close_worker_connection(
            poll,
            snapshot.token,
            connection,
            io_state,
            app_executor,
            worker_event_sender,
        );
        return;
    }

    if refresh_worker_interest(poll, snapshot.token, &mut connection).is_err() {
        close_worker_connection(
            poll,
            snapshot.token,
            connection,
            io_state,
            app_executor,
            worker_event_sender,
        );
        return;
    }

    let _ = io_state.connections.insert(snapshot.token, connection);
}

fn read_worker_connection_bytes(
    token: Token,
    connection: &mut WorkerConnection,
    app_executor: &AppExecutor,
    io_state: &mut WorkerIoState,
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
                    io_state,
                    backpressure,
                    WORKER_PARSE_BUDGET_PER_READ,
                ) {
                    ParseDrainAction::ContinueParserDrain
                    | ParseDrainAction::ContinueSocketRead => {}
                    ParseDrainAction::StopReadTurn => {
                        enqueue_active_parse_connection(io_state, token);
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
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
    parse_budget: usize,
) -> ParseDrainAction {
    let mut remaining_budget = parse_budget;
    loop {
        if remaining_budget == 0 {
            return ParseDrainAction::StopReadTurn;
        }
        remaining_budget = remaining_budget.saturating_sub(1);
        if worker_command_backpressure_armed(connection, io_state, backpressure) {
            return ParseDrainAction::StopReadTurn;
        }
        match connection.logical.parser.try_pop_command() {
            Ok(Some(parsed)) => {
                if !execute_worker_parsed_command(
                    token,
                    connection,
                    app_executor,
                    io_state,
                    backpressure,
                    parsed,
                ) {
                    return ParseDrainAction::CloseConnection;
                }
            }
            Ok(None) => return ParseDrainAction::ContinueSocketRead,
            Err(error) => {
                match handle_worker_parse_error(connection, io_state, backpressure, &error) {
                    ParseDrainAction::ContinueParserDrain => {}
                    action => return action,
                }
            }
        }
    }
}

fn worker_command_backpressure_armed(
    connection: &mut WorkerConnection,
    io_state: &WorkerIoState,
    backpressure: BackpressureThresholds,
) -> bool {
    let armed = connection.pending_request_count >= backpressure.max_pending_per_connection
        || io_state.pending_requests_for_worker >= backpressure.max_pending_per_worker;
    if armed {
        connection.update_command_backpressure_state(
            io_state.pending_requests_for_worker,
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
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
    parsed: ParsedCommand,
) -> bool {
    let request_id = connection.next_request_id;
    connection.next_request_id = connection.next_request_id.saturating_add(1);
    mark_worker_request_submitted(connection, &mut io_state.pending_requests_for_worker);

    match app_executor.execute_parsed_command_deferred(&mut connection.logical, parsed) {
        ParsedCommandExecution::Immediate(reply) => {
            record_completed_worker_request(connection, request_id, reply, io_state, backpressure);
            true
        }
        ParsedCommandExecution::Deferred(ticket) => {
            connection
                .pending_runtime_requests
                .push(PendingRuntimeRequest { request_id, ticket });
            enqueue_active_deferred_connection(io_state, token);
            connection.update_command_backpressure_state(
                io_state.pending_requests_for_worker,
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
    io_state: &mut WorkerIoState,
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
                    io_state,
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
                    io_state,
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
            io_state,
            backpressure,
        );
    }
}

fn enqueue_active_parse_connection(io_state: &mut WorkerIoState, token: Token) {
    if io_state.in_active_parse_set.insert(token) {
        io_state.active_parse_tokens.push_back(token);
    }
}

fn enqueue_active_deferred_connection(io_state: &mut WorkerIoState, token: Token) {
    if io_state.in_active_deferred_set.insert(token) {
        io_state.active_deferred_tokens.push_back(token);
    }
}

fn record_completed_worker_request(
    connection: &mut WorkerConnection,
    request_id: u64,
    reply: Option<Vec<u8>>,
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
) {
    let _ = connection.reply_order.completed.insert(request_id, reply);
    flush_worker_ordered_replies(connection, io_state, backpressure);
}

fn flush_worker_ordered_replies(
    connection: &mut WorkerConnection,
    io_state: &mut WorkerIoState,
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
            connection.write_buffer.extend_from_slice(&reply);
        }
        complete_worker_request(connection, &mut io_state.pending_requests_for_worker);
    }
    connection.update_backpressure_state(backpressure.high, backpressure.low);
    connection.update_command_backpressure_state(
        io_state.pending_requests_for_worker,
        backpressure.max_pending_per_connection,
        backpressure.max_pending_per_worker,
    );
}

fn handle_worker_parse_error(
    connection: &mut WorkerConnection,
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
    error: &DflyError,
) -> ParseDrainAction {
    let request_id = connection.next_request_id;
    connection.next_request_id = connection.next_request_id.saturating_add(1);
    mark_worker_request_submitted(connection, &mut io_state.pending_requests_for_worker);
    let protocol = connection.logical.context.protocol;
    let reply = protocol_parse_error_reply(protocol, error);
    record_completed_worker_request(connection, request_id, Some(reply), io_state, backpressure);

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
    if io_state.detached_runtime_tickets.is_empty() {
        return;
    }
    let processed_snapshot = app_executor.runtime_processed_sequences_snapshot().ok();
    let mut index = 0_usize;
    while index < io_state.detached_runtime_tickets.len() {
        let ready = {
            let ticket = &mut io_state.detached_runtime_tickets[index];
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
        let mut ticket = io_state.detached_runtime_tickets.swap_remove(index);
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
    while !connection.write_buffer.is_empty() && remaining_budget > 0 {
        match connection.socket.write(connection.write_buffer.as_slice()) {
            Ok(0) => {
                connection.mark_closing();
                return;
            }
            Ok(written) => {
                remaining_budget = remaining_budget.saturating_sub(written);
                let _ = connection.write_buffer.drain(..written);
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
    if !connection.write_buffer.is_empty() {
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
    io_state: &mut WorkerIoState,
    app_executor: &AppExecutor,
    worker_event_sender: &Sender<WorkerEvent>,
) {
    let _ = io_state.in_active_parse_set.remove(&token);
    let _ = io_state.in_active_deferred_set.remove(&token);
    if connection.pending_request_count > 0 {
        io_state.pending_requests_for_worker = io_state
            .pending_requests_for_worker
            .saturating_sub(connection.pending_request_count);
    }
    for pending in connection.pending_runtime_requests.drain(..) {
        io_state.detached_runtime_tickets.push(pending.ticket);
    }
    let _ = poll.registry().deregister(&mut connection.socket);
    app_executor.disconnect_connection(&mut connection.logical);
    let _ = io_state.live_connection_count.fetch_update(
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
        && connection.write_buffer.is_empty()
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
    let _ = io_state.in_active_parse_set.remove(&token);
    let _ = io_state.in_active_deferred_set.remove(&token);
    let _ = poll.registry().deregister(&mut connection.socket);
    let _ = io_state.live_connection_count.fetch_update(
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
    let token = Token(io_state.next_token);
    io_state.next_token = io_state.next_token.saturating_add(1);
    migrated.connection.interest = if migrated.connection.can_read() {
        Interest::READABLE
    } else {
        Interest::WRITABLE
    };
    if !migrated.connection.write_buffer.is_empty() {
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
        .live_connection_count
        .fetch_add(1, Ordering::AcqRel);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionLifecycle, ReactorConnection, ServerReactor, ServerReactorConfig,
        ThreadedServerReactor,
    };
    use crate::app::{AppExecutor, ParsedCommandExecution, ServerApp};
    use dfly_common::config::RuntimeConfig;
    use dfly_core::command::CommandFrame;
    use dfly_facade::net_proactor::{MultiplexApi, MultiplexSelection, select_multiplex_backend};
    use dfly_facade::protocol::{ClientProtocol, ParsedCommand};
    use googletest::prelude::*;
    use mio::{Poll, Token};
    use rstest::rstest;
    use std::io::{Read, Write};
    use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, atomic::AtomicUsize};
    use std::time::{Duration, Instant};

    #[rstest]
    fn reactor_executes_resp_ping_roundtrip() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut reactor = ServerReactor::bind(bind_addr, ServerReactorConfig::default())
            .expect("reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local addr should be available");

        let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
        client
            .set_nonblocking(true)
            .expect("nonblocking client should be configurable");
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .expect("write ping should succeed");

        let deadline = Instant::now() + Duration::from_millis(600);
        let mut response = Vec::new();
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(&mut app, Some(Duration::from_millis(5)))
                .expect("reactor poll should succeed");

            let mut chunk = [0_u8; 64];
            match client.read(&mut chunk) {
                Ok(0) => break,
                Ok(read_len) => {
                    response.extend_from_slice(&chunk[..read_len]);
                    if response.ends_with(b"+PONG\r\n") {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read from client failed: {error}"),
            }
        }

        assert_that!(&response, eq(&b"+PONG\r\n".to_vec()));
    }

    #[rstest]
    fn reactor_executes_memcache_roundtrip_on_optional_listener() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let resp_bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let memcache_bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut reactor = ServerReactor::bind_with_memcache(
            resp_bind_addr,
            Some(memcache_bind_addr),
            ServerReactorConfig::default(),
        )
        .expect("reactor bind with memcache should succeed");
        let memcache_addr = reactor
            .memcache_local_addr()
            .expect("memcache local addr should be available");

        let mut client = TcpStream::connect(memcache_addr).expect("connect should succeed");
        client
            .set_nonblocking(true)
            .expect("nonblocking client should be configurable");
        client
            .write_all(b"set user:42 0 0 5\r\nalice\r\nget user:42\r\n")
            .expect("write memcache commands should succeed");

        let deadline = Instant::now() + Duration::from_millis(600);
        let mut response = Vec::new();
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(&mut app, Some(Duration::from_millis(5)))
                .expect("reactor poll should succeed");

            let mut chunk = [0_u8; 128];
            match client.read(&mut chunk) {
                Ok(0) => break,
                Ok(read_len) => {
                    response.extend_from_slice(&chunk[..read_len]);
                    if response.ends_with(b"END\r\n") {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read from memcache client failed: {error}"),
            }
        }

        assert_that!(
            &response,
            eq(&b"STORED\r\nVALUE user:42 0 5\r\nalice\r\nEND\r\n".to_vec())
        );
    }

    #[rstest]
    fn reactor_memcache_parse_error_uses_client_error_and_keeps_connection_open() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut reactor = ServerReactor::bind_with_memcache(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            ServerReactorConfig::default(),
        )
        .expect("reactor bind with memcache should succeed");
        let memcache_addr = reactor
            .memcache_local_addr()
            .expect("memcache local addr should be available");

        let mut client = TcpStream::connect(memcache_addr).expect("connect should succeed");
        client
            .set_nonblocking(true)
            .expect("nonblocking client should be configurable");

        client
            .write_all(b"set user:42 0 0 5\r\nalice\r\n")
            .expect("write memcache set should succeed");

        let store_deadline = Instant::now() + Duration::from_millis(600);
        let mut store_reply = Vec::new();
        while Instant::now() < store_deadline {
            let _ = reactor
                .poll_once(&mut app, Some(Duration::from_millis(5)))
                .expect("reactor poll should succeed");

            let mut chunk = [0_u8; 64];
            match client.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => {
                    store_reply.extend_from_slice(&chunk[..read_len]);
                    if store_reply.ends_with(b"STORED\r\n") {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read from memcache client failed: {error}"),
            }
        }
        assert_that!(&store_reply, eq(&b"STORED\r\n".to_vec()));

        client
            .write_all(b"set broken 0 0 -1\r\nget user:42\r\n")
            .expect("write invalid+valid memcache pipeline should succeed");

        let deadline = Instant::now() + Duration::from_millis(800);
        let mut response = Vec::new();
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(&mut app, Some(Duration::from_millis(5)))
                .expect("reactor poll should succeed");

            let mut chunk = [0_u8; 256];
            match client.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => {
                    response.extend_from_slice(&chunk[..read_len]);
                    if response.ends_with(b"END\r\n") {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read from memcache client failed: {error}"),
            }
        }

        let text = String::from_utf8_lossy(&response);
        assert_that!(text.contains("CLIENT_ERROR"), eq(true));
        assert_that!(
            text.contains("VALUE user:42 0 5\r\nalice\r\nEND\r\n"),
            eq(true)
        );
    }

    #[rstest]
    fn reactor_drops_connection_state_after_peer_close() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut reactor = ServerReactor::bind(bind_addr, ServerReactorConfig::default())
            .expect("reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local addr should be available");

        let client = TcpStream::connect(listen_addr).expect("connect should succeed");
        drop(client);

        let deadline = Instant::now() + Duration::from_millis(600);
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(&mut app, Some(Duration::from_millis(5)))
                .expect("reactor poll should succeed");
            if reactor.connection_count() == 0 {
                break;
            }
        }

        assert_that!(reactor.connection_count(), eq(0_usize));
    }

    #[rstest]
    fn reactor_read_stops_same_cycle_after_backpressure_arms() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("listener bind should succeed");
        let listen_addr = listener
            .local_addr()
            .expect("listener must expose local addr");

        let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
        let (server_stream, _) = listener.accept().expect("accept should succeed");
        server_stream
            .set_nonblocking(true)
            .expect("accepted socket should be nonblocking");

        let mut connection = ReactorConnection::new(
            mio::net::TcpStream::from_std(server_stream),
            ClientProtocol::Resp,
        );
        let one_ping = b"*1\r\n$4\r\nPING\r\n";
        let mut payload = Vec::with_capacity(one_ping.len() * 1024);
        for _ in 0..1024 {
            payload.extend_from_slice(one_ping);
        }
        client
            .write_all(&payload)
            .expect("client payload write should succeed");
        client
            .shutdown(Shutdown::Write)
            .expect("client write-half shutdown should succeed");

        ServerReactor::read_connection_bytes(&mut app, &mut connection, 64, 32);

        assert_that!(connection.read_paused_by_backpressure, eq(true));
        assert_that!(connection.lifecycle, eq(ConnectionLifecycle::Active));
        assert_that!(connection.write_buffer.is_empty(), eq(false));
    }

    #[rstest]
    fn reactor_config_accepts_small_custom_backpressure_values() {
        let config = ServerReactorConfig {
            max_events: 64,
            write_high_watermark_bytes: 512,
            write_low_watermark_bytes: 128,
            io_worker_count: 1,
            max_pending_requests_per_connection: 32,
            max_pending_requests_per_worker: 128,
            ..ServerReactorConfig::default()
        };
        let watermarks = config
            .normalized_backpressure_watermarks()
            .expect("custom backpressure values should be accepted");

        assert_that!(&watermarks, eq(&(512_usize, 128_usize)));
    }

    #[rstest]
    fn reactor_config_rejects_non_increasing_backpressure_values() {
        let config = ServerReactorConfig {
            max_events: 64,
            write_high_watermark_bytes: 256,
            write_low_watermark_bytes: 256,
            io_worker_count: 1,
            max_pending_requests_per_connection: 32,
            max_pending_requests_per_worker: 128,
            ..ServerReactorConfig::default()
        };
        let error = config
            .normalized_backpressure_watermarks()
            .expect_err("low >= high should be rejected");

        assert_that!(
            format!("{error}").contains("low watermark must be smaller than high watermark"),
            eq(true)
        );
    }

    #[rstest]
    fn backend_fallback_selection() {
        let selection = select_multiplex_backend(&RuntimeConfig {
            force_epoll: true,
            ..RuntimeConfig::default()
        });
        assert_that!(selection.api_label(), eq("epoll"));
        assert_that!(selection.fallback_reason.is_some(), eq(true));
    }

    #[cfg(not(target_os = "linux"))]
    #[rstest]
    fn net_engine_rejects_io_uring_backend_on_non_linux() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let selection = MultiplexSelection {
            api: MultiplexApi::IoUring,
            fallback_reason: None,
        };
        let bind_result = super::NetEngine::bind_with_memcache(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            None,
            ServerReactorConfig::default(),
            selection,
            &app_executor,
        );
        let error_text = match bind_result {
            Ok(_) => panic!("io_uring backend should not bind on non-linux"),
            Err(error) => format!("{error}"),
        };
        assert_that!(error_text.contains("Linux-only"), eq(true));
    }

    #[cfg(target_os = "linux")]
    #[rstest]
    fn net_engine_binds_io_uring_backend_on_linux() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let selection = MultiplexSelection {
            api: MultiplexApi::IoUring,
            fallback_reason: None,
        };
        let bind_result = super::NetEngine::bind_with_memcache(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            None,
            ServerReactorConfig::default(),
            selection,
            &app_executor,
        );
        assert_that!(bind_result.is_ok(), eq(true));
    }

    #[rstest]
    fn listener_affinity_distribution() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 4,
            io_worker_dispatch_start: 1,
            io_worker_dispatch_count: 2,
            ..ServerReactorConfig::default()
        };
        let mut reactor = ThreadedServerReactor::bind(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            config,
            &app_executor,
        )
        .expect("threaded reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local address should be available");

        let mut clients = Vec::new();
        for _ in 0..8 {
            let client = TcpStream::connect(listen_addr).expect("connect should succeed");
            clients.push(client);
        }

        let deadline = Instant::now() + Duration::from_millis(800);
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            let counts = reactor.worker_connection_counts();
            if counts[1] > 0 && counts[2] > 0 {
                assert_that!(counts[0], eq(0_usize));
                assert_that!(counts[3], eq(0_usize));
                return;
            }
        }
        panic!("expected connection distribution to stay within configured dispatch window");
    }

    #[rstest]
    fn connection_migration_preserves_order() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 2,
            io_worker_dispatch_count: 2,
            enable_connection_migration: true,
            ..ServerReactorConfig::default()
        };
        let mut reactor = ThreadedServerReactor::bind(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            config,
            &app_executor,
        )
        .expect("threaded reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local address should be available");
        let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
        client
            .set_nonblocking(true)
            .expect("client should be nonblocking");

        let connection_id = {
            let deadline = Instant::now() + Duration::from_millis(800);
            let mut connection_id = None;
            while Instant::now() < deadline {
                let _ = reactor
                    .poll_once(Some(Duration::from_millis(5)))
                    .expect("threaded reactor poll should succeed");
                if let Some(id) = reactor.active_connection_ids().first().copied() {
                    connection_id = Some(id);
                    break;
                }
            }
            connection_id.expect("accepted connection should be tracked")
        };

        let owner = reactor
            .connection_owner(connection_id)
            .expect("connection owner should be known");
        let target_worker = (owner + 1) % 2;
        reactor
            .migrate_connection(connection_id, target_worker)
            .expect("migration should succeed");

        let second_migration = reactor.migrate_connection(connection_id, owner);
        assert_that!(second_migration.is_err(), eq(true));

        let mut request = Vec::new();
        let mut expected = Vec::new();
        for index in 0..24 {
            let key = format!("mk{index}");
            let value = format!("mv{index}");
            request.extend_from_slice(
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value
                )
                .as_bytes(),
            );
            request.extend_from_slice(
                format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).as_bytes(),
            );
            expected.extend_from_slice(b"+OK\r\n");
            expected.extend_from_slice(format!("${}\r\n{}\r\n", value.len(), value).as_bytes());
        }
        client
            .write_all(&request)
            .expect("pipeline write should succeed");

        let mut response = Vec::new();
        let deadline = Instant::now() + Duration::from_millis(2500);
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");

            let mut chunk = [0_u8; 4096];
            match client.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => response.extend_from_slice(&chunk[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("pipeline read failed: {error}"),
            }
            if response.len() >= expected.len() {
                break;
            }
        }

        assert_that!(&response, eq(&expected));
    }

    #[rstest]
    fn connection_migration_failure_rolls_back_to_source_owner() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 2,
            io_worker_dispatch_count: 2,
            enable_connection_migration: true,
            ..ServerReactorConfig::default()
        };
        let mut reactor = ThreadedServerReactor::bind(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            config,
            &app_executor,
        )
        .expect("threaded reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local address should be available");
        let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
        client
            .set_nonblocking(true)
            .expect("client should be nonblocking");

        let connection_id = {
            let deadline = Instant::now() + Duration::from_millis(800);
            let mut connection_id = None;
            while Instant::now() < deadline {
                let _ = reactor
                    .poll_once(Some(Duration::from_millis(5)))
                    .expect("threaded reactor poll should succeed");
                if let Some(id) = reactor.active_connection_ids().first().copied() {
                    connection_id = Some(id);
                    break;
                }
            }
            connection_id.expect("accepted connection should be tracked")
        };

        let owner = reactor
            .connection_owner(connection_id)
            .expect("connection owner should be known");
        let target_worker = (owner + 1) % 2;
        reactor.workers[target_worker]
            .sender
            .send(super::WorkerCommand::Shutdown)
            .expect("target worker shutdown signal should send");
        if let Some(join) = reactor.workers[target_worker].join.take() {
            let _ = join.join();
        }

        let migration = reactor.migrate_connection(connection_id, target_worker);
        assert_that!(migration.is_err(), eq(true));
        assert_that!(reactor.connection_owner(connection_id), eq(Some(owner)));

        client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .expect("ping write should succeed after rollback");
        let deadline = Instant::now() + Duration::from_millis(1200);
        let mut response = Vec::new();
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            let mut chunk = [0_u8; 64];
            match client.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => {
                    response.extend_from_slice(&chunk[..read_len]);
                    if response.ends_with(b"+PONG\r\n") {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("ping read failed after rollback: {error}"),
            }
        }

        assert_that!(&response, eq(&b"+PONG\r\n".to_vec()));
    }

    #[rstest]
    fn connection_close_cleans_migrated_marker() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 2,
            io_worker_dispatch_count: 2,
            enable_connection_migration: true,
            ..ServerReactorConfig::default()
        };
        let mut reactor = ThreadedServerReactor::bind(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            config,
            &app_executor,
        )
        .expect("threaded reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local address should be available");
        let client = TcpStream::connect(listen_addr).expect("connect should succeed");
        client
            .set_nonblocking(true)
            .expect("client should be nonblocking");

        let connection_id = {
            let deadline = Instant::now() + Duration::from_millis(800);
            let mut connection_id = None;
            while Instant::now() < deadline {
                let _ = reactor
                    .poll_once(Some(Duration::from_millis(5)))
                    .expect("threaded reactor poll should succeed");
                if let Some(id) = reactor.active_connection_ids().first().copied() {
                    connection_id = Some(id);
                    break;
                }
            }
            connection_id.expect("accepted connection should be tracked")
        };

        let owner = reactor
            .connection_owner(connection_id)
            .expect("connection owner should be known");
        let target_worker = (owner + 1) % 2;
        reactor
            .migrate_connection(connection_id, target_worker)
            .expect("migration should succeed");
        assert_that!(
            reactor.migrated_connections.contains(&connection_id),
            eq(true)
        );

        let _ = client.shutdown(Shutdown::Both);
        drop(client);

        let deadline = Instant::now() + Duration::from_millis(1200);
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            if reactor.connection_owner(connection_id).is_none() {
                break;
            }
        }

        assert_that!(reactor.connection_owner(connection_id).is_none(), eq(true));
        assert_that!(
            reactor.migrated_connections.contains(&connection_id),
            eq(false)
        );
    }

    #[rstest]
    fn threaded_reactor_distributes_connections_across_workers() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        };
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut reactor = ThreadedServerReactor::bind(bind_addr, config, &app_executor)
            .expect("threaded reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local addr should be available");

        let _client_a = TcpStream::connect(listen_addr).expect("first connect should succeed");
        let _client_b = TcpStream::connect(listen_addr).expect("second connect should succeed");

        let deadline = Instant::now() + Duration::from_millis(800);
        let mut counts = Vec::new();
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            counts = reactor.worker_connection_counts();
            if counts.iter().filter(|count| **count > 0).count() == 2 {
                break;
            }
        }

        assert_that!(counts.len(), eq(2_usize));
        assert_that!(
            counts.iter().filter(|count| **count > 0).count(),
            eq(2_usize)
        );
    }

    #[rstest]
    fn threaded_reactor_workers_progress_two_pipelines_concurrently() {
        const PIPELINE_GETS: usize = 16;

        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        };
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut reactor = ThreadedServerReactor::bind(bind_addr, config, &app_executor)
            .expect("threaded reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local addr should be available");

        let mut client_a = TcpStream::connect(listen_addr).expect("first connect should succeed");
        let mut client_b = TcpStream::connect(listen_addr).expect("second connect should succeed");
        client_a
            .set_nonblocking(true)
            .expect("first client should be nonblocking");
        client_b
            .set_nonblocking(true)
            .expect("second client should be nonblocking");

        let distribution_deadline = Instant::now() + Duration::from_millis(800);
        let mut counts = Vec::new();
        while Instant::now() < distribution_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            counts = reactor.worker_connection_counts();
            if counts.iter().filter(|count| **count > 0).count() == 2 {
                break;
            }
        }
        assert_that!(counts.len(), eq(2_usize));
        assert_that!(
            counts.iter().filter(|count| **count > 0).count(),
            eq(2_usize)
        );

        client_a
            .write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nka\r\n$1\r\n1\r\n")
            .expect("first SET should write");
        client_b
            .write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nkb\r\n$1\r\n2\r\n")
            .expect("second SET should write");

        let mut set_reply_a = Vec::new();
        let mut set_reply_b = Vec::new();
        let set_deadline = Instant::now() + Duration::from_millis(900);
        while Instant::now() < set_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");

            let mut chunk_a = [0_u8; 64];
            match client_a.read(&mut chunk_a) {
                Ok(0) => {}
                Ok(read_len) => set_reply_a.extend_from_slice(&chunk_a[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("first set reply read failed: {error}"),
            }

            let mut chunk_b = [0_u8; 64];
            match client_b.read(&mut chunk_b) {
                Ok(0) => {}
                Ok(read_len) => set_reply_b.extend_from_slice(&chunk_b[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("second set reply read failed: {error}"),
            }

            if set_reply_a.ends_with(b"+OK\r\n") && set_reply_b.ends_with(b"+OK\r\n") {
                break;
            }
        }
        assert_that!(&set_reply_a, eq(&b"+OK\r\n".to_vec()));
        assert_that!(&set_reply_b, eq(&b"+OK\r\n".to_vec()));

        let mut pipeline_a = Vec::new();
        let mut pipeline_b = Vec::new();
        let one_get_a = b"*2\r\n$3\r\nGET\r\n$2\r\nka\r\n";
        let one_get_b = b"*2\r\n$3\r\nGET\r\n$2\r\nkb\r\n";
        for _ in 0..PIPELINE_GETS {
            pipeline_a.extend_from_slice(one_get_a);
            pipeline_b.extend_from_slice(one_get_b);
        }

        client_a
            .write_all(&pipeline_a)
            .expect("first pipeline write should succeed");
        client_b
            .write_all(&pipeline_b)
            .expect("second pipeline write should succeed");

        let mut expected_a = Vec::new();
        let mut expected_b = Vec::new();
        for _ in 0..PIPELINE_GETS {
            expected_a.extend_from_slice(b"$1\r\n1\r\n");
            expected_b.extend_from_slice(b"$1\r\n2\r\n");
        }

        let mut pipeline_reply_a = Vec::new();
        let mut pipeline_reply_b = Vec::new();
        let pipeline_deadline = Instant::now() + Duration::from_millis(2000);
        while Instant::now() < pipeline_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");

            let mut chunk_a = [0_u8; 512];
            match client_a.read(&mut chunk_a) {
                Ok(0) => {}
                Ok(read_len) => pipeline_reply_a.extend_from_slice(&chunk_a[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("first pipeline reply read failed: {error}"),
            }

            let mut chunk_b = [0_u8; 512];
            match client_b.read(&mut chunk_b) {
                Ok(0) => {}
                Ok(read_len) => pipeline_reply_b.extend_from_slice(&chunk_b[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("second pipeline reply read failed: {error}"),
            }

            if pipeline_reply_a.len() >= expected_a.len()
                && pipeline_reply_b.len() >= expected_b.len()
            {
                break;
            }
        }

        assert_that!(&pipeline_reply_a, eq(&expected_a));
        assert_that!(&pipeline_reply_b, eq(&expected_b));
    }

    #[rstest]
    fn many_idle_connections_do_not_slow_deferred_progress() {
        const PIPELINE_GETS: usize = 256;
        const PING_COUNT: usize = 12;
        const IDLE_CLIENTS: usize = 32;

        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        };
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut reactor = ThreadedServerReactor::bind(bind_addr, config, &app_executor)
            .expect("threaded reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local addr should be available");

        let mut client_a = TcpStream::connect(listen_addr).expect("first connect should succeed");
        let mut client_b = TcpStream::connect(listen_addr).expect("second connect should succeed");
        client_a
            .set_nonblocking(true)
            .expect("first client should be nonblocking");
        client_b
            .set_nonblocking(true)
            .expect("second client should be nonblocking");
        let mut idle_clients = Vec::with_capacity(IDLE_CLIENTS);
        for _ in 0..IDLE_CLIENTS {
            let mut attempts = 0_u32;
            let idle = loop {
                attempts = attempts.saturating_add(1);
                assert_that!(attempts <= 200, eq(true));
                match TcpStream::connect(listen_addr) {
                    Ok(stream) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                        let _ = reactor
                            .poll_once(Some(Duration::from_millis(1)))
                            .expect("threaded reactor poll should succeed");
                    }
                    Err(error) => panic!("idle connect should succeed: {error}"),
                }
            };
            idle.set_nonblocking(true)
                .expect("idle client should be nonblocking");
            idle_clients.push(idle);
        }
        assert_that!(idle_clients.len(), eq(IDLE_CLIENTS));

        let distribution_deadline = Instant::now() + Duration::from_millis(800);
        let mut counts = Vec::new();
        while Instant::now() < distribution_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            counts = reactor.worker_connection_counts();
            if counts.iter().filter(|count| **count > 0).count() == 2 {
                break;
            }
        }
        assert_that!(
            counts.iter().filter(|count| **count > 0).count(),
            eq(2_usize)
        );

        client_a
            .write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nka\r\n$1\r\n1\r\n")
            .expect("seed set should write");
        let mut set_reply = Vec::new();
        let set_deadline = Instant::now() + Duration::from_millis(900);
        while Instant::now() < set_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            let mut chunk = [0_u8; 64];
            match client_a.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => set_reply.extend_from_slice(&chunk[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("seed set read failed: {error}"),
            }
            if set_reply.ends_with(b"+OK\r\n") {
                break;
            }
        }
        assert_that!(&set_reply, eq(&b"+OK\r\n".to_vec()));

        let mut deferred_pipeline = Vec::new();
        for _ in 0..PIPELINE_GETS {
            deferred_pipeline.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$2\r\nka\r\n");
        }
        client_a
            .write_all(&deferred_pipeline)
            .expect("deferred pipeline write should succeed");

        let mut ping_pipeline = Vec::new();
        for _ in 0..PING_COUNT {
            ping_pipeline.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        }
        client_b
            .write_all(&ping_pipeline)
            .expect("ping pipeline write should succeed");

        let expected_ping = b"+PONG\r\n".repeat(PING_COUNT);
        let mut ping_reply = Vec::new();
        let ping_deadline = Instant::now() + Duration::from_millis(1200);
        while Instant::now() < ping_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");

            let mut chunk_b = [0_u8; 512];
            match client_b.read(&mut chunk_b) {
                Ok(0) => {}
                Ok(read_len) => ping_reply.extend_from_slice(&chunk_b[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("ping reply read failed: {error}"),
            }
            if ping_reply.len() >= expected_ping.len() {
                break;
            }
        }

        assert_that!(&ping_reply, eq(&expected_ping));
    }

    #[rstest]
    fn threaded_reactor_mixed_read_write_pipelines_progress_without_hol_blocking() {
        const PAIRS: usize = 16;

        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        };
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut reactor = ThreadedServerReactor::bind(bind_addr, config, &app_executor)
            .expect("threaded reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("local addr should be available");

        let mut client_a = TcpStream::connect(listen_addr).expect("first connect should succeed");
        let mut client_b = TcpStream::connect(listen_addr).expect("second connect should succeed");
        client_a
            .set_nonblocking(true)
            .expect("first client should be nonblocking");
        client_b
            .set_nonblocking(true)
            .expect("second client should be nonblocking");

        let distribution_deadline = Instant::now() + Duration::from_millis(800);
        let mut counts = Vec::new();
        while Instant::now() < distribution_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            counts = reactor.worker_connection_counts();
            if counts.iter().filter(|count| **count > 0).count() == 2 {
                break;
            }
        }
        assert_that!(
            counts.iter().filter(|count| **count > 0).count(),
            eq(2_usize)
        );

        let mut pipeline_a = Vec::new();
        let mut pipeline_b = Vec::new();
        let mut expected_a = Vec::new();
        let mut expected_b = Vec::new();
        for index in 0..PAIRS {
            let key_a = format!("ka{index}");
            let value_a = format!("va{index}");
            pipeline_a.extend_from_slice(
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_a.len(),
                    key_a,
                    value_a.len(),
                    value_a
                )
                .as_bytes(),
            );
            pipeline_a.extend_from_slice(
                format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key_a.len(), key_a).as_bytes(),
            );
            expected_a.extend_from_slice(b"+OK\r\n");
            expected_a
                .extend_from_slice(format!("${}\r\n{}\r\n", value_a.len(), value_a).as_bytes());

            let key_b = format!("kb{index}");
            let value_b = format!("vb{index}");
            pipeline_b.extend_from_slice(
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_b.len(),
                    key_b,
                    value_b.len(),
                    value_b
                )
                .as_bytes(),
            );
            pipeline_b.extend_from_slice(
                format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key_b.len(), key_b).as_bytes(),
            );
            expected_b.extend_from_slice(b"+OK\r\n");
            expected_b
                .extend_from_slice(format!("${}\r\n{}\r\n", value_b.len(), value_b).as_bytes());
        }

        client_a
            .write_all(&pipeline_a)
            .expect("first pipeline write should succeed");
        client_b
            .write_all(&pipeline_b)
            .expect("second pipeline write should succeed");

        let mut reply_a = Vec::new();
        let mut reply_b = Vec::new();
        let deadline = Instant::now() + Duration::from_millis(2500);
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");

            let mut chunk_a = [0_u8; 4096];
            match client_a.read(&mut chunk_a) {
                Ok(0) => {}
                Ok(read_len) => reply_a.extend_from_slice(&chunk_a[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("first mixed pipeline read failed: {error}"),
            }

            let mut chunk_b = [0_u8; 4096];
            match client_b.read(&mut chunk_b) {
                Ok(0) => {}
                Ok(read_len) => reply_b.extend_from_slice(&chunk_b[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("second mixed pipeline read failed: {error}"),
            }

            if reply_a.len() >= expected_a.len() && reply_b.len() >= expected_b.len() {
                break;
            }
        }

        assert_that!(&reply_a, eq(&expected_a));
        assert_that!(&reply_b, eq(&expected_b));
    }

    #[rstest]
    fn threaded_reactor_executes_resp_and_memcache_roundtrip() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        };
        let mut reactor = ThreadedServerReactor::bind_with_memcache(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            config,
            &app_executor,
        )
        .expect("threaded reactor bind should succeed");
        let resp_addr = reactor
            .local_addr()
            .expect("resp local addr should be available");
        let memcache_addr = reactor
            .memcache_local_addr()
            .expect("memcache local addr should be available");

        let mut resp_client = TcpStream::connect(resp_addr).expect("resp connect should succeed");
        let mut mem_client =
            TcpStream::connect(memcache_addr).expect("memcache connect should succeed");
        resp_client
            .set_nonblocking(true)
            .expect("resp client should be nonblocking");
        mem_client
            .set_nonblocking(true)
            .expect("memcache client should be nonblocking");

        resp_client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .expect("RESP ping write should succeed");
        mem_client
            .write_all(b"set mk 0 0 2\r\nok\r\nget mk\r\n")
            .expect("Memcache roundtrip write should succeed");

        let deadline = Instant::now() + Duration::from_millis(1000);
        let mut resp_reply = Vec::new();
        let mut mem_reply = Vec::new();
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");

            let mut resp_chunk = [0_u8; 64];
            match resp_client.read(&mut resp_chunk) {
                Ok(0) => {}
                Ok(read_len) => resp_reply.extend_from_slice(&resp_chunk[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read from resp client failed: {error}"),
            }

            let mut mem_chunk = [0_u8; 256];
            match mem_client.read(&mut mem_chunk) {
                Ok(0) => {}
                Ok(read_len) => mem_reply.extend_from_slice(&mem_chunk[..read_len]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read from memcache client failed: {error}"),
            }

            if resp_reply.ends_with(b"+PONG\r\n") && mem_reply.ends_with(b"END\r\n") {
                break;
            }
        }

        assert_that!(&resp_reply, eq(&b"+PONG\r\n".to_vec()));
        assert_that!(
            &mem_reply,
            eq(&b"STORED\r\nVALUE mk 0 2\r\nok\r\nEND\r\n".to_vec())
        );
    }

    #[rstest]
    fn deferred_ready_queue_preserves_reply_order() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let config = ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        };
        let mut reactor = ThreadedServerReactor::bind(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            config,
            &app_executor,
        )
        .expect("threaded reactor bind should succeed");
        let listen_addr = reactor
            .local_addr()
            .expect("resp local addr should be available");

        let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
        client
            .set_nonblocking(true)
            .expect("client should be nonblocking");

        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
            .expect("SET write should succeed");

        let mut set_reply = Vec::new();
        let set_deadline = Instant::now() + Duration::from_millis(800);
        while Instant::now() < set_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            let mut chunk = [0_u8; 64];
            match client.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => {
                    set_reply.extend_from_slice(&chunk[..read_len]);
                    if set_reply.ends_with(b"+OK\r\n") {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read set reply failed: {error}"),
            }
        }
        assert_that!(&set_reply, eq(&b"+OK\r\n".to_vec()));

        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*1\r\n$4\r\nPING\r\n")
            .expect("pipeline write should succeed");

        let mut pipeline_reply = Vec::new();
        let pipeline_deadline = Instant::now() + Duration::from_millis(1000);
        while Instant::now() < pipeline_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            let mut chunk = [0_u8; 128];
            match client.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => {
                    pipeline_reply.extend_from_slice(&chunk[..read_len]);
                    if pipeline_reply.ends_with(b"+PONG\r\n") {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read pipeline reply failed: {error}"),
            }
        }

        assert_that!(&pipeline_reply, eq(&b"$1\r\nv\r\n+PONG\r\n".to_vec()));
    }

    #[rstest]
    fn threaded_reactor_memcache_parse_error_uses_client_error_and_keeps_connection_open() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let mut reactor = ThreadedServerReactor::bind_with_memcache(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            ServerReactorConfig {
                io_worker_count: 2,
                ..ServerReactorConfig::default()
            },
            &app_executor,
        )
        .expect("threaded reactor bind should succeed");
        let memcache_addr = reactor
            .memcache_local_addr()
            .expect("memcache local addr should be available");

        let mut client = TcpStream::connect(memcache_addr).expect("connect should succeed");
        client
            .set_nonblocking(true)
            .expect("client should be nonblocking");

        client
            .write_all(b"set user:42 0 0 5\r\nalice\r\n")
            .expect("write set should succeed");
        let store_deadline = Instant::now() + Duration::from_millis(600);
        let mut store_reply = Vec::new();
        while Instant::now() < store_deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            let mut chunk = [0_u8; 64];
            match client.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => {
                    store_reply.extend_from_slice(&chunk[..read_len]);
                    if store_reply.ends_with(b"STORED\r\n") {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read store reply failed: {error}"),
            }
        }
        assert_that!(&store_reply, eq(&b"STORED\r\n".to_vec()));

        client
            .write_all(b"set broken 0 0 -1\r\nget user:42\r\n")
            .expect("write invalid+valid pipeline should succeed");
        let deadline = Instant::now() + Duration::from_millis(1000);
        let mut response = Vec::new();
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            let mut chunk = [0_u8; 256];
            match client.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => {
                    response.extend_from_slice(&chunk[..read_len]);
                    if response.ends_with(b"END\r\n") {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("read memcache reply failed: {error}"),
            }
        }
        let text = String::from_utf8_lossy(&response);
        assert_that!(text.contains("CLIENT_ERROR"), eq(true));
        assert_that!(
            text.contains("VALUE user:42 0 5\r\nalice\r\nEND\r\n"),
            eq(true)
        );
    }

    #[rstest]
    fn worker_read_pauses_when_command_queue_limit_is_reached() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("listener bind should succeed");
        let listen_addr = listener
            .local_addr()
            .expect("listener must expose local addr");
        let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
        let (server_stream, _) = listener.accept().expect("accept should succeed");
        server_stream
            .set_nonblocking(true)
            .expect("accepted socket should be nonblocking");
        client
            .set_nonblocking(false)
            .expect("client blocking mode should be configurable");

        let mut connection = super::WorkerConnection::new(
            1,
            mio::net::TcpStream::from_std(server_stream),
            ClientProtocol::Resp,
        );
        let mut payload = Vec::new();
        for _ in 0..8 {
            payload.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
        }
        client
            .write_all(&payload)
            .expect("pipeline write should succeed");
        client
            .shutdown(Shutdown::Write)
            .expect("write-half shutdown should succeed");

        let mut io_state = super::WorkerIoState::new(Arc::new(AtomicUsize::new(0)));
        super::read_worker_connection_bytes(
            Token(2),
            &mut connection,
            &app_executor,
            &mut io_state,
            super::BackpressureThresholds {
                high: usize::MAX,
                low: usize::MAX / 2,
                max_pending_per_connection: 2,
                max_pending_per_worker: 2,
            },
        );

        assert_that!(connection.read_paused_by_command_backpressure, eq(true));
        assert_that!(connection.pending_request_count, eq(2_usize));
        assert_that!(io_state.pending_requests_for_worker, eq(2_usize));
    }

    #[rstest]
    fn disconnected_worker_connection_reaps_detached_deferred_replies() {
        let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
        let frame = CommandFrame::new("GET", vec![b"leak-check".to_vec()]);
        let mut command_connection = ServerApp::new_connection(ClientProtocol::Resp);
        let execution = app_executor.execute_parsed_command_deferred(
            &mut command_connection,
            ParsedCommand {
                name: frame.name.clone(),
                args: frame.args.clone(),
            },
        );
        let ParsedCommandExecution::Deferred(ticket) = execution else {
            panic!("GET should dispatch via deferred runtime path");
        };
        let _shard = ticket
            .barriers
            .first()
            .map(|barrier| barrier.shard)
            .expect("deferred ticket should have one shard barrier");
        let mut probe_ticket = ticket.clone();

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("listener bind should succeed");
        let listen_addr = listener
            .local_addr()
            .expect("listener must expose local addr");
        let _client = TcpStream::connect(listen_addr).expect("connect should succeed");
        let (server_stream, _) = listener.accept().expect("accept should succeed");
        server_stream
            .set_nonblocking(true)
            .expect("accepted socket should be nonblocking");

        let mut connection = super::WorkerConnection::new(
            1,
            mio::net::TcpStream::from_std(server_stream),
            ClientProtocol::Resp,
        );
        connection.pending_request_count = 1;
        connection
            .pending_runtime_requests
            .push(super::PendingRuntimeRequest {
                request_id: 1,
                ticket,
            });

        let poll = Poll::new().expect("poll creation should succeed");
        let mut io_state = super::WorkerIoState::new(Arc::new(AtomicUsize::new(1)));
        let (event_sender, _event_receiver) = std::sync::mpsc::channel::<super::WorkerEvent>();
        super::close_worker_connection(
            &poll,
            Token(2),
            connection,
            &mut io_state,
            &app_executor,
            &event_sender,
        );
        assert_that!(io_state.detached_runtime_tickets.len(), eq(1_usize));
        let deadline = std::time::Instant::now() + Duration::from_millis(800);
        let mut reply_ready = false;
        while std::time::Instant::now() < deadline {
            if app_executor
                .runtime_reply_ticket_ready(&mut probe_ticket)
                .expect("runtime readiness probe should succeed")
            {
                reply_ready = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_that!(reply_ready, eq(true));

        super::reap_detached_runtime_replies(&app_executor, &mut io_state);
        assert_that!(io_state.detached_runtime_tickets.is_empty(), eq(true));
        let missing_after_reap = app_executor.take_runtime_reply_ticket_ready(&mut probe_ticket);
        assert_that!(missing_after_reap.is_err(), eq(true));
    }
}
