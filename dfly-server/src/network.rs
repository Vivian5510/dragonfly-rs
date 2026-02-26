//! Reactor-style network event loop for RESP and Memcache ingress.
//!
//! Dragonfly keeps socket ownership in dedicated I/O threads and advances parsing/execution
//! from readiness events. This module provides the same shape for `dragonfly-rs`:
//! per-protocol listeners + per-connection state machine driven by `mio::Poll`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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
use dfly_facade::protocol::{ClientProtocol, ParsedCommand};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

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
const WORKER_READ_BUDGET_PER_EVENT: usize = 8;
const WORKER_PARSE_BUDGET_PER_READ: usize = 32;
const WORKER_WRITE_BUDGET_BYTES_PER_EVENT: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerReactorConfig {
    pub max_events: usize,
    pub write_high_watermark_bytes: usize,
    pub write_low_watermark_bytes: usize,
    pub io_worker_count: usize,
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

#[derive(Debug)]
enum WorkerCommand {
    AddConnection {
        protocol: ClientProtocol,
        socket: TcpStream,
    },
    Shutdown,
}

#[derive(Debug)]
struct WorkerConnection {
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
    fn new(socket: TcpStream, protocol: ClientProtocol) -> Self {
        Self {
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

/// Acceptor + fixed I/O worker runtime.
///
/// The acceptor thread only accepts/dispatches sockets and coordinates command execution against
/// `ServerApp`. Every socket/parser/outbuf stays on one worker thread.
pub struct ThreadedServerReactor {
    acceptor_poll: Poll,
    acceptor_events: Events,
    listeners: Vec<ReactorListener>,
    workers: Vec<WorkerHandle>,
    #[cfg(test)]
    worker_connection_counts: Vec<Arc<AtomicUsize>>,
    next_worker: usize,
}

impl ThreadedServerReactor {
    /// Binds one RESP listener and starts worker threads.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` if listener bind/registration or worker spawn fails.
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
        let max_events = config.normalized_max_events();

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
            #[cfg(test)]
            worker_connection_counts,
            next_worker: 0,
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
                    let worker_id = self.allocate_worker_index();
                    self.dispatch_to_worker(
                        worker_id,
                        WorkerCommand::AddConnection { protocol, socket },
                    )?;
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

    fn allocate_worker_index(&mut self) -> usize {
        let worker_id = self.next_worker;
        let worker_count = self.workers.len().max(1);
        self.next_worker = (self.next_worker + 1) % worker_count;
        worker_id
    }
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
        progress_worker_runtime_replies(&poll, app_executor, &mut io_state, backpressure);

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
            );
        }
        progress_worker_runtime_replies(&poll, app_executor, &mut io_state, backpressure);
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
                let _ = io_state
                    .connections
                    .insert(token, WorkerConnection::new(socket, protocol));
                let _ = io_state
                    .live_connection_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            Ok(WorkerCommand::Shutdown) | Err(TryRecvError::Disconnected) => return false,
            Err(TryRecvError::Empty) => return true,
        }
    }
}

fn progress_worker_runtime_replies(
    poll: &Poll,
    app_executor: &AppExecutor,
    io_state: &mut WorkerIoState,
    backpressure: BackpressureThresholds,
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
            close_worker_connection(poll, token, connection, io_state, app_executor);
            continue;
        }
        if refresh_worker_interest(poll, token, &mut connection).is_err() {
            close_worker_connection(poll, token, connection, io_state, app_executor);
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
        close_worker_connection(poll, snapshot.token, connection, io_state, app_executor);
        return;
    }

    if refresh_worker_interest(poll, snapshot.token, &mut connection).is_err() {
        close_worker_connection(poll, snapshot.token, connection, io_state, app_executor);
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
                    ParseDrainAction::StopReadTurn | ParseDrainAction::CloseConnection => return,
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
    let mut index = 0_usize;
    while index < connection.pending_runtime_requests.len() {
        let ready_result = {
            let pending = &mut connection.pending_runtime_requests[index];
            app_executor.runtime_reply_ticket_ready(&mut pending.ticket)
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
        let reply = match app_executor.take_runtime_reply_ticket(&mut pending.ticket) {
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
    let mut index = 0_usize;
    while index < io_state.detached_runtime_tickets.len() {
        let ready = {
            let ticket = &mut io_state.detached_runtime_tickets[index];
            app_executor
                .runtime_reply_ticket_ready(ticket)
                .unwrap_or(true)
        };
        if !ready {
            index = index.saturating_add(1);
            continue;
        }
        let mut ticket = io_state.detached_runtime_tickets.swap_remove(index);
        let _ = app_executor.take_runtime_reply_ticket(&mut ticket);
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
) {
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
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionLifecycle, ReactorConnection, ServerReactor, ServerReactorConfig,
        ThreadedServerReactor,
    };
    use crate::app::{AppExecutor, ServerApp};
    use dfly_common::config::RuntimeConfig;
    use dfly_core::command::CommandFrame;
    use dfly_core::runtime::RuntimeEnvelope;
    use dfly_facade::protocol::ClientProtocol;
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
        let shard = app_executor.with_app(|app| app.core.resolve_target_shard(&frame));
        let sequence = app_executor.with_app(|app| {
            app.runtime
                .submit_with_sequence(RuntimeEnvelope {
                    target_shard: shard,
                    db: 0,
                    execute_on_worker: true,
                    command: frame.clone(),
                })
                .expect("runtime submit should succeed")
        });

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
            mio::net::TcpStream::from_std(server_stream),
            ClientProtocol::Resp,
        );
        connection.pending_request_count = 1;
        connection
            .pending_runtime_requests
            .push(super::PendingRuntimeRequest {
                request_id: 1,
                ticket: crate::app::RuntimeReplyTicket {
                    db: 0,
                    txid: None,
                    protocol: ClientProtocol::Resp,
                    frame: frame.clone(),
                    barriers: vec![crate::app::RuntimeSequenceBarrier { shard, sequence }],
                    aggregation: crate::app::RuntimeReplyAggregation::Worker { shard, sequence },
                },
            });

        let poll = Poll::new().expect("poll creation should succeed");
        let mut io_state = super::WorkerIoState::new(Arc::new(AtomicUsize::new(1)));
        super::close_worker_connection(&poll, Token(2), connection, &mut io_state, &app_executor);
        assert_that!(io_state.detached_runtime_tickets.len(), eq(1_usize));

        app_executor.with_app(|app| {
            app.runtime
                .wait_until_processed_sequence(shard, sequence)
                .expect("runtime should process deferred request");
        });
        assert_that!(
            app_executor.with_app(|app| app
                .runtime
                .pending_reply_count(shard)
                .expect("pending reply count should be available"))
                >= 1,
            eq(true)
        );

        super::reap_detached_runtime_replies(&app_executor, &mut io_state);
        assert_that!(io_state.detached_runtime_tickets.is_empty(), eq(true));
        assert_that!(
            app_executor.with_app(|app| app
                .runtime
                .pending_reply_count(shard)
                .expect("pending reply count should be available")),
            eq(0_usize)
        );
    }
}
