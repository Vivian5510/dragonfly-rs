//! Reactor-style network event loop for RESP and Memcache ingress.
//!
//! Dragonfly keeps socket ownership in dedicated I/O threads and advances parsing/execution
//! from readiness events. This module provides the same shape for `dragonfly-rs`:
//! per-protocol listeners + per-connection state machine driven by `mio::Poll`.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dfly_common::error::{DflyError, DflyResult};
use dfly_facade::connection::{ConnectionContext, ConnectionState};
use dfly_facade::protocol::{ClientProtocol, ParsedCommand};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use crate::app::{ParsedCommandExecution, RuntimeReplyTicket, ServerApp, ServerConnection};
#[cfg(test)]
use crate::ingress::ingress_connection_bytes;

const RESP_LISTENER_TOKEN: Token = Token(0);
const MEMCACHE_LISTENER_TOKEN: Token = Token(1);
const CONNECTION_TOKEN_START: usize = 2;
const READ_CHUNK_BYTES: usize = 8192;
const DEFAULT_WRITE_HIGH_WATERMARK_BYTES: usize = 256 * 1024;
const DEFAULT_WRITE_LOW_WATERMARK_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerReactorConfig {
    pub max_events: usize,
    pub write_high_watermark_bytes: usize,
    pub write_low_watermark_bytes: usize,
    pub io_worker_count: usize,
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
                            connection
                                .write_buffer
                                .extend_from_slice(format!("-ERR {error}\r\n").as_bytes());
                            connection.mark_draining();
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
        connection_id: u64,
        protocol: ClientProtocol,
        socket: TcpStream,
    },
    QueueReply {
        connection_id: u64,
        reply: Vec<u8>,
    },
    Shutdown,
}

#[derive(Debug)]
enum WorkerEvent {
    ExecuteParsed {
        worker_id: usize,
        connection_id: u64,
        request_id: u64,
        parsed: ParsedCommand,
    },
    Disconnected {
        connection_id: u64,
    },
}

#[derive(Debug)]
struct WorkerConnection {
    connection_id: u64,
    socket: TcpStream,
    parser: ConnectionState,
    write_buffer: Vec<u8>,
    next_request_id: u64,
    lifecycle: ConnectionLifecycle,
    read_paused_by_backpressure: bool,
    interest: Interest,
}

impl WorkerConnection {
    fn new(connection_id: u64, socket: TcpStream, protocol: ClientProtocol) -> Self {
        let context = ConnectionContext {
            protocol,
            db_index: 0,
            privileged: false,
        };
        Self {
            connection_id,
            socket,
            parser: ConnectionState::new(context),
            write_buffer: Vec::new(),
            next_request_id: 1,
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

#[derive(Debug, Clone, Copy)]
struct BackpressureThresholds {
    high: usize,
    low: usize,
}

#[derive(Debug)]
struct PendingRuntimeRequest {
    worker_id: usize,
    connection_id: u64,
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
    worker_events: Receiver<WorkerEvent>,
    next_connection_id: u64,
    next_worker: usize,
    logical_connections: HashMap<u64, ServerConnection>,
    connection_workers: HashMap<u64, usize>,
    pending_runtime_requests: Vec<PendingRuntimeRequest>,
    reply_order_by_connection: HashMap<u64, ConnectionReplyOrder>,
}

impl ThreadedServerReactor {
    /// Binds one RESP listener and starts worker threads.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` if listener bind/registration or worker spawn fails.
    pub fn bind(addr: SocketAddr, config: ServerReactorConfig) -> DflyResult<Self> {
        Self::bind_with_memcache(addr, None, config)
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
    ) -> DflyResult<Self> {
        let acceptor_poll =
            Poll::new().map_err(|error| DflyError::Io(format!("create poll failed: {error}")))?;
        let mut listeners = Vec::with_capacity(if memcache_addr.is_some() { 2 } else { 1 });
        let (write_high_watermark, write_low_watermark) =
            config.normalized_backpressure_watermarks()?;
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

        let (event_sender, event_receiver) = mpsc::channel::<WorkerEvent>();
        let mut workers = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let (command_sender, command_receiver) = mpsc::channel::<WorkerCommand>();
            let sender = event_sender.clone();
            let join = thread::Builder::new()
                .name(format!("dfly-io-worker-{worker_id}"))
                .spawn(move || {
                    io_worker_thread_main(
                        worker_id,
                        &command_receiver,
                        &sender,
                        max_events,
                        BackpressureThresholds {
                            high: write_high_watermark,
                            low: write_low_watermark,
                        },
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
            worker_events: event_receiver,
            next_connection_id: 1,
            next_worker: 0,
            logical_connections: HashMap::new(),
            connection_workers: HashMap::new(),
            pending_runtime_requests: Vec::new(),
            reply_order_by_connection: HashMap::new(),
        })
    }

    /// Processes one acceptor readiness cycle and drains parsed-command events from workers.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` when polling, accepting, or worker IPC fails.
    pub fn poll_once(
        &mut self,
        app: &mut ServerApp,
        timeout: Option<Duration>,
    ) -> DflyResult<usize> {
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

        let processed = self.drain_worker_events(app)?;
        Ok(snapshots
            .len()
            .saturating_add(accepted)
            .saturating_add(processed))
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
        let mut counts = vec![0_usize; self.workers.len()];
        for worker_index in self.connection_workers.values() {
            if let Some(count) = counts.get_mut(*worker_index) {
                *count = count.saturating_add(1);
            }
        }
        counts
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
                    let worker_id = self.allocate_worker_index();
                    self.dispatch_to_worker(
                        worker_id,
                        WorkerCommand::AddConnection {
                            connection_id,
                            protocol,
                            socket,
                        },
                    )?;
                    let previous = self
                        .logical_connections
                        .insert(connection_id, ServerApp::new_connection(protocol));
                    debug_assert!(previous.is_none());
                    let _ = self.connection_workers.insert(connection_id, worker_id);
                    let _ = self
                        .reply_order_by_connection
                        .insert(connection_id, ConnectionReplyOrder::new());
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

    fn drain_worker_events(&mut self, app: &mut ServerApp) -> DflyResult<usize> {
        let mut drained = self.drain_pending_runtime_replies(app)?;
        loop {
            match self.worker_events.try_recv() {
                Ok(WorkerEvent::ExecuteParsed {
                    worker_id,
                    connection_id,
                    request_id,
                    parsed,
                }) => {
                    drained = drained.saturating_add(1);
                    self.handle_parsed_worker_event(
                        app,
                        worker_id,
                        connection_id,
                        request_id,
                        parsed,
                    );
                }
                Ok(WorkerEvent::Disconnected { connection_id }) => {
                    drained = drained.saturating_add(1);
                    self.disconnect_logical_connection(app, connection_id);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(DflyError::InvalidState(
                        "all io workers have stopped unexpectedly",
                    ));
                }
            }
        }
        let completed_after_events = self.drain_pending_runtime_replies(app)?;
        Ok(drained.saturating_add(completed_after_events))
    }

    fn handle_parsed_worker_event(
        &mut self,
        app: &mut ServerApp,
        worker_id: usize,
        connection_id: u64,
        request_id: u64,
        parsed: ParsedCommand,
    ) {
        let execution = {
            let Some(connection) = self.logical_connections.get_mut(&connection_id) else {
                return;
            };
            app.execute_parsed_command_deferred(connection, parsed)
        };

        match execution {
            ParsedCommandExecution::Immediate(reply) => {
                self.record_completed_connection_request(
                    app,
                    connection_id,
                    worker_id,
                    request_id,
                    reply,
                );
            }
            ParsedCommandExecution::Deferred(ticket) => {
                self.pending_runtime_requests.push(PendingRuntimeRequest {
                    worker_id,
                    connection_id,
                    request_id,
                    ticket,
                });
            }
        }
    }

    fn drain_pending_runtime_replies(&mut self, app: &mut ServerApp) -> DflyResult<usize> {
        let mut drained = 0_usize;
        let mut index = 0_usize;
        while index < self.pending_runtime_requests.len() {
            let ready = {
                let pending = &self.pending_runtime_requests[index];
                app.runtime_reply_ticket_ready(&pending.ticket)?
            };
            if !ready {
                index = index.saturating_add(1);
                continue;
            }

            drained = drained.saturating_add(1);
            let pending = self.pending_runtime_requests.swap_remove(index);
            let reply = match app.take_runtime_reply_ticket(&pending.ticket) {
                Ok(reply) => Some(reply),
                Err(error) => {
                    Some(format!("-ERR runtime dispatch failed: {error}\r\n").into_bytes())
                }
            };

            if self
                .logical_connections
                .contains_key(&pending.connection_id)
            {
                self.record_completed_connection_request(
                    app,
                    pending.connection_id,
                    pending.worker_id,
                    pending.request_id,
                    reply,
                );
            }
        }
        Ok(drained)
    }

    fn record_completed_connection_request(
        &mut self,
        app: &mut ServerApp,
        connection_id: u64,
        worker_id: usize,
        request_id: u64,
        reply: Option<Vec<u8>>,
    ) {
        let entry = self
            .reply_order_by_connection
            .entry(connection_id)
            .or_insert_with(ConnectionReplyOrder::new);
        let _ = entry.completed.insert(request_id, reply);
        self.flush_connection_ordered_replies(app, connection_id, worker_id);
    }

    fn flush_connection_ordered_replies(
        &mut self,
        app: &mut ServerApp,
        connection_id: u64,
        worker_id: usize,
    ) {
        let mut outgoing = Vec::new();
        {
            let Some(state) = self.reply_order_by_connection.get_mut(&connection_id) else {
                return;
            };
            loop {
                let Some(reply) = state.completed.remove(&state.next_request_id) else {
                    break;
                };
                state.next_request_id = state.next_request_id.saturating_add(1);
                if let Some(reply) = reply {
                    outgoing.push(reply);
                }
            }
        }

        for reply in outgoing {
            if self
                .dispatch_to_worker(
                    worker_id,
                    WorkerCommand::QueueReply {
                        connection_id,
                        reply,
                    },
                )
                .is_err()
            {
                self.disconnect_logical_connection(app, connection_id);
                return;
            }
        }
    }

    fn disconnect_logical_connection(&mut self, app: &mut ServerApp, connection_id: u64) {
        if let Some(mut connection) = self.logical_connections.remove(&connection_id) {
            app.disconnect_connection(&mut connection);
        }
        let _ = self.connection_workers.remove(&connection_id);
        let _ = self.reply_order_by_connection.remove(&connection_id);
    }

    fn allocate_connection_id(&mut self) -> u64 {
        let connection_id = self.next_connection_id;
        self.next_connection_id = self.next_connection_id.saturating_add(1);
        connection_id
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
    worker_id: usize,
    command_receiver: &Receiver<WorkerCommand>,
    event_sender: &Sender<WorkerEvent>,
    max_events: usize,
    backpressure: BackpressureThresholds,
) {
    let Ok(mut poll) = Poll::new() else {
        return;
    };
    let mut events = Events::with_capacity(max_events);
    let mut connections = HashMap::<Token, WorkerConnection>::new();
    let mut connection_tokens = HashMap::<u64, Token>::new();
    let mut next_token = CONNECTION_TOKEN_START;

    loop {
        if !drain_worker_commands(
            &poll,
            command_receiver,
            event_sender,
            &mut connections,
            &mut connection_tokens,
            &mut next_token,
            backpressure,
        ) {
            return;
        }

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
                worker_id,
                event_sender,
                &mut connections,
                &mut connection_tokens,
                backpressure,
            );
        }
    }
}

fn drain_worker_commands(
    poll: &Poll,
    command_receiver: &Receiver<WorkerCommand>,
    event_sender: &Sender<WorkerEvent>,
    connections: &mut HashMap<Token, WorkerConnection>,
    connection_tokens: &mut HashMap<u64, Token>,
    next_token: &mut usize,
    backpressure: BackpressureThresholds,
) -> bool {
    loop {
        match command_receiver.try_recv() {
            Ok(WorkerCommand::AddConnection {
                connection_id,
                protocol,
                mut socket,
            }) => {
                let token = Token(*next_token);
                *next_token = next_token.saturating_add(1);
                if poll
                    .registry()
                    .register(&mut socket, token, Interest::READABLE)
                    .is_err()
                {
                    let _ = event_sender.send(WorkerEvent::Disconnected { connection_id });
                    continue;
                }
                let _ = connection_tokens.insert(connection_id, token);
                let _ = connections.insert(
                    token,
                    WorkerConnection::new(connection_id, socket, protocol),
                );
            }
            Ok(WorkerCommand::QueueReply {
                connection_id,
                reply,
            }) => {
                let Some(token) = connection_tokens.get(&connection_id).copied() else {
                    continue;
                };

                let mut close_now = false;
                if let Some(connection) = connections.get_mut(&token) {
                    connection.write_buffer.extend_from_slice(&reply);
                    connection.update_backpressure_state(backpressure.high, backpressure.low);
                    if refresh_worker_interest(poll, token, connection).is_err() {
                        close_now = true;
                    }
                }
                if close_now && let Some(connection) = connections.remove(&token) {
                    close_worker_connection(poll, connection, connection_tokens, event_sender);
                }
            }
            Ok(WorkerCommand::Shutdown) | Err(TryRecvError::Disconnected) => return false,
            Err(TryRecvError::Empty) => return true,
        }
    }
}

fn handle_worker_connection_event(
    poll: &Poll,
    snapshot: EventSnapshot,
    worker_id: usize,
    event_sender: &Sender<WorkerEvent>,
    connections: &mut HashMap<Token, WorkerConnection>,
    connection_tokens: &mut HashMap<u64, Token>,
    backpressure: BackpressureThresholds,
) {
    let Some(mut connection) = connections.remove(&snapshot.token) else {
        return;
    };

    if snapshot.closed_or_error() {
        connection.on_peer_closed_or_error();
    }

    if snapshot.readable() && connection.can_read() {
        read_worker_connection_bytes(
            &mut connection,
            worker_id,
            event_sender,
            backpressure.high,
            backpressure.low,
        );
    }
    if snapshot.writable() && connection.should_try_flush() {
        flush_worker_connection_writes(&mut connection, backpressure.high, backpressure.low);
    }

    if connection.should_close_now() {
        close_worker_connection(poll, connection, connection_tokens, event_sender);
        return;
    }

    if refresh_worker_interest(poll, snapshot.token, &mut connection).is_err() {
        close_worker_connection(poll, connection, connection_tokens, event_sender);
        return;
    }

    let _ = connections.insert(snapshot.token, connection);
}

fn read_worker_connection_bytes(
    connection: &mut WorkerConnection,
    worker_id: usize,
    event_sender: &Sender<WorkerEvent>,
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
                connection.parser.feed_bytes(&chunk[..read_len]);
                loop {
                    match connection.parser.try_pop_command() {
                        Ok(Some(parsed)) => {
                            let request_id = connection.next_request_id;
                            connection.next_request_id =
                                connection.next_request_id.saturating_add(1);
                            if event_sender
                                .send(WorkerEvent::ExecuteParsed {
                                    worker_id,
                                    connection_id: connection.connection_id,
                                    request_id,
                                    parsed,
                                })
                                .is_err()
                            {
                                connection.mark_closing();
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            connection
                                .write_buffer
                                .extend_from_slice(format!("-ERR {error}\r\n").as_bytes());
                            connection.mark_draining();
                            connection.update_backpressure_state(
                                write_high_watermark,
                                write_low_watermark,
                            );
                            return;
                        }
                    }
                }
                if connection.read_paused_by_backpressure {
                    // Once backpressure is armed we must stop draining socket bytes in
                    // this readiness turn and leave remaining input in kernel buffers.
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

fn flush_worker_connection_writes(
    connection: &mut WorkerConnection,
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
    mut connection: WorkerConnection,
    connection_tokens: &mut HashMap<u64, Token>,
    event_sender: &Sender<WorkerEvent>,
) {
    let _ = poll.registry().deregister(&mut connection.socket);
    let _ = connection_tokens.remove(&connection.connection_id);
    let _ = event_sender.send(WorkerEvent::Disconnected {
        connection_id: connection.connection_id,
    });
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionLifecycle, ReactorConnection, ServerReactor, ServerReactorConfig,
        ThreadedServerReactor,
    };
    use crate::app::ServerApp;
    use dfly_common::config::RuntimeConfig;
    use dfly_facade::protocol::ClientProtocol;
    use googletest::prelude::*;
    use rstest::rstest;
    use std::io::{Read, Write};
    use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
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
        let mut app = ServerApp::new(RuntimeConfig::default());
        let config = ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        };
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut reactor = ThreadedServerReactor::bind(bind_addr, config)
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
                .poll_once(&mut app, Some(Duration::from_millis(5)))
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
    fn threaded_reactor_executes_resp_and_memcache_roundtrip() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let config = ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        };
        let mut reactor = ThreadedServerReactor::bind_with_memcache(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            config,
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
                .poll_once(&mut app, Some(Duration::from_millis(5)))
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
    fn threaded_reactor_preserves_order_when_get_reply_is_deferred() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let config = ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        };
        let mut reactor =
            ThreadedServerReactor::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config)
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
                .poll_once(&mut app, Some(Duration::from_millis(5)))
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
                .poll_once(&mut app, Some(Duration::from_millis(5)))
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
}
