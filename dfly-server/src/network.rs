//! Reactor-style network event loop for RESP and Memcache ingress.
//!
//! Dragonfly keeps socket ownership in dedicated I/O threads and advances parsing/execution
//! from readiness events. This module provides the same shape for `dragonfly-rs`:
//! per-protocol listeners + per-connection state machine driven by `mio::Poll`.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::Duration;

use dfly_common::error::{DflyError, DflyResult};
use dfly_facade::protocol::ClientProtocol;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use crate::app::{ServerApp, ServerConnection};
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
}

impl ServerReactorConfig {
    #[must_use]
    pub fn normalized_max_events(self) -> usize {
        self.max_events.max(64)
    }

    #[must_use]
    pub fn normalized_backpressure_watermarks(self) -> (usize, usize) {
        let high = self
            .write_high_watermark_bytes
            .max(DEFAULT_WRITE_HIGH_WATERMARK_BYTES);
        let mut low = self
            .write_low_watermark_bytes
            .max(DEFAULT_WRITE_LOW_WATERMARK_BYTES);
        if low >= high {
            low = high.saturating_sub(1);
        }
        (high, low)
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

#[derive(Debug)]
struct ReactorConnection {
    socket: TcpStream,
    logical: ServerConnection,
    write_buffer: Vec<u8>,
    lifecycle: ConnectionLifecycle,
    read_paused_by_backpressure: bool,
    interest: Interest,
}

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
            config.normalized_backpressure_watermarks();

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

#[cfg(test)]
mod tests {
    use super::{ConnectionLifecycle, ReactorConnection, ServerReactor, ServerReactorConfig};
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
}
