//! Reactor-style network event loop for RESP ingress.
//!
//! Dragonfly keeps socket ownership in dedicated I/O threads and advances parsing/execution
//! from readiness events. This module provides the same shape for `dragonfly-rs`:
//! one listener + per-connection state machine driven by `mio::Poll`.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::Duration;

use dfly_common::error::{DflyError, DflyResult};
use dfly_facade::protocol::ClientProtocol;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use crate::app::{ServerApp, ServerConnection};

const LISTENER_TOKEN: Token = Token(0);
const CONNECTION_TOKEN_START: usize = 1;
const READ_CHUNK_BYTES: usize = 8192;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerReactorConfig {
    pub max_events: usize,
}

impl ServerReactorConfig {
    #[must_use]
    pub fn normalized_max_events(self) -> usize {
        self.max_events.max(64)
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

#[derive(Debug)]
struct ReactorConnection {
    socket: TcpStream,
    logical: ServerConnection,
    write_buffer: Vec<u8>,
    closing: bool,
    interest: Interest,
}

impl ReactorConnection {
    fn new(socket: TcpStream) -> Self {
        Self {
            socket,
            logical: ServerApp::new_connection(ClientProtocol::Resp),
            write_buffer: Vec::new(),
            closing: false,
            interest: Interest::READABLE,
        }
    }
}

/// One reactor instance managing a single RESP listener and all accepted connections.
#[derive(Debug)]
pub struct ServerReactor {
    poll: Poll,
    events: Events,
    listener: TcpListener,
    next_token: usize,
    connections: HashMap<Token, ReactorConnection>,
}

impl ServerReactor {
    /// Binds one RESP listener and registers it in the reactor poller.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` if the listener or poll registration fails.
    pub fn bind(addr: SocketAddr, config: ServerReactorConfig) -> DflyResult<Self> {
        let mut listener = TcpListener::bind(addr)
            .map_err(|error| DflyError::Io(format!("bind listener failed: {error}")))?;
        let poll =
            Poll::new().map_err(|error| DflyError::Io(format!("create poll failed: {error}")))?;
        poll.registry()
            .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)
            .map_err(|error| DflyError::Io(format!("register listener in poll failed: {error}")))?;

        Ok(Self {
            poll,
            events: Events::with_capacity(config.normalized_max_events()),
            listener,
            next_token: CONNECTION_TOKEN_START,
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
            if snapshot.token == LISTENER_TOKEN {
                self.accept_new_connections()?;
                continue;
            }
            self.handle_connection_event(app, *snapshot)?;
        }

        Ok(snapshots.len())
    }

    #[cfg(test)]
    fn local_addr(&self) -> DflyResult<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|error| DflyError::Io(format!("query local address failed: {error}")))
    }

    #[cfg(test)]
    fn connection_count(&self) -> usize {
        self.connections.len()
    }

    fn accept_new_connections(&mut self) -> DflyResult<()> {
        loop {
            match self.listener.accept() {
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
                        .insert(token, ReactorConnection::new(socket));
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
            connection.closing = true;
        }

        if snapshot.readable() && !connection.closing {
            Self::read_connection_bytes(app, &mut connection);
        }
        if snapshot.writable() && !connection.write_buffer.is_empty() {
            Self::flush_connection_writes(&mut connection);
        }

        if connection.closing && connection.write_buffer.is_empty() {
            self.close_connection(app, snapshot.token, connection)?;
            return Ok(());
        }

        self.refresh_connection_interest(snapshot.token, &mut connection)?;
        let _ = self.connections.insert(snapshot.token, connection);
        Ok(())
    }

    fn read_connection_bytes(app: &mut ServerApp, connection: &mut ReactorConnection) {
        let mut chunk = [0_u8; READ_CHUNK_BYTES];
        loop {
            match connection.socket.read(&mut chunk) {
                Ok(0) => {
                    connection.closing = true;
                    return;
                }
                Ok(read_len) => {
                    match app.feed_connection_bytes(&mut connection.logical, &chunk[..read_len]) {
                        Ok(replies) => {
                            for reply in replies {
                                connection.write_buffer.extend_from_slice(&reply);
                            }
                        }
                        Err(error) => {
                            connection
                                .write_buffer
                                .extend_from_slice(format!("-ERR {error}\r\n").as_bytes());
                            connection.closing = true;
                            return;
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
                Err(_error) => {
                    connection.closing = true;
                    return;
                }
            }
        }
    }

    fn flush_connection_writes(connection: &mut ReactorConnection) {
        while !connection.write_buffer.is_empty() {
            match connection.socket.write(connection.write_buffer.as_slice()) {
                Ok(0) => {
                    connection.closing = true;
                    return;
                }
                Ok(written) => {
                    let _ = connection.write_buffer.drain(..written);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
                Err(_error) => {
                    connection.closing = true;
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
        let next_interest = if connection.write_buffer.is_empty() {
            Interest::READABLE
        } else {
            Interest::READABLE | Interest::WRITABLE
        };
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
    use super::{ServerReactor, ServerReactorConfig};
    use crate::app::ServerApp;
    use dfly_common::config::RuntimeConfig;
    use googletest::prelude::*;
    use rstest::rstest;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
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
}
