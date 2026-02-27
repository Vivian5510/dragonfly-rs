use super::*;

#[derive(Debug)]
pub(super) struct ReactorConnection {
    pub(super) socket: TcpStream,
    pub(super) logical: ServerConnection,
    pub(super) write_buffer: Vec<u8>,
    pub(super) lifecycle: ConnectionLifecycle,
    pub(super) read_paused_by_backpressure: bool,
    pub(super) interest: Interest,
}

impl ReactorConnection {
    pub(super) fn new(socket: TcpStream, protocol: ClientProtocol) -> Self {
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

/// One reactor instance managing protocol listeners and all accepted connections.
#[derive(Debug)]
pub(super) struct ServerReactor {
    poll: Poll,
    events: Events,
    ready_events: Vec<(Token, bool, bool, bool)>,
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
    pub(super) fn bind(addr: SocketAddr, config: ServerReactorConfig) -> DflyResult<Self> {
        Self::bind_with_memcache(addr, None, config)
    }

    /// Binds RESP listener and optional Memcache listener in one reactor.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::InvalidState` if any listener bind/registration fails.
    pub(super) fn bind_with_memcache(
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
            ready_events: Vec::with_capacity(config.normalized_max_events()),
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
    pub(super) fn poll_once(
        &mut self,
        app: &mut ServerApp,
        timeout: Option<Duration>,
    ) -> DflyResult<usize> {
        self.poll
            .poll(&mut self.events, timeout)
            .map_err(|error| DflyError::Io(format!("poll wait failed: {error}")))?;
        self.ready_events.clear();
        for event in &self.events {
            self.ready_events.push((
                event.token(),
                event.is_readable(),
                event.is_writable(),
                event.is_read_closed() || event.is_write_closed() || event.is_error(),
            ));
        }

        let ready_count = self.ready_events.len();
        for index in 0..ready_count {
            let (token, readable, writable, closed_or_error) = self.ready_events[index];
            if let Some(listener_index) = self.listener_index(token) {
                self.accept_new_connections(listener_index)?;
                continue;
            }
            self.handle_connection_event(app, token, readable, writable, closed_or_error)?;
        }

        Ok(self.ready_events.len())
    }

    pub(super) fn local_addr(&self) -> DflyResult<SocketAddr> {
        self.listener_local_addr(ClientProtocol::Resp)
    }

    pub(super) fn memcache_local_addr(&self) -> DflyResult<SocketAddr> {
        self.listener_local_addr(ClientProtocol::Memcache)
    }

    pub(super) fn connection_count(&self) -> usize {
        self.connections.len()
    }

    fn listener_index(&self, token: Token) -> Option<usize> {
        self.listeners
            .iter()
            .position(|listener| listener.token == token)
    }

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
        token: Token,
        readable: bool,
        writable: bool,
        closed_or_error: bool,
    ) -> DflyResult<()> {
        let Some(mut connection) = self.connections.remove(&token) else {
            return Ok(());
        };

        if closed_or_error {
            connection.on_peer_closed_or_error();
        }

        if readable && connection.can_read() {
            Self::read_connection_bytes(
                app,
                &mut connection,
                self.write_high_watermark,
                self.write_low_watermark,
            );
        }
        if writable && connection.should_try_flush() {
            Self::flush_connection_writes(
                &mut connection,
                self.write_high_watermark,
                self.write_low_watermark,
            );
        }

        if connection.should_close_now() {
            self.close_connection(app, token, connection)?;
            return Ok(());
        }

        self.refresh_connection_interest(token, &mut connection)?;
        let _ = self.connections.insert(token, connection);
        Ok(())
    }

    pub(super) fn read_connection_bytes(
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
