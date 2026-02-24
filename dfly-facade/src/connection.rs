//! Connection-scoped context and streaming parser state.

use dfly_common::error::DflyResult;

use crate::protocol::ClientProtocol;
use crate::protocol::{ParseStatus, ParsedCommand, parse_next_command};

/// Per-connection execution context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionContext {
    /// Active wire protocol.
    pub protocol: ClientProtocol,
    /// Selected logical DB index for this client.
    pub db_index: u16,
    /// Whether this connection has privileged/admin rights.
    pub privileged: bool,
}

/// Per-socket state used while reading client bytes.
///
/// In Dragonfly, a connection parser keeps unread bytes in a buffer and repeatedly tries to
/// extract complete commands as new network chunks arrive. This struct models that behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionState {
    /// Stable connection metadata.
    pub context: ConnectionContext,
    /// Bytes received but not yet consumed by command parsing.
    read_buffer: Vec<u8>,
}

impl ConnectionState {
    /// Creates a parser state object for one client connection.
    #[must_use]
    pub fn new(context: ConnectionContext) -> Self {
        Self {
            context,
            read_buffer: Vec::new(),
        }
    }

    /// Appends newly received network bytes into the connection buffer.
    pub fn feed_bytes(&mut self, bytes: &[u8]) {
        self.read_buffer.extend_from_slice(bytes);
    }

    /// Tries to decode one command from buffered bytes.
    ///
    /// Returns `Ok(None)` when more bytes are required.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` when buffered bytes violate the selected
    /// client protocol framing rules.
    pub fn try_pop_command(&mut self) -> DflyResult<Option<ParsedCommand>> {
        match parse_next_command(self.context.protocol, &self.read_buffer)? {
            ParseStatus::Incomplete => Ok(None),
            ParseStatus::Complete { command, consumed } => {
                self.read_buffer.drain(..consumed);
                Ok(Some(command))
            }
        }
    }

    /// Returns the number of pending bytes still waiting to be parsed.
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.read_buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{ConnectionContext, ConnectionState};
    use crate::protocol::ClientProtocol;
    use googletest::prelude::*;
    use rstest::rstest;

    fn make_resp_connection() -> ConnectionState {
        ConnectionState::new(ConnectionContext {
            protocol: ClientProtocol::Resp,
            db_index: 0,
            privileged: false,
        })
    }

    fn make_memcache_connection() -> ConnectionState {
        ConnectionState::new(ConnectionContext {
            protocol: ClientProtocol::Memcache,
            db_index: 0,
            privileged: false,
        })
    }

    #[rstest]
    fn connection_parses_command_across_multiple_feeds() {
        let mut connection = make_resp_connection();
        connection.feed_bytes(b"*2\r\n$4\r\nECHO\r\n$5\r\nhe");

        let first_attempt = connection
            .try_pop_command()
            .expect("parser should not fail on partial input");
        assert_that!(&first_attempt, eq(&None));

        connection.feed_bytes(b"llo\r\n");
        let parsed = connection
            .try_pop_command()
            .expect("command should parse once bytes are complete")
            .expect("one command should be available");
        let expected_args = vec![b"hello".to_vec()];
        assert_that!(parsed.name, eq("ECHO"));
        assert_that!(&parsed.args, eq(&expected_args));
        assert_that!(connection.pending_bytes(), eq(0));
    }

    #[rstest]
    fn connection_keeps_remaining_bytes_for_next_command() {
        let mut connection = make_resp_connection();
        connection.feed_bytes(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n");

        let first = connection
            .try_pop_command()
            .expect("first parse should succeed")
            .expect("first command exists");
        assert_that!(first.name, eq("PING"));
        assert_that!(connection.pending_bytes() > 0, eq(true));

        let second = connection
            .try_pop_command()
            .expect("second parse should succeed")
            .expect("second command exists");
        let expected_args = vec![b"hello".to_vec()];
        assert_that!(second.name, eq("ECHO"));
        assert_that!(&second.args, eq(&expected_args));
        assert_that!(connection.pending_bytes(), eq(0));
    }

    #[rstest]
    fn memcache_connection_waits_until_set_payload_is_complete() {
        let mut connection = make_memcache_connection();
        connection.feed_bytes(b"set profile:1 0 0 5\r\nali");

        let first_attempt = connection
            .try_pop_command()
            .expect("partial payload should not fail");
        assert_that!(&first_attempt, eq(&None));

        connection.feed_bytes(b"ce\r\n");
        let parsed = connection
            .try_pop_command()
            .expect("set command should parse once payload is complete")
            .expect("one command should be available");
        let expected_args = vec![b"profile:1".to_vec(), b"alice".to_vec()];
        assert_that!(parsed.name, eq("SET"));
        assert_that!(&parsed.args, eq(&expected_args));
    }
}
