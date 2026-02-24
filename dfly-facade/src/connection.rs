//! Connection-scoped context placeholders.

use crate::protocol::ClientProtocol;

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
