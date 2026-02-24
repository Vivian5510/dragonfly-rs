//! Protocol-level command containers.

/// Supported client wire protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientProtocol {
    /// Redis RESP protocol family.
    Resp,
    /// Memcached text protocol.
    Memcache,
}

/// Protocol-decoded command representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCommand {
    /// Command name in canonical uppercase form.
    pub name: String,
    /// Raw argument payload.
    pub args: Vec<Vec<u8>>,
}
