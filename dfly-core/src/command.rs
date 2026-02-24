//! Canonical command frame types.

/// Command payload representation used between facade and coordinator/runtime layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandFrame {
    /// Command name in uppercase canonical form (e.g. `SET`, `MGET`, `FT.SEARCH`).
    pub name: String,
    /// Raw byte arguments preserving wire-level payload.
    pub args: Vec<Vec<u8>>,
}

impl CommandFrame {
    /// Creates a command frame from a command name and argument list.
    #[must_use]
    pub fn new(name: impl Into<String>, args: Vec<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            args,
        }
    }
}

/// Canonical command reply representation.
///
/// The reply enum is kept protocol-neutral. Encoding to RESP (or other protocols) happens
/// at the facade boundary, so coordinator/runtime logic can stay independent from wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandReply {
    /// `+OK` style replies.
    SimpleString(String),
    /// `$<len> ...` style binary-safe payload.
    BulkString(Vec<u8>),
    /// RESP null bulk string (`$-1`).
    Null,
    /// RESP integer reply (`:<n>`).
    Integer(i64),
    /// RESP array reply (`*<n> ...`).
    Array(Vec<CommandReply>),
    /// RESP null array (`*-1`) used by `EXEC` abort on watched key changes.
    NullArray,
    /// `-ERR ...` style error.
    Error(String),
}

impl CommandReply {
    /// Encodes the reply into RESP bytes for Redis-compatible clients.
    #[must_use]
    pub fn to_resp_bytes(&self) -> Vec<u8> {
        match self {
            Self::SimpleString(value) => {
                let mut output = Vec::with_capacity(value.len() + 3);
                output.extend_from_slice(b"+");
                output.extend_from_slice(value.as_bytes());
                output.extend_from_slice(b"\r\n");
                output
            }
            Self::BulkString(value) => {
                let mut output = Vec::new();
                output.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
                output.extend_from_slice(value);
                output.extend_from_slice(b"\r\n");
                output
            }
            Self::Null => b"$-1\r\n".to_vec(),
            Self::Integer(value) => format!(":{value}\r\n").into_bytes(),
            Self::Array(items) => {
                let mut output = format!("*{}\r\n", items.len()).into_bytes();
                for item in items {
                    output.extend_from_slice(&item.to_resp_bytes());
                }
                output
            }
            Self::NullArray => b"*-1\r\n".to_vec(),
            Self::Error(message) => {
                let mut output = Vec::with_capacity(message.len() + 6);
                output.extend_from_slice(b"-ERR ");
                output.extend_from_slice(message.as_bytes());
                output.extend_from_slice(b"\r\n");
                output
            }
        }
    }
}
