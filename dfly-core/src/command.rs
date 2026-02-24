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
