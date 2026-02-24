//! Shared error model for cross-crate APIs.

use thiserror::Error;

/// Unified result type used by all public interfaces in `dragonfly-rs`.
pub type DflyResult<T> = Result<T, DflyError>;

/// High-level error categories used during early scaffolding.
///
/// The variants remain intentionally broad in Unit 0. Later units split these into protocol,
/// storage, transaction, replication, and cluster specific categories.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DflyError {
    /// Configuration is invalid for the requested operation.
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),

    /// Runtime state does not allow this operation.
    #[error("invalid runtime state: {0}")]
    InvalidState(&'static str),

    /// Client protocol payload is malformed or semantically invalid.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Placeholder for not-yet-implemented feature paths.
    #[error("feature is not implemented yet: {0}")]
    NotImplemented(&'static str),

    /// Filesystem I/O failed.
    #[error("io error: {0}")]
    Io(String),
}
