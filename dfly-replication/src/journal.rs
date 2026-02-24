//! Journal entry model used by replication pathways.

use dfly_common::ids::{DbIndex, TxId};

/// Journal operation kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalOp {
    /// Regular command payload.
    Command,
    /// Expiry-driven delete payload.
    Expired,
    /// Keepalive/ping marker.
    Ping,
    /// LSN marker entry.
    Lsn,
}

/// One append-only journal record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    /// Transaction id associated with this record.
    pub txid: TxId,
    /// Database id associated with this record.
    pub db: DbIndex,
    /// Operation kind.
    pub op: JournalOp,
    /// Raw serialized command payload.
    pub payload: Vec<u8>,
}
