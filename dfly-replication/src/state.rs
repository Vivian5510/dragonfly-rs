//! Replication state tracking placeholders.

/// Mutable replication state for one replica session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReplicationState {
    /// Whether full sync has finished.
    pub full_sync_done: bool,
    /// Last known replicated LSN.
    pub last_lsn: u64,
}
