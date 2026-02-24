//! Replication and journal interface layer.

pub mod journal;
pub mod state;

use journal::{InMemoryJournal, JournalEntry};
use state::{FlowSyncType, ReplicaSyncState, ReplicationState, SyncSessionError};

/// Replication subsystem bootstrap module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationModule {
    /// Whether replication is enabled at bootstrap.
    pub enabled: bool,
    /// Append-only journal buffer.
    pub journal: InMemoryJournal,
    /// Replication state and control-plane metadata.
    pub state: ReplicationState,
}

impl ReplicationModule {
    /// Creates the replication module bootstrap object.
    #[must_use]
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            journal: InMemoryJournal::new(),
            state: ReplicationState::default(),
        }
    }

    /// Appends one entry into replication journal when replication is enabled.
    pub fn append_journal(&mut self, entry: JournalEntry) {
        if self.enabled {
            let _ = self.journal.append(entry);
            self.state
                .set_last_lsn_from_next_cursor(self.journal.current_lsn());
        }
    }

    /// Returns current journal snapshot.
    #[must_use]
    pub fn journal_entries(&self) -> Vec<JournalEntry> {
        self.journal.entries()
    }

    /// Returns current replication LSN cursor.
    #[must_use]
    pub fn journal_lsn(&self) -> u64 {
        self.journal.current_lsn()
    }

    /// Returns master replication id.
    #[must_use]
    pub fn master_replid(&self) -> &str {
        &self.state.master_replid
    }

    /// Returns current master replication offset.
    #[must_use]
    pub fn replication_offset(&self) -> u64 {
        self.state.last_lsn
    }

    /// Returns whether one LSN can be served from current in-memory backlog.
    #[must_use]
    pub fn journal_contains_lsn(&self, lsn: u64) -> bool {
        self.journal.is_lsn_in_buffer(lsn)
    }

    /// Returns journal entry for one LSN when present in backlog.
    #[must_use]
    pub fn journal_entry_at_lsn(&self, lsn: u64) -> Option<JournalEntry> {
        self.journal.entry_at_lsn(lsn)
    }

    /// Records one replica ACK offset.
    pub fn record_replica_ack(&mut self, ack_lsn: u64) {
        self.state.record_ack_lsn(ack_lsn);
    }

    /// Returns highest acknowledged LSN from replicas.
    #[must_use]
    pub fn last_acked_lsn(&self) -> u64 {
        self.state.last_acked_lsn
    }

    /// Returns connected replica count reported by control-plane state.
    #[must_use]
    pub fn connected_replicas(&self) -> usize {
        self.state.connected_replicas
    }

    /// Registers one replica endpoint from `REPLCONF` metadata.
    pub fn register_replica_endpoint(&mut self, address: String, listening_port: u16) {
        self.state
            .register_replica_endpoint(address, listening_port);
    }

    /// Marks all registered replicas as being in full sync.
    pub fn mark_replicas_full_sync(&mut self) {
        self.state
            .set_all_replica_states(ReplicaSyncState::FullSync);
    }

    /// Marks all registered replicas as being in stable sync.
    pub fn mark_replicas_stable_sync(&mut self) {
        self.state
            .set_all_replica_states(ReplicaSyncState::StableSync);
    }

    /// Returns role-compatible replica rows: `(address, port, state)`.
    #[must_use]
    pub fn replica_role_rows(&self) -> Vec<(String, u16, &'static str)> {
        self.state
            .replicas
            .iter()
            .map(|replica| {
                (
                    replica.address.clone(),
                    replica.listening_port,
                    replica.state.as_role_state(),
                )
            })
            .collect()
    }

    /// Returns `INFO REPLICATION` rows: `(address, port, state, lag)`.
    #[must_use]
    pub fn replica_info_rows(&self) -> Vec<(String, u16, &'static str, u64)> {
        let current_offset = self.replication_offset();
        self.state
            .replicas
            .iter()
            .map(|replica| {
                (
                    replica.address.clone(),
                    replica.listening_port,
                    replica.state.as_role_state(),
                    current_offset.saturating_sub(replica.last_acked_lsn),
                )
            })
            .collect()
    }

    /// Records one replica ACK offset for a specific endpoint.
    ///
    /// Returns `false` when endpoint is not registered.
    pub fn record_replica_ack_for_endpoint(
        &mut self,
        address: &str,
        listening_port: u16,
        ack_lsn: u64,
    ) -> bool {
        self.state
            .record_replica_ack_for_endpoint(address, listening_port, ack_lsn)
    }

    /// Counts replicas acknowledged at or above one target offset.
    #[must_use]
    pub fn acked_replica_count_at_or_above(&self, offset: u64) -> usize {
        self.state.acked_replica_count_at_or_above(offset)
    }

    /// Creates one sync session id used by `DFLY` replication commands.
    pub fn create_sync_session(&mut self, flow_count: usize) -> String {
        self.state.create_sync_session(flow_count)
    }

    /// Returns whether one sync session id is currently known.
    #[must_use]
    pub fn is_known_sync_session(&self, sync_id: &str) -> bool {
        self.state.is_known_sync_session(sync_id)
    }

    /// Registers one flow under one existing sync session.
    ///
    /// # Errors
    ///
    /// Returns `SyncSessionError` when session id is unknown, flow id is out of range,
    /// or session state does not allow additional flow registration.
    pub fn register_sync_flow(
        &mut self,
        sync_id: &str,
        flow_id: usize,
        sync_type: FlowSyncType,
        start_offset: Option<u64>,
        eof_token: String,
    ) -> Result<(), SyncSessionError> {
        self.state
            .register_sync_flow(sync_id, flow_id, sync_type, start_offset, eof_token)
    }

    /// Marks one sync session as full-sync.
    ///
    /// # Errors
    ///
    /// Returns `SyncSessionError` when the session is unknown, not in preparation,
    /// or still missing one or more registered flows.
    pub fn mark_sync_session_full_sync(&mut self, sync_id: &str) -> Result<(), SyncSessionError> {
        self.state.mark_sync_session_full_sync(sync_id)
    }

    /// Marks one sync session as stable-sync.
    ///
    /// # Errors
    ///
    /// Returns `SyncSessionError` when the session is unknown or already stable.
    pub fn mark_sync_session_stable_sync(&mut self, sync_id: &str) -> Result<(), SyncSessionError> {
        self.state.mark_sync_session_stable_sync(sync_id)
    }

    /// Allocates one flow EOF token for `DFLY FLOW` response.
    pub fn allocate_flow_eof_token(&mut self) -> String {
        self.state.allocate_flow_eof_token()
    }

    /// Returns whether partial sync can continue from one replica offset.
    ///
    /// The offset semantics follow Redis/Dragonfly handshake:
    /// replica sends the last applied offset, so the first required entry is `offset + 1`.
    #[must_use]
    pub fn can_partial_sync_from_offset(&self, offset: u64) -> bool {
        let current_offset = self.replication_offset();
        if offset > current_offset {
            return false;
        }
        if offset == current_offset {
            return true;
        }
        self.journal_contains_lsn(offset.saturating_add(1))
    }
}
