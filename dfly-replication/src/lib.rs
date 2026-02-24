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

    /// Returns journal suffix from one starting LSN (inclusive) when available in backlog.
    #[must_use]
    pub fn journal_entries_from_lsn(&self, start_lsn: u64) -> Option<Vec<JournalEntry>> {
        self.journal.entries_from_lsn(start_lsn)
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

    /// Removes one replica endpoint when present.
    ///
    /// Returns `true` when one endpoint was removed.
    pub fn remove_replica_endpoint(&mut self, address: &str, listening_port: u16) -> bool {
        self.state.remove_replica_endpoint(address, listening_port)
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
                let lag = if replica.state == ReplicaSyncState::StableSync {
                    current_offset.saturating_sub(replica.last_acked_lsn)
                } else {
                    0
                };
                (
                    replica.address.clone(),
                    replica.listening_port,
                    replica.state.as_role_state(),
                    lag,
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

    /// Returns the journal payload slice visible to one negotiated sync flow.
    ///
    /// `FULL` flows do not read from journal backlog and therefore return an empty entry list.
    /// `PARTIAL` flows return entries starting from `start_offset + 1` (inclusive).
    ///
    /// Returns `None` when the sync/session flow tuple is unknown or when partial backlog
    /// entries are no longer available in memory.
    #[must_use]
    pub fn flow_journal_entries(&self, sync_id: &str, flow_id: usize) -> Option<Vec<JournalEntry>> {
        let flow = self.state.sync_flow(sync_id, flow_id)?;
        match flow.sync_type {
            FlowSyncType::Full => Some(Vec::new()),
            FlowSyncType::Partial => {
                let start_offset = flow.start_offset?;
                self.journal_entries_from_lsn(start_offset.saturating_add(1))
            }
        }
    }

    /// Resets journal buffer and replication metadata after restoring state from a snapshot.
    pub fn reset_after_snapshot_load(&mut self) {
        self.journal.reset();
        self.state.reset_after_snapshot_load();
    }
}

#[cfg(test)]
mod tests {
    use super::ReplicationModule;
    use crate::journal::{InMemoryJournal, JournalEntry, JournalOp};
    use crate::state::FlowSyncType;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn flow_journal_entries_returns_suffix_for_partial_flow() {
        let mut replication = ReplicationModule::new(true);
        replication.append_journal(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET flow a".to_vec(),
        });
        replication.append_journal(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET flow b".to_vec(),
        });
        replication.append_journal(JournalEntry {
            txid: 3,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET flow c".to_vec(),
        });

        let sync_id = replication.create_sync_session(1);
        let eof_token = replication.allocate_flow_eof_token();
        assert_that!(
            replication.register_sync_flow(&sync_id, 0, FlowSyncType::Partial, Some(1), eof_token),
            ok(())
        );

        let suffix = replication
            .flow_journal_entries(&sync_id, 0)
            .expect("partial flow should return journal suffix");
        assert_that!(suffix.len(), eq(2_usize));
        assert_that!(suffix[0].txid, eq(2_u64));
        assert_that!(suffix[1].txid, eq(3_u64));
    }

    #[rstest]
    fn flow_journal_entries_returns_empty_for_full_flow() {
        let mut replication = ReplicationModule::new(true);
        replication.append_journal(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET flow v".to_vec(),
        });

        let sync_id = replication.create_sync_session(1);
        let eof_token = replication.allocate_flow_eof_token();
        assert_that!(
            replication.register_sync_flow(&sync_id, 0, FlowSyncType::Full, None, eof_token),
            ok(())
        );

        let entries = replication
            .flow_journal_entries(&sync_id, 0)
            .expect("full flow should return a valid empty journal slice");
        assert_that!(entries.is_empty(), eq(true));
    }

    #[rstest]
    fn flow_journal_entries_returns_none_for_unknown_or_stale_flow_state() {
        let mut replication = ReplicationModule::new(true);
        replication.journal = InMemoryJournal::with_backlog(1);
        replication.append_journal(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET flow old".to_vec(),
        });
        replication.append_journal(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET flow new".to_vec(),
        });

        assert_that!(
            replication.flow_journal_entries("SYNC404", 0).is_none(),
            eq(true)
        );

        let sync_id = replication.create_sync_session(2);
        let stale_flow_token = replication.allocate_flow_eof_token();
        assert_that!(
            replication.register_sync_flow(
                &sync_id,
                0,
                FlowSyncType::Partial,
                Some(0),
                stale_flow_token
            ),
            ok(())
        );
        assert_that!(
            replication.flow_journal_entries(&sync_id, 0).is_none(),
            eq(true)
        );
        assert_that!(
            replication.flow_journal_entries(&sync_id, 1).is_none(),
            eq(true)
        );
    }

    #[rstest]
    fn reset_after_snapshot_load_rewinds_journal_and_replication_offsets() {
        let mut replication = ReplicationModule::new(true);
        let original_replid = replication.master_replid().to_owned();
        replication.append_journal(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET reset key".to_vec(),
        });
        replication.register_replica_endpoint("10.0.0.1".to_owned(), 7001);
        let _ = replication.record_replica_ack_for_endpoint("10.0.0.1", 7001, 1);
        let sync_id = replication.create_sync_session(1);
        let eof_token = replication.allocate_flow_eof_token();
        assert_that!(
            replication.register_sync_flow(&sync_id, 0, FlowSyncType::Full, None, eof_token),
            ok(())
        );

        assert_that!(replication.replication_offset(), eq(1_u64));
        assert_that!(replication.connected_replicas(), eq(1_usize));
        assert_that!(replication.journal_lsn(), eq(2_u64));

        replication.reset_after_snapshot_load();

        assert_that!(replication.master_replid(), eq(original_replid.as_str()));
        assert_that!(replication.replication_offset(), eq(0_u64));
        assert_that!(replication.connected_replicas(), eq(0_usize));
        assert_that!(replication.last_acked_lsn(), eq(0_u64));
        assert_that!(replication.journal_entries().is_empty(), eq(true));
        assert_that!(replication.journal_lsn(), eq(1_u64));
        assert_that!(replication.is_known_sync_session(&sync_id), eq(false));
    }
}
