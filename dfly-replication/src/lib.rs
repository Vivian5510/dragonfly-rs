//! Replication and journal interface layer.

pub mod journal;
pub mod state;

use journal::{InMemoryJournal, JournalEntry};
use state::ReplicationState;

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
