//! Replication and journal interface layer.

pub mod journal;
pub mod state;

use journal::{InMemoryJournal, JournalEntry};

/// Replication subsystem bootstrap module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationModule {
    /// Whether replication is enabled at bootstrap.
    pub enabled: bool,
    /// Append-only journal buffer.
    pub journal: InMemoryJournal,
}

impl ReplicationModule {
    /// Creates the replication module bootstrap object.
    #[must_use]
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            journal: InMemoryJournal::new(),
        }
    }

    /// Appends one entry into replication journal when replication is enabled.
    pub fn append_journal(&mut self, entry: JournalEntry) {
        if self.enabled {
            let _ = self.journal.append(entry);
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
}
