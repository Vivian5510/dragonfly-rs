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
            self.journal.append(entry);
        }
    }

    /// Returns current journal snapshot.
    #[must_use]
    pub fn journal_entries(&self) -> &[JournalEntry] {
        self.journal.entries()
    }
}
