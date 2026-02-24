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

/// In-memory append-only journal used by early replication flow.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InMemoryJournal {
    entries: Vec<JournalEntry>,
}

impl InMemoryJournal {
    /// Creates an empty journal.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Appends one entry to the journal tail.
    pub fn append(&mut self, entry: JournalEntry) {
        self.entries.push(entry);
    }

    /// Returns all journal entries in append order.
    #[must_use]
    pub fn entries(&self) -> &[JournalEntry] {
        &self.entries
    }

    /// Number of currently buffered entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the journal is currently empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryJournal, JournalEntry, JournalOp};
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn in_memory_journal_appends_entries_in_order() {
        let mut journal = InMemoryJournal::new();
        journal.append(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a b".to_vec(),
        });
        journal.append(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Ping,
            payload: Vec::new(),
        });

        assert_that!(journal.len(), eq(2_usize));
        assert_that!(journal.entries()[0].txid, eq(1_u64));
        assert_that!(journal.entries()[1].op, eq(JournalOp::Ping));
    }
}
