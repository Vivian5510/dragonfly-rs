//! Journal entry model used by replication pathways.

use std::collections::VecDeque;

use dfly_common::ids::{DbIndex, TxId};

/// Default number of journal entries kept in partial-sync backlog.
pub const DEFAULT_BACKLOG_LEN: usize = 8192;

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

/// Journal record with globally ordered LSN.
#[derive(Debug, Clone, PartialEq, Eq)]
struct JournalRecord {
    lsn: u64,
    entry: JournalEntry,
}

/// In-memory append-only journal with bounded backlog and monotonic LSN tracking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InMemoryJournal {
    records: VecDeque<JournalRecord>,
    backlog_len: usize,
    next_lsn: u64,
}

impl Default for InMemoryJournal {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryJournal {
    /// Creates an empty journal with default backlog size.
    #[must_use]
    pub fn new() -> Self {
        Self::with_backlog(DEFAULT_BACKLOG_LEN)
    }

    /// Creates an empty journal with custom backlog size.
    ///
    /// Backlog size of zero is coerced to one entry.
    #[must_use]
    pub fn with_backlog(backlog_len: usize) -> Self {
        let backlog_len = backlog_len.max(1);
        Self {
            records: VecDeque::with_capacity(backlog_len),
            backlog_len,
            next_lsn: 1,
        }
    }

    /// Appends one entry to the journal tail and returns its assigned LSN.
    pub fn append(&mut self, entry: JournalEntry) -> u64 {
        let lsn = self.next_lsn;
        self.next_lsn = self.next_lsn.saturating_add(1);

        if self.records.len() == self.backlog_len {
            let _ = self.records.pop_front();
        }
        self.records.push_back(JournalRecord { lsn, entry });
        lsn
    }

    /// Returns current journal LSN cursor (LSN of the next appended entry).
    #[must_use]
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Returns whether one LSN can be served from current in-memory backlog.
    #[must_use]
    pub fn is_lsn_in_buffer(&self, lsn: u64) -> bool {
        let Some(front) = self.records.front() else {
            return false;
        };
        let Some(back) = self.records.back() else {
            return false;
        };
        front.lsn <= lsn && lsn <= back.lsn
    }

    /// Returns one journal entry by LSN when present in current backlog.
    #[must_use]
    pub fn entry_at_lsn(&self, lsn: u64) -> Option<JournalEntry> {
        if !self.is_lsn_in_buffer(lsn) {
            return None;
        }
        let front_lsn = self.records.front().map(|record| record.lsn)?;
        let offset = usize::try_from(lsn.saturating_sub(front_lsn)).ok()?;
        self.records.get(offset).map(|record| record.entry.clone())
    }

    /// Returns all journal entries in append order.
    #[must_use]
    pub fn entries(&self) -> Vec<JournalEntry> {
        self.records
            .iter()
            .map(|record| record.entry.clone())
            .collect()
    }

    /// Returns journal entries from one starting LSN (inclusive).
    ///
    /// `start_lsn` follows backlog cursor semantics:
    /// - `start_lsn == current_lsn()` returns an empty suffix.
    /// - stale/future cursors return `None`.
    #[must_use]
    pub fn entries_from_lsn(&self, start_lsn: u64) -> Option<Vec<JournalEntry>> {
        if start_lsn == self.current_lsn() {
            return Some(Vec::new());
        }
        if start_lsn > self.current_lsn() {
            return None;
        }
        if !self.is_lsn_in_buffer(start_lsn) {
            return None;
        }
        Some(
            self.records
                .iter()
                .filter(|record| record.lsn >= start_lsn)
                .map(|record| record.entry.clone())
                .collect(),
        )
    }

    /// Number of currently buffered entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the journal is currently empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Clears buffered entries and resets the LSN cursor to the initial state.
    pub fn reset(&mut self) {
        self.records.clear();
        self.next_lsn = 1;
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
        let lsn1 = journal.append(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a b".to_vec(),
        });
        let lsn2 = journal.append(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Ping,
            payload: Vec::new(),
        });

        assert_that!(lsn1, eq(1_u64));
        assert_that!(lsn2, eq(2_u64));
        assert_that!(journal.current_lsn(), eq(3_u64));
        assert_that!(journal.len(), eq(2_usize));

        let entries = journal.entries();
        assert_that!(entries[0].txid, eq(1_u64));
        assert_that!(entries[1].op, eq(JournalOp::Ping));
    }

    #[rstest]
    fn in_memory_journal_backlog_evicts_old_entries() {
        let mut journal = InMemoryJournal::with_backlog(2);
        let _ = journal.append(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a 1".to_vec(),
        });
        let _ = journal.append(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET b 2".to_vec(),
        });
        let _ = journal.append(JournalEntry {
            txid: 3,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET c 3".to_vec(),
        });

        assert_that!(journal.len(), eq(2_usize));
        assert_that!(journal.is_lsn_in_buffer(1), eq(false));
        assert_that!(journal.is_lsn_in_buffer(2), eq(true));
        assert_that!(journal.is_lsn_in_buffer(3), eq(true));
        assert_that!(journal.entry_at_lsn(1).is_none(), eq(true));
        let lsn2_payload = journal
            .entry_at_lsn(2)
            .map(|entry| String::from_utf8_lossy(&entry.payload).to_string());
        let lsn3_payload = journal
            .entry_at_lsn(3)
            .map(|entry| String::from_utf8_lossy(&entry.payload).to_string());
        assert_that!(&lsn2_payload, eq(&Some("SET b 2".to_owned())));
        assert_that!(&lsn3_payload, eq(&Some("SET c 3".to_owned())));
    }

    #[rstest]
    fn in_memory_journal_entries_from_lsn_returns_suffix_or_empty_cursor() {
        let mut journal = InMemoryJournal::new();
        let _ = journal.append(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a 1".to_vec(),
        });
        let _ = journal.append(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a 2".to_vec(),
        });
        let _ = journal.append(JournalEntry {
            txid: 3,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a 3".to_vec(),
        });

        let suffix = journal
            .entries_from_lsn(2)
            .expect("lsn 2 should be available");
        assert_that!(suffix.len(), eq(2_usize));
        assert_that!(suffix[0].txid, eq(2_u64));
        assert_that!(suffix[1].txid, eq(3_u64));

        let empty_suffix = journal
            .entries_from_lsn(journal.current_lsn())
            .expect("current cursor should return empty suffix");
        assert_that!(empty_suffix.is_empty(), eq(true));
    }

    #[rstest]
    fn in_memory_journal_entries_from_lsn_rejects_stale_or_future_cursor() {
        let mut journal = InMemoryJournal::with_backlog(2);
        let _ = journal.append(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a 1".to_vec(),
        });
        let _ = journal.append(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a 2".to_vec(),
        });
        let _ = journal.append(JournalEntry {
            txid: 3,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a 3".to_vec(),
        });

        assert_that!(journal.entries_from_lsn(1).is_none(), eq(true));
        assert_that!(
            journal
                .entries_from_lsn(journal.current_lsn().saturating_add(1))
                .is_none(),
            eq(true)
        );
    }

    #[rstest]
    fn in_memory_journal_reset_clears_records_and_rewinds_cursor() {
        let mut journal = InMemoryJournal::new();
        let _ = journal.append(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a 1".to_vec(),
        });
        let _ = journal.append(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Command,
            payload: b"SET a 2".to_vec(),
        });
        assert_that!(journal.current_lsn(), eq(3_u64));
        assert_that!(journal.len(), eq(2_usize));

        journal.reset();
        assert_that!(journal.current_lsn(), eq(1_u64));
        assert_that!(journal.is_empty(), eq(true));
        assert_that!(journal.entry_at_lsn(1).is_none(), eq(true));
    }
}
