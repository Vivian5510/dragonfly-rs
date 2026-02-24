//! Connection-scoped transaction queue state.
//!
//! Dragonfly keeps transaction intent at the client-connection level (`MULTI` queue) and
//! delegates actual execution to coordinator logic when `EXEC` arrives. This module models
//! that first stage so server code can stay explicit and testable.

use dfly_core::command::CommandFrame;

/// One optimistic-watch descriptor tracked for a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedKey {
    /// Logical DB index.
    pub db: u16,
    /// Watched key bytes.
    pub key: Vec<u8>,
    /// Key version captured at `WATCH` time.
    pub version: u64,
}

/// Mutable transaction state attached to one client connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionSession {
    in_multi: bool,
    has_queued_error: bool,
    queued_commands: Vec<CommandFrame>,
    watched_keys: Vec<WatchedKey>,
}

impl TransactionSession {
    /// Starts transaction queue mode for this connection.
    ///
    /// Returns `false` when a transaction is already open.
    #[must_use]
    pub fn begin_multi(&mut self) -> bool {
        if self.in_multi {
            return false;
        }
        self.in_multi = true;
        self.has_queued_error = false;
        self.queued_commands.clear();
        true
    }

    /// Queues one command for later `EXEC`.
    ///
    /// Returns `false` when the session is not currently in `MULTI` mode.
    #[must_use]
    pub fn queue_command(&mut self, frame: CommandFrame) -> bool {
        if !self.in_multi {
            return false;
        }
        self.queued_commands.push(frame);
        true
    }

    /// Discards queued transaction commands and exits `MULTI` mode.
    ///
    /// Returns `false` when no transaction is active.
    #[must_use]
    pub fn discard(&mut self) -> bool {
        if !self.in_multi {
            return false;
        }
        self.in_multi = false;
        self.has_queued_error = false;
        self.queued_commands.clear();
        self.watched_keys.clear();
        true
    }

    /// Exits `MULTI` mode and returns queued commands for execution.
    ///
    /// Returns `None` when no transaction is active.
    pub fn take_queued_for_exec(&mut self) -> Option<Vec<CommandFrame>> {
        if !self.in_multi {
            return None;
        }
        self.in_multi = false;
        self.has_queued_error = false;
        self.watched_keys.clear();
        Some(std::mem::take(&mut self.queued_commands))
    }

    /// Returns whether this connection is currently in `MULTI` mode.
    #[must_use]
    pub fn in_multi(&self) -> bool {
        self.in_multi
    }

    /// Marks the current multi-queue as containing one pre-execution command error.
    pub fn mark_queued_error(&mut self) {
        if self.in_multi {
            self.has_queued_error = true;
        }
    }

    /// Returns whether queue collection phase has already observed one command error.
    #[must_use]
    pub fn has_queued_error(&self) -> bool {
        self.has_queued_error
    }

    /// Adds or refreshes a watched key when outside of `MULTI`.
    ///
    /// Returns `false` when a transaction queue is already open.
    #[must_use]
    pub fn watch_key(&mut self, db: u16, key: Vec<u8>, version: u64) -> bool {
        if self.in_multi {
            return false;
        }

        if let Some(existing) = self
            .watched_keys
            .iter_mut()
            .find(|entry| entry.db == db && entry.key == key)
        {
            existing.version = version;
            return true;
        }

        self.watched_keys.push(WatchedKey { db, key, version });
        true
    }

    /// Clears all optimistic watch descriptors.
    pub fn unwatch(&mut self) {
        self.watched_keys.clear();
    }

    /// Returns whether all watched keys still match their captured versions.
    #[must_use]
    pub fn watched_keys_are_clean<F>(&self, mut current_version: F) -> bool
    where
        F: FnMut(u16, &[u8]) -> u64,
    {
        self.watched_keys
            .iter()
            .all(|entry| current_version(entry.db, &entry.key) == entry.version)
    }

    /// Returns whether this session watches any keys outside of the provided logical DB.
    #[must_use]
    pub fn watching_other_dbs(&self, db: u16) -> bool {
        self.watched_keys.iter().any(|entry| entry.db != db)
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionSession;
    use dfly_core::command::CommandFrame;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn begin_multi_rejects_nested_open() {
        let mut session = TransactionSession::default();
        assert_that!(session.begin_multi(), eq(true));
        assert_that!(session.begin_multi(), eq(false));
    }

    #[rstest]
    fn queue_and_exec_transfers_commands() {
        let mut session = TransactionSession::default();
        let _ = session.begin_multi();
        let _ = session.queue_command(CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]));

        let queued = session
            .take_queued_for_exec()
            .expect("session should contain queued commands");
        assert_that!(queued.len(), eq(1_usize));
        assert_that!(session.in_multi(), eq(false));
    }

    #[rstest]
    fn discard_clears_queue_and_state() {
        let mut session = TransactionSession::default();
        let _ = session.begin_multi();
        let _ = session.queue_command(CommandFrame::new("PING", Vec::new()));

        assert_that!(session.discard(), eq(true));
        assert_that!(session.in_multi(), eq(false));
        assert_that!(session.take_queued_for_exec().is_none(), eq(true));
    }

    #[rstest]
    fn watch_keys_track_version_and_unwatch_clears_them() {
        let mut session = TransactionSession::default();
        assert_that!(session.watch_key(0, b"k".to_vec(), 2), eq(true));
        assert_that!(
            session
                .watched_keys_are_clean(|db, key| { if db == 0 && key == b"k" { 2 } else { 0 } }),
            eq(true)
        );
        assert_that!(
            session
                .watched_keys_are_clean(|db, key| { if db == 0 && key == b"k" { 3 } else { 0 } }),
            eq(false)
        );

        session.unwatch();
        assert_that!(session.watched_keys_are_clean(|_, _| 0), eq(true));
    }

    #[rstest]
    fn watching_other_dbs_reflects_registered_watch_scope() {
        let mut session = TransactionSession::default();
        assert_that!(session.watching_other_dbs(0), eq(false));

        assert_that!(session.watch_key(0, b"same-db".to_vec(), 1), eq(true));
        assert_that!(session.watching_other_dbs(0), eq(false));

        assert_that!(session.watch_key(2, b"other-db".to_vec(), 1), eq(true));
        assert_that!(session.watching_other_dbs(0), eq(true));
    }

    #[rstest]
    fn queued_error_flag_lifecycle_follows_multi_scope() {
        let mut session = TransactionSession::default();
        assert_that!(session.has_queued_error(), eq(false));

        let _ = session.begin_multi();
        session.mark_queued_error();
        assert_that!(session.has_queued_error(), eq(true));

        let _ = session.discard();
        assert_that!(session.has_queued_error(), eq(false));

        let _ = session.begin_multi();
        session.mark_queued_error();
        let _ = session.take_queued_for_exec();
        assert_that!(session.has_queued_error(), eq(false));
    }
}
