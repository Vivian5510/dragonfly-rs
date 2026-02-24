//! Connection-scoped transaction queue state.
//!
//! Dragonfly keeps transaction intent at the client-connection level (`MULTI` queue) and
//! delegates actual execution to coordinator logic when `EXEC` arrives. This module models
//! that first stage so server code can stay explicit and testable.

use dfly_core::command::CommandFrame;

/// Mutable transaction state attached to one client connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionSession {
    in_multi: bool,
    queued_commands: Vec<CommandFrame>,
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
        self.queued_commands.clear();
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
        Some(std::mem::take(&mut self.queued_commands))
    }

    /// Returns whether this connection is currently in `MULTI` mode.
    #[must_use]
    pub fn in_multi(&self) -> bool {
        self.in_multi
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
}
