//! Replication state tracking model.

use std::sync::atomic::{AtomicU64, Ordering};

/// Redis-compatible replication id length (`CONFIG_RUN_ID_SIZE` in Dragonfly C++).
const MASTER_REPLID_HEX_LEN: usize = 40;

/// Monotonic seed used to generate deterministic, process-local replid values.
static NEXT_REPLID_SEED: AtomicU64 = AtomicU64::new(1);

/// Mutable replication state for one server instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationState {
    /// Whether one full sync has finished.
    pub full_sync_done: bool,
    /// Whether full sync is currently in progress.
    pub full_sync_in_progress: bool,
    /// Current master replication id.
    pub master_replid: String,
    /// Last known replicated LSN (master offset in this learning-path implementation).
    pub last_lsn: u64,
    /// Highest replica ACK we accepted so far.
    pub last_acked_lsn: u64,
    /// Number of connected replicas from master's perspective.
    pub connected_replicas: usize,
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationState {
    /// Creates the default master-side replication state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            full_sync_done: false,
            full_sync_in_progress: false,
            master_replid: generate_master_replid(),
            last_lsn: 0,
            last_acked_lsn: 0,
            connected_replicas: 0,
        }
    }

    /// Updates the replicated offset from journal's `next_lsn` cursor.
    pub fn set_last_lsn_from_next_cursor(&mut self, next_lsn: u64) {
        self.last_lsn = next_lsn.saturating_sub(1);
    }

    /// Records one replica ACK offset.
    ///
    /// ACK is clamped to current master offset and applied monotonically.
    pub fn record_ack_lsn(&mut self, ack_lsn: u64) {
        let clamped = ack_lsn.min(self.last_lsn);
        if clamped > self.last_acked_lsn {
            self.last_acked_lsn = clamped;
        }
    }
}

/// Generates one pseudo-random looking replication id.
///
/// The output is deterministic and stable for tests while preserving the same
/// 40-hex-character shape used by Dragonfly.
fn generate_master_replid() -> String {
    let seed = NEXT_REPLID_SEED.fetch_add(1, Ordering::Relaxed);
    format!("{seed:0MASTER_REPLID_HEX_LEN$x}")
}

#[cfg(test)]
mod tests {
    use super::ReplicationState;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn default_state_uses_redis_compatible_replid_shape() {
        let state = ReplicationState::default();
        assert_that!(state.master_replid.len(), eq(40_usize));
        assert_that!(
            state
                .master_replid
                .chars()
                .all(|character| character.is_ascii_hexdigit()),
            eq(true)
        );
    }

    #[rstest]
    fn lsn_tracking_uses_next_lsn_cursor_semantics() {
        let mut state = ReplicationState::default();
        state.set_last_lsn_from_next_cursor(1);
        assert_that!(state.last_lsn, eq(0_u64));

        state.set_last_lsn_from_next_cursor(4);
        assert_that!(state.last_lsn, eq(3_u64));
    }

    #[rstest]
    fn ack_tracking_is_monotonic_and_clamped_to_master_offset() {
        let mut state = ReplicationState::default();
        state.set_last_lsn_from_next_cursor(6);

        state.record_ack_lsn(3);
        assert_that!(state.last_acked_lsn, eq(3_u64));

        state.record_ack_lsn(2);
        assert_that!(state.last_acked_lsn, eq(3_u64));

        state.record_ack_lsn(999);
        assert_that!(state.last_acked_lsn, eq(5_u64));
    }
}
