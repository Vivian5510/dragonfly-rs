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
    /// Registered replica endpoints known by control plane.
    pub replicas: Vec<ReplicaEndpoint>,
    /// Monotonic session id sequence used by `REPLCONF CAPA dragonfly`.
    pub next_sync_session_id: u64,
    /// Monotonic seed for 40-hex EOF token generation.
    pub next_eof_token_seed: u64,
    /// Active sync sessions accepted by `DFLY FLOW/SYNC/STARTSTABLE`.
    pub active_sync_sessions: Vec<String>,
}

/// One replica endpoint tracked by master-side control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEndpoint {
    /// Replica IP/address announced by `REPLCONF`.
    pub address: String,
    /// Replica listening port announced by `REPLCONF`.
    pub listening_port: u16,
    /// High-level sync stage used by `ROLE` output.
    pub state: ReplicaSyncState,
}

/// Replica state labels aligned with Dragonfly's role reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaSyncState {
    /// Replica has connected but has not started stream sync yet.
    Preparation,
    /// Replica is performing full sync.
    FullSync,
    /// Replica has entered stable incremental sync.
    StableSync,
}

impl ReplicaSyncState {
    /// Returns role-compatible textual state.
    #[must_use]
    pub const fn as_role_state(self) -> &'static str {
        match self {
            Self::Preparation => "preparation",
            Self::FullSync => "full_sync",
            Self::StableSync => "stable_sync",
        }
    }
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
            replicas: Vec::new(),
            next_sync_session_id: 1,
            next_eof_token_seed: 1,
            active_sync_sessions: Vec::new(),
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

    /// Registers one replica endpoint.
    ///
    /// Existing endpoint registration is refreshed in-place and moved back to
    /// `preparation` state, matching a new handshake attempt.
    pub fn register_replica_endpoint(&mut self, address: String, listening_port: u16) {
        if let Some(existing) = self
            .replicas
            .iter_mut()
            .find(|entry| entry.address == address && entry.listening_port == listening_port)
        {
            existing.state = ReplicaSyncState::Preparation;
        } else {
            self.replicas.push(ReplicaEndpoint {
                address,
                listening_port,
                state: ReplicaSyncState::Preparation,
            });
        }
        self.connected_replicas = self.replicas.len();
    }

    /// Sets sync state on all registered replica endpoints.
    pub fn set_all_replica_states(&mut self, state: ReplicaSyncState) {
        for replica in &mut self.replicas {
            replica.state = state;
        }
    }

    /// Allocates one new sync session id (`SYNC<n>`) and registers it as active.
    pub fn create_sync_session(&mut self) -> String {
        let session = format!("SYNC{}", self.next_sync_session_id);
        self.next_sync_session_id = self.next_sync_session_id.saturating_add(1);
        self.active_sync_sessions.push(session.clone());
        session
    }

    /// Returns whether one sync session id is currently known.
    #[must_use]
    pub fn is_known_sync_session(&self, sync_id: &str) -> bool {
        self.active_sync_sessions
            .iter()
            .any(|entry| entry == sync_id)
    }

    /// Allocates one 40-hex EOF token for `DFLY FLOW` handshake.
    pub fn allocate_flow_eof_token(&mut self) -> String {
        let token = format!("{:040x}", self.next_eof_token_seed);
        self.next_eof_token_seed = self.next_eof_token_seed.saturating_add(1);
        token
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
    use super::{ReplicaSyncState, ReplicationState};
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

    #[rstest]
    fn replica_registration_and_state_transition_are_tracked() {
        let mut state = ReplicationState::default();
        state.register_replica_endpoint("127.0.0.1".to_owned(), 6380);

        assert_that!(state.connected_replicas, eq(1_usize));
        assert_that!(state.replicas.len(), eq(1_usize));
        assert_that!(state.replicas[0].state, eq(ReplicaSyncState::Preparation));

        state.set_all_replica_states(ReplicaSyncState::FullSync);
        assert_that!(state.replicas[0].state, eq(ReplicaSyncState::FullSync));

        state.set_all_replica_states(ReplicaSyncState::StableSync);
        assert_that!(state.replicas[0].state, eq(ReplicaSyncState::StableSync));
    }

    #[rstest]
    fn sync_session_ids_and_flow_tokens_are_monotonic() {
        let mut state = ReplicationState::default();
        let sync_1 = state.create_sync_session();
        let sync_2 = state.create_sync_session();

        assert_that!(sync_1.as_str(), eq("SYNC1"));
        assert_that!(sync_2.as_str(), eq("SYNC2"));
        assert_that!(state.is_known_sync_session("SYNC1"), eq(true));
        assert_that!(state.is_known_sync_session("SYNC2"), eq(true));

        let token_1 = state.allocate_flow_eof_token();
        let token_2 = state.allocate_flow_eof_token();
        assert_that!(token_1.len(), eq(40_usize));
        assert_that!(token_2.len(), eq(40_usize));
        assert_that!(token_1 == token_2, eq(false));
    }
}
