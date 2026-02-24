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
    pub sync_sessions: Vec<SyncSession>,
}

/// One replica endpoint tracked by master-side control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEndpoint {
    /// Replica IP/address announced by `REPLCONF`.
    pub address: String,
    /// Replica listening port announced by `REPLCONF`.
    pub listening_port: u16,
    /// Last acknowledged LSN observed from this replica endpoint.
    pub last_acked_lsn: u64,
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

/// Session-level replication state used by Dragonfly's `DFLY` sync commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncSessionState {
    /// Replica is still registering flow sockets.
    Preparation,
    /// Full sync phase has started.
    FullSync,
    /// Session transitioned into stable incremental sync.
    StableSync,
}

/// One flow sync mode negotiated by `DFLY FLOW`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowSyncType {
    /// Full sync is required for this flow.
    Full,
    /// Partial sync can resume from backlog.
    Partial,
}

/// One negotiated flow descriptor stored under one sync session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncFlow {
    /// Negotiated flow mode.
    pub sync_type: FlowSyncType,
    /// Requested starting offset for partial sync when present.
    pub start_offset: Option<u64>,
    /// Generated EOF token returned by `DFLY FLOW`.
    pub eof_token: String,
}

/// One active sync session with per-flow registration slots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncSession {
    /// Session id (`SYNC<n>`).
    pub id: String,
    /// Session phase.
    pub state: SyncSessionState,
    /// Per-flow registration map by flow id.
    pub flows: Vec<Option<SyncFlow>>,
}

/// Session transition errors produced by `DFLY` command lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncSessionError {
    /// Referenced sync session does not exist.
    SyncIdNotFound,
    /// Command is invalid for current session phase.
    InvalidState,
    /// Requested flow id is outside session flow count.
    FlowOutOfRange,
    /// Transition requires all flows to be registered first.
    IncompleteFlows,
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
            sync_sessions: Vec::new(),
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
            existing.last_acked_lsn = 0;
            self.last_acked_lsn = self
                .replicas
                .iter()
                .map(|replica| replica.last_acked_lsn)
                .max()
                .unwrap_or(0);
        } else {
            self.replicas.push(ReplicaEndpoint {
                address,
                listening_port,
                last_acked_lsn: 0,
                state: ReplicaSyncState::Preparation,
            });
        }
        self.connected_replicas = self.replicas.len();
    }

    /// Records one replica ACK offset for a specific endpoint.
    ///
    /// ACK is clamped to current master offset and applied monotonically for that endpoint.
    ///
    pub fn record_replica_ack_for_endpoint(
        &mut self,
        address: &str,
        listening_port: u16,
        ack_lsn: u64,
    ) -> bool {
        let Some(replica) = self
            .replicas
            .iter_mut()
            .find(|entry| entry.address == address && entry.listening_port == listening_port)
        else {
            return false;
        };

        let clamped = ack_lsn.min(self.last_lsn);
        if clamped > replica.last_acked_lsn {
            replica.last_acked_lsn = clamped;
        }
        if clamped > self.last_acked_lsn {
            self.last_acked_lsn = clamped;
        }
        replica.state = ReplicaSyncState::StableSync;
        true
    }

    /// Counts replicas with acknowledged LSN at or above one target offset.
    #[must_use]
    pub fn acked_replica_count_at_or_above(&self, offset: u64) -> usize {
        self.replicas
            .iter()
            .filter(|replica| replica.last_acked_lsn >= offset)
            .count()
    }

    /// Sets sync state on all registered replica endpoints.
    pub fn set_all_replica_states(&mut self, state: ReplicaSyncState) {
        for replica in &mut self.replicas {
            replica.state = state;
        }
    }

    /// Allocates one new sync session id (`SYNC<n>`) and registers it as active.
    pub fn create_sync_session(&mut self, flow_count: usize) -> String {
        let flow_count = flow_count.max(1);
        let session = format!("SYNC{}", self.next_sync_session_id);
        self.next_sync_session_id = self.next_sync_session_id.saturating_add(1);
        self.sync_sessions.push(SyncSession {
            id: session.clone(),
            state: SyncSessionState::Preparation,
            flows: vec![None; flow_count],
        });
        session
    }

    /// Returns whether one sync session id is currently known.
    #[must_use]
    pub fn is_known_sync_session(&self, sync_id: &str) -> bool {
        self.sync_sessions.iter().any(|entry| entry.id == sync_id)
    }

    /// Returns current state for one sync session when present.
    #[must_use]
    pub fn sync_session_state(&self, sync_id: &str) -> Option<SyncSessionState> {
        self.sync_sessions
            .iter()
            .find(|session| session.id == sync_id)
            .map(|session| session.state)
    }

    /// Returns negotiated flow descriptor for one sync session and flow id.
    ///
    /// Returns `None` when session id is unknown, flow id is out of range, or the flow
    /// slot has not been registered yet.
    #[must_use]
    pub fn sync_flow(&self, sync_id: &str, flow_id: usize) -> Option<&SyncFlow> {
        let session = self
            .sync_sessions
            .iter()
            .find(|session| session.id == sync_id)?;
        session.flows.get(flow_id)?.as_ref()
    }

    /// Registers one flow under one sync session.
    ///
    /// This command is only valid while the session is in `Preparation`.
    ///
    /// # Errors
    ///
    /// Returns `SyncSessionError::SyncIdNotFound` when the session id is unknown,
    /// `SyncSessionError::InvalidState` when the session is no longer in preparation,
    /// and `SyncSessionError::FlowOutOfRange` when flow id is outside session range.
    pub fn register_sync_flow(
        &mut self,
        sync_id: &str,
        flow_id: usize,
        sync_type: FlowSyncType,
        start_offset: Option<u64>,
        eof_token: String,
    ) -> Result<(), SyncSessionError> {
        let session = self
            .sync_sessions
            .iter_mut()
            .find(|session| session.id == sync_id)
            .ok_or(SyncSessionError::SyncIdNotFound)?;

        if session.state != SyncSessionState::Preparation {
            return Err(SyncSessionError::InvalidState);
        }
        if flow_id >= session.flows.len() {
            return Err(SyncSessionError::FlowOutOfRange);
        }
        if session.flows[flow_id].is_some() {
            return Err(SyncSessionError::InvalidState);
        }

        session.flows[flow_id] = Some(SyncFlow {
            sync_type,
            start_offset,
            eof_token,
        });
        Ok(())
    }

    /// Transitions one sync session into full-sync phase.
    ///
    /// Session must be in `Preparation`, and all flows must be registered.
    ///
    /// # Errors
    ///
    /// Returns `SyncSessionError::SyncIdNotFound` when the session id is unknown,
    /// `SyncSessionError::InvalidState` when the session is not in preparation,
    /// and `SyncSessionError::IncompleteFlows` when one or more flows are still missing.
    pub fn mark_sync_session_full_sync(&mut self, sync_id: &str) -> Result<(), SyncSessionError> {
        let session = self
            .sync_sessions
            .iter_mut()
            .find(|session| session.id == sync_id)
            .ok_or(SyncSessionError::SyncIdNotFound)?;

        if session.state != SyncSessionState::Preparation {
            return Err(SyncSessionError::InvalidState);
        }
        if session.flows.iter().any(Option::is_none) {
            return Err(SyncSessionError::IncompleteFlows);
        }

        session.state = SyncSessionState::FullSync;
        self.full_sync_in_progress = true;
        self.full_sync_done = false;
        Ok(())
    }

    /// Transitions one sync session into stable sync phase.
    ///
    /// Dragonfly accepts this transition from both `Preparation` and `FullSync`.
    ///
    /// # Errors
    ///
    /// Returns `SyncSessionError::SyncIdNotFound` when the session id is unknown,
    /// and `SyncSessionError::InvalidState` when the session is already stable.
    pub fn mark_sync_session_stable_sync(&mut self, sync_id: &str) -> Result<(), SyncSessionError> {
        let session = self
            .sync_sessions
            .iter_mut()
            .find(|session| session.id == sync_id)
            .ok_or(SyncSessionError::SyncIdNotFound)?;

        if session.state == SyncSessionState::StableSync {
            return Err(SyncSessionError::InvalidState);
        }

        session.state = SyncSessionState::StableSync;
        self.full_sync_in_progress = false;
        self.full_sync_done = true;
        Ok(())
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
    use super::{
        FlowSyncType, ReplicaSyncState, ReplicationState, SyncSessionError, SyncSessionState,
    };
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
        assert_that!(state.replicas[0].last_acked_lsn, eq(0_u64));

        state.set_all_replica_states(ReplicaSyncState::FullSync);
        assert_that!(state.replicas[0].state, eq(ReplicaSyncState::FullSync));

        state.set_all_replica_states(ReplicaSyncState::StableSync);
        assert_that!(state.replicas[0].state, eq(ReplicaSyncState::StableSync));
    }

    #[rstest]
    fn sync_session_ids_and_flow_tokens_are_monotonic() {
        let mut state = ReplicationState::default();
        let sync_1 = state.create_sync_session(2);
        let sync_2 = state.create_sync_session(2);

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

    #[rstest]
    fn sync_session_state_machine_requires_complete_flow_registration() {
        let mut state = ReplicationState::default();
        let sync = state.create_sync_session(2);

        assert_that!(
            state.mark_sync_session_full_sync(&sync),
            eq(Err(SyncSessionError::IncompleteFlows))
        );

        let token_0 = state.allocate_flow_eof_token();
        assert_that!(
            state.register_sync_flow(&sync, 0, FlowSyncType::Partial, Some(9), token_0),
            eq(Ok(()))
        );
        assert_that!(
            state.mark_sync_session_full_sync(&sync),
            eq(Err(SyncSessionError::IncompleteFlows))
        );

        let token_1 = state.allocate_flow_eof_token();
        assert_that!(
            state.register_sync_flow(&sync, 1, FlowSyncType::Full, None, token_1),
            eq(Ok(()))
        );
        assert_that!(state.mark_sync_session_full_sync(&sync), eq(Ok(())));
        assert_that!(
            state.sync_session_state(&sync),
            eq(Some(SyncSessionState::FullSync))
        );

        let late_token = state.allocate_flow_eof_token();
        assert_that!(
            state.register_sync_flow(&sync, 0, FlowSyncType::Full, None, late_token),
            eq(Err(SyncSessionError::InvalidState))
        );

        assert_that!(state.mark_sync_session_stable_sync(&sync), eq(Ok(())));
        assert_that!(
            state.sync_session_state(&sync),
            eq(Some(SyncSessionState::StableSync))
        );
        assert_that!(
            state.mark_sync_session_stable_sync(&sync),
            eq(Err(SyncSessionError::InvalidState))
        );
    }

    #[rstest]
    fn sync_session_rejects_duplicate_flow_registration() {
        let mut state = ReplicationState::default();
        let sync = state.create_sync_session(1);

        let token_0 = state.allocate_flow_eof_token();
        assert_that!(
            state.register_sync_flow(&sync, 0, FlowSyncType::Full, None, token_0),
            eq(Ok(()))
        );
        let token_1 = state.allocate_flow_eof_token();
        assert_that!(
            state.register_sync_flow(&sync, 0, FlowSyncType::Partial, Some(5), token_1),
            eq(Err(SyncSessionError::InvalidState))
        );
    }

    #[rstest]
    fn endpoint_ack_updates_per_replica_and_global_ack() {
        let mut state = ReplicationState::default();
        state.set_last_lsn_from_next_cursor(6);
        state.register_replica_endpoint("10.0.0.1".to_owned(), 7001);
        state.register_replica_endpoint("10.0.0.2".to_owned(), 7002);

        assert_that!(
            state.record_replica_ack_for_endpoint("10.0.0.1", 7001, 3),
            eq(true)
        );
        assert_that!(
            state.record_replica_ack_for_endpoint("10.0.0.2", 7002, 2),
            eq(true)
        );
        assert_that!(
            state.record_replica_ack_for_endpoint("10.0.0.9", 7009, 2),
            eq(false)
        );

        assert_that!(state.last_acked_lsn, eq(3_u64));
        assert_that!(state.acked_replica_count_at_or_above(3), eq(1_usize));
        assert_that!(state.acked_replica_count_at_or_above(2), eq(2_usize));
    }

    #[rstest]
    fn re_registering_replica_endpoint_resets_endpoint_ack_progress() {
        let mut state = ReplicationState::default();
        state.set_last_lsn_from_next_cursor(4);
        state.register_replica_endpoint("10.0.0.1".to_owned(), 7001);
        assert_that!(
            state.record_replica_ack_for_endpoint("10.0.0.1", 7001, 3),
            eq(true)
        );
        assert_that!(state.last_acked_lsn, eq(3_u64));
        assert_that!(state.acked_replica_count_at_or_above(3), eq(1_usize));

        state.register_replica_endpoint("10.0.0.1".to_owned(), 7001);
        assert_that!(state.replicas[0].state, eq(ReplicaSyncState::Preparation));
        assert_that!(state.replicas[0].last_acked_lsn, eq(0_u64));
        assert_that!(state.acked_replica_count_at_or_above(1), eq(0_usize));
        assert_that!(state.last_acked_lsn, eq(0_u64));
    }

    #[rstest]
    fn re_registering_one_replica_recomputes_global_ack_from_remaining_endpoints() {
        let mut state = ReplicationState::default();
        state.set_last_lsn_from_next_cursor(6);
        state.register_replica_endpoint("10.0.0.1".to_owned(), 7001);
        state.register_replica_endpoint("10.0.0.2".to_owned(), 7002);
        assert_that!(
            state.record_replica_ack_for_endpoint("10.0.0.1", 7001, 5),
            eq(true)
        );
        assert_that!(
            state.record_replica_ack_for_endpoint("10.0.0.2", 7002, 3),
            eq(true)
        );
        assert_that!(state.last_acked_lsn, eq(5_u64));

        state.register_replica_endpoint("10.0.0.1".to_owned(), 7001);
        assert_that!(state.replicas[0].last_acked_lsn, eq(0_u64));
        assert_that!(state.replicas[1].last_acked_lsn, eq(3_u64));
        assert_that!(state.last_acked_lsn, eq(3_u64));
    }
}
