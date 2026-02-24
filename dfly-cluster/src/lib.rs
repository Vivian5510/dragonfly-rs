//! Cluster routing and migration interfaces.

pub mod migration;
pub mod slot;

use std::sync::atomic::{AtomicU64, Ordering};

use dfly_common::config::ClusterMode;
use dfly_common::ids::MAX_SLOT_ID;
use slot::{SlotRange, SlotRouter, key_slot};

/// Monotonic seed for deterministic cluster node id allocation.
static NEXT_NODE_ID_SEED: AtomicU64 = AtomicU64::new(1);

/// Cluster subsystem bootstrap module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterModule {
    /// Startup cluster mode.
    pub mode: ClusterMode,
    /// Local node id (40-hex, Redis-compatible shape).
    pub node_id: String,
    /// Local cluster config epoch.
    pub config_epoch: u64,
    /// Slot ownership router for local node.
    pub slot_router: SlotRouter,
    /// Redirect endpoint used in MOVED replies for non-owned slots.
    pub redirect_endpoint: String,
}

impl ClusterModule {
    /// Builds a cluster module from startup mode.
    #[must_use]
    pub fn new(mode: ClusterMode) -> Self {
        Self {
            mode,
            node_id: generate_node_id(),
            config_epoch: 1,
            slot_router: SlotRouter::new(vec![SlotRange {
                start: 0,
                end: MAX_SLOT_ID,
            }]),
            redirect_endpoint: "127.0.0.1:7000".to_owned(),
        }
    }

    /// Replaces local slot ownership ranges.
    pub fn set_owned_ranges(&mut self, ranges: Vec<SlotRange>) {
        self.slot_router = SlotRouter::new(ranges);
    }

    /// Returns whether local node owns the provided key in current cluster mode.
    #[must_use]
    pub fn owns_key(&self, key: &[u8]) -> bool {
        match self.mode {
            ClusterMode::Disabled => true,
            ClusterMode::Emulated | ClusterMode::Real => self.slot_router.owns_slot(key_slot(key)),
        }
    }

    /// Returns `Some(slot)` when key should be redirected with MOVED.
    #[must_use]
    pub fn moved_slot_for_key(&self, key: &[u8]) -> Option<u16> {
        let slot = key_slot(key);
        if self.owns_key(key) { None } else { Some(slot) }
    }

    /// Returns currently owned slot ranges for this node.
    #[must_use]
    pub fn owned_ranges(&self) -> &[SlotRange] {
        self.slot_router.owned_ranges()
    }

    /// Returns how many slots are currently assigned to this node.
    #[must_use]
    pub fn assigned_slots(&self) -> usize {
        self.slot_router
            .owned_ranges()
            .iter()
            .map(|range| usize::from(range.end.saturating_sub(range.start)) + 1)
            .sum()
    }
}

fn generate_node_id() -> String {
    let seed = NEXT_NODE_ID_SEED.fetch_add(1, Ordering::Relaxed);
    format!("{seed:040x}")
}

#[cfg(test)]
mod tests {
    use super::ClusterModule;
    use crate::slot::{SlotRange, key_slot};
    use dfly_common::config::ClusterMode;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn disabled_mode_always_owns_key() {
        let mut cluster = ClusterModule::new(ClusterMode::Disabled);
        cluster.set_owned_ranges(Vec::new());

        assert_that!(cluster.owns_key(b"user:1"), eq(true));
        assert_that!(cluster.moved_slot_for_key(b"user:1"), eq(None));
    }

    #[rstest]
    fn emulated_mode_returns_moved_for_unowned_slot() {
        let key = b"user:{42}";
        let slot = key_slot(key);

        let mut cluster = ClusterModule::new(ClusterMode::Emulated);
        let owned = if slot == 0 {
            SlotRange { start: 1, end: 100 }
        } else {
            SlotRange {
                start: 0,
                end: slot - 1,
            }
        };
        cluster.set_owned_ranges(vec![owned]);

        assert_that!(cluster.owns_key(key), eq(false));
        assert_that!(cluster.moved_slot_for_key(key), eq(Some(slot)));
    }

    #[rstest]
    fn node_id_uses_40_hex_shape() {
        let cluster = ClusterModule::new(ClusterMode::Disabled);
        assert_that!(cluster.node_id.len(), eq(40_usize));
        assert_that!(
            cluster
                .node_id
                .chars()
                .all(|character| character.is_ascii_hexdigit()),
            eq(true)
        );
    }

    #[rstest]
    fn assigned_slots_sums_all_ranges() {
        let mut cluster = ClusterModule::new(ClusterMode::Disabled);
        cluster.set_owned_ranges(vec![
            SlotRange { start: 0, end: 9 },
            SlotRange { start: 20, end: 29 },
        ]);
        assert_that!(cluster.assigned_slots(), eq(20_usize));
    }
}
