//! Cluster routing and migration interfaces.

pub mod migration;
pub mod slot;

use dfly_common::config::ClusterMode;
use dfly_common::ids::MAX_SLOT_ID;
use slot::{SlotRange, SlotRouter, key_slot};

/// Cluster subsystem bootstrap module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterModule {
    /// Startup cluster mode.
    pub mode: ClusterMode,
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
}
