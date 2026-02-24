//! Slot routing model.

use dfly_common::ids::{SlotId, MAX_SLOT_ID};

/// Closed slot range `[start, end]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotRange {
    /// Range start.
    pub start: SlotId,
    /// Range end.
    pub end: SlotId,
}

impl SlotRange {
    /// Checks whether a slot is within this range.
    #[must_use]
    pub fn contains(self, slot: SlotId) -> bool {
        self.start <= slot && slot <= self.end
    }
}

/// Simple slot ownership router placeholder.
#[derive(Debug, Clone)]
pub struct SlotRouter {
    owned: Vec<SlotRange>,
}

impl SlotRouter {
    /// Creates a router from owned ranges.
    #[must_use]
    pub fn new(owned: Vec<SlotRange>) -> Self {
        Self { owned }
    }

    /// Returns true if this node owns the given slot.
    #[must_use]
    pub fn owns_slot(&self, slot: SlotId) -> bool {
        debug_assert!(slot <= MAX_SLOT_ID);
        self.owned.iter().copied().any(|range| range.contains(slot))
    }
}

#[cfg(test)]
mod tests {
    use super::{SlotRange, SlotRouter};
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    #[case(10)]
    #[case(15)]
    #[case(20)]
    fn router_matches_owned_ranges(#[case] slot: u16) {
        let router = SlotRouter::new(vec![SlotRange { start: 10, end: 20 }]);
        assert_that!(router.owns_slot(slot), eq(true));
    }

    #[rstest]
    #[case(50)]
    #[case(201)]
    fn router_rejects_unowned_slots(#[case] slot: u16) {
        let router = SlotRouter::new(vec![SlotRange { start: 100, end: 200 }]);
        assert_that!(router.owns_slot(slot), eq(false));
    }
}
