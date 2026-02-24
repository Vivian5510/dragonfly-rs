//! Slot routing model.

use dfly_common::ids::{MAX_SLOT_ID, SlotId};

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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Computes Redis-compatible hash slot for one key.
///
/// The hash follows the same `{...}` hashtag extraction rule used by Dragonfly and Redis
/// cluster implementations: if a non-empty hashtag is present, only that segment is hashed.
#[must_use]
pub fn key_slot(key: &[u8]) -> SlotId {
    crc16_xmodem(hash_tag_slice(key)) & MAX_SLOT_ID
}

fn hash_tag_slice(key: &[u8]) -> &[u8] {
    let Some(start) = key.iter().position(|byte| *byte == b'{') else {
        return key;
    };
    let Some(end_offset) = key[start + 1..].iter().position(|byte| *byte == b'}') else {
        return key;
    };
    if end_offset == 0 {
        return key;
    }
    &key[start + 1..start + 1 + end_offset]
}

fn crc16_xmodem(payload: &[u8]) -> u16 {
    let mut crc = 0_u16;
    for byte in payload {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::{SlotRange, SlotRouter, hash_tag_slice, key_slot};
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
        let router = SlotRouter::new(vec![SlotRange {
            start: 100,
            end: 200,
        }]);
        assert_that!(router.owns_slot(slot), eq(false));
    }

    #[rstest]
    fn key_slot_matches_known_crc16_reference_value() {
        // CRC16/XMODEM("123456789") == 0x31C3, and cluster slot is crc & 0x3FFF.
        assert_that!(key_slot(b"123456789"), eq(0x31C3_u16 & 0x3FFF_u16));
    }

    #[rstest]
    fn key_slot_uses_hashtag_when_present() {
        assert_that!(key_slot(b"user:{42}:a"), eq(key_slot(b"prefix:{42}:b")));
    }

    #[rstest]
    fn key_slot_ignores_empty_hashtag() {
        assert_that!(hash_tag_slice(b"a{}z"), eq(b"a{}z".as_slice()));
    }
}
