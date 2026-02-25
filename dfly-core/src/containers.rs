//! Hot-path container aliases used by core runtime/data structures.
//!
//! Dragonfly's C++ core uses specialized hash containers and allocator policies on hot paths.
//! This module keeps Rust-side container choices centralized so future allocator/container
//! upgrades can be done in one place without touching command logic modules.

use hashbrown::{HashMap as HbMap, HashSet as HbSet};

/// Hot-path hash map used by shard-local state tables.
pub type HotMap<K, V> = HbMap<K, V>;

/// Hot-path hash set used by slot/expire secondary indexes.
pub type HotSet<T> = HbSet<T>;
