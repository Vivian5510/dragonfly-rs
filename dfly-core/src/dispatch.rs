//! Command registration and dispatch skeleton.
//!
//! This module intentionally mirrors Dragonfly's "command table + execution path" concept.
//! Unit 1 keeps the implementation minimal while preserving the same architectural idea:
//! protocol parsing produces a canonical command frame, then a registry resolves and executes
//! the matching handler.

use dfly_common::ids::DbIndex;

use crate::command::{CommandFrame, CommandReply};

#[path = "dispatch/parse_opts.rs"]
mod parse_opts;
#[path = "dispatch/parse_numbers.rs"]
mod parse_numbers;
#[path = "dispatch/command_spec.rs"]
mod command_spec;
#[path = "dispatch/state.rs"]
mod state;
#[path = "dispatch/handlers_string.rs"]
mod handlers_string;
#[path = "dispatch/handlers_expiry.rs"]
mod handlers_expiry;
#[path = "dispatch/handlers_counter.rs"]
mod handlers_counter;
#[path = "dispatch/handlers_keyspace.rs"]
mod handlers_keyspace;
#[path = "dispatch/registry.rs"]
mod registry;

pub use handlers_keyspace::{copy_between_states, rename_between_states};
pub use registry::CommandRegistry;
pub use command_spec::{CommandArity, CommandSpec};
pub use state::{DbTable, DispatchState, SlotStats, SlotStatsSnapshot, StoredValue, ValueEntry};

/// Handler function signature used by command registry entries.
pub type CommandHandler = fn(DbIndex, &CommandFrame, &mut DispatchState) -> CommandReply;

#[cfg(test)]
#[path = "dispatch/tests.rs"]
mod tests;



