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
pub use state::{DbTable, DispatchState, SlotStats, SlotStatsSnapshot, StoredValue, ValueEntry};

/// Handler function signature used by command registry entries.
pub type CommandHandler = fn(DbIndex, &CommandFrame, &mut DispatchState) -> CommandReply;

/// Arity constraints for a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandArity {
    /// Command must have exactly this many arguments.
    Exact(usize),
    /// Command must have at least this many arguments.
    AtLeast(usize),
}

/// Metadata and callback for one command table entry.
#[derive(Debug, Clone)]
pub struct CommandSpec {
    /// Canonical uppercase command name.
    pub name: &'static str,
    /// Arity constraint used for lightweight input validation.
    pub arity: CommandArity,
    /// Handler callback.
    pub handler: CommandHandler,
}

#[cfg(test)]
#[path = "dispatch/tests.rs"]
mod tests;



