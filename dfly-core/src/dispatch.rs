//! Command registration and dispatch skeleton.
//!
//! This module intentionally mirrors Dragonfly's "command table + execution path" concept.
//! Unit 1 keeps the implementation minimal while preserving the same architectural idea:
//! protocol parsing produces a canonical command frame, then a registry resolves and executes
//! the matching handler.

use std::collections::HashMap;

use crate::command::{CommandFrame, CommandReply};

/// Handler function signature used by command registry entries.
pub type CommandHandler = fn(&CommandFrame, &mut DispatchState) -> CommandReply;

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

/// Mutable execution state used by command handlers.
///
/// The `kv` map is a minimal placeholder representing per-node key-value state.
/// In later units this will be replaced by shard-aware storage and transaction views.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DispatchState {
    /// In-memory key/value dictionary for learning-path command execution.
    pub kv: HashMap<Vec<u8>, Vec<u8>>,
}

/// Runtime command registry.
#[derive(Debug, Clone, Default)]
pub struct CommandRegistry {
    entries: HashMap<String, CommandSpec>,
}

impl CommandRegistry {
    /// Builds an empty command registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Builds a registry preloaded with core learning-path commands.
    #[must_use]
    pub fn with_builtin_commands() -> Self {
        let mut registry = Self::new();
        registry.register(CommandSpec {
            name: "PING",
            arity: CommandArity::AtLeast(0),
            handler: handle_ping,
        });
        registry.register(CommandSpec {
            name: "ECHO",
            arity: CommandArity::Exact(1),
            handler: handle_echo,
        });
        registry.register(CommandSpec {
            name: "SET",
            arity: CommandArity::Exact(2),
            handler: handle_set,
        });
        registry.register(CommandSpec {
            name: "GET",
            arity: CommandArity::Exact(1),
            handler: handle_get,
        });
        registry
    }

    /// Registers or replaces one command in the table.
    pub fn register(&mut self, spec: CommandSpec) {
        self.entries.insert(spec.name.to_owned(), spec);
    }

    /// Dispatches one canonical command frame to its registered handler.
    #[must_use]
    pub fn dispatch(&self, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
        let command_name = frame.name.to_ascii_uppercase();
        let Some(spec) = self.entries.get(&command_name) else {
            return CommandReply::Error(format!("unknown command '{command_name}'"));
        };

        match spec.arity {
            CommandArity::Exact(expected) if frame.args.len() != expected => {
                return CommandReply::Error(format!(
                    "wrong number of arguments for '{}' command",
                    spec.name
                ));
            }
            CommandArity::AtLeast(minimum) if frame.args.len() < minimum => {
                return CommandReply::Error(format!(
                    "wrong number of arguments for '{}' command",
                    spec.name
                ));
            }
            _ => {}
        }

        (spec.handler)(frame, state)
    }
}

fn handle_ping(frame: &CommandFrame, _state: &mut DispatchState) -> CommandReply {
    if frame.args.is_empty() {
        return CommandReply::SimpleString("PONG".to_owned());
    }
    if frame.args.len() == 1 {
        return CommandReply::BulkString(frame.args[0].clone());
    }
    CommandReply::Error("wrong number of arguments for 'PING' command".to_owned())
}

fn handle_echo(frame: &CommandFrame, _state: &mut DispatchState) -> CommandReply {
    CommandReply::BulkString(frame.args[0].clone())
}

fn handle_set(frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let value = frame.args[1].clone();
    state.kv.insert(key, value);
    CommandReply::SimpleString("OK".to_owned())
}

fn handle_get(frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    match state.kv.get(key) {
        Some(value) => CommandReply::BulkString(value.clone()),
        None => CommandReply::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::{CommandRegistry, DispatchState};
    use crate::command::{CommandFrame, CommandReply};
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn dispatch_ping_and_echo() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let ping = registry.dispatch(&CommandFrame::new("PING", Vec::new()), &mut state);
        assert_that!(&ping, eq(&CommandReply::SimpleString("PONG".to_owned())));

        let echo = registry.dispatch(
            &CommandFrame::new("ECHO", vec![b"hello".to_vec()]),
            &mut state,
        );
        assert_that!(&echo, eq(&CommandReply::BulkString(b"hello".to_vec())));
    }

    #[rstest]
    fn dispatch_set_then_get_roundtrip() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let set_reply = registry.dispatch(
            &CommandFrame::new("SET", vec![b"user:1".to_vec(), b"alice".to_vec()]),
            &mut state,
        );
        assert_that!(
            &set_reply,
            eq(&CommandReply::SimpleString("OK".to_owned()))
        );

        let get_reply = registry.dispatch(
            &CommandFrame::new("GET", vec![b"user:1".to_vec()]),
            &mut state,
        );
        assert_that!(
            &get_reply,
            eq(&CommandReply::BulkString(b"alice".to_vec()))
        );
    }

    #[rstest]
    fn dispatch_unknown_command_returns_error() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(&CommandFrame::new("NOPE", Vec::new()), &mut state);
        let CommandReply::Error(message) = reply else {
            panic!("expected error reply");
        };
        assert_that!(message.contains("unknown command"), eq(true));
    }
}
