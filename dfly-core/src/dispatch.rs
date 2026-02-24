//! Command registration and dispatch skeleton.
//!
//! This module intentionally mirrors Dragonfly's "command table + execution path" concept.
//! Unit 1 keeps the implementation minimal while preserving the same architectural idea:
//! protocol parsing produces a canonical command frame, then a registry resolves and executes
//! the matching handler.

use std::collections::HashMap;
use std::str;
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub kv: HashMap<Vec<u8>, ValueEntry>,
}

/// Stored value with optional expiration metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueEntry {
    /// User payload.
    pub value: Vec<u8>,
    /// Unix timestamp in seconds when the key expires.
    pub expire_at_unix_secs: Option<u64>,
}

impl DispatchState {
    /// Returns current Unix timestamp in seconds.
    #[must_use]
    fn now_unix_seconds() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(_) => 0,
        }
    }

    /// Removes one key when its expiration timestamp has already passed.
    fn purge_expired_key(&mut self, key: &[u8]) {
        let should_remove = self
            .kv
            .get(key)
            .and_then(|entry| entry.expire_at_unix_secs)
            .is_some_and(|expire_at| expire_at <= Self::now_unix_seconds());

        if should_remove {
            let _ = self.kv.remove(key);
        }
    }
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
        registry.register(CommandSpec {
            name: "EXPIRE",
            arity: CommandArity::Exact(2),
            handler: handle_expire,
        });
        registry.register(CommandSpec {
            name: "TTL",
            arity: CommandArity::Exact(1),
            handler: handle_ttl,
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
    state.kv.insert(
        key,
        ValueEntry {
            value,
            expire_at_unix_secs: None,
        },
    );
    CommandReply::SimpleString("OK".to_owned())
}

fn handle_get(frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(key);
    match state.kv.get(key) {
        Some(value) => CommandReply::BulkString(value.value.clone()),
        None => CommandReply::Null,
    }
}

fn handle_expire(frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(key);

    let seconds = match parse_i64_arg(&frame.args[1], "EXPIRE seconds") {
        Ok(seconds) => seconds,
        Err(error) => return CommandReply::Error(error),
    };
    if !state.kv.contains_key(key) {
        return CommandReply::Integer(0);
    }

    if seconds <= 0 {
        let _ = state.kv.remove(key);
        return CommandReply::Integer(1);
    }

    let Ok(expire_delta) = u64::try_from(seconds) else {
        return CommandReply::Error("EXPIRE seconds is out of range".to_owned());
    };
    let expire_at = DispatchState::now_unix_seconds().saturating_add(expire_delta);

    if let Some(entry) = state.kv.get_mut(key) {
        entry.expire_at_unix_secs = Some(expire_at);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

fn handle_ttl(frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(key);

    let Some(expire_at) = state
        .kv
        .get(key)
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.kv.contains_key(key) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    let now = DispatchState::now_unix_seconds();
    if expire_at <= now {
        let _ = state.kv.remove(key);
        return CommandReply::Integer(-2);
    }
    let remaining = expire_at.saturating_sub(now);
    match i64::try_from(remaining) {
        Ok(remaining) => CommandReply::Integer(remaining),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

fn parse_i64_arg(arg: &[u8], field_name: &str) -> Result<i64, String> {
    let text = str::from_utf8(arg).map_err(|_| format!("{field_name} must be valid UTF-8"))?;
    text.parse::<i64>()
        .map_err(|_| format!("{field_name} must be an integer"))
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
        assert_that!(&set_reply, eq(&CommandReply::SimpleString("OK".to_owned())));

        let get_reply = registry.dispatch(
            &CommandFrame::new("GET", vec![b"user:1".to_vec()]),
            &mut state,
        );
        assert_that!(&get_reply, eq(&CommandReply::BulkString(b"alice".to_vec())));
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

    #[rstest]
    fn dispatch_ttl_and_expire_lifecycle() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            &CommandFrame::new("SET", vec![b"temp".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let ttl_without_expire = registry.dispatch(
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_without_expire, eq(&CommandReply::Integer(-1)));

        let expire_now = registry.dispatch(
            &CommandFrame::new("EXPIRE", vec![b"temp".to_vec(), b"0".to_vec()]),
            &mut state,
        );
        assert_that!(&expire_now, eq(&CommandReply::Integer(1)));

        let get_after_expire = registry.dispatch(
            &CommandFrame::new("GET", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&get_after_expire, eq(&CommandReply::Null));

        let ttl_after_expire = registry.dispatch(
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_after_expire, eq(&CommandReply::Integer(-2)));
    }

    #[rstest]
    fn dispatch_expire_missing_key_returns_zero() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(
            &CommandFrame::new("EXPIRE", vec![b"nope".to_vec(), b"10".to_vec()]),
            &mut state,
        );
        assert_that!(&reply, eq(&CommandReply::Integer(0)));
    }
}
