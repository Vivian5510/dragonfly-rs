//! Command registration and dispatch skeleton.
//!
//! This module intentionally mirrors Dragonfly's "command table + execution path" concept.
//! Unit 1 keeps the implementation minimal while preserving the same architectural idea:
//! protocol parsing produces a canonical command frame, then a registry resolves and executes
//! the matching handler.

use std::collections::HashMap;
use std::str;
use std::time::{SystemTime, UNIX_EPOCH};

use dfly_common::ids::DbIndex;

use crate::command::{CommandFrame, CommandReply};

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

/// Mutable execution state used by command handlers.
///
/// The `kv` map is a minimal placeholder representing per-node key-value state.
/// In later units this will be replaced by shard-aware storage and transaction views.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DispatchState {
    /// In-memory key/value dictionary for learning-path command execution.
    ///
    /// First key is logical DB index (`SELECT`), second key is user key bytes.
    pub db_kv: HashMap<DbIndex, HashMap<Vec<u8>, ValueEntry>>,
    /// Monotonic per-key versions used by optimistic transaction checks (`WATCH`).
    db_versions: HashMap<DbIndex, HashMap<Vec<u8>, u64>>,
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

    /// Returns mutable map for one logical DB, creating it when missing.
    fn db_map_mut(&mut self, db: DbIndex) -> &mut HashMap<Vec<u8>, ValueEntry> {
        self.db_kv.entry(db).or_default()
    }

    /// Returns immutable map for one logical DB.
    fn db_map(&self, db: DbIndex) -> Option<&HashMap<Vec<u8>, ValueEntry>> {
        self.db_kv.get(&db)
    }

    /// Returns tracked version for one key.
    #[must_use]
    pub fn key_version(&self, db: DbIndex, key: &[u8]) -> u64 {
        self.db_versions
            .get(&db)
            .and_then(|versions| versions.get(key))
            .copied()
            .unwrap_or(0)
    }

    /// Bumps version of one key after successful mutation.
    fn bump_key_version(&mut self, db: DbIndex, key: &[u8]) {
        let versions = self.db_versions.entry(db).or_default();
        let next = versions.get(key).copied().unwrap_or(0).saturating_add(1);
        versions.insert(key.to_vec(), next);
    }

    /// Marks one key as loaded from snapshot and advances its mutation version.
    ///
    /// This keeps optimistic watch checks consistent after `LOAD`/snapshot import.
    pub fn mark_key_loaded(&mut self, db: DbIndex, key: &[u8]) {
        self.bump_key_version(db, key);
    }

    /// Removes one key when its expiration timestamp has already passed.
    fn purge_expired_key(&mut self, db: DbIndex, key: &[u8]) {
        let should_remove = self
            .db_map(db)
            .and_then(|map| map.get(key))
            .and_then(|entry| entry.expire_at_unix_secs)
            .is_some_and(|expire_at| expire_at <= Self::now_unix_seconds());

        if should_remove
            && let Some(map) = self.db_kv.get_mut(&db)
            && map.remove(key).is_some()
        {
            self.bump_key_version(db, key);
        }
    }

    /// Number of keys currently stored in one logical DB.
    #[must_use]
    pub fn db_len(&self, db: DbIndex) -> usize {
        self.db_map(db).map_or(0, HashMap::len)
    }

    /// Removes all keys in one logical DB and bumps their key versions.
    ///
    /// Returns the number of removed keys.
    pub fn flush_db(&mut self, db: DbIndex) -> usize {
        let Some(keyspace) = self.db_kv.get(&db) else {
            return 0;
        };

        let keys = keyspace.keys().cloned().collect::<Vec<_>>();
        let removed = keys.len();
        for key in &keys {
            self.bump_key_version(db, key);
        }
        let _ = self.db_kv.remove(&db);
        removed
    }

    /// Removes all keys in all logical DBs and bumps their key versions.
    ///
    /// Returns the number of removed keys.
    pub fn flush_all(&mut self) -> usize {
        let mut keys_per_db = Vec::new();
        let mut removed = 0_usize;

        for (db, keyspace) in &self.db_kv {
            let keys = keyspace.keys().cloned().collect::<Vec<_>>();
            removed = removed.saturating_add(keys.len());
            keys_per_db.push((*db, keys));
        }

        for (db, keys) in keys_per_db {
            for key in keys {
                self.bump_key_version(db, &key);
            }
        }
        self.db_kv.clear();
        removed
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
            name: "DEL",
            arity: CommandArity::AtLeast(1),
            handler: handle_del,
        });
        registry.register(CommandSpec {
            name: "EXISTS",
            arity: CommandArity::AtLeast(1),
            handler: handle_exists,
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
        registry.register(CommandSpec {
            name: "PERSIST",
            arity: CommandArity::Exact(1),
            handler: handle_persist,
        });
        registry
    }

    /// Registers or replaces one command in the table.
    pub fn register(&mut self, spec: CommandSpec) {
        self.entries.insert(spec.name.to_owned(), spec);
    }

    /// Validates command existence and arity without executing handler logic.
    ///
    /// # Errors
    ///
    /// Returns user-facing error text for unknown command names or invalid argument count.
    pub fn validate_frame(&self, frame: &CommandFrame) -> Result<(), String> {
        let command_name = frame.name.to_ascii_uppercase();
        let Some(spec) = self.entries.get(&command_name) else {
            return Err(format!("unknown command '{command_name}'"));
        };

        match spec.arity {
            CommandArity::Exact(expected) if frame.args.len() != expected => Err(format!(
                "wrong number of arguments for '{}' command",
                spec.name
            )),
            CommandArity::AtLeast(minimum) if frame.args.len() < minimum => Err(format!(
                "wrong number of arguments for '{}' command",
                spec.name
            )),
            _ => Ok(()),
        }
    }

    /// Dispatches one canonical command frame to its registered handler.
    #[must_use]
    pub fn dispatch(
        &self,
        db: DbIndex,
        frame: &CommandFrame,
        state: &mut DispatchState,
    ) -> CommandReply {
        if let Err(message) = self.validate_frame(frame) {
            return CommandReply::Error(message);
        }

        let command_name = frame.name.to_ascii_uppercase();
        let Some(spec) = self.entries.get(&command_name) else {
            return CommandReply::Error(format!("unknown command '{command_name}'"));
        };
        (spec.handler)(db, frame, state)
    }
}

fn handle_ping(_db: DbIndex, frame: &CommandFrame, _state: &mut DispatchState) -> CommandReply {
    if frame.args.is_empty() {
        return CommandReply::SimpleString("PONG".to_owned());
    }
    if frame.args.len() == 1 {
        return CommandReply::BulkString(frame.args[0].clone());
    }
    CommandReply::Error("wrong number of arguments for 'PING' command".to_owned())
}

fn handle_echo(_db: DbIndex, frame: &CommandFrame, _state: &mut DispatchState) -> CommandReply {
    CommandReply::BulkString(frame.args[0].clone())
}

fn handle_set(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let value = frame.args[1].clone();
    state.db_map_mut(db).insert(
        key.clone(),
        ValueEntry {
            value,
            expire_at_unix_secs: None,
        },
    );
    state.bump_key_version(db, &key);
    CommandReply::SimpleString("OK".to_owned())
}

fn handle_get(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);
    match state.db_map(db).and_then(|map| map.get(key)) {
        Some(value) => CommandReply::BulkString(value.value.clone()),
        None => CommandReply::Null,
    }
}

fn handle_del(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let mut deleted = 0_i64;

    for key in &frame.args {
        state.purge_expired_key(db, key);
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            deleted = deleted.saturating_add(1);
        }
    }

    CommandReply::Integer(deleted)
}

fn handle_exists(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let mut existing = 0_i64;

    for key in &frame.args {
        state.purge_expired_key(db, key);
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            existing = existing.saturating_add(1);
        }
    }

    CommandReply::Integer(existing)
}

fn handle_expire(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let seconds = match parse_i64_arg(&frame.args[1], "EXPIRE seconds") {
        Ok(seconds) => seconds,
        Err(error) => return CommandReply::Error(error),
    };
    if !state.db_map(db).is_some_and(|map| map.contains_key(key)) {
        return CommandReply::Integer(0);
    }

    if seconds <= 0 {
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    let Ok(expire_delta) = u64::try_from(seconds) else {
        return CommandReply::Error("EXPIRE seconds is out of range".to_owned());
    };
    let expire_at = DispatchState::now_unix_seconds().saturating_add(expire_delta);

    let mut updated = false;
    if let Some(entry) = state.db_map_mut(db).get_mut(key) {
        entry.expire_at_unix_secs = Some(expire_at);
        updated = true;
    }
    if updated {
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

fn handle_ttl(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(expire_at) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    let now = DispatchState::now_unix_seconds();
    if expire_at <= now {
        let _ = state.db_map_mut(db).remove(key);
        return CommandReply::Integer(-2);
    }
    let remaining = expire_at.saturating_sub(now);
    match i64::try_from(remaining) {
        Ok(remaining) => CommandReply::Integer(remaining),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

fn handle_persist(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let mut changed = false;
    if let Some(entry) = state.db_map_mut(db).get_mut(key)
        && entry.expire_at_unix_secs.is_some()
    {
        entry.expire_at_unix_secs = None;
        changed = true;
    }

    if changed {
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
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

        let ping = registry.dispatch(0, &CommandFrame::new("PING", Vec::new()), &mut state);
        assert_that!(&ping, eq(&CommandReply::SimpleString("PONG".to_owned())));

        let echo = registry.dispatch(
            0,
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
            0,
            &CommandFrame::new("SET", vec![b"user:1".to_vec(), b"alice".to_vec()]),
            &mut state,
        );
        assert_that!(&set_reply, eq(&CommandReply::SimpleString("OK".to_owned())));

        let get_reply = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"user:1".to_vec()]),
            &mut state,
        );
        assert_that!(&get_reply, eq(&CommandReply::BulkString(b"alice".to_vec())));
    }

    #[rstest]
    fn dispatch_del_and_exists_follow_redis_counting_semantics() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let exists = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXISTS",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&exists, eq(&CommandReply::Integer(3)));

        let deleted = registry.dispatch(
            0,
            &CommandFrame::new(
                "DEL",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&deleted, eq(&CommandReply::Integer(2)));

        let exists_after_delete = registry.dispatch(
            0,
            &CommandFrame::new("EXISTS", vec![b"k1".to_vec(), b"k2".to_vec()]),
            &mut state,
        );
        assert_that!(&exists_after_delete, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_unknown_command_returns_error() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(0, &CommandFrame::new("NOPE", Vec::new()), &mut state);
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
            0,
            &CommandFrame::new("SET", vec![b"temp".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let ttl_without_expire = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_without_expire, eq(&CommandReply::Integer(-1)));

        let expire_now = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![b"temp".to_vec(), b"0".to_vec()]),
            &mut state,
        );
        assert_that!(&expire_now, eq(&CommandReply::Integer(1)));

        let get_after_expire = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&get_after_expire, eq(&CommandReply::Null));

        let ttl_after_expire = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_after_expire, eq(&CommandReply::Integer(-2)));
    }

    #[rstest]
    fn dispatch_persist_removes_expiry_without_deleting_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"temp".to_vec(), b"value".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![b"temp".to_vec(), b"10".to_vec()]),
            &mut state,
        );

        let persist = registry.dispatch(
            0,
            &CommandFrame::new("PERSIST", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&persist, eq(&CommandReply::Integer(1)));

        let ttl = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl, eq(&CommandReply::Integer(-1)));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"value".to_vec())));
    }

    #[rstest]
    fn dispatch_expire_missing_key_returns_zero() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![b"nope".to_vec(), b"10".to_vec()]),
            &mut state,
        );
        assert_that!(&reply, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_keeps_values_isolated_between_databases() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"shared".to_vec(), b"db0".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            1,
            &CommandFrame::new("SET", vec![b"shared".to_vec(), b"db1".to_vec()]),
            &mut state,
        );

        let db0 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"shared".to_vec()]),
            &mut state,
        );
        let db1 = registry.dispatch(
            1,
            &CommandFrame::new("GET", vec![b"shared".to_vec()]),
            &mut state,
        );
        assert_that!(&db0, eq(&CommandReply::BulkString(b"db0".to_vec())));
        assert_that!(&db1, eq(&CommandReply::BulkString(b"db1".to_vec())));
    }

    #[rstest]
    fn dispatch_bumps_key_version_on_mutations() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"watched:key".to_vec();

        assert_that!(state.key_version(0, &key), eq(0_u64));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"v1".to_vec()]),
            &mut state,
        );
        assert_that!(state.key_version(0, &key), eq(1_u64));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![key.clone(), b"10".to_vec()]),
            &mut state,
        );
        assert_that!(state.key_version(0, &key), eq(2_u64));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![key.clone(), b"0".to_vec()]),
            &mut state,
        );
        assert_that!(state.key_version(0, &key), eq(3_u64));
    }
}
